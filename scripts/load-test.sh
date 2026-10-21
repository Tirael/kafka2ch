#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

ENV_FILE=${ENV_FILE:-.env.example}
COMPOSE="docker compose --env-file $ENV_FILE"
CH_PASSWORD=$(grep '^CLICKHOUSE_PASSWORD=' "$ENV_FILE" | cut -d= -f2-)

MESSAGES=${LOAD_MESSAGES:-300000}
PARTITIONS=${LOAD_PARTITIONS:-4}
PUBLISH_PARALLELISM=${LOAD_PUBLISH_PARALLELISM:-128}
DEFAULT_RATE=${LOAD_DEFAULT_RATE:-2}
STEADY_RATE=${LOAD_STEADY_RATE:-20}
STEADY_SECONDS=${LOAD_STEADY_SECONDS:-60}
DRAIN_TIMEOUT=${LOAD_DRAIN_TIMEOUT:-900}
SCENARIO_FILTER=${LOAD_SCENARIOS:-}
OUT=${LOAD_OUT:-artifacts/load-test}
KEEP_STACK=${LOAD_KEEP_STACK:-0}

QUEUE_SQL=docker/clickhouse/init/01_orders_queue.sql
PIPELINE_SQL=docker/clickhouse/init/02_pipeline.sql
RUN_ID=$(date +%s)
BACKLOG_TOPIC="load_backlog_${RUN_ID}"

DRAIN_SCENARIOS=(
  "d1-baseline|1||1"
  "d2-no-udf|1||0"
  "d3-c2-shared-thread|2|kafka_thread_per_consumer = 0|1"
  "d4-c2-thread-per-consumer|2|kafka_thread_per_consumer = 1|1"
  "d5-c4-thread-per-consumer|4|kafka_thread_per_consumer = 1|1"
  "d6-c4-thread-per-consumer-no-udf|4|kafka_thread_per_consumer = 1|0"
  "d7-block-65536|1|kafka_max_block_size = 65536|1"
  "d8-c4-tpc-block-65536-flush-1000|4|kafka_thread_per_consumer = 1, kafka_max_block_size = 65536, kafka_flush_interval_ms = 1000|1"
)

STEADY_SCENARIOS=(
  "s1-default-flush|1||1"
  "s2-flush-1000|1|kafka_flush_interval_ms = 1000|1"
  "s3-flush-1000-no-udf|1|kafka_flush_interval_ms = 1000|0"
  "s4-c4-tpc-flush-1000|4|kafka_thread_per_consumer = 1, kafka_flush_interval_ms = 1000|1"
)

SUMMARY_HEADER="scenario,kind,partitions,consumers,settings,udf,messages,first_row_s,drain_s,throughput_msg_s,cpu_s,net_cpu_s,cpu_ms_per_1k,avg_cpu_cores,peak_rss_mb,new_parts,avg_rows_per_part,commits,p50_ms,p95_ms,p99_ms,max_ms"

log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }

ch() { docker exec clickhouse clickhouse-client --password "$CH_PASSWORD" "$@"; }

ch_script() { docker exec -i clickhouse clickhouse-client --password "$CH_PASSWORD" --multiquery; }

now() { date +%s.%N; }

calc() { awk "BEGIN { printf \"%.${2:-3}f\", ($1) }"; }

cpu_usec() {
  docker exec clickhouse sh -c '
    if [ -f /sys/fs/cgroup/cpu.stat ]; then
      awk "/^usage_usec/ { print \$2 }" /sys/fs/cgroup/cpu.stat
    else
      echo $(( $(cat /sys/fs/cgroup/cpuacct/cpuacct.usage) / 1000 ))
    fi'
}

kafka() { docker exec kafka "$@"; }

create_topic() {
  kafka kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
    --topic "$1" --partitions "$PARTITIONS" --replication-factor 1 >/dev/null
}

end_offsets() {
  kafka kafka-get-offsets --bootstrap-server kafka:9092 --topic "$1" | awk -F: '{ s += $NF } END { print s + 0 }'
}

publish() {
  local topic=$1 count=$2 rate=$3 parallelism=$4 output="$OUT/logs/publish-$1.log" status=0
  $COMPOSE run --rm --no-deps \
    -e PublishOrders__Topic="$topic" \
    -e PublishOrders__Count="$count" \
    -e PublishOrders__RatePerSecond="$rate" \
    -e PublishOrders__IntervalMs=0 \
    -e PublishOrders__Parallelism="$parallelism" \
    -e PublishOrders__LogEvery=50000 \
    -e PublishOrders__StopApplicationWhenDone=true \
    -e ReadAggregates__IntervalMs=3600000 \
    sandbox-app > "$output" 2>&1 || status=$?
  grep -E 'Publishing finished|Failed to publish|Unhandled' "$output" | head -20 >&2 || true
  return "$status"
}

queue_ddl() {
  local topic=$1 group=$2 consumers=$3 settings=$4 extra=""
  [[ -n $settings ]] && extra=", $settings"
  (( consumers > 1 )) && extra="$extra, kafka_disable_num_consumers_limit = 1"
  awk '/^CREATE TABLE orders_queue$/ { p = 1 } p { print } p && /;$/ { exit }' "$QUEUE_SQL" \
    | sed -e 's/^CREATE TABLE orders_queue$/CREATE TABLE load_queue/' \
          -e "s/kafka_topic_list = '[^']*'/kafka_topic_list = '$topic'/" \
          -e "s/kafka_group_name = '[^']*'/kafka_group_name = '$group'/" \
          -e "s/kafka_num_consumers = [0-9]*/kafka_num_consumers = ${consumers}${extra}/"
}

mv_ddl() {
  local udf=$1
  awk '/^CREATE MATERIALIZED VIEW orders_mv TO orders AS$/ { p = 1 } p { print } p && /;$/ { exit }' "$PIPELINE_SQL" \
    | sed -e 's/^CREATE MATERIALIZED VIEW orders_mv TO orders AS$/CREATE MATERIALIZED VIEW load_mv TO load_orders AS/' \
          -e 's/^FROM orders_queue$/FROM load_queue/' \
    | if [[ $udf == 1 ]]; then cat; else sed '/protobufWire/d'; fi
}

drop_scenario_objects() {
  ch --query "DROP TABLE IF EXISTS load_mv SYNC"
  ch --query "DROP TABLE IF EXISTS load_queue SYNC"
  ch --query "DROP TABLE IF EXISTS load_orders SYNC"
}

prepare_scenario() {
  local topic=$1 group=$2 consumers=$3 settings=$4
  drop_scenario_objects
  ch --query "CREATE TABLE load_orders AS orders"
  ch --query "ALTER TABLE load_orders ADD COLUMN ingested_at DateTime64(3) DEFAULT now64(3)"
  queue_ddl "$topic" "$group" "$consumers" "$settings" | ch_script
}

sample() {
  ch --format TSV --query "
    SELECT
      (SELECT count() FROM load_orders),
      (SELECT sum(arraySum(arrayMap(o -> greatest(o, 0), assignments.current_offset))) FROM system.kafka_consumers WHERE table = 'load_queue'),
      (SELECT sum(num_commits) FROM system.kafka_consumers WHERE table = 'load_queue'),
      (SELECT round(value / 1048576) FROM system.asynchronous_metrics WHERE metric = 'MemoryResident')"
}

wait_for_rows() {
  local expected=$1 rate=$2 started=$3 cpu_start=$4 series=$5 deadline
  deadline=$(calc "$started + $DRAIN_TIMEOUT" 0)
  FIRST_ROW_AT=""
  DONE_AT=""
  PEAK_RSS=0
  COMMITS=0
  echo "t_s,produced,rows,backlog_rows,consumer_lag,commits,cpu_s,rss_mb" > "$series"
  while :; do
    local t elapsed produced rows position commits rss cpu
    t=$(now)
    read -r rows position commits rss < <(sample)
    cpu=$(cpu_usec)
    elapsed=$(calc "$t - $started" 1)
    produced=$expected
    if [[ $rate != 0 ]]; then
      produced=$(calc "$rate * $elapsed" 0)
      (( produced > expected )) && produced=$expected
    fi
    echo "$elapsed,$produced,$rows,$((produced - rows)),$((produced - position)),$commits,$(calc "($cpu - $cpu_start) / 1000000" 2),$rss" >> "$series"
    (( rss > PEAK_RSS )) && PEAK_RSS=$rss
    COMMITS=$commits
    [[ -z $FIRST_ROW_AT && $rows -gt 0 ]] && FIRST_ROW_AT=$t
    if (( rows >= expected )); then
      DONE_AT=$t
      CPU_END=$cpu
      return 0
    fi
    if (( $(calc "$t > $deadline" 0) )); then
      log "timeout: $rows/$expected rows"
      ch --query "SELECT consumer_id, exceptions.text FROM system.kafka_consumers WHERE table = 'load_queue'" >&2 || true
      DONE_AT=$t
      CPU_END=$cpu
      return 1
    fi
    sleep 1
  done
}

new_parts_since() {
  ch --query "SYSTEM FLUSH LOGS"
  ch --query "SELECT count() FROM system.part_log WHERE database = currentDatabase() AND table = 'load_orders' AND event_type = 'NewPart' AND event_time >= toDateTime($1)" 2>/dev/null || echo ""
}

latency() {
  ch --format TSV --query "
    SELECT
      round(quantile(0.5)(d)), round(quantile(0.95)(d)), round(quantile(0.99)(d)), max(d)
    FROM (SELECT dateDiff('millisecond', event_time, ingested_at) AS d FROM load_orders)"
}

matches() {
  [[ -z $SCENARIO_FILTER ]] && return 0
  [[ ",$SCENARIO_FILTER," == *",$1,"* ]]
}

record() {
  local name=$1 kind=$2 consumers=$3 settings=$4 udf=$5 messages=$6 started=$7 cpu_start=$8 lat=$9
  local drain first cpu net parts commits_csv rows_per_part
  drain=$(calc "$DONE_AT - $started" 2)
  first=$( [[ -n $FIRST_ROW_AT ]] && calc "$FIRST_ROW_AT - $started" 2 || echo "")
  cpu=$(calc "($CPU_END - $cpu_start) / 1000000" 2)
  net=$(calc "$cpu - $IDLE_CORES * $drain" 2)
  parts=$(new_parts_since "${started%.*}")
  rows_per_part=$( [[ -n $parts && $parts -gt 0 ]] && calc "$messages / $parts" 0 || echo "")
  commits_csv=$COMMITS
  read -r p50 p95 p99 pmax <<< "$lat"
  echo "$name,$kind,$PARTITIONS,$consumers,\"$settings\",$udf,$messages,$first,$drain,$(calc "$messages / $drain" 0),$cpu,$net,$(calc "$net * 1000000 / $messages" 1),$(calc "$cpu / $drain" 2),$PEAK_RSS,$parts,$rows_per_part,$commits_csv,$p50,$p95,$p99,$pmax" >> "$OUT/summary.csv"
}

run_drain() {
  local name consumers settings udf
  IFS='|' read -r name consumers settings udf <<< "$1"
  matches "$name" || return 0
  log "drain $name: consumers=$consumers settings='$settings' udf=$udf"
  prepare_scenario "$BACKLOG_TOPIC" "load_${name}_${RUN_ID}" "$consumers" "$settings"
  local started cpu_start
  cpu_start=$(cpu_usec)
  started=$(now)
  mv_ddl "$udf" | ch_script
  wait_for_rows "$BACKLOG_TOTAL" 0 "$started" "$cpu_start" "$OUT/timeseries/$name.csv" || true
  record "$name" drain "$consumers" "$settings" "$udf" "$BACKLOG_TOTAL" "$started" "$cpu_start" ""
  [[ $name == d1-baseline ]] && udf_microbench
  drop_scenario_objects
}

run_steady() {
  local name consumers settings udf
  IFS='|' read -r name consumers settings udf <<< "$1"
  matches "$name" || return 0
  local topic="load_steady_${name//-/_}_${RUN_ID}" count
  count=$(calc "$STEADY_RATE * $STEADY_SECONDS" 0)
  log "steady $name: ${STEADY_RATE} msg/s x ${STEADY_SECONDS}s consumers=$consumers settings='$settings' udf=$udf"
  create_topic "$topic"
  prepare_scenario "$topic" "load_${name}_${RUN_ID}" "$consumers" "$settings"
  mv_ddl "$udf" | ch_script
  for _ in $(seq 60); do
    [[ $(ch --query "SELECT countIf(length(assignments.partition_id) > 0) FROM system.kafka_consumers WHERE table = 'load_queue'") -ge 1 ]] && break
    sleep 1
  done
  local started cpu_start
  cpu_start=$(cpu_usec)
  started=$(now)
  publish "$topic" "$count" "$STEADY_RATE" 4 &
  local publisher=$!
  wait_for_rows "$count" "$STEADY_RATE" "$started" "$cpu_start" "$OUT/timeseries/$name.csv" || true
  wait "$publisher" || true
  record "$name" steady "$consumers" "$settings" "$udf" "$count" "$started" "$cpu_start" "$(latency)"
  drop_scenario_objects
}

udf_microbench() {
  log "UDF microbenchmark on $(ch --query 'SELECT count() FROM load_orders') rows"
  local tag="udf_${RUN_ID}"
  for _ in 1 2 3; do
    ch --query "SELECT sum(length(substring(kafka_key, 7))) FROM load_orders SETTINGS max_threads = 1, use_query_cache = 0, log_comment = '${tag}_raw'" >/dev/null
    ch --query "SELECT sum(length(CAST(protobufWireBytes(substring(kafka_key, 7), 1) AS String))) FROM load_orders SETTINGS max_threads = 1, use_query_cache = 0, log_comment = '${tag}_decode'" >/dev/null
  done
  ch --query "SYSTEM FLUSH LOGS"
  {
    echo "variant,rows,duration_ms,user_time_us,ns_per_row"
    ch --format CSV --query "
      SELECT
        replaceOne(log_comment, '${tag}_', ''),
        any(read_rows),
        min(query_duration_ms),
        min(ProfileEvents['UserTimeMicroseconds']),
        round(min(ProfileEvents['UserTimeMicroseconds']) * 1000 / any(read_rows))
      FROM system.query_log
      WHERE type = 'QueryFinish' AND startsWith(log_comment, '${tag}_')
      GROUP BY log_comment
      ORDER BY log_comment DESC"
  } > "$OUT/udf-microbench.csv"
  ch --format TSV --query "
    SELECT round(avg(length(kafka_key)), 1), countIf(CAST(protobufWireBytes(substring(kafka_key, 7), 1) AS String) = order_id), count()
    FROM load_orders" > "$OUT/udf-key-stats.tsv"
}

write_report() {
  local version cores
  version=$(ch --query "SELECT version()")
  cores=$(ch --query "SELECT getSetting('max_threads')")
  {
    echo "# Load test report"
    echo
    echo "- Date (UTC): $(date -u '+%Y-%m-%d %H:%M')"
    echo "- Host: $(nproc) vCPU, $(free -g | awk '/^Mem:/ { print $2 }') GiB RAM, $(uname -sr)"
    echo "- ClickHouse: $version (max_threads=$cores); images from \`$ENV_FILE\`"
    echo "- Topic partitions: $PARTITIONS; backlog: $BACKLOG_TOTAL messages ($(calc "$BACKLOG_TOTAL / $DEFAULT_RATE / 3600" 1) h of the default ${DEFAULT_RATE} msg/s flow); publish: ${PUBLISH_SECONDS}s"
    echo "- Steady flow: ${STEADY_RATE} msg/s (x$(calc "$STEADY_RATE / $DEFAULT_RATE" 0) default) for ${STEADY_SECONDS}s"
    echo "- Idle ClickHouse CPU: ${IDLE_CORES} cores (subtracted in net_cpu_s)"
    echo
    echo "## Summary"
    echo
    awk -F, -v OFS=' | ' '
      NR == 1 {
        print "| scenario | consumers | settings | UDF | msgs | first row s | drain s | msg/s | net CPU s | CPU ms/1k msgs | avg cores | peak RSS MB | new parts | rows/part | p50 ms | p95 ms | max ms |"
        print "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"
        next
      }
      {
        line = $0
        settings = ""
        if (match(line, /"[^"]*"/)) { settings = substr(line, RSTART + 1, RLENGTH - 2); line = substr(line, 1, RSTART - 1) "S" substr(line, RSTART + RLENGTH) }
        split(line, f, ",")
        printf "| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n", f[1], f[4], (settings == "" ? "defaults" : settings), (f[6] == 1 ? "yes" : "no"), f[7], f[8], f[9], f[10], f[12], f[13], f[14], f[15], f[16], f[17], f[19], f[20], f[22]
      }' "$OUT/summary.csv"
    if [[ -f $OUT/udf-microbench.csv ]]; then
      echo
      echo "## Key decode microbenchmark (single thread, \`protobufWireBytes\` vs raw key)"
      echo
      echo "| variant | rows | duration ms | user time us | ns/row |"
      echo "|---|---|---|---|---|"
      tail -n +2 "$OUT/udf-microbench.csv" | tr -d '"' | awk -F, '{ printf "| %s | %s | %s | %s | %s |\n", $1, $2, $3, $4, $5 }'
      read -r key_len decoded total < "$OUT/udf-key-stats.tsv"
      echo
      echo "Average \`kafka_key\` length: ${key_len} bytes; decoded \`order_id\` equals value \`order_id\` in ${decoded}/${total} rows."
    fi
    echo
    echo "Raw data: \`summary.csv\`, \`timeseries/*.csv\` (1 s samples: rows, backlog, consumer lag by offsets, commits, CPU, RSS), \`udf-microbench.csv\`."
  } > "$OUT/report.md"
}

cleanup() {
  local status=$?
  if (( status != 0 )); then
    mkdir -p "$OUT/logs"
    docker logs clickhouse > "$OUT/logs/clickhouse.log" 2>&1 || true
    docker logs kafka > "$OUT/logs/kafka.log" 2>&1 || true
  fi
  [[ $KEEP_STACK == 1 ]] || $COMPOSE down -v --remove-orphans >/dev/null 2>&1 || true
  exit "$status"
}

main() {
  rm -rf "$OUT"
  mkdir -p "$OUT/timeseries" "$OUT/logs"
  echo "$SUMMARY_HEADER" > "$OUT/summary.csv"
  trap cleanup EXIT

  log "starting single-node stack"
  $COMPOSE build clickhouse-migrate sandbox-app >/dev/null
  $COMPOSE up -d --wait kafka schema-registry clickhouse
  $COMPOSE up -d --no-deps clickhouse-migrate
  [[ $(docker wait clickhouse-migrate) == 0 ]] || { docker logs clickhouse-migrate >&2; exit 1; }

  local idle_cpu idle_started
  idle_cpu=$(cpu_usec)
  idle_started=$(now)
  sleep 10
  IDLE_CORES=$(calc "($(cpu_usec) - $idle_cpu) / 1000000 / ($(now) - $idle_started)" 3)

  log "publishing $MESSAGES messages to $BACKLOG_TOPIC ($PARTITIONS partitions)"
  create_topic "$BACKLOG_TOPIC"
  local publish_started
  publish_started=$(now)
  publish "$BACKLOG_TOPIC" "$MESSAGES" 0 "$PUBLISH_PARALLELISM"
  PUBLISH_SECONDS=$(calc "$(now) - $publish_started" 1)
  BACKLOG_TOTAL=$(end_offsets "$BACKLOG_TOPIC")
  log "backlog: $BACKLOG_TOTAL messages in ${PUBLISH_SECONDS}s"
  (( BACKLOG_TOTAL > 0 )) || { log "nothing published"; exit 1; }

  for scenario in "${DRAIN_SCENARIOS[@]}"; do run_drain "$scenario"; done
  for scenario in "${STEADY_SCENARIOS[@]}"; do run_steady "$scenario"; done

  write_report
  log "report: $OUT/report.md"
  cat "$OUT/report.md"
}

main "$@"
