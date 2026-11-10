#!/usr/bin/env bash
# P0-2 / ADR-0001 calibration for TR-2..TR-4 on the real orders → orders_agg_mv path.
# Unlike scripts/load-test.sh (temporary load_* objects), this drives the demo pipeline
# and records refresh duration, ReadAggregates latency with/without ingest, CPU, and lag after spike.
set -euo pipefail

cd "$(dirname "$0")/.."

ENV_FILE=${ENV_FILE:-.env.example}
COMPOSE="docker compose --env-file $ENV_FILE"
CH_PASSWORD=$(grep '^CLICKHOUSE_PASSWORD=' "$ENV_FILE" | cut -d= -f2-)

SEED_MESSAGES=${CAL_SEED_MESSAGES:-300000}
STEADY_RATE=${CAL_STEADY_RATE:-20}
STEADY_SECONDS=${CAL_STEADY_SECONDS:-90}
SPIKE_MESSAGES=${CAL_SPIKE_MESSAGES:-20000}
QUERY_SAMPLES=${CAL_QUERY_SAMPLES:-40}
REFRESH_FORCED=${CAL_REFRESH_FORCED:-5}
REFRESH_WAIT_SECONDS=${CAL_REFRESH_WAIT_SECONDS:-200}
PUBLISH_PARALLELISM=${CAL_PUBLISH_PARALLELISM:-128}
DRAIN_TIMEOUT=${CAL_DRAIN_TIMEOUT:-900}
LAG_RECOVER_TIMEOUT=${CAL_LAG_RECOVER_TIMEOUT:-180}
DEFAULT_RATE=${CAL_DEFAULT_RATE:-2}
OUT=${CAL_OUT:-artifacts/load-calibration}
KEEP_STACK=${CAL_KEEP_STACK:-0}
TOPIC=${CAL_TOPIC:-orders}

log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }
ch() { docker exec clickhouse clickhouse-client --password "$CH_PASSWORD" "$@"; }
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

end_offsets() {
  kafka kafka-get-offsets --bootstrap-server kafka:9092 --topic "$1" 2>/dev/null \
    | awk -F: '{ s += $NF } END { print s + 0 }'
}

consumer_position() {
  ch --query "
    SELECT coalesce(sum(arraySum(arrayMap(o -> greatest(o, 0), assignments.current_offset))), 0)
    FROM system.kafka_consumers
    WHERE table = 'orders_queue'"
}

orders_count() { ch --query "SELECT count() FROM orders"; }

publish() {
  local count=$1 rate=$2 parallelism=$3 label=$4
  local output="$OUT/logs/publish-$label.log" status=0
  $COMPOSE run --rm --no-deps \
    -e PublishOrders__Topic="$TOPIC" \
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

wait_orders() {
  local expected=$1 started=$2 series=$3
  local deadline cpu_start
  deadline=$(calc "$started + $DRAIN_TIMEOUT" 0)
  cpu_start=$(cpu_usec)
  echo "t_s,end_offsets,orders_rows,consumer_position,lag_msgs,cpu_s" > "$series"
  while :; do
    local t elapsed ends rows pos lag cpu
    t=$(now)
    ends=$(end_offsets "$TOPIC")
    rows=$(orders_count)
    pos=$(consumer_position)
    lag=$(( ends > pos ? ends - pos : 0 ))
    cpu=$(cpu_usec)
    elapsed=$(calc "$t - $started" 1)
    echo "$elapsed,$ends,$rows,$pos,$lag,$(calc "($cpu - $cpu_start) / 1000000" 2)" >> "$series"
    if (( rows >= expected )); then
      CPU_END=$cpu
      DRAIN_DONE_AT=$t
      return 0
    fi
    if (( $(calc "$t > $deadline" 0) )); then
      log "timeout waiting for orders rows: $rows/$expected (lag=$lag)"
      CPU_END=$cpu
      DRAIN_DONE_AT=$t
      return 1
    fi
    sleep 1
  done
}

# Lag in "seconds of backlog" at the reference ×10 rate (STEADY_RATE), not wall-clock age.
lag_seconds_at_rate() {
  local lag_msgs=$1
  calc "$lag_msgs / $STEADY_RATE" 2
}

wait_lag_le() {
  local max_lag_s=$1 started=$2 series=$3
  local deadline
  deadline=$(calc "$started + $LAG_RECOVER_TIMEOUT" 0)
  echo "t_s,end_offsets,consumer_position,lag_msgs,lag_s_at_${STEADY_RATE}_msg_s" > "$series"
  while :; do
    local t ends pos lag lag_s
    t=$(now)
    ends=$(end_offsets "$TOPIC")
    pos=$(consumer_position)
    lag=$(( ends > pos ? ends - pos : 0 ))
    lag_s=$(lag_seconds_at_rate "$lag")
    echo "$(calc "$t - $started" 1),$ends,$pos,$lag,$lag_s" >> "$series"
    if (( $(calc "$lag_s <= $max_lag_s" 0) )); then
      LAG_RECOVERED_AT=$t
      LAG_FINAL=$lag
      LAG_FINAL_S=$lag_s
      return 0
    fi
    if (( $(calc "$t > $deadline" 0) )); then
      LAG_RECOVERED_AT=$t
      LAG_FINAL=$lag
      LAG_FINAL_S=$lag_s
      return 1
    fi
    sleep 1
  done
}

read_aggregates_once_ms() {
  # Same shape as Sandbox.App ReadAggregatesWorker (refresh stamp + agg window query).
  local t0 t1
  t0=$(now)
  ch --multiquery --query "
    SELECT max(last_success_time) FROM system.view_refreshes
    WHERE database = currentDatabase() AND view = 'orders_agg_mv';
    SELECT minute, category,
           sum(orders_count) AS orders_count,
           sum(total_amount) AS total_amount,
           sum(total_qty) AS total_qty
    FROM orders_agg_1m
    WHERE minute >= now() - INTERVAL 60 MINUTE
    GROUP BY minute, category
    ORDER BY minute DESC, category
    FORMAT Null;" >/dev/null
  t1=$(now)
  calc "($t1 - $t0) * 1000" 1
}

sample_queries() {
  local label=$1 n=$2 file=$3
  local i ms
  echo "sample_i,elapsed_ms" > "$file"
  for i in $(seq 1 "$n"); do
    ms=$(read_aggregates_once_ms)
    [[ -n $ms ]] || ms="nan"
    echo "$i,$ms" >> "$file"
  done
  # p50/p95/max ignoring nan (POSIX awk — no gawk asort)
  awk -F, 'NR>1 && $2+0==$2 { print $2+0 }' "$file" | sort -n | awk '
    { a[++n]=$1 }
    END {
      if (n == 0) { print ",,,"; exit }
      p50 = a[int((n-1)*0.50)+1]
      p95 = a[int((n-1)*0.95)+1]
      printf "%.1f,%.1f,%.1f,%d\n", p50, p95, a[n], n
    }'
}

refresh_snapshot() {
  ch --format TSV --query "
    SELECT
      view,
      status,
      last_success_time,
      last_success_duration_ms,
      last_refresh_time,
      next_refresh_time,
      retry,
      exception
    FROM system.view_refreshes
    WHERE database = currentDatabase() AND view = 'orders_agg_mv'"
}

force_refresh_samples() {
  local n=$1 file=$2
  echo "sample_i,duration_ms,read_rows,written_rows,status,exception" > "$file"
  local i dur status exc read_rows written_rows
  for i in $(seq 1 "$n"); do
    if ! ch --query "SYSTEM REFRESH VIEW orders_agg_mv" >/dev/null 2>"$OUT/logs/refresh-$i.err"; then
      log "SYSTEM REFRESH VIEW failed (sample $i); see logs/refresh-$i.err — falling back to scheduled wait"
      return 1
    fi
    # Wait for the attempt to finish (ClickHouse 26.x).
    if ! ch --query "SYSTEM WAIT VIEW orders_agg_mv" >/dev/null 2>>"$OUT/logs/refresh-$i.err"; then
      log "SYSTEM WAIT VIEW failed (sample $i); falling back to scheduled wait"
      return 1
    fi
    read -r dur read_rows written_rows status exc < <(ch --format TSV --query "
      SELECT last_success_duration_ms, read_rows, written_rows, status, replaceAll(exception, '\t', ' ')
      FROM system.view_refreshes
      WHERE database = currentDatabase() AND view = 'orders_agg_mv'")
    echo "$i,$dur,$read_rows,$written_rows,$status,$exc" >> "$file"
    log "forced refresh #$i duration_ms=$dur read_rows=$read_rows written_rows=$written_rows status=$status"
  done
  return 0
}

wait_scheduled_refresh_samples() {
  local want=$1 file=$2 timeout_s=$3
  echo "sample_i,duration_ms,status,last_success_time,exception" > "$file"
  local seen=0 last="" deadline
  deadline=$(calc "$(now) + $timeout_s" 0)
  while (( seen < want )); do
    local dur status ts exc
    read -r dur status ts exc < <(ch --format TSV --query "
      SELECT last_success_duration_ms, status, last_success_time, replaceAll(exception, '\t', ' ')
      FROM system.view_refreshes
      WHERE database = currentDatabase() AND view = 'orders_agg_mv'") || true
    if [[ -n $ts && $ts != "$last" && $dur != "0" && -n $dur ]]; then
      seen=$((seen + 1))
      last=$ts
      echo "$seen,$dur,$status,$ts,$exc" >> "$file"
      log "scheduled refresh #$seen duration_ms=$dur at $ts"
    fi
    if (( $(calc "$(now) > $deadline" 0) )); then
      log "scheduled refresh sampling timed out after ${timeout_s}s (got $seen/$want)"
      break
    fi
    sleep 2
  done
}

quantile_from_col() {
  local file=$1 col=$2 q=$3
  awk -F, -v col="$col" 'NR>1 && $(col)+0==$(col) { print $(col)+0 }' "$file" | sort -n | awk -v q="$q" '
    { a[++n]=$1 }
    END {
      if (n==0) { print ""; exit }
      idx = int((n-1)*q)+1
      if (idx < 1) idx=1
      if (idx > n) idx=n
      printf "%.1f", a[idx]
    }'
}

verdict_tr2() {
  local p95=$1
  # Red if p95 > 50% of 60s interval = 30000 ms
  if [[ -z $p95 ]]; then echo "UNKNOWN"; return; fi
  awk -v p="$p95" 'BEGIN { if (p > 30000) print "RED"; else if (p > 20000) print "YELLOW"; else print "GREEN" }'
}

verdict_tr3_cpu() {
  local pct=$1
  if [[ -z $pct ]]; then echo "UNKNOWN"; return; fi
  awk -v p="$pct" 'BEGIN { if (p > 30) print "RED"; else if (p > 20) print "YELLOW"; else print "GREEN" }'
}

verdict_tr3_query() {
  local base=$1 load=$2
  if [[ -z $base || -z $load || $(calc "$base == 0" 0) -eq 1 ]]; then echo "UNKNOWN"; return; fi
  local ratio
  ratio=$(calc "($load - $base) / $base * 100" 1)
  awk -v r="$ratio" 'BEGIN { if (r > 25) print "RED"; else if (r > 15) print "YELLOW"; else print "GREEN" }'
}

verdict_tr4() {
  local recovered=$1 lag_s=$2
  if [[ $recovered != 0 ]]; then echo "RED"; return; fi
  awk -v l="$lag_s" 'BEGIN { if (l > 60) print "RED"; else print "GREEN" }'
}

write_report() {
  local host_cores host_ram ch_version
  host_cores=$(nproc)
  host_ram=$(free -g | awk '/^Mem:/ { print $2 }')
  ch_version=$(ch --query "SELECT version()")
  local tr2_p50 tr2_p95 tr2_max tr2_n tr2_v
  tr2_p50=$(quantile_from_col "$OUT/refresh-samples.csv" 2 0.50)
  tr2_p95=$(quantile_from_col "$OUT/refresh-samples.csv" 2 0.95)
  tr2_max=$(quantile_from_col "$OUT/refresh-samples.csv" 2 1.0)
  tr2_n=$(awk 'NR>1{c++} END{print c+0}' "$OUT/refresh-samples.csv")
  tr2_v=$(verdict_tr2 "$tr2_p95")

  local base_p50 base_p95 base_max base_n
  local load_p50 load_p95 load_max load_n
  IFS=, read -r base_p50 base_p95 base_max base_n < "$OUT/read-aggregates-baseline.stats"
  IFS=, read -r load_p50 load_p95 load_max load_n < "$OUT/read-aggregates-under-load.stats"

  local cpu_pct_peak tr3_cpu_v tr3_q_v tr3_v
  cpu_pct_peak=$(calc "$PEAK_CPU_CORES / $host_cores * 100" 1)
  tr3_cpu_v=$(verdict_tr3_cpu "$cpu_pct_peak")
  tr3_q_v=$(verdict_tr3_query "$base_p95" "$load_p95")
  if [[ $tr3_cpu_v == RED || $tr3_q_v == RED ]]; then tr3_v=RED
  elif [[ $tr3_cpu_v == YELLOW || $tr3_q_v == YELLOW ]]; then tr3_v=YELLOW
  elif [[ $tr3_cpu_v == UNKNOWN || $tr3_q_v == UNKNOWN ]]; then tr3_v=UNKNOWN
  else tr3_v=GREEN; fi

  local tr4_v
  tr4_v=$(verdict_tr4 "$LAG_RECOVER_STATUS" "$LAG_FINAL_S")

  local path_a
  if [[ $tr2_v == GREEN && $tr3_v == GREEN && $tr4_v == GREEN ]]; then
    path_a="GO — path A OK for P0-6 (after remaining P0 items). Do not Accept ADR in this PR."
  elif [[ $tr2_v == RED || $tr3_v == RED || $tr4_v == RED ]]; then
    path_a="NO-GO — do NOT Accept path A blindly. Escalate P1-1 (aggregates) and/or P2-1 (external consumer) per production-blockers-plan."
  else
    path_a="CONDITIONAL — one or more TR yellow/unknown; do not Accept ADR until re-run or owner judgment."
  fi

  {
    echo "# ADR-0001 load calibration (TR-2..TR-4)"
    echo
    echo "- Date (UTC): $(date -u '+%Y-%m-%d %H:%M')"
    echo "- Host: ${host_cores} vCPU, ${host_ram} GiB RAM, $(uname -sr)"
    echo "- ClickHouse: $ch_version; images from \`$ENV_FILE\`"
    echo "- Topic: \`$TOPIC\`; seed: $SEED_MESSAGES messages; steady: ${STEADY_RATE} msg/s (×$(calc "$STEADY_RATE / $DEFAULT_RATE" 0) demo ${DEFAULT_RATE} msg/s) for ${STEADY_SECONDS}s; spike: $SPIKE_MESSAGES"
    echo "- Pipeline: real \`orders_queue\` → \`orders\` → refreshable \`orders_agg_mv\` (\`REFRESH EVERY 1 MINUTE\`)"
    echo "- Demo queue setting: \`kafka_num_consumers = 1\` (TR-4 formal condition prefers consumers = partitions/node; see notes)"
    echo "- Seed ingest: ${SEED_ROWS} rows in ${SEED_DRAIN_S}s ($(calc "$SEED_ROWS / $SEED_DRAIN_S" 0) msg/s)"
    echo
    echo "## Go / no-go (path A)"
    echo
    echo "**$path_a**"
    echo
    echo "| Trigger | Measured | Threshold | Verdict |"
    echo "|---|---|---|---|"
    echo "| TR-2 refresh p95 | ${tr2_p95:-n/a} ms (n=$tr2_n, p50=${tr2_p50:-n/a}, max=${tr2_max:-n/a}) | > 30000 ms (50% of 60s) → red | **$tr2_v** |"
    echo "| TR-3 ingestion CPU peak | ${cpu_pct_peak}% of node (${PEAK_CPU_CORES} / ${host_cores} cores during under-load window) | > 30% → red | **$tr3_cpu_v** |"
    echo "| TR-3 ReadAggregates p95 | baseline ${base_p95:-n/a} ms → under load ${load_p95:-n/a} ms (n=$base_n/$load_n) | > +25% vs baseline → red | **$tr3_q_v** |"
    echo "| TR-4 lag after spike | recovered_in=${LAG_RECOVER_S:-n/a}s; final_lag=${LAG_FINAL} msgs (~${LAG_FINAL_S}s at ${STEADY_RATE} msg/s) | lag must return to ≤ 60s | **$tr4_v** |"
    echo
    echo "Combined TR-3: **$tr3_v** (red if either CPU or query leg is red)."
    echo
    echo "## Method notes"
    echo
    echo "- Refresh samples: $REFRESH_METHOD"
    echo "- ReadAggregates query mirrors \`ReadAggregatesWorker\` (stamp from \`system.view_refreshes\` + 60-minute window on \`orders_agg_1m\`)."
    echo "- Under-load window: concurrent publish at ${STEADY_RATE} msg/s while sampling queries; CPU from cgroup \`usage_usec\`."
    echo "- Spike: publish $SPIKE_MESSAGES as fast as possible after catch-up; recovery clock stops when lag ≤ 60 s of ${STEADY_RATE} msg/s backlog."
    echo "- This stand is single-node compose (GitHub \`ubuntu-latest\` or local Docker), not a sized prod cluster."
    echo
    echo "## Raw artifacts"
    echo
    echo "\`refresh-samples.csv\`, \`read-aggregates-*.csv\`, \`timeseries/\`, \`summary.env\`."
  } > "$OUT/report.md"

  {
    echo "TR2_P50_MS=${tr2_p50}"
    echo "TR2_P95_MS=${tr2_p95}"
    echo "TR2_MAX_MS=${tr2_max}"
    echo "TR2_N=${tr2_n}"
    echo "TR2_VERDICT=${tr2_v}"
    echo "TR3_CPU_PCT=${cpu_pct_peak}"
    echo "TR3_CPU_VERDICT=${tr3_cpu_v}"
    echo "TR3_BASE_P95_MS=${base_p95}"
    echo "TR3_LOAD_P95_MS=${load_p95}"
    echo "TR3_QUERY_VERDICT=${tr3_q_v}"
    echo "TR3_VERDICT=${tr3_v}"
    echo "TR4_RECOVER_S=${LAG_RECOVER_S}"
    echo "TR4_FINAL_LAG_MSGS=${LAG_FINAL}"
    echo "TR4_FINAL_LAG_S=${LAG_FINAL_S}"
    echo "TR4_VERDICT=${tr4_v}"
    echo "PATH_A_DECISION=$([[ $tr2_v == GREEN && $tr3_v == GREEN && $tr4_v == GREEN ]] && echo GO || echo NO_GO_OR_CONDITIONAL)"
    echo "SEED_ROWS=${SEED_ROWS}"
    echo "SEED_DRAIN_S=${SEED_DRAIN_S}"
  } > "$OUT/summary.env"
}

cleanup() {
  local status=$?
  if (( status != 0 )); then
    mkdir -p "$OUT/logs"
    docker logs clickhouse > "$OUT/logs/clickhouse.log" 2>&1 || true
    docker logs kafka > "$OUT/logs/kafka.log" 2>&1 || true
    refresh_snapshot > "$OUT/logs/view_refreshes.tsv" 2>&1 || true
  fi
  [[ $KEEP_STACK == 1 ]] || $COMPOSE down -v --remove-orphans >/dev/null 2>&1 || true
  exit "$status"
}

main() {
  rm -rf "$OUT"
  mkdir -p "$OUT/timeseries" "$OUT/logs"
  trap cleanup EXIT

  log "starting single-node stack for ADR calibration"
  $COMPOSE build clickhouse-migrate sandbox-app >/dev/null
  $COMPOSE up -d --wait kafka schema-registry clickhouse
  $COMPOSE up -d --no-deps clickhouse-migrate
  [[ $(docker wait clickhouse-migrate) == 0 ]] || { docker logs clickhouse-migrate >&2; exit 1; }

  # Ensure topic exists (PublishOrders / Kafka may auto-create; create explicitly for offsets).
  kafka kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
    --topic "$TOPIC" --partitions 1 --replication-factor 1 >/dev/null 2>&1 || true

  for _ in $(seq 60); do
    [[ $(ch --query "SELECT countIf(length(assignments.partition_id) > 0) FROM system.kafka_consumers WHERE table = 'orders_queue'") -ge 1 ]] && break
    sleep 1
  done

  local before_seed
  before_seed=$(orders_count)
  log "seeding $SEED_MESSAGES messages onto topic $TOPIC (orders already=$before_seed)"
  local seed_publish_started seed_wait_started
  seed_publish_started=$(now)
  publish "$SEED_MESSAGES" 0 "$PUBLISH_PARALLELISM" seed
  log "publish finished in $(calc "$(now) - $seed_publish_started" 1)s; waiting for ingest"
  seed_wait_started=$(now)
  local expected
  expected=$(( before_seed + SEED_MESSAGES ))
  wait_orders "$expected" "$seed_wait_started" "$OUT/timeseries/seed-drain.csv" || true
  SEED_ROWS=$(orders_count)
  SEED_DRAIN_S=$(calc "$DRAIN_DONE_AT - $seed_wait_started" 2)
  log "orders rows now=$SEED_ROWS (drain ${SEED_DRAIN_S}s)"

  REFRESH_METHOD="SYSTEM REFRESH VIEW ×${REFRESH_FORCED}"
  if ! force_refresh_samples "$REFRESH_FORCED" "$OUT/refresh-samples.csv"; then
    REFRESH_METHOD="scheduled REFRESH EVERY 1 MINUTE (forced refresh unavailable)"
    wait_scheduled_refresh_samples 3 "$OUT/refresh-samples.csv" "$REFRESH_WAIT_SECONDS"
  fi
  refresh_snapshot > "$OUT/view_refreshes_after_seed.tsv"

  log "TR-3 baseline ReadAggregates ×${QUERY_SAMPLES}"
  sample_queries baseline "$QUERY_SAMPLES" "$OUT/read-aggregates-baseline.csv" > "$OUT/read-aggregates-baseline.stats"

  log "TR-3 under-load: publish ${STEADY_RATE} msg/s × ${STEADY_SECONDS}s + queries"
  local load_count load_started cpu0
  load_count=$(calc "$STEADY_RATE * $STEADY_SECONDS" 0)
  cpu0=$(cpu_usec)
  load_started=$(now)
  publish "$load_count" "$STEADY_RATE" 4 steady &
  local publisher=$!
  # Concurrent query samples + 1s CPU/lag series while publisher runs.
  (
    echo "t_s,cpu_cores_window,end_offsets,consumer_position,lag_msgs"
    prev_cpu=$cpu0
    prev_t=$load_started
    while kill -0 "$publisher" 2>/dev/null; do
      sleep 1
      t=$(now)
      cpu=$(cpu_usec)
      cores=$(calc "($cpu - $prev_cpu) / 1000000 / ($t - $prev_t)" 3)
      ends=$(end_offsets "$TOPIC")
      pos=$(consumer_position)
      lag=$(( ends > pos ? ends - pos : 0 ))
      echo "$(calc "$t - $load_started" 1),$cores,$ends,$pos,$lag"
      prev_cpu=$cpu
      prev_t=$t
    done
  ) > "$OUT/timeseries/under-load.csv" &
  local sampler=$!
  sample_queries under-load "$QUERY_SAMPLES" "$OUT/read-aggregates-under-load.csv" > "$OUT/read-aggregates-under-load.stats" &
  local querier=$!
  wait "$publisher" || true
  wait "$querier" || true
  wait "$sampler" || true
  PEAK_CPU_CORES=$(awk -F, 'NR>1 && $2+0==$2 { if ($2>m) m=$2 } END { printf "%.3f", m+0 }' "$OUT/timeseries/under-load.csv")
  [[ -n $PEAK_CPU_CORES ]] || PEAK_CPU_CORES=0

  # Allow catch-up before spike (only if consumer still behind end offsets).
  log "catch-up before spike"
  {
    local ends pos expected
    ends=$(end_offsets "$TOPIC")
    pos=$(consumer_position)
    expected=$ends
    if (( ends > pos )); then
      log "waiting for ingest catch-up: position=$pos end=$ends"
      # wait_orders keys off row count; approximate with current rows + remaining lag.
      wait_orders "$(( $(orders_count) + ends - pos ))" "$(now)" "$OUT/timeseries/pre-spike-catchup.csv" || true
    else
      log "already caught up (position=$pos end=$ends)"
      echo "t_s,end_offsets,orders_rows,consumer_position,lag_msgs,cpu_s" > "$OUT/timeseries/pre-spike-catchup.csv"
      echo "0,$ends,$(orders_count),$pos,0,0" >> "$OUT/timeseries/pre-spike-catchup.csv"
    fi
  }

  log "TR-4 spike: publish $SPIKE_MESSAGES ASAP"
  local spike_started ends_before
  ends_before=$(end_offsets "$TOPIC")
  spike_started=$(now)
  publish "$SPIKE_MESSAGES" 0 "$PUBLISH_PARALLELISM" spike
  local ends_after
  ends_after=$(end_offsets "$TOPIC")
  log "spike published; offsets $ends_before → $ends_after; waiting for lag ≤ 60s at ${STEADY_RATE} msg/s"
  if wait_lag_le 60 "$spike_started" "$OUT/timeseries/spike-lag-recovery.csv"; then
    LAG_RECOVER_STATUS=0
  else
    LAG_RECOVER_STATUS=1
  fi
  LAG_RECOVER_S=$(calc "$LAG_RECOVERED_AT - $spike_started" 2)

  write_report
  log "report: $OUT/report.md"
  cat "$OUT/report.md"
}

main "$@"
