# Наблюдаемость пайплайна Kafka → ClickHouse

Kafka engine потребляет сообщения внутри ClickHouse, поэтому стандартные метрики consumer group видны только частично. Ниже — минимальный набор сигналов, по которому видно, что пайплайн жив, не отстаёт и не теряет сообщения на ошибках парсинга (риски ATAM R2 — poison message, T1 — непрозрачность consumer внутри ClickHouse).

| Сигнал | Источник | Проблема, которую ловит |
|---|---|---|
| Назначения партиций | `system.kafka_consumers.assignments.*`, метрика `KafkaAssignedPartitions` | нет брокера, ребаланс, удалена очередь или её MV |
| Последний poll / commit | `last_poll_time`, `last_commit_time`, `num_commits`, событие `KafkaCommits` | consumer завис; вставка блока в MV падает, offset не двигается |
| Исключения consumer | `exceptions.time` / `exceptions.text` | ошибки librdkafka, брокера, вставки |
| Lag | `assignments.current_offset` + end offset из Kafka | ClickHouse не успевает за потоком |
| Ошибки парсинга | `{queue}_errors` (`orders_queue_errors`), событие `KafkaMessagesFailed` | битые сообщения (`kafka_handle_error_mode = 'stream'`) |
| Refreshable-агрегаты | `system.view_refreshes` | `orders_agg_mv` не пересчитывается |

Те же проверки выполняет фича `ConsumerHealth` в `Sandbox.App` (см. [ниже](#health-check-в-sandboxapp)).

## `system.kafka_consumers`

Одна строка на consumer Kafka-таблицы (`kafka_num_consumers` штук на ноду). Single-node:

```bash
docker exec clickhouse clickhouse-client --password sandbox --query "
SELECT
    table,
    consumer_id,
    assignments.topic,
    assignments.partition_id,
    assignments.current_offset,
    last_poll_time,
    last_commit_time,
    num_commits,
    num_messages_read,
    num_rebalance_assignments,
    is_currently_used,
    missing_dependencies,
    if(empty(exceptions.time), NULL, arrayMax(exceptions.time)) AS last_exception_time,
    if(empty(exceptions.text), '', exceptions.text[indexOf(exceptions.time, arrayMax(exceptions.time))]) AS last_exception
FROM system.kafka_consumers
WHERE database = currentDatabase() AND table = 'orders_queue'
FORMAT Vertical"
```

Кластер (`docker-compose.cluster.yml`): consumers живут на каждой ноде, общий `kafka_group_name` делит партиции между ними. `hostName()` показывает ноду:

```bash
docker exec clickhouse clickhouse-client --password sandbox --query "
SELECT hostName() AS host, consumer_id, assignments.partition_id, last_poll_time, last_commit_time, num_commits
FROM clusterAllReplicas('kafka2ch', system.kafka_consumers)
WHERE table = 'orders_queue'
ORDER BY host"
```

Для Replicated database добавьте `--database kafka2ch` (или `WHERE database = 'kafka2ch'`).

Как читать:

- `assignments.partition_id` пуст у **всех** consumers таблицы — партиции никому не назначены. Пустые назначения у отдельных consumers нормальны, если consumers больше, чем партиций.
- `last_poll_time` старше минуты при непустых назначениях — consumer не опрашивает брокер.
- `last_commit_time` не растёт, а lag > 0 — сообщения читаются, но блок не вставляется (poison message без `kafka_handle_error_mode`, ошибка целевой таблицы). При lag = 0 отсутствие commit — просто пустой топик.
- `exceptions.*` — кольцевой буфер последних ошибок consumer'а.
- `missing_dependencies` не пуст — Kafka-таблица ссылается на отсутствующие MV, чтения нет.

## Lag

`assignments.current_offset` — позиция consumer'а ClickHouse; `-1001` значит, что по партиции ещё ничего не прочитано. Lag = end offset партиции − `current_offset`. End offset знает только Kafka:

```bash
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 \
  --describe --group clickhouse-orders
```

`CURRENT-OFFSET` здесь — закоммиченный offset группы (`kafka_group_name`), `LOG-END-OFFSET` — конец партиции, `LAG` — разница. Отставание `current_offset` в ClickHouse от `CURRENT-OFFSET` группы — блок, который прочитан, но ещё не закоммичен.

`ConsumerHealth` считает lag так же: читает назначения из `system.kafka_consumers` и end offsets через Kafka Admin API (`ListOffsets` с `OffsetSpec.Latest`).

## Ошибки парсинга: `{queue}_errors`

С `kafka_handle_error_mode = 'stream'` битые сообщения не блокируют партицию, а попадают в `orders_queue_errors` (`received_at` — время вставки). Рост таблицы — главный сигнал R2:

```bash
docker exec clickhouse clickhouse-client --password sandbox --query "
SELECT
    countIf(received_at >= now() - INTERVAL 5 MINUTE) AS last_5m,
    count() AS total,
    max(received_at) AS last_error_at
FROM orders_queue_errors"

docker exec clickhouse clickhouse-client --password sandbox --query "
SELECT received_at, _topic, _partition, _offset, _error
FROM orders_queue_errors
ORDER BY received_at DESC
LIMIT 20"

docker exec clickhouse clickhouse-client --password sandbox --query "
SELECT toStartOfMinute(received_at) AS minute, count() AS errors
FROM orders_queue_errors
WHERE received_at >= now() - INTERVAL 1 HOUR
GROUP BY minute
ORDER BY minute"
```

В кластере `orders_queue_errors` — `Distributed` поверх `orders_queue_errors_local`, те же запросы работают с любой ноды.

## Refreshable-агрегаты: `system.view_refreshes`

`orders_agg_mv` пересчитывает `orders_agg_1m` раз в минуту. Статус и последняя ошибка:

```bash
docker exec clickhouse clickhouse-client --password sandbox --query "
SELECT view, status, last_success_time, last_success_duration_ms, last_refresh_time,
       next_refresh_time, retry, exception
FROM system.view_refreshes
WHERE database = currentDatabase() AND view = 'orders_agg_mv'
FORMAT Vertical"
```

Проблема — непустой `exception`, растущий `retry` или `last_success_time` старше двух интервалов (2 минуты). В кластере — `clusterAllReplicas('kafka2ch', system.view_refreshes)`. `ReadAggregates` в `Sandbox.App` логирует `last_success_time` рядом с агрегатами.

## Prometheus

ClickHouse отдаёт метрики в формате Prometheus через [`docker/clickhouse/config/prometheus.xml`](../docker/clickhouse/config/prometheus.xml) (`http://<node>:9363/metrics`: `metrics`, `events`, `asynchronous_metrics`, `errors`). Конфиг смонтирован на single-node и на всех нодах кластера; с хоста доступен `clickhouse` на `localhost:9363`.

```bash
curl -s http://localhost:9363/metrics | grep -E '^ClickHouse(Metrics|ProfileEvents)_Kafka'
```

ClickHouse 26.6 отвечает `500 Not found: User-Agent` на запрос без заголовка `User-Agent` (Prometheus и `curl` его шлют; свой scraper / `HttpClient` должен задать его явно).

Метрики Kafka engine:

| Метрика | Тип | Смысл |
|---|---|---|
| `ClickHouseMetrics_KafkaConsumers` | gauge | активные consumers |
| `ClickHouseMetrics_KafkaConsumersWithAssignment` | gauge | consumers с назначенными партициями |
| `ClickHouseMetrics_KafkaAssignedPartitions` | gauge | назначенные партиции |
| `ClickHouseProfileEvents_KafkaMessagesPolled` | counter | сообщения, полученные из librdkafka |
| `ClickHouseProfileEvents_KafkaMessagesFailed` | counter | сообщения, которые не удалось распарсить (в т.ч. ушедшие в `{queue}_errors`) |
| `ClickHouseProfileEvents_KafkaCommits` | counter | успешные commit offsets |
| `ClickHouseProfileEvents_KafkaCommitFailures` | counter | неудачные commit |
| `ClickHouseProfileEvents_KafkaRebalanceErrors` | counter | ошибки ребаланса |
| `ClickHouseProfileEvents_KafkaConsumerErrors` | counter | ошибки consumer'а librdkafka |

Lag и размер `{queue}_errors` встроенный endpoint не экспортирует: lag — через kafka-exporter / Admin API, ошибки — через `KafkaMessagesFailed` (счётчик) и SQL выше (точные offsets).

### Prometheus в compose

Сервис `prometheus` (образ `PROMETHEUS_IMAGE`, по умолчанию `prom/prometheus:v3.5.0`) включается профилем `observability`:

```bash
docker compose --profile observability up -d
# кластер: scrape всех четырёх нод (docker/prometheus/prometheus.cluster.yml)
docker compose -f docker-compose.yml -f docker-compose.cluster.yml --profile observability up -d
```

UI: http://localhost:9090 (вкладка Alerts).

### Алерты

[`docker/prometheus/alerts/clickhouse-kafka.rules.yml`](../docker/prometheus/alerts/clickhouse-kafka.rules.yml):

| Алерт | Условие | Severity |
|---|---|---|
| `ClickHouseKafkaNoCommits` | 10 минут нет `KafkaCommits`, но `KafkaMessagesPolled` растёт (сообщения читаются, offsets стоят) | critical |
| `ClickHouseKafkaParseErrors` | `increase(KafkaMessagesFailed[5m]) > 0` | warning |
| `ClickHouseKafkaNoAssignments` | `sum(KafkaAssignedPartitions) == 0` по всем нодам 5 минут | critical |

`NoCommits` требует роста `KafkaMessagesPolled`, чтобы не срабатывать на пустом топике. Окно (10 минут) — это N из «нет commit > N минут»; меняется в `expr`.

Проверка правил (`promtool` из дистрибутива Prometheus):

```bash
promtool check rules docker/prometheus/alerts/clickhouse-kafka.rules.yml
promtool test rules docker/prometheus/tests/clickhouse-kafka.rules.test.yml
```

## Health-check в Sandbox.App

Фича [`Features/ConsumerHealth`](../src/Sandbox.App/Features/ConsumerHealth) раз в `ConsumerHealth__IntervalMs` (30 с) читает `system.kafka_consumers` и счётчики `{queue}_errors`, добирает end offsets через Kafka Admin API и пишет одну строку в лог. Запросы и правила оценки — в [`src/Sandbox.Observability`](../src/Sandbox.Observability) (без зависимости от `Sandbox.Contracts`, покрыты unit- и E2E-тестами).

| Статус | Уровень лога | Условия |
|---|---|---|
| `Unhealthy` | Error | нет consumers; ни одной назначенной партиции; consumer с назначениями не делал poll дольше `MaxPollAgeSeconds`; нет commit дольше `MaxCommitAgeMinutes` при lag > 0 |
| `Degraded` | Warning | ошибки парсинга в `{queue}_errors` за `ErrorWindowMinutes`; исключение consumer за то же окно; нет commit дольше `MaxCommitAgeMinutes`, а lag неизвестен (Kafka недоступна) |
| `Healthy` | Information | остальное (в т.ч. нет commit при lag = 0 — пустой топик) |

Пример:

```text
warn: Sandbox.App.Features.ConsumerHealth.ConsumerHealthWorker[0]
      Kafka consumers of orders_queue: Degraded consumers=1 partitions=1 lag=0 lastCommit=2026-10-21 12:00:05Z errors=1/5m total=1; 1 parse errors in the last 5 min
```

| Env | По умолчанию | Описание |
|---|---|---|
| `ConsumerHealth__IntervalMs` | `30000` | период проверки |
| `ConsumerHealth__QueueTable` / `ErrorsTable` | `orders_queue` / `orders_queue_errors` | Kafka-таблица и её таблица ошибок |
| `ConsumerHealth__Cluster` | — | имя кластера: `clusterAllReplicas(...)` вместо локальной `system.kafka_consumers` (в `docker-compose.cluster.yml` — `kafka2ch`) |
| `ConsumerHealth__MaxPollAgeSeconds` | `60` | допустимый возраст `last_poll_time` |
| `ConsumerHealth__MaxCommitAgeMinutes` | `5` | допустимый возраст `last_commit_time` при lag > 0 |
| `ConsumerHealth__ErrorWindowMinutes` | `5` | окно для ошибок парсинга и исключений |
| `ConsumerHealth__KafkaTimeoutMs` | `10000` | таймаут запроса end offsets |
