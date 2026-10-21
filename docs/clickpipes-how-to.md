# Как реализовать ClickPipes (в контексте kafka2ch)

**Коротко:** ClickPipes — это **managed ingestion в ClickHouse Cloud**, а не table engine и не то, что ставится в локальный `docker compose` kafka2ch. Self-hosted / demo-стенд → остаётся classic Kafka engine (+ MV). Cloud → ClickPipe читает Kafka и **пушит** строки в destination MergeTree; очередь `ENGINE = Kafka` не нужна.

Связанный разбор classic vs Kafka2: [kafka2-production-assessment.md](./kafka2-production-assessment.md).

Официальные гайды: [create Kafka ClickPipe](https://clickhouse.com/docs/integrations/clickpipes/kafka/create-kafka-clickpipe), [schema registries](https://clickhouse.com/docs/integrations/clickpipes/kafka/schema-registries), [reference / virtual columns](https://clickhouse.com/docs/integrations/clickpipes/kafka/reference), [FAQ](https://clickhouse.com/docs/integrations/clickpipes/kafka/faq), [Terraform](https://clickhouse.com/docs/integrations/clickpipes/programmatic-access/terraform), [OpenAPI](https://clickhouse.com/docs/integrations/clickpipes/programmatic-access/openapi).

---

## 1. Что это архитектурно

| | Kafka table engine (kafka2ch сейчас) | ClickPipes |
| --- | --- | --- |
| Где крутится | Внутри процесса ClickHouse | Отдельный Cloud-сервис |
| Модель | Pull: CH сам ходит в Kafka | Push: ClickPipes → INSERT в Cloud service |
| Нужна queue-таблица | Да (`orders_queue` ENGINE=Kafka) | Нет |
| Destination | Обычно через MV → MergeTree | Сразу таблица (managed или existing) |
| Масштаб ingestion | Вместе с CH-нодой | Отдельно (replicas / CPU / memory) |
| Self-hosted | Да | **Нет** (Cloud; BYOC «coming soon» в доках) |
| Семантика | at-least-once | at-least-once |

Типичная замена пайплайна kafka2ch в Cloud:

```
Kafka (Protobuf + Confluent SR)
        │
        ▼
   Kafka ClickPipe  ──INSERT──►  orders / shipments (MergeTree)
                                      │
                                      ▼
                              orders_agg_mv / … (как сейчас)
```

Убираются: `*_queue`, `*_mv` из queue, `kafka_schema_registry_skip_bytes`, DETACH/ATTACH queue при миграциях. Остаются: MergeTree + agg MV (и при необходимости transform MV поверх destination, если схема ClickPipe «сырая»).

---

## 2. Предпосылки

1. **ClickHouse Cloud** service с включёнными ClickPipes.
2. Kafka, доступный из Cloud: публичные брокеры + whitelist egress IP ClickHouse Cloud, либо **AWS PrivateLink / reverse private endpoint**, либо **SSH tunnel** через bastion.
3. Для Avro/Protobuf — либо **Confluent-compatible Schema Registry по HTTPS**, либо (только Protobuf) upload `.proto` / `FileDescriptorSet` (≤ 768 KiB). SR URL должен быть `https://`.
4. API key org (для OpenAPI/Terraform): org id, service id, key id/secret.

Локальный `cp-kafka` из compose kafka2ch из Cloud без туннеля/PrivateLink **недоступен** — ClickPipes туда сам не «залезет».

---

## 3. Реализация: три способа

### A. UI (быстрый старт)

1. Cloud console → **Data Sources** → **Set up a ClickPipe**.
2. Источник: Kafka / Confluent / MSK / Redpanda / …
3. Имя, brokers, auth (PLAIN / SCRAM / IAM / …), consumer group.
4. Schema registry (опционально, но для kafka2ch-Protobuf — да): root URL / subject / schema id + credentials.
5. Topic (`orders`, затем второй pipe на `shipments` — обычно **один topic = один ClickPipe**).
6. Format: Protobuf (или AvroConfluent). Без SR — upload proto при выборе топика.
7. Destination: new managed table **или** existing (схема колонок из sample / mapping).
8. Virtual columns при необходимости: `_key`, `_timestamp`, `_partition`, `_offset`, `_header_keys` / `_header_values`, …
9. Permissions для internal writer:
   - **Only destination table** — только INSERT в target;
   - **Full access** — если на destination висят MV/Dictionary.
10. **Create ClickPipe** → мониторинг в Data Sources; ошибки — в ClickPipes error table.

### B. Terraform (рекомендуемый prod-path)

Провайдер `clickhouse_clickpipe`. Эскиз под Confluent + Protobuf + SR (упрощённо; имена колонок подогнать под ваш proto/codegen):

```hcl
resource "clickhouse_clickpipe" "orders" {
  name       = "orders-protobuf"
  service_id = var.clickhouse_service_id

  scaling {
    replicas               = 1
    replica_cpu_millicores = 250
    replica_memory_gb      = 1.0
  }

  source {
    kafka {
      type           = "confluent"
      format         = "Protobuf"
      brokers        = var.kafka_brokers
      topics         = "orders"
      consumer_group = "clickpipes-orders"
      authentication = "PLAIN"

      credentials {
        username = var.kafka_username
        password = var.kafka_password
      }

      schema_registry {
        url            = "https://schema-registry.example.com"
        authentication = "PLAIN"
        credentials {
          username = var.sr_username
          password = var.sr_password
        }
      }

      offset {
        strategy = "from_latest" # или from_beginning — осознанно
      }
    }
  }

  destination {
    database      = "default"
    table         = "orders"
    managed_table = true

    table_definition {
      engine {
        type = "MergeTree"
      }
      sorting_key = ["event_time", "order_id"]
    }

    columns {
      name = "order_id"
      type = "String"
    }
    # … остальные поля из OrderEvent + meta
    columns {
      name = "kafka_key"
      type = "String"
    }
    # structured key: map source _key.order_id → column (в UI/API mapping)
  }
}
```

Аналогичный resource для `shipments`. Полные примеры: [Terraform reference](https://clickhouse.com/docs/integrations/clickpipes/programmatic-access/terraform).

### C. OpenAPI

`POST /v1/organizations/{org}/services/{service}/clickpipes` с телом `source.kafka` + `destination` (см. [OpenAPI docs](https://clickhouse.com/docs/integrations/clickpipes/programmatic-access/openapi)). Удобно для CI/bootstrap; для долгоживущего prod чаще Terraform.

---

## 4. Protobuf + Schema Registry + keys (как в kafka2ch)

| Тема kafka2ch | Как в ClickPipes |
| --- | --- |
| Confluent envelope + `kafka_schema_registry_skip_bytes = 6` | Не нужно: ClickPipes сам читает schema id из wire format через SR |
| Локальные `format_schemas/*.proto` на CH | Либо SR (subject/id/root URL), либо upload proto/FDS; импорты Protobuf SR подтягиваются как dependencies |
| Value → колонки queue | Mapping schema fields → destination columns при создании pipe |
| `_key` raw + SQL UDF `protobufWire*` | Structured keys: mapping `_key.<field>` (отдельный schema id ок, **тот же** registry family + format, что у value). Либо `_key` → `String` без декода |
| Headers → `Map` через `mapFromArrays` | Virtual `_header_keys` / `_header_values` (параллельные массивы) → колонки; при необходимости MV поверх destination соберёт `Map` |
| Nested / oneof / wrappers | Зависит от того, как ClickPipes развернёт Protobuf в колонки; сложный denorm из codegen kafka2ch **не переносится 1:1** — сверить sample в UI и при необходимости упростить destination + transform MV |

Ошибки парсинга/схемы: сообщение уходит в **ClickPipes error table** (`SOURCE_SCHEMA_ERROR`, `DATA_PARSING_ERROR`), а не в «зависший» Kafka engine poll (поведение другое, чем у classic engine).

---

## 5. MV и агрегаты после ClickPipe

1. ClickPipe пишет в `orders` / `shipments` (или в staging `orders_raw`).
2. Если нужна денормализация как в `orders_mv` — создать **MV FROM destination → final** (не FROM Kafka queue).
3. Agg MV (`orders_agg_1m`, …) оставить как сейчас, источник — final/raw MergeTree.
4. Writer role: **Full access**, если MV/Dictionary читают/пишут за пределами одной INSERT-таблицы.

Идемпотентность: ClickPipes — at-least-once; при дублях — ReplacingMergeTree / дедуп-ключи / идемпотентные агрегаты.

---

## 6. Что это значит для репозитория kafka2ch

| Цель | Действие |
| --- | --- |
| Локальный demo / self-hosted | **Не внедрять ClickPipes**. Classic Kafka engine остаётся правильным путём. |
| Prod на ClickHouse Cloud | Отдельный Cloud service + 2 ClickPipes (orders/shipments) + Terraform/OpenAPI; codegen queue SQL **не использовать** как runtime; схему destination можно всё ещё генерить из proto, но target = MergeTree columns + mapping, не `ENGINE = Kafka`. |
| Гибрид | Producer/SR как сейчас; Cloud только как sink через ClickPipes; локальный CH для dev. |

В репо **нет** готового модуля ClickPipes — это ожидаемо: compose-стенд self-hosted. «Реализовать ClickPipes» = Cloud-конфиг (UI/TF/API) + сетевой доступ к Kafka/SR, а не патч `KafkaTableGenerator`.

---

## 7. Чеклист внедрения (Cloud)

1. Поднять/выбрать ClickHouse Cloud service.
2. Открыть сеть до Kafka (+ SR HTTPS): whitelist / PrivateLink / SSH bastion.
3. Создать ClickPipe на `orders` (Protobuf + Confluent SR), destination `orders`.
4. Создать ClickPipe на `shipments` → `shipments`.
5. Добавить mapping `_key.*` / headers при необходимости.
6. Выкатить transform/agg MV; роль writer = Full access если нужно.
7. Прогнать smoke: produce → rows in destination → agg растёт; error table пуста.
8. Закрепить в Terraform; алертить lag / error table / pipe health.

**Не смешивать** на одном Cloud service classic Kafka engine queue и ClickPipe на тот же consumer group без явной стратегии — обычно выбирают один путь ingestion.
