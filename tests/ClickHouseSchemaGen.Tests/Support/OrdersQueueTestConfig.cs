using ClickHouseSchemaGen.Models;

namespace ClickHouseSchemaGen.Tests.Support;

internal static class OrdersQueueTestConfig
{
    public static KafkaTableConfig Create() => new()
    {
        MessageType = "Sandbox.Contracts.OrderEvent, Sandbox.Contracts",
        TableName = "orders_queue",
        ProtoFile = "order_event",
        MessageName = "OrderEvent",
        OutputPath = "ignored.sql",
        Kafka = new KafkaSettingsConfig
        {
            BrokerList = "kafka:9092",
            Topic = "orders",
            GroupName = "clickhouse-orders",
            SkipBytes = 6,
            NumConsumers = 1
        },
        FieldOverrides = new Dictionary<string, FieldOverrideConfig>(StringComparer.OrdinalIgnoreCase)
        {
            ["category"] = new() { Type = "LowCardinality(String)" },
            ["status"] = new() { Enum8 = true },
            ["tags"] = new() { Type = "Array(LowCardinality(String))" }
        }
    };
}
