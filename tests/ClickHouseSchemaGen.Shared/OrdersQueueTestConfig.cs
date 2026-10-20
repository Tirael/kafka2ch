namespace ClickHouseSchemaGen.Shared;

public static class OrdersQueueTestConfig
{
    public static CodegenDefaults Defaults => new()
    {
        MaxFlattenDepth = 3,
        RepeatedMessageStrategy = "nested",
        OptionalAsNullable = true,
        OneofPresence = true,
        EnumMaxValuesForEnum8 = 127
    };

    public static KafkaTableConfig Create() => new()
    {
        MessageType = "ClickHouseSchemaGen.TestContracts.OrderEvent, ClickHouseSchemaGen.TestContracts",
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
            NumConsumers = 1,
            FlattenNested = false
        },
        FieldOverrides = new Dictionary<string, FieldOverrideConfig>(StringComparer.OrdinalIgnoreCase)
        {
            ["category"] = new() { Type = "LowCardinality(String)" },
            ["status"] = new() { Enum8 = true },
            ["tags"] = new() { Type = "Array(LowCardinality(String))" },
            ["status_history"] = new() { Enum8 = true }
        }
    };

    public static KafkaTableConfig CreateCompositeKeyTable()
    {
        var table = Create();
        table.Key = new KafkaKeyConfig
        {
            Format = KafkaKeyFormats.Protobuf,
            MessageType = "ClickHouseSchemaGen.TestContracts.Fixtures.CompositeKey, ClickHouseSchemaGen.TestContracts",
            FieldOverrides = new Dictionary<string, FieldOverrideConfig>(StringComparer.OrdinalIgnoreCase)
            {
                ["tenant"] = new() { Type = "LowCardinality(String)" }
            }
        };
        return table;
    }
}
