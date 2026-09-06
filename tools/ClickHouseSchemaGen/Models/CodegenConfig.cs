namespace ClickHouseSchemaGen.Models;

public sealed class CodegenConfig
{
    public List<KafkaTableConfig> KafkaTables { get; set; } = [];
}

public sealed class KafkaTableConfig
{
    public required string MessageType { get; set; }

    public required string TableName { get; set; }

    public required string ProtoFile { get; set; }

    public required string MessageName { get; set; }

    public required string OutputPath { get; set; }

    public KafkaSettingsConfig Kafka { get; set; } = new();

    public Dictionary<string, FieldOverrideConfig> FieldOverrides { get; set; } = new(StringComparer.OrdinalIgnoreCase);
}

public sealed class KafkaSettingsConfig
{
    public string BrokerList { get; set; } = "kafka:9092";

    public string Topic { get; set; } = "orders";

    public string GroupName { get; set; } = "clickhouse-orders";

    public int SkipBytes { get; set; } = 6;

    public int NumConsumers { get; set; } = 1;
}

public sealed class FieldOverrideConfig
{
    public string? Type { get; set; }

    public bool Enum8 { get; set; }
}
