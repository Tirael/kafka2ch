namespace ClickHouseSchemaGen.Models;

public sealed class PersistKafkaMetaConfig
{
    public bool Key { get; set; }

    public bool Headers { get; set; }

    public bool Topic { get; set; }

    public bool Partition { get; set; }

    public bool Offset { get; set; }

    public bool TimestampMs { get; set; }

    public bool AnyEnabled => Key || Headers || Topic || Partition || Offset || TimestampMs;
}
