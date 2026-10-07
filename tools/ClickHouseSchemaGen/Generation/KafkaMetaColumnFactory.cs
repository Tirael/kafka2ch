namespace ClickHouseSchemaGen.Generation;

public static class KafkaMetaColumnFactory
{
    public const string KeyFieldPath = "kafka:_key";
    public const string HeadersFieldPath = "kafka:_headers";
    public const string TopicFieldPath = "kafka:_topic";
    public const string PartitionFieldPath = "kafka:_partition";
    public const string OffsetFieldPath = "kafka:_offset";
    public const string TimestampFieldPath = "kafka:_timestamp_ms";

    public const string KeyColumnName = "kafka_key";
    public const string HeadersColumnName = "kafka_headers";
    public const string TopicColumnName = "kafka_topic";
    public const string PartitionColumnName = "kafka_partition";
    public const string OffsetColumnName = "kafka_offset";
    public const string TimestampColumnName = "kafka_timestamp";

    public static PersistKafkaMetaConfig Resolve(
        PersistKafkaMetaConfig? defaults,
        PersistKafkaMetaConfig? tableOverride,
        bool? includeKafkaMeta)
    {
        var resolved = new PersistKafkaMetaConfig
        {
            Key = tableOverride?.Key ?? defaults?.Key ?? false,
            Headers = tableOverride?.Headers ?? defaults?.Headers ?? false,
            Topic = tableOverride?.Topic ?? defaults?.Topic ?? false,
            Partition = tableOverride?.Partition ?? defaults?.Partition ?? false,
            Offset = tableOverride?.Offset ?? defaults?.Offset ?? false,
            TimestampMs = tableOverride?.TimestampMs ?? defaults?.TimestampMs ?? false
        };

        if (includeKafkaMeta == false)
        {
            return new PersistKafkaMetaConfig();
        }

        if (includeKafkaMeta == true && !resolved.AnyEnabled)
        {
            return new PersistKafkaMetaConfig { Key = true, Headers = true };
        }

        return resolved;
    }

    public static IReadOnlyList<PipelineColumnConfig> CreateMergeTreeColumns(PersistKafkaMetaConfig meta)
    {
        List<PipelineColumnConfig> columns = [];

        if (meta.Key)
        {
            columns.Add(new PipelineColumnConfig
            {
                Name = KeyColumnName,
                Type = "String",
                FieldNumberPath = KeyFieldPath
            });
        }

        if (meta.Headers)
        {
            columns.Add(new PipelineColumnConfig
            {
                Name = HeadersColumnName,
                Type = "Map(String, String)",
                FieldNumberPath = HeadersFieldPath
            });
        }

        if (meta.Topic)
        {
            columns.Add(new PipelineColumnConfig
            {
                Name = TopicColumnName,
                Type = "LowCardinality(String)",
                FieldNumberPath = TopicFieldPath
            });
        }

        if (meta.Partition)
        {
            columns.Add(new PipelineColumnConfig
            {
                Name = PartitionColumnName,
                Type = "UInt64",
                FieldNumberPath = PartitionFieldPath
            });
        }

        if (meta.Offset)
        {
            columns.Add(new PipelineColumnConfig
            {
                Name = OffsetColumnName,
                Type = "UInt64",
                FieldNumberPath = OffsetFieldPath
            });
        }

        if (meta.TimestampMs)
        {
            columns.Add(new PipelineColumnConfig
            {
                Name = TimestampColumnName,
                Type = "Nullable(DateTime64(3))",
                FieldNumberPath = TimestampFieldPath
            });
        }

        return columns;
    }

    public static IReadOnlyList<PipelineColumnMapping> CreateMappings(PersistKafkaMetaConfig meta)
    {
        List<PipelineColumnMapping> mappings = [];

        if (meta.Key)
        {
            mappings.Add(new PipelineColumnMapping
            {
                Source = "_key",
                Target = KeyColumnName
            });
        }

        if (meta.Headers)
        {
            mappings.Add(new PipelineColumnMapping
            {
                Source = "_headers.name",
                Target = HeadersColumnName,
                Expression = "mapFromArrays(`_headers.name`, `_headers.value`)"
            });
        }

        if (meta.Topic)
        {
            mappings.Add(new PipelineColumnMapping
            {
                Source = "_topic",
                Target = TopicColumnName
            });
        }

        if (meta.Partition)
        {
            mappings.Add(new PipelineColumnMapping
            {
                Source = "_partition",
                Target = PartitionColumnName
            });
        }

        if (meta.Offset)
        {
            mappings.Add(new PipelineColumnMapping
            {
                Source = "_offset",
                Target = OffsetColumnName
            });
        }

        if (meta.TimestampMs)
        {
            mappings.Add(new PipelineColumnMapping
            {
                Source = "_timestamp_ms",
                Target = TimestampColumnName
            });
        }

        return mappings;
    }

    public static bool IsKafkaMetaPath(string? fieldNumberPath) =>
        !string.IsNullOrEmpty(fieldNumberPath)
        && fieldNumberPath.StartsWith("kafka:", StringComparison.Ordinal);
}
