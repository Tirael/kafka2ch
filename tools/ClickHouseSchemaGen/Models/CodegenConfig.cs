namespace ClickHouseSchemaGen.Models;

public sealed class CodegenConfig
{


    public string? Extends { get; set; }


    public string? OutputDirectory { get; set; }

    public ClusterConfig Cluster { get; set; } = new();

    public CodegenDefaults Defaults { get; set; } = new();

    public List<KafkaTableConfig> KafkaTables { get; set; } = [];

    public Dictionary<string, FieldOverrideConfig> FieldOverrides { get; set; } =
        new(StringComparer.OrdinalIgnoreCase);

    public PipelineConfig? Pipeline { get; set; }

    public MigrationsConfig Migrations { get; set; } = new();
}

public sealed class MigrationsConfig
{
    public string SnapshotPath { get; set; } = "../../docker/clickhouse/init/schema.snapshot.json";

    public string MigrationsDirectory { get; set; } = "../../docker/clickhouse/migrations";

    public string VersionsOutputPath { get; set; } =
        $"../../docker/clickhouse/init/{SchemaMigrationsTable.DefaultScriptFileName}";
}

public sealed class CodegenDefaults
{
    public int MaxFlattenDepth { get; set; } = 3;

    public string RepeatedMessageStrategy { get; set; } = "nested";

    public bool OptionalAsNullable { get; set; } = true;

    public bool OneofPresence { get; set; } = true;

    public int EnumMaxValuesForEnum8 { get; set; } = 127;

    public PersistKafkaMetaConfig PersistKafkaMeta { get; set; } = new();
}

public sealed class KafkaTableConfig
{
    public required string MessageType { get; set; }

    public required string TableName { get; set; }

    public required string ProtoFile { get; set; }

    public required string MessageName { get; set; }

    public required string OutputPath { get; set; }

    public KafkaSettingsConfig Kafka { get; set; } = new();

    public PersistKafkaMetaConfig? PersistKafkaMeta { get; set; }

    public KafkaKeyConfig Key { get; set; } = new();

    public Dictionary<string, FieldOverrideConfig> FieldOverrides { get; set; } =
        new(StringComparer.OrdinalIgnoreCase);
}

public static class KafkaKeyFormats
{
    public const string String = "string";

    public const string Protobuf = "protobuf";

    public static readonly string[] All = [String, Protobuf];
}


public sealed class KafkaKeyConfig
{
    public string Format { get; set; } = KafkaKeyFormats.String;


    public string? MessageType { get; set; }


    public int? SkipBytes { get; set; }

    public Dictionary<string, FieldOverrideConfig> FieldOverrides { get; set; } =
        new(StringComparer.OrdinalIgnoreCase);

    [System.Text.Json.Serialization.JsonIgnore]
    public bool IsProtobuf => string.Equals(Format, KafkaKeyFormats.Protobuf, StringComparison.OrdinalIgnoreCase);
}

public sealed class KafkaSettingsConfig
{
    public string BrokerList { get; set; } = "kafka:9092";

    public string Topic { get; set; } = "orders";

    public string GroupName { get; set; } = "clickhouse-orders";

    public int SkipBytes { get; set; } = 6;

    public int NumConsumers { get; set; } = 1;

    public bool FlattenNested { get; set; }

    public bool ProtobufOneofPresence { get; set; } = true;

    public bool ProtobufFlattenGoogleWrappers { get; set; } = true;
}

public sealed class FieldOverrideConfig
{
    public string? Type { get; set; }

    public bool Enum8 { get; set; }

    public string? Strategy { get; set; }

    public int? MaxDepth { get; set; }

    public bool? Nullable { get; set; }
}

public sealed class PipelineConfig
{
    public required string OutputPath { get; set; }

    public List<MergeTreeTableConfig> MergeTreeTables { get; set; } = [];

    public List<MaterializedViewConfig> MaterializedViews { get; set; } = [];

    public string? TrailingSql { get; set; }
}

public sealed class MergeTreeTableConfig
{
    public required string TableName { get; set; }

    public required string OrderBy { get; set; }

    public string? Ttl { get; set; }

    public string? ShardingKey { get; set; }

    public bool? MaterializedViewsWriteThroughDistributed { get; set; }

    public string? SourceTable { get; set; }


    public bool? IncludeKafkaMeta { get; set; }


    public List<PipelineColumnConfig> Columns { get; set; } = [];
}

public sealed class MaterializedViewConfig
{
    public required string Name { get; set; }

    public required string TargetTable { get; set; }

    public required string SourceTable { get; set; }


    public bool? IncludeKafkaMeta { get; set; }


    public List<PipelineColumnMapping> Columns { get; set; } = [];
}

public sealed class PipelineColumnConfig
{
    public required string Name { get; set; }

    public required string Type { get; set; }

    public string? FieldNumberPath { get; set; }
}

public sealed class PipelineColumnMapping
{
    public required string Source { get; set; }

    public required string Target { get; set; }

    public string? Expression { get; set; }
}
