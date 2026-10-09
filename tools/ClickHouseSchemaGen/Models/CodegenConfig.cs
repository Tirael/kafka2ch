namespace ClickHouseSchemaGen.Models;

public sealed class CodegenConfig
{
    /// <summary>
    /// Base config merged under this one (objects deep-merged, arrays and scalars replaced).
    /// Relative to this file. Relative paths of the merged config resolve against this file.
    /// </summary>
    public string? Extends { get; set; }

    /// <summary>
    /// When set, every init script is written here under the file name of its <c>outputPath</c>.
    /// </summary>
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

/// <summary>
/// Kafka message key contract. ClickHouse exposes the key only as the raw <c>_key</c> String,
/// so protobuf keys are decoded in materialized views as <c>_key.&lt;field path&gt;</c> columns.
/// </summary>
public sealed class KafkaKeyConfig
{
    public string Format { get; set; } = KafkaKeyFormats.String;

    /// <summary>
    /// CLR protobuf key type (<c>FullName, Assembly</c>). Required when <see cref="Format"/> is <c>protobuf</c>.
    /// </summary>
    public string? MessageType { get; set; }

    /// <summary>
    /// Bytes to skip before the key payload. Defaults to <see cref="KafkaSettingsConfig.SkipBytes"/>.
    /// </summary>
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

    /// <summary>
    /// Cluster mode only: sharding key of the <c>Distributed</c> table. Defaults to <see cref="ClusterConfig.ShardingKey"/>.
    /// </summary>
    public string? ShardingKey { get; set; }

    /// <summary>
    /// Kafka queue table to copy columns from when <see cref="Columns"/> is empty.
    /// </summary>
    public string? SourceTable { get; set; }

    /// <summary>
    /// When true, appends canonical kafka meta columns even for explicit column lists.
    /// </summary>
    public bool? IncludeKafkaMeta { get; set; }

    /// <summary>
    /// Explicit MergeTree columns. When empty, columns are taken from <see cref="SourceTable"/>.
    /// </summary>
    public List<PipelineColumnConfig> Columns { get; set; } = [];
}

public sealed class MaterializedViewConfig
{
    public required string Name { get; set; }

    public required string TargetTable { get; set; }

    public required string SourceTable { get; set; }

    /// <summary>
    /// When true, appends canonical kafka meta mappings even for explicit column lists.
    /// </summary>
    public bool? IncludeKafkaMeta { get; set; }

    /// <summary>
    /// Explicit column mappings. When empty, all columns from <see cref="SourceTable"/>
    /// are mapped 1:1 (<c>source</c> → <c>target</c> with the same name).
    /// </summary>
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
