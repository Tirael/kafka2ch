namespace ClickHouseSchemaGen.Snapshot;

public sealed record SchemaSnapshot
{
    public const int CurrentVersion = 1;

    public int Version { get; init; } = CurrentVersion;

    public string? ParentChecksum { get; init; }

    public string? TrailingSqlHash { get; init; }

    public IReadOnlyList<KafkaTableSnapshot> KafkaTables { get; init; } = [];

    public IReadOnlyList<MergeTreeTableSnapshot> MergeTreeTables { get; init; } = [];

    public IReadOnlyList<MaterializedViewSnapshot> MaterializedViews { get; init; } = [];
}

public sealed record KafkaTableSnapshot
{
    public required string TableName { get; init; }

    public required string ProtoFile { get; init; }

    public required string MessageName { get; init; }

    public required KafkaSettingsSnapshot Kafka { get; init; }

    public required PersistKafkaMetaConfig PersistKafkaMeta { get; init; }

    public IReadOnlyList<SnapshotColumn> Columns { get; init; } = [];
}

public sealed record KafkaSettingsSnapshot
{
    public string BrokerList { get; init; } = "kafka:9092";

    public string Topic { get; init; } = "";

    public string GroupName { get; init; } = "";

    public int SkipBytes { get; init; } = 6;

    public int NumConsumers { get; init; } = 1;

    public bool FlattenNested { get; init; }

    public bool ProtobufOneofPresence { get; init; } = true;

    public bool ProtobufFlattenGoogleWrappers { get; init; } = true;
}

public sealed record MergeTreeTableSnapshot
{
    public required string TableName { get; init; }

    public required PlanOrigin Origin { get; init; }

    public string? SourceTable { get; init; }

    public required string OrderBy { get; init; }

    public string? Ttl { get; init; }

    public IReadOnlyList<SnapshotColumn> Columns { get; init; } = [];
}

public sealed record MaterializedViewSnapshot
{
    public required string Name { get; init; }

    public required PlanOrigin Origin { get; init; }

    public required string SourceTable { get; init; }

    public required string TargetTable { get; init; }

    public IReadOnlyList<SnapshotColumnMapping> Columns { get; init; } = [];
}

public sealed record SnapshotColumn
{
    public required string Name { get; init; }

    public required string Type { get; init; }

    public string FieldNumberPath { get; init; } = "";
}

public sealed record SnapshotColumnMapping
{
    public required string Source { get; init; }

    public required string Target { get; init; }

    public string? Expression { get; init; }
}
