namespace ClickHouseSchemaGen.Snapshot;

public sealed record SchemaSnapshot
{
    public const int CurrentVersion = 2;

    public int Version { get; init; } = CurrentVersion;

    public string? ParentChecksum { get; init; }

    public string? TrailingSqlHash { get; init; }

    public ClusterSnapshot? Cluster { get; init; }

    public IReadOnlyList<KafkaTableSnapshot> KafkaTables { get; init; } = [];

    public IReadOnlyList<MergeTreeTableSnapshot> MergeTreeTables { get; init; } = [];

    public IReadOnlyList<MaterializedViewSnapshot> MaterializedViews { get; init; } = [];

    public IReadOnlyList<AggregateSnapshot>? Aggregates { get; init; }
}

public sealed record ClusterSnapshot
{
    public required string Name { get; init; }

    public required string DdlMode { get; init; }

    public required string LocalTableSuffix { get; init; }

    public required string ShardingKey { get; init; }

    public bool MaterializedViewsWriteThroughDistributed { get; init; }
}

public sealed record KafkaTableSnapshot
{
    public required string TableName { get; init; }

    public required string ProtoFile { get; init; }

    public required string MessageName { get; init; }

    public required KafkaSettingsSnapshot Kafka { get; init; }

    public KafkaKeySnapshot? Key { get; init; }

    public required PersistKafkaMetaConfig PersistKafkaMeta { get; init; }

    public IReadOnlyList<SnapshotColumn> Columns { get; init; } = [];
}

public sealed record KafkaSettingsSnapshot
{
    public string? BrokerList { get; init; }

    public string? NamedCollection { get; init; }

    public string Topic { get; init; } = "";

    public string GroupName { get; init; } = "";

    public int SkipBytes { get; init; } = 6;

    public int NumConsumers { get; init; } = 1;

    public bool FlattenNested { get; init; }

    public bool ProtobufOneofPresence { get; init; } = true;

    public bool ProtobufFlattenGoogleWrappers { get; init; } = true;

    public string? HandleErrorMode { get; init; }

    public int? SkipBrokenMessages { get; init; }
}

public sealed record KafkaKeySnapshot
{
    public string Format { get; init; } = KafkaKeyFormats.String;

    public string? MessageType { get; init; }

    public int? SkipBytes { get; init; }
}

public sealed record MergeTreeTableSnapshot
{
    public required string TableName { get; init; }

    public required PlanOrigin Origin { get; init; }

    public string? SourceTable { get; init; }

    public required string OrderBy { get; init; }

    public string? Engine { get; init; }

    public string? Ttl { get; init; }

    public string? ShardingKey { get; init; }

    public bool? MaterializedViewsWriteThroughDistributed { get; init; }

    public IReadOnlyList<SnapshotColumn> Columns { get; init; } = [];
}

public sealed record MaterializedViewSnapshot
{
    public required string Name { get; init; }

    public required PlanOrigin Origin { get; init; }

    public required string SourceTable { get; init; }

    public required string TargetTable { get; init; }

    public string? Where { get; init; }

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

public sealed record AggregateSnapshot
{
    public required string Name { get; init; }

    public required string ViewName { get; init; }

    public required string SourceTable { get; init; }

    public string? Engine { get; init; }

    public required string OrderBy { get; init; }

    public bool Final { get; init; }

    public required string RefreshMode { get; init; }

    public required string RefreshEvery { get; init; }

    public AggregateTimeBucketSnapshot? TimeBucket { get; init; }

    public IReadOnlyList<AggregateColumnSnapshot> GroupBy { get; init; } = [];

    public IReadOnlyList<AggregateMeasureSnapshot> Measures { get; init; } = [];
}

public sealed record AggregateTimeBucketSnapshot
{
    public required string Column { get; init; }

    public required string Function { get; init; }

    public required string Alias { get; init; }

    public required string Type { get; init; }
}

public sealed record AggregateColumnSnapshot
{
    public required string Name { get; init; }

    public required string Type { get; init; }
}

public sealed record AggregateMeasureSnapshot
{
    public required string Function { get; init; }

    public string? Column { get; init; }

    public required string Alias { get; init; }

    public required string Type { get; init; }
}
