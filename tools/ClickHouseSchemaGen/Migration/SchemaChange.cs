namespace ClickHouseSchemaGen.Migration;

public abstract record SchemaChange;

public sealed record TableAdded(string TableName, string Engine) : SchemaChange;

public sealed record TableRemoved(string TableName, string Engine) : SchemaChange;

public sealed record ColumnAdded(
    string TableName,
    string ColumnName,
    string ColumnType,
    string FieldNumberPath,
    string? AfterColumn) : SchemaChange;

public sealed record ColumnRenamed(
    string TableName,
    string OldName,
    string NewName,
    string FieldNumberPath) : SchemaChange;

public sealed record ColumnTypeChanged(
    string TableName,
    string ColumnName,
    string OldType,
    string NewType,
    string FieldNumberPath,
    TypeChangeKind Kind) : SchemaChange;

public sealed record ColumnRemoved(
    string TableName,
    string ColumnName,
    string FieldNumberPath) : SchemaChange;

public sealed record KafkaTableChanged(string TableName, string Reason) : SchemaChange;

public sealed record ViewChanged(string ViewName, string SourceTable, string TargetTable) : SchemaChange;

public sealed record TrailingSqlChanged(string? OldHash, string? NewHash) : SchemaChange;

public sealed record OrderByOrTtlChanged(
    string TableName,
    string? OldOrderBy,
    string? NewOrderBy,
    string? OldTtl,
    string? NewTtl) : SchemaChange;

public sealed record EngineChanged(string TableName, string OldEngine, string NewEngine) : SchemaChange;

public sealed record OriginChanged(string TableName, PlanOrigin OldOrigin, PlanOrigin NewOrigin) : SchemaChange;

public sealed record SourceTableChanged(string TableName, string? OldSourceTable, string? NewSourceTable) : SchemaChange;

public sealed record ClusterTopologyChanged(string Scope, string Reason) : SchemaChange;

public sealed record AggregateAdded(string Name) : SchemaChange;

public sealed record AggregateRemoved(string Name, string ViewName) : SchemaChange;

public sealed record AggregateChanged(string Name, string OldViewName, string Reason, bool RecreatesTarget) : SchemaChange;

public sealed record AggregatesAdoptedFromTrailingSql(IReadOnlyList<string> Names, string TrailingSqlHash) : SchemaChange;

public sealed record SchemaDiff
{
    public IReadOnlyList<SchemaChange> Changes { get; init; } = [];

    public IReadOnlyList<string> Warnings { get; init; } = [];
}

public sealed record MigrationPlan
{
    public IReadOnlyList<SchemaChange> AutomaticChanges { get; init; } = [];

    public IReadOnlyList<SchemaChange> ManualChanges { get; init; } = [];

    public IReadOnlyList<string> Warnings { get; init; } = [];

    public bool RequiresManualApproval => ManualChanges.Any(IsManualOrDestructive);

    public static bool IsManualOrDestructive(SchemaChange change) =>
        change switch
        {
            ColumnRemoved => true,
            ColumnRenamed => true,
            ColumnAdded => true,
            ColumnTypeChanged { Kind: TypeChangeKind.Destructive or TypeChangeKind.Manual } => true,
            OrderByOrTtlChanged => true,
            EngineChanged => true,
            OriginChanged => true,
            ClusterTopologyChanged => true,
            TrailingSqlChanged => true,
            TableRemoved => true,
            AggregateRemoved => true,
            AggregateChanged { RecreatesTarget: true } => true,
            _ => false
        };
}
