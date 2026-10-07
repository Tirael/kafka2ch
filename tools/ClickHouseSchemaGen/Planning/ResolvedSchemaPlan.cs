namespace ClickHouseSchemaGen.Planning;

public sealed record ResolvedSchemaPlan
{
    public required CodegenConfig Config { get; init; }

    public required IReadOnlyList<KafkaTablePlan> KafkaTables { get; init; }

    public required IReadOnlyList<MergeTreeTablePlan> MergeTreeTables { get; init; }

    public required IReadOnlyList<MaterializedViewPlan> MaterializedViews { get; init; }

    public string? TrailingSql { get; init; }
}

public sealed record KafkaTablePlan
{
    public required KafkaTableConfig Config { get; init; }

    public required IReadOnlyList<ClickHouseColumn> Columns { get; init; }
}

public sealed record MergeTreeTablePlan
{
    public required MergeTreeTableConfig Config { get; init; }

    public required PlanOrigin Origin { get; init; }
}

public sealed record MaterializedViewPlan
{
    public required MaterializedViewConfig Config { get; init; }

    public required PlanOrigin Origin { get; init; }
}
