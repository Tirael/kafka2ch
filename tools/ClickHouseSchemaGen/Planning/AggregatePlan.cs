namespace ClickHouseSchemaGen.Planning;

public sealed record AggregatePlan
{
    public required string Name { get; init; }

    public required string ViewName { get; init; }

    public required string SourceTable { get; init; }

    public required string Engine { get; init; }

    public required string OrderBy { get; init; }

    public required bool Final { get; init; }

    public required string RefreshMode { get; init; }

    public required string RefreshEvery { get; init; }

    public AggregateTimeBucketPlan? TimeBucket { get; init; }

    public IReadOnlyList<AggregateColumnPlan> GroupBy { get; init; } = [];

    public IReadOnlyList<AggregateMeasurePlan> Measures { get; init; } = [];

    public IReadOnlyList<string> GroupKeys =>
        (TimeBucket is null ? [] : new[] { TimeBucket.Alias })
            .Concat(GroupBy.Select(column => column.Name))
            .ToList();

    public IReadOnlyList<PipelineColumnConfig> TargetColumns =>
        (TimeBucket is null ? [] : new[] { new PipelineColumnConfig { Name = TimeBucket.Alias, Type = TimeBucket.Type } })
            .Concat(GroupBy.Select(column => new PipelineColumnConfig { Name = column.Name, Type = column.Type }))
            .Concat(Measures.Select(measure => new PipelineColumnConfig { Name = measure.Alias, Type = measure.Type }))
            .ToList();
}

public sealed record AggregateTimeBucketPlan(string Column, string Function, string Alias, string Type)
{
    public string Expression => $"{Function}({Column})";
}

public sealed record AggregateColumnPlan(string Name, string Type);

public sealed record AggregateMeasurePlan(string Function, string? Column, string Alias, string Type)
{
    public string Expression => $"{Function}({Column})";
}
