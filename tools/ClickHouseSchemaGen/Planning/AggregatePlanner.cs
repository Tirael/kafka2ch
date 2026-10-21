namespace ClickHouseSchemaGen.Planning;

public static class AggregatePlanner
{
    public static AggregatePlan Resolve(AggregateConfig config, MergeTreeTableConfig source)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(source);

        var columns = source.Columns.ToDictionary(column => column.Name, column => column.Type, StringComparer.Ordinal);
        string SourceType(string column) =>
            columns.TryGetValue(column, out var type)
                ? type
                : throw new InvalidOperationException(
                    $"Aggregate '{config.Name}' references unknown column '{column}' of source table '{source.TableName}'. " +
                    $"Available columns: {string.Join(", ", source.Columns.Select(c => c.Name))}.");

        var bucket = config.TimeBucket is null
            ? null
            : ResolveTimeBucket(config.TimeBucket, SourceType);
        var groupBy = config.GroupBy
            .Select(column => new AggregateColumnPlan(column.Trim(), SourceType(column.Trim())))
            .ToList();
        var measures = config.Measures
            .Select(measure => ResolveMeasure(config.Name, measure, SourceType))
            .ToList();

        var plan = new AggregatePlan
        {
            Name = config.Name.Trim(),
            ViewName = config.ResolvedViewName,
            SourceTable = source.TableName,
            Engine = MergeTreeEngines.Normalize(config.Engine),
            OrderBy = string.Empty,
            Final = config.Final ?? SupportsFinal(source.Engine),
            RefreshMode = AggregateRefreshModes.All.First(mode =>
                string.Equals(mode, config.Refresh.Mode.Trim(), StringComparison.OrdinalIgnoreCase)),
            RefreshEvery = NormalizeInterval(config.Refresh.Every!),
            TimeBucket = bucket,
            GroupBy = groupBy,
            Measures = measures
        };

        return plan with
        {
            OrderBy = string.IsNullOrWhiteSpace(config.OrderBy) ? DefaultOrderBy(plan.GroupKeys) : config.OrderBy.Trim()
        };
    }

    public static bool SupportsFinal(string? engine) =>
        !string.Equals(MergeTreeEngines.Normalize(engine), MergeTreeEngines.Default, StringComparison.Ordinal);

    public static string NormalizeInterval(string interval) =>
        string.Join(' ', interval.Trim().Split(' ', StringSplitOptions.RemoveEmptyEntries)).ToUpperInvariant();

    internal static string? InferMeasureType(string function, string? sourceType)
    {
        if (function is AggregateFunctions.Count or "uniq" or "uniqExact")
            return "UInt64";

        if (sourceType is null)
            return null;

        if (function is "min" or "max" or "any")
            return sourceType;

        var (inner, nullable) = Unwrap(sourceType);
        var resolved = function switch
        {
            "avg" when IsNumeric(inner) => "Float64",
            "sum" when inner.StartsWith("UInt", StringComparison.Ordinal) && IsNumeric(inner) => "UInt64",
            "sum" when inner.StartsWith("Int", StringComparison.Ordinal) && IsNumeric(inner) => "Int64",
            "sum" when inner.StartsWith("Float", StringComparison.Ordinal) => "Float64",
            _ => null
        };

        return resolved is null || !nullable ? resolved : $"Nullable({resolved})";
    }

    private static AggregateTimeBucketPlan ResolveTimeBucket(
        AggregateTimeBucketConfig bucket,
        Func<string, string> sourceType)
    {
        var column = bucket.Column.Trim();
        _ = sourceType(column);
        var function = bucket.Function.Trim();
        var type = string.IsNullOrWhiteSpace(bucket.Type) ? AggregateFunctions.TimeBuckets[function] : bucket.Type.Trim();
        return new AggregateTimeBucketPlan(column, function, bucket.Alias.Trim(), type);
    }

    private static AggregateMeasurePlan ResolveMeasure(
        string aggregateName,
        AggregateMeasureConfig measure,
        Func<string, string> sourceType)
    {
        var function = AggregateFunctions.Canonical(measure.Function)!;
        var column = string.IsNullOrWhiteSpace(measure.Column) ? null : measure.Column.Trim();
        var columnType = column is null ? null : sourceType(column);
        var type = string.IsNullOrWhiteSpace(measure.Type) ? InferMeasureType(function, columnType) : measure.Type.Trim();
        if (type is null)
        {
            throw new InvalidOperationException(
                $"Aggregate '{aggregateName}' cannot infer the type of {function}({column}) over '{columnType}'. " +
                $"Set measures[].type for '{measure.Alias}'.");
        }

        return new AggregateMeasurePlan(function, column, measure.Alias.Trim(), type);
    }

    private static string DefaultOrderBy(IReadOnlyList<string> keys) =>
        keys.Count == 0 ? "tuple()" : $"({string.Join(", ", keys)})";

    private static (string Inner, bool Nullable) Unwrap(string type)
    {
        var trimmed = type.Trim();
        if (trimmed.StartsWith("LowCardinality(", StringComparison.Ordinal) && trimmed.EndsWith(')'))
            trimmed = trimmed["LowCardinality(".Length..^1].Trim();

        return trimmed.StartsWith("Nullable(", StringComparison.Ordinal) && trimmed.EndsWith(')')
            ? (trimmed["Nullable(".Length..^1].Trim(), true)
            : (trimmed, false);
    }

    private static bool IsNumeric(string type) =>
        type is "Int8" or "Int16" or "Int32" or "Int64" or "Int128" or "Int256"
            or "UInt8" or "UInt16" or "UInt32" or "UInt64" or "UInt128" or "UInt256"
            or "Float32" or "Float64";
}
