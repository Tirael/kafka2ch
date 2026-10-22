namespace ClickHouseSchemaGen.Models;

public sealed class AggregateConfig
{
    public required string Name { get; set; }

    public string? ViewName { get; set; }

    public required string SourceTable { get; set; }

    public string Engine { get; set; } = MergeTreeEngines.Default;

    public string? OrderBy { get; set; }

    public bool? Final { get; set; }

    public AggregateTimeBucketConfig? TimeBucket { get; set; }

    public List<string> GroupBy { get; set; } = [];

    public List<AggregateMeasureConfig> Measures { get; set; } = [];

    public AggregateRefreshConfig Refresh { get; set; } = new();

    [JsonIgnore]
    public string ResolvedViewName =>
        string.IsNullOrWhiteSpace(ViewName) ? AggregateDefaults.ViewName(Name) : ViewName.Trim();
}

public sealed class AggregateTimeBucketConfig
{
    public required string Column { get; set; }

    public string Function { get; set; } = AggregateFunctions.DefaultTimeBucket;

    public required string Alias { get; set; }

    public string? Type { get; set; }
}

public sealed class AggregateMeasureConfig
{
    public required string Function { get; set; }

    public string? Column { get; set; }

    public required string Alias { get; set; }

    public string? Type { get; set; }
}

public sealed class AggregateRefreshConfig
{
    public string Mode { get; set; } = AggregateRefreshModes.Refreshable;

    public string? Every { get; set; }
}

public static class AggregateDefaults
{
    public static string ViewName(string aggregateName) => $"{aggregateName}_mv";
}

public static class AggregateRefreshModes
{
    public const string Refreshable = "refreshable";

    public static readonly string[] All = [Refreshable];
}

public static class AggregateFunctions
{
    public const string Count = "count";

    public const string DefaultTimeBucket = "toStartOfMinute";

    public static readonly string[] Measures = [Count, "sum", "avg", "min", "max", "any", "uniq", "uniqExact"];

    public static readonly IReadOnlyDictionary<string, string> TimeBuckets = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["toStartOfMinute"] = "DateTime",
        ["toStartOfFiveMinutes"] = "DateTime",
        ["toStartOfFifteenMinutes"] = "DateTime",
        ["toStartOfHour"] = "DateTime",
        ["toStartOfDay"] = "DateTime",
        ["toDate"] = "Date",
        ["toMonday"] = "Date",
        ["toStartOfMonth"] = "Date"
    };

    public static string? Canonical(string? function) =>
        Measures.FirstOrDefault(name => string.Equals(name, function?.Trim(), StringComparison.OrdinalIgnoreCase));
}
