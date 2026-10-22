namespace ClickHouseSchemaGen;

public static partial class TopologiesConfigLoader
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
        AllowTrailingCommas = true
    };

    public static TopologySet Load(string rootConfigPath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rootConfigPath);

        var config = JsonSerializer.Deserialize<TopologiesConfig>(File.ReadAllText(rootConfigPath), JsonOptions)
            ?? throw new InvalidOperationException($"Topologies config '{rootConfigPath}' is empty or invalid.");

        if (config.Topologies.Count == 0)
            throw new InvalidOperationException($"Topologies config '{rootConfigPath}' must list at least one codegen config in 'topologies'.");

        if (config.MigrationsBaseline is { } baseline && !MigrationVersionPattern().IsMatch(baseline))
            throw new InvalidOperationException(
                $"Topologies config '{rootConfigPath}': migrationsBaseline '{baseline}' must be a yyyyMMddHHmmss migration version.");

        return new TopologySet(
            config.Topologies.Select(path => CodegenConfigLoader.ResolvePath(rootConfigPath, path)).ToList(),
            config.MigrationsBaseline);
    }

    [GeneratedRegex(@"^\d{14}$")]
    private static partial Regex MigrationVersionPattern();
}

public sealed record TopologySet(
    IReadOnlyList<string> ConfigPaths,
    string? MigrationsBaseline);
