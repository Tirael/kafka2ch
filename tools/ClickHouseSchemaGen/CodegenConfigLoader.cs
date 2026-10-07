namespace ClickHouseSchemaGen;

public static class CodegenConfigLoader
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
        AllowTrailingCommas = true
    };

    public static CodegenConfig Load(string configPath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(configPath);

        var config = JsonSerializer.Deserialize<CodegenConfig>(File.ReadAllText(configPath), JsonOptions)
            ?? throw new InvalidOperationException($"Config file '{configPath}' is empty or invalid.");

        return config;
    }

    public static string ResolvePath(string configPath, string relativePath)
    {
        var configDirectory = Path.GetDirectoryName(Path.GetFullPath(configPath))
            ?? throw new InvalidOperationException($"Could not resolve directory for config '{configPath}'.");
        return Path.GetFullPath(Path.Combine(configDirectory, relativePath));
    }
}
