using System.Text.Json.Nodes;

namespace ClickHouseSchemaGen;

public static class CodegenConfigLoader
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
        AllowTrailingCommas = true
    };

    private static readonly JsonNodeOptions NodeOptions = new() { PropertyNameCaseInsensitive = true };

    private static readonly JsonDocumentOptions DocumentOptions = new()
    {
        CommentHandling = JsonCommentHandling.Skip,
        AllowTrailingCommas = true
    };

    public static CodegenConfig Load(string configPath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(configPath);

        var node = LoadMerged(Path.GetFullPath(configPath), []);
        var config = node.Deserialize<CodegenConfig>(JsonOptions)
            ?? throw new InvalidOperationException($"Config file '{configPath}' is empty or invalid.");

        return config;
    }

    public static string ResolvePath(string configPath, string relativePath)
    {
        var configDirectory = Path.GetDirectoryName(Path.GetFullPath(configPath))
            ?? throw new InvalidOperationException($"Could not resolve directory for config '{configPath}'.");
        return Path.GetFullPath(Path.Combine(configDirectory, relativePath));
    }

    /// <summary>
    /// Init script path relative to the config directory, relocated under <see cref="CodegenConfig.OutputDirectory"/> when set.
    /// </summary>
    public static string ResolveInitScriptPath(CodegenConfig config, string outputPath) =>
        string.IsNullOrWhiteSpace(config.OutputDirectory)
            ? outputPath
            : Path.Combine(config.OutputDirectory, Path.GetFileName(outputPath));

    private static JsonObject LoadMerged(string fullPath, HashSet<string> visited)
    {
        if (!visited.Add(fullPath))
            throw new InvalidOperationException($"Config '{fullPath}' extends itself (cycle in 'extends').");

        var node = JsonNode.Parse(File.ReadAllText(fullPath), NodeOptions, DocumentOptions) as JsonObject
            ?? throw new InvalidOperationException($"Config file '{fullPath}' is empty or invalid.");

        if (!node.TryGetPropertyValue("extends", out var extendsNode) || extendsNode is null)
            return node;

        var basePath = ResolvePath(fullPath, extendsNode.GetValue<string>());
        var merged = LoadMerged(basePath, visited);
        node.Remove("extends");
        Merge(merged, node);
        return merged;
    }

    private static void Merge(JsonObject target, JsonObject overlay)
    {
        foreach (var (key, value) in overlay.ToList())
        {
            if (value is JsonObject overlayObject && target[key] is JsonObject targetObject)
            {
                Merge(targetObject, overlayObject);
                continue;
            }

            target[key] = value?.DeepClone();
        }
    }
}
