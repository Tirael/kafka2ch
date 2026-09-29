using ClickHouseSchemaGen.Validation;

namespace ClickHouseSchemaGen;

public sealed class ClickHouseSchemaGenerator(
    DenormalizationPlanner planner,
    CodegenConfigValidator? configValidator = null)
{
    private readonly CodegenConfigValidator _configValidator = configValidator ?? new();
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
        AllowTrailingCommas = true
    };

    public string GenerateKafkaTableSql(
        KafkaTableConfig config,
        CodegenDefaults defaults,
        CodegenConfig? rootConfig = null)
    {
        var descriptor = ProtoDescriptorResolver.ResolveDescriptor(config.MessageType);
        var overrides = MergeFieldOverrides(rootConfig?.FieldOverrides, config.FieldOverrides);
        var columns = planner.MapMessage(descriptor, defaults, overrides);
        return KafkaTableGenerator.Generate(config, columns);
    }

    public string GenerateKafkaTableSql(KafkaTableConfig config, CodegenDefaults defaults) =>
        GenerateKafkaTableSql(config, defaults, rootConfig: null);

    public void GenerateFromConfigFile(string configPath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(configPath);

        var configDirectory = Path.GetDirectoryName(Path.GetFullPath(configPath))
            ?? throw new InvalidOperationException($"Could not resolve directory for config '{configPath}'.");

        var config = JsonSerializer.Deserialize<CodegenConfig>(File.ReadAllText(configPath), JsonOptions)
            ?? throw new InvalidOperationException($"Config file '{configPath}' is empty or invalid.");

        _configValidator.ValidateAndThrow(config);

        var queueColumnsByTable = new Dictionary<string, IReadOnlyList<ClickHouseColumn>>(StringComparer.OrdinalIgnoreCase);

        foreach (var table in config.KafkaTables)
        {
            var columns = MapKafkaTableColumns(table, config);
            queueColumnsByTable[table.TableName] = columns;
            WriteGeneratedSql(configDirectory, table.OutputPath, KafkaTableGenerator.Generate(table, columns));
        }

        if (config.Pipeline is null)
            return;

        WriteGeneratedSql(configDirectory, config.Pipeline.OutputPath, BuildPipelineSql(config.Pipeline, queueColumnsByTable));
    }

    private static string BuildPipelineSql(
        PipelineConfig pipeline,
        IReadOnlyDictionary<string, IReadOnlyList<ClickHouseColumn>> queueColumnsByTable)
    {
        var pipelineBuilder = new StringBuilder()
            .AppendLine(SqlScriptWriter.GeneratedHeader)
            .AppendLine();

        var expandedTables = pipeline.MergeTreeTables
            .Select(mergeTreeTable => PipelineColumnExpander.ExpandMergeTreeTable(mergeTreeTable, queueColumnsByTable))
            .ToList();

        foreach (var mergeTreeTable in expandedTables)
        {
            pipelineBuilder.Append(MergeTreeTableGenerator.Generate(mergeTreeTable));
        }

        var materializedViews = pipeline.MaterializedViews
            .Select(materializedView => PipelineColumnExpander.ExpandMaterializedView(materializedView, queueColumnsByTable))
            .Concat(MaterializedViewAutoGenerator.CreateForAutoColumns(
                pipeline.MergeTreeTables,
                expandedTables,
                pipeline.MaterializedViews));

        foreach (var materializedView in materializedViews)
        {
            pipelineBuilder.Append(MaterializedViewGenerator.Generate(materializedView));
        }

        if (!string.IsNullOrWhiteSpace(pipeline.TrailingSql))
            pipelineBuilder.AppendLine(pipeline.TrailingSql.Trim());

        return pipelineBuilder.ToString();
    }

    private IReadOnlyList<ClickHouseColumn> MapKafkaTableColumns(KafkaTableConfig table, CodegenConfig rootConfig)
    {
        var descriptor = ProtoDescriptorResolver.ResolveDescriptor(table.MessageType);
        var overrides = MergeFieldOverrides(rootConfig.FieldOverrides, table.FieldOverrides);
        return planner.MapMessage(descriptor, rootConfig.Defaults, overrides);
    }

    private static void WriteGeneratedSql(string configDirectory, string outputPath, string sql)
    {
        var fullPath = Path.GetFullPath(Path.Combine(configDirectory, outputPath));
        Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);
        File.WriteAllText(fullPath, sql);
    }

    private static Dictionary<string, FieldOverrideConfig> MergeFieldOverrides(
        IReadOnlyDictionary<string, FieldOverrideConfig>? rootOverrides,
        IReadOnlyDictionary<string, FieldOverrideConfig> tableOverrides) =>
        new Dictionary<string, FieldOverrideConfig>(StringComparer.OrdinalIgnoreCase)
            .MergeInto(rootOverrides)
            .MergeInto(tableOverrides);
}

internal static class ProtoDescriptorResolver
{
    public static MessageDescriptor ResolveDescriptor(string messageType)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageType);

        var type = Type.GetType(messageType, throwOnError: true)
            ?? throw new InvalidOperationException($"Message type '{messageType}' was not found.");

        if (!typeof(IMessage).IsAssignableFrom(type))
            throw new InvalidOperationException($"Type '{messageType}' is not a protobuf message.");

        var descriptorProperty = type.GetProperty(
            "Descriptor",
            BindingFlags.Public | BindingFlags.Static)
            ?? throw new InvalidOperationException($"Type '{messageType}' has no Descriptor property.");

        return (MessageDescriptor)descriptorProperty.GetValue(null)!;
    }
}
