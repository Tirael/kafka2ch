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
        var columns = planner.MapMessage(descriptor, defaults, overrides)
            .Concat(KafkaQueueMetadataColumns.Create(config))
            .ToList();
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

        foreach (var table in config.KafkaTables)
            WriteGeneratedSql(configDirectory, table.OutputPath, GenerateKafkaTableSql(table, config.Defaults, config));

        if (config.Pipeline is null)
            return;

        WriteGeneratedSql(configDirectory, config.Pipeline.OutputPath, BuildPipelineSql(config));
    }

    private static string BuildPipelineSql(CodegenConfig config)
    {
        var pipeline = config.Pipeline
            ?? throw new InvalidOperationException("Pipeline config is required.");
        AppendHeaders(config);
        AppendKeys(config);

        var pipelineBuilder = new StringBuilder()
            .AppendLine(SqlScriptWriter.GeneratedHeader)
            .AppendLine();

        if (config.KafkaTables.Any(table => table.Key is not null))
        {
            pipelineBuilder
                .AppendLine("-- Kafka message key/headers are exposed on the Kafka queue table (`_key`, ALIAS key_*),")
                .AppendLine("-- then copied into MergeTree by the materialized views below.")
                .AppendLine();
        }

        foreach (var mergeTreeTable in pipeline.MergeTreeTables)
            pipelineBuilder.Append(MergeTreeTableGenerator.Generate(mergeTreeTable));

        foreach (var materializedView in pipeline.MaterializedViews)
            pipelineBuilder.Append(MaterializedViewGenerator.Generate(materializedView));

        if (!string.IsNullOrWhiteSpace(pipeline.TrailingSql))
            pipelineBuilder.AppendLine(pipeline.TrailingSql.Trim());

        return pipelineBuilder.ToString();
    }

    private static void AppendHeaders(CodegenConfig config)
    {
        foreach (var (view, _, target) in EnumerateKafkaProjections(config))
        {
            AddProjectedColumn(target, view, "headers_name", "Array(LowCardinality(String))", "headers_name");
            AddProjectedColumn(target, view, "headers_value", "Array(String)", "headers_value");
        }
    }

    private static void AppendKeys(CodegenConfig config)
    {
        foreach (var (view, kafkaTable, target) in EnumerateKafkaProjections(config))
        {
            if (kafkaTable.Key is not { } key)
                continue;

            var fields = ProtobufKeyFieldMapper.MapFields(
                ProtoDescriptorResolver.ResolveDescriptor(key.MessageType));

            foreach (var field in fields)
            {
                var columnName = $"key_{field.Name}";
                AddProjectedColumn(target, view, columnName, field.ClickHouseType, columnName);
            }
        }
    }

    private static IEnumerable<(MaterializedViewConfig View, KafkaTableConfig KafkaTable, MergeTreeTableConfig Target)>
        EnumerateKafkaProjections(CodegenConfig config)
    {
        var pipeline = config.Pipeline!;

        foreach (var view in pipeline.MaterializedViews)
        {
            var kafkaTable = config.KafkaTables.FirstOrDefault(table =>
                string.Equals(table.TableName, view.SourceTable, StringComparison.OrdinalIgnoreCase));
            if (kafkaTable is null)
                continue;

            var target = pipeline.MergeTreeTables.FirstOrDefault(table =>
                string.Equals(table.TableName, view.TargetTable, StringComparison.OrdinalIgnoreCase));
            if (target is null)
                continue;

            yield return (view, kafkaTable, target);
        }
    }

    private static void AddProjectedColumn(
        MergeTreeTableConfig target,
        MaterializedViewConfig view,
        string name,
        string type,
        string source)
    {
        if (target.Columns.Any(column => string.Equals(column.Name, name, StringComparison.OrdinalIgnoreCase))
            || view.Columns.Any(column => string.Equals(column.Target, name, StringComparison.OrdinalIgnoreCase)))
            return;

        target.Columns.Add(new PipelineColumnConfig { Name = name, Type = type });
        view.Columns.Add(new PipelineColumnMapping
        {
            Source = source,
            Target = name
        });
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
