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
        var decodeKeys = AppendKeys(config);

        var pipelineBuilder = new StringBuilder()
            .AppendLine(SqlScriptWriter.GeneratedHeader)
            .AppendLine();

        if (decodeKeys)
        {
            pipelineBuilder
                .AppendLine("-- Kafka ProtobufSingle parses the message value. The key is the same Confluent")
                .AppendLine("-- protobuf envelope plus one singular string field, decoded by field number.")
                .AppendLine(ProtobufKeyDecoder.CreateFunctionStatement())
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
            AddProjectedColumn(
                target,
                view,
                "headers_name",
                "Array(LowCardinality(String))",
                "_headers.name",
                "CAST(_headers.name, 'Array(LowCardinality(String))')");
            AddProjectedColumn(
                target,
                view,
                "headers_value",
                "Array(String)",
                "_headers.value",
                "_headers.value");
        }
    }

    private static bool AppendKeys(CodegenConfig config)
    {
        var decodeKeys = false;

        foreach (var (view, kafkaTable, target) in EnumerateKafkaProjections(config))
        {
            if (kafkaTable.Key is not { } key)
                continue;

            var field = ResolveSingularStringField(key);
            decodeKeys = true;
            AddProjectedColumn(
                target,
                view,
                $"key_{field.Name}",
                "String",
                field.Name,
                ProtobufKeyDecoder.StringFieldExpression(key.SkipBytes, field.FieldNumber));
        }

        return decodeKeys;
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

    private static FieldDescriptor ResolveSingularStringField(KeyMessageConfig key)
    {
        var fields = ProtoDescriptorResolver.ResolveDescriptor(key.MessageType).Fields.InFieldNumberOrder().ToList();
        if (fields.Count != 1
            || fields[0].IsRepeated
            || fields[0].IsMap
            || fields[0].FieldType != FieldType.String)
        {
            throw new InvalidOperationException(
                $"Kafka key message '{key.MessageName}' must contain exactly one singular string field.");
        }

        var field = fields[0];
        if (!ValidationRules.IsSqlIdentifier(field.Name))
            throw new InvalidOperationException($"Kafka key field '{field.Name}' is not a valid column name.");

        return field;
    }

    private static void AddProjectedColumn(
        MergeTreeTableConfig target,
        MaterializedViewConfig view,
        string name,
        string type,
        string source,
        string expression)
    {
        if (target.Columns.Any(column => string.Equals(column.Name, name, StringComparison.OrdinalIgnoreCase))
            || view.Columns.Any(column => string.Equals(column.Target, name, StringComparison.OrdinalIgnoreCase)))
            return;

        target.Columns.Add(new PipelineColumnConfig { Name = name, Type = type });
        view.Columns.Add(new PipelineColumnMapping
        {
            Source = source,
            Target = name,
            Expression = expression
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
