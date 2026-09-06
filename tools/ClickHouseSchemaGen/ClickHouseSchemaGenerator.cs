using System.Text.Json;
using ClickHouseSchemaGen.Models;

namespace ClickHouseSchemaGen;

public sealed class ClickHouseSchemaGenerator(
    ProtoToClickHouseMapper mapper,
    KafkaTableGenerator kafkaTableGenerator)
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
        AllowTrailingCommas = true
    };

    public string GenerateKafkaTableSql(KafkaTableConfig config)
    {
        var descriptor = ProtoToClickHouseMapper.ResolveDescriptor(config.MessageType);
        var columns = mapper.MapMessage(descriptor, config.FieldOverrides);
        return kafkaTableGenerator.Generate(config, columns);
    }

    public void GenerateFromConfigFile(string configPath)
    {
        var configDirectory = Path.GetDirectoryName(Path.GetFullPath(configPath))
            ?? throw new InvalidOperationException($"Could not resolve directory for config '{configPath}'.");

        var json = File.ReadAllText(configPath);
        var config = JsonSerializer.Deserialize<CodegenConfig>(json, JsonOptions)
            ?? throw new InvalidOperationException($"Config file '{configPath}' is empty or invalid.");

        foreach (var table in config.KafkaTables)
        {
            var sql = GenerateKafkaTableSql(table);
            var outputPath = Path.GetFullPath(Path.Combine(configDirectory, table.OutputPath));
            Directory.CreateDirectory(Path.GetDirectoryName(outputPath)!);
            File.WriteAllText(outputPath, sql);
        }
    }
}
