namespace ClickHouseSchemaGen.Shared;

public static class KafkaRecreateMigrationFixture
{
    public const string Version = "20990102000000";

    public const string FileName = $"{Version}_bump_num_consumers.sql";

    public const int NewNumConsumers = 2;

    public const string FormatSchemaFile = "order_event.proto";

    public static string GenerateSql(
        string configPath,
        IReadOnlyDictionary<string, string>? formatSchemaChecksums = null)
    {
        var generator = SchemaGeneratorFactory.Create();
        var oldPlan = generator.BuildPlan(CodegenConfigLoader.Load(configPath));

        var config = CodegenConfigLoader.Load(configPath);
        config.KafkaTables.Single(table => table.TableName == "orders_queue").Kafka.NumConsumers = NewNumConsumers;
        var newPlan = generator.BuildPlan(config);

        var plan = MigrationPolicy.Apply(SchemaDiffer.Diff(oldPlan, newPlan), oldPlan, newPlan);
        return MigrationSqlGenerator.Generate(
            plan,
            oldPlan,
            newPlan,
            Path.GetFileNameWithoutExtension(FileName),
            parentChecksum: null,
            targetChecksum: "target",
            new DateTimeOffset(2099, 1, 2, 0, 0, 0, TimeSpan.Zero),
            formatSchemaChecksums);
    }

    public static IReadOnlyDictionary<string, string> FormatSchemaChecksums(string formatSchemasDirectory) =>
        FormatSchemaRequirement.ComputeChecksums(
            CodegenConfigLoader.Load(TestContractsPaths.CodegenConfigPath).KafkaTables,
            formatSchemasDirectory);
}
