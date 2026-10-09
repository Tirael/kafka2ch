using ClickHouseSchemaGen.Migration;

namespace ClickHouseSchemaGen.Shared;


public static class PersistPartitionMigrationFixture
{
    public const string Version = "20990101000000";

    public const string FileName = $"{Version}_persist_partition.sql";

    public const string AddedColumn = "kafka_partition";

    public static string GenerateSql(string configPath)
    {
        var generator = SchemaGeneratorFactory.Create();
        var oldPlan = generator.BuildPlan(CodegenConfigLoader.Load(configPath));

        var config = CodegenConfigLoader.Load(configPath);
        config.Defaults.PersistKafkaMeta.Partition = true;
        var newPlan = generator.BuildPlan(config);

        var plan = MigrationPolicy.Apply(SchemaDiffer.Diff(oldPlan, newPlan), oldPlan, newPlan);
        return MigrationSqlGenerator.Generate(
            plan,
            oldPlan,
            newPlan,
            Path.GetFileNameWithoutExtension(FileName),
            parentChecksum: null,
            targetChecksum: "target",
            new DateTimeOffset(2099, 1, 1, 0, 0, 0, TimeSpan.Zero));
    }
}
