namespace ClickHouseSchemaGen.UnitTests;

public sealed class SchemaMigrationsTableTests
{
    [Fact]
    public void GivenInitScript_WhenAppendInitRecord_ThenRecordsVersionNameAndBodyChecksum()
    {
        const string body = "CREATE TABLE t (id String) ENGINE = Memory;\n";

        var sql = SchemaMigrationsTable.AppendInitRecord("../../docker/clickhouse/init/02_pipeline.sql", body);

        sql.Should().StartWith(body.TrimEnd());
        sql.TrimEnd().Should().EndWith(
            "INSERT INTO schema_migrations (version, name, checksum, applied_at, kind) VALUES " +
            $"('02', '02_pipeline.sql', '{SchemaSnapshotSerializer.ComputeChecksum(body)}', now(), 'init');");
    }

    [Fact]
    public void GivenPlan_WhenRenderInitScripts_ThenEveryScriptRecordsItself()
    {
        var config = CodegenConfigLoader.Load(RepoPaths.CodegenConfigPath);
        var plan = SchemaGeneratorFactory.Create().BuildPlan(config);

        var scripts = SchemaPlanRenderer.RenderInitScripts(plan);

        scripts.Should().NotBeEmpty();
        foreach (var (outputPath, sql) in scripts)
        {
            var fileName = Path.GetFileName(outputPath);
            sql.TrimEnd().Should().EndWith("'init');");
            sql.Should().Contain($"('{SchemaMigrationsTable.ParseInitVersion(fileName)}', '{fileName}', ");
        }
    }

    [Fact]
    public void GivenMigrations_WhenGenerateVersionsScript_ThenCreatesTableWithKindAndRecordsBaseline()
    {
        var sql = SchemaMigrationsScriptGenerator.Generate(
        [
            new MigrationFileInfo("20261007120000", "add_note", "abc", "unused")
        ]);

        sql.Should().Contain(SchemaMigrationsTable.CreateTableSql);
        sql.Should().Contain("kind                LowCardinality(String) DEFAULT 'migration'");
        sql.Should().Contain(
            "VALUES ('20261007120000', 'add_note', 'abc', toDateTime(0), 'migration');");
    }

    [Fact]
    public void GivenCommittedInitDirectory_ThenVersionsScriptRunsFirstAndInitScriptsRecordThemselves()
    {
        var initDirectory = Path.Combine(RepoPaths.RepositoryRoot, "docker", "clickhouse", "init");
        var scripts = Directory.GetFiles(initDirectory, "*.sql")
            .Select(Path.GetFileName)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToList();

        scripts[0].Should().Be(SchemaMigrationsTable.DefaultScriptFileName);
        foreach (var script in scripts.Skip(1))
        {
            File.ReadAllText(Path.Combine(initDirectory, script!)).Should()
                .Contain($"('{SchemaMigrationsTable.ParseInitVersion(script!)}', '{script}', ");
        }
    }
}
