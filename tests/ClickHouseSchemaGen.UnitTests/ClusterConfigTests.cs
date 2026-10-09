namespace ClickHouseSchemaGen.UnitTests;

public sealed class ClusterConfigTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"kafka2ch-cluster-config-{Guid.NewGuid():N}");

    public ClusterConfigTests() => Directory.CreateDirectory(_directory);

    public void Dispose() => Directory.Delete(_directory, recursive: true);

    [Fact]
    public void GivenRepoBaseConfig_WhenLoad_ThenClusterIsDisabled()
    {
        var config = CodegenConfigLoader.Load(RepoPaths.CodegenConfigPath);

        config.Cluster.Enabled.Should().BeFalse();
        config.OutputDirectory.Should().BeNull();
    }

    [Fact]
    public void GivenConfigExtendingBase_WhenLoad_ThenMergesObjectsAndKeepsBaseArrays()
    {
        File.WriteAllText(Path.Combine(_directory, "base.json"), """
            {
              "defaults": { "maxFlattenDepth": 5, "optionalAsNullable": false },
              "kafkaTables": [ { "messageType": "M", "tableName": "q", "protoFile": "p", "messageName": "M", "outputPath": "init/01_q.sql" } ],
              "migrations": { "migrationsDirectory": "base-migrations" }
            }
            """);
        var overlayPath = Path.Combine(_directory, "cluster.json");
        File.WriteAllText(overlayPath, """
            {
              // comments are allowed
              "extends": "base.json",
              "outputDirectory": "cluster-init",
              "cluster": { "name": "kafka2ch" },
              "defaults": { "maxFlattenDepth": 2 },
            }
            """);

        var config = CodegenConfigLoader.Load(overlayPath);

        config.Cluster.Name.Should().Be("kafka2ch");
        config.Cluster.ReplicatedPath.Should().Be(ClusterConfig.DefaultReplicatedPath);
        config.Defaults.MaxFlattenDepth.Should().Be(2);
        config.Defaults.OptionalAsNullable.Should().BeFalse();
        config.KafkaTables.Should().ContainSingle(table => table.TableName == "q");
        config.Migrations.MigrationsDirectory.Should().Be("base-migrations");
        CodegenConfigLoader.ResolveInitScriptPath(config, "init/01_q.sql")
            .Should().Be(Path.Combine("cluster-init", "01_q.sql"));
    }

    [Fact]
    public void GivenExtendsCycle_WhenLoad_ThenThrows()
    {
        var path = Path.Combine(_directory, "self.json");
        File.WriteAllText(path, """{ "extends": "self.json" }""");

        var act = () => CodegenConfigLoader.Load(path);

        act.Should().Throw<InvalidOperationException>().WithMessage("*cycle*");
    }

    [Fact]
    public void GivenRepoClusterConfig_WhenLoad_ThenExtendsBaseAndWritesToClusterDirectories()
    {
        var config = CodegenConfigLoader.Load(RepoPaths.ClusterCodegenConfigPath);

        config.Cluster.Name.Should().Be("kafka2ch");
        config.KafkaTables.Select(table => table.TableName).Should().Equal("orders_queue");
        CodegenConfigLoader.ResolvePath(RepoPaths.ClusterCodegenConfigPath, config.Migrations.MigrationsDirectory)
            .Should().Be(Path.Combine(RepoPaths.RepositoryRoot, "docker", "clickhouse-cluster", "migrations"));
        new CodegenConfigValidator().Validate(config).IsValid.Should().BeTrue();
    }

    [Theory]
    [InlineData("bad-name", null, null, null, "Cluster name")]
    [InlineData("c", "/clickhouse/tables/{shard}", null, null, "replicatedPath")]
    [InlineData("c", null, "/clickhouse/{shard}/schema_migrations", null, "historyReplicatedPath")]
    [InlineData("c", null, null, "nope", "ddlMode")]
    public void GivenInvalidCluster_WhenValidate_ThenFails(
        string name,
        string? replicatedPath,
        string? historyPath,
        string? ddlMode,
        string expectedMessage)
    {
        var config = CodegenConfigLoader.Load(RepoPaths.CodegenConfigPath);
        config.Cluster = new ClusterConfig
        {
            Name = name,
            ReplicatedPath = replicatedPath ?? ClusterConfig.DefaultReplicatedPath,
            HistoryReplicatedPath = historyPath ?? ClusterConfig.DefaultHistoryReplicatedPath,
            DdlMode = ddlMode ?? ClusterDdlModes.OnCluster
        };

        var result = new CodegenConfigValidator().Validate(config);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(error => error.ErrorMessage.Contains(expectedMessage, StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void GivenRepoReplicatedDbConfig_WhenValidate_ThenSucceeds()
    {
        var config = CodegenConfigLoader.Load(RepoPaths.ClusterReplicatedDbCodegenConfigPath);

        new CodegenConfigValidator().Validate(config).IsValid.Should().BeTrue();
        config.Cluster.UsesReplicatedDatabase.Should().BeTrue();
        config.Cluster.MaterializedViewsWriteThroughDistributed.Should().BeTrue();
    }

    [Fact]
    public void GivenLocalNameCollidingWithExistingTable_WhenValidate_ThenFails()
    {
        var config = CodegenConfigLoader.Load(RepoPaths.CodegenConfigPath);
        config.Cluster = new ClusterConfig { Name = "c", LocalTableSuffix = "_queue" };

        var result = new CodegenConfigValidator().Validate(config);

        result.Errors.Should().Contain(error => error.ErrorMessage.Contains("'orders_queue'"));
    }

    [Fact]
    public void GivenCluster_WhenCreateHistoryTable_ThenReplicatesAcrossAllNodesOnCluster()
    {
        var cluster = new ClusterDdl(new ClusterConfig { Name = "kafka2ch" });

        var sql = SchemaMigrationsTable.CreateTableSqlFor(cluster);

        sql.Should().StartWith("CREATE TABLE IF NOT EXISTS schema_migrations ON CLUSTER kafka2ch");
        sql.Should().Contain("ENGINE = ReplicatedMergeTree('/clickhouse/tables/all/{database}/{table}', '{shard}-{replica}')");
        SchemaMigrationsTable.UpgradeStatementsFor(cluster).Should().ContainSingle()
            .Which.Should().StartWith("ALTER TABLE schema_migrations ON CLUSTER kafka2ch ADD COLUMN IF NOT EXISTS kind");
        SchemaMigrationsTable.CreateTableSqlFor(ClusterDdl.SingleNode).Should().Be(SchemaMigrationsTable.CreateTableSql);
    }

    [Fact]
    public void GivenCluster_WhenGenerateVersionsScript_ThenUsesClusterHistoryTable()
    {
        var cluster = new ClusterDdl(new ClusterConfig { Name = "kafka2ch" });

        var sql = SchemaMigrationsScriptGenerator.Generate([new MigrationFileInfo("1", "a", "c", "p")], cluster);

        sql.Should().Contain(SchemaMigrationsTable.CreateTableSqlFor(cluster));
        sql.Should().Contain("VALUES ('1', 'a', 'c', toDateTime(0), 'migration');");
    }
}
