namespace ClickHouseSchemaGen.UnitTests;

public sealed class MigrationMutationControlTests
{
    [Fact]
    public void GivenRewriteTypeChange_WhenApplyPolicyWithDefaults_ThenChangeIsManual()
    {
        var (oldPlan, newPlan) = CreateRewritePlans(autoRewrite: false);

        var migrationPlan = MigrationPolicy.Apply(SchemaDiffer.Diff(oldPlan, newPlan), oldPlan, newPlan);

        migrationPlan.ManualChanges.OfType<ColumnTypeChanged>().Should().ContainSingle(change =>
            change.ColumnName == "event_time" && change.Kind == TypeChangeKind.Rewrite);
        migrationPlan.AutomaticChanges.OfType<ColumnTypeChanged>().Should().BeEmpty();
        migrationPlan.RequiresManualApproval.Should().BeTrue();
    }

    [Fact]
    public void GivenRewriteTypeChangeAndAutoRewriteOptIn_WhenApplyPolicy_ThenChangeIsAutomatic()
    {
        var (oldPlan, newPlan) = CreateRewritePlans(autoRewrite: true);

        var migrationPlan = MigrationPolicy.Apply(SchemaDiffer.Diff(oldPlan, newPlan), oldPlan, newPlan);

        migrationPlan.AutomaticChanges.OfType<ColumnTypeChanged>().Should().ContainSingle(change =>
            change.ColumnName == "event_time" && change.Kind == TypeChangeKind.Rewrite);
        migrationPlan.ManualChanges.Should().BeEmpty();
        migrationPlan.RequiresManualApproval.Should().BeFalse();
    }

    [Fact]
    public void GivenAutoRewriteInConfigJson_WhenLoad_ThenOptInIsRead()
    {
        var path = Path.Combine(Path.GetTempPath(), $"kafka2ch-autorewrite-{Guid.NewGuid():N}.json");
        File.WriteAllText(path, """{ "kafkaTables": [], "migrations": { "autoRewrite": true } }""");
        try
        {
            CodegenConfigLoader.Load(path).Migrations.AutoRewrite.Should().BeTrue();
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void GivenDefaultConfig_WhenLoad_ThenAutoRewriteIsDisabled()
        => new MigrationsConfig().AutoRewrite.Should().BeFalse();

    [Fact]
    public void GivenAutomaticModifyColumnOnSingleNode_WhenGenerateSql_ThenWaitsForMutationsOnTable()
    {
        var (oldPlan, newPlan) = CreateRewritePlans(autoRewrite: true);

        var sql = GenerateSql(oldPlan, newPlan);

        sql.Should().Contain(
            "ALTER TABLE orders MODIFY COLUMN event_time DateTime64(3) SETTINGS mutations_sync = 2;\n"
                + "-- await:mutations_done orders\n");
    }

    [Fact]
    public void GivenAutomaticModifyColumnOnCluster_WhenGenerateSql_ThenWaitsForMutationsOnStorageTableAcrossCluster()
    {
        var (oldPlan, newPlan) = CreateRewritePlans(autoRewrite: true, cluster: new ClusterConfig { Name = "kafka2ch" });

        var sql = GenerateSql(oldPlan, newPlan);

        sql.Should().Contain(
            "ALTER TABLE orders_local ON CLUSTER kafka2ch MODIFY COLUMN event_time DateTime64(3) SETTINGS mutations_sync = 2;\n"
                + "-- await:mutations_done orders_local ON CLUSTER kafka2ch\n"
                + "ALTER TABLE orders ON CLUSTER kafka2ch MODIFY COLUMN event_time DateTime64(3);\n");
    }

    [Fact]
    public void GivenSafeModifyColumn_WhenGenerateSql_ThenAlsoWaitsForMutations()
    {
        var config = new CodegenConfig { KafkaTables = [] };
        var oldPlan = CreatePlan(config, "Int32");
        var newPlan = CreatePlan(config, "Int64");

        var sql = GenerateSql(oldPlan, newPlan);

        sql.Should().Contain(
            "ALTER TABLE orders MODIFY COLUMN event_time Int64 SETTINGS mutations_sync = 2;\n"
                + "-- await:mutations_done orders\n");
    }

    [Theory]
    [InlineData("-- await:mutations_done orders", "orders", null)]
    [InlineData("-- await:mutations_done orders_local ON CLUSTER kafka2ch", "orders_local", "kafka2ch")]
    public void GivenAwaitMutationsMarker_WhenParse_ThenReturnsTableAndCluster(string marker, string table, string? cluster)
    {
        MigrationRunner.TryParseAwaitMutationsMarker(marker, out var parsedTable, out var parsedCluster).Should().BeTrue();
        parsedTable.Should().Be(table);
        parsedCluster.Should().Be(cluster);
    }

    [Fact]
    public void GivenOtherComment_WhenParseAwaitMutationsMarker_ThenReturnsFalse()
        => MigrationRunner.TryParseAwaitMutationsMarker("-- await:kafka_consumers_empty orders_queue", out _, out _)
            .Should().BeFalse();

    private static string GenerateSql(ResolvedSchemaPlan oldPlan, ResolvedSchemaPlan newPlan) =>
        MigrationSqlGenerator.Generate(
            MigrationPolicy.Apply(SchemaDiffer.Diff(oldPlan, newPlan), oldPlan, newPlan),
            oldPlan,
            newPlan,
            "modify",
            parentChecksum: null,
            targetChecksum: "checksum",
            DateTimeOffset.UnixEpoch);

    private static (ResolvedSchemaPlan Old, ResolvedSchemaPlan New) CreateRewritePlans(
        bool autoRewrite,
        ClusterConfig? cluster = null)
    {
        var config = new CodegenConfig
        {
            KafkaTables = [],
            Cluster = cluster ?? new ClusterConfig(),
            Migrations = new MigrationsConfig { AutoRewrite = autoRewrite }
        };
        return (CreatePlan(config, "DateTime"), CreatePlan(config, "DateTime64(3)"));
    }

    private static ResolvedSchemaPlan CreatePlan(CodegenConfig config, string eventTimeType) =>
        new()
        {
            Config = config,
            KafkaTables = [],
            MergeTreeTables =
            [
                new MergeTreeTablePlan
                {
                    Origin = PlanOrigin.Explicit,
                    Config = new MergeTreeTableConfig
                    {
                        TableName = "orders",
                        OrderBy = "(order_id)",
                        Columns =
                        [
                            new PipelineColumnConfig { Name = "order_id", Type = "String", FieldNumberPath = "1" },
                            new PipelineColumnConfig { Name = "event_time", Type = eventTimeType, FieldNumberPath = "2" }
                        ]
                    }
                }
            ],
            MaterializedViews = []
        };
}
