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

    private static (ResolvedSchemaPlan Old, ResolvedSchemaPlan New) CreateRewritePlans(bool autoRewrite)
    {
        var config = new CodegenConfig { KafkaTables = [], Migrations = new MigrationsConfig { AutoRewrite = autoRewrite } };
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
