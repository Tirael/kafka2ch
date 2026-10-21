namespace ClickHouseSchemaGen.UnitTests;

public sealed class KafkaErrorHandlingTests
{
    private const string ValidRowsFilter = "WHERE length(_error) = 0";
    private const string ErrorRowsFilter = "WHERE length(_error) > 0";

    [Fact]
    public void GivenDefaultErrorSettings_WhenGenerateKafkaTable_ThenOmitsErrorHandlingSettings()
    {
        var config = OrdersQueueTestConfig.Create();

        var sql = KafkaTableGenerator.Generate(config, MapColumns(config));

        sql.Should().NotContain("kafka_handle_error_mode");
        sql.Should().NotContain("kafka_skip_broken_messages");
    }

    [Theory]
    [InlineData("stream", "kafka_handle_error_mode = 'stream'")]
    [InlineData("Dead_Letter_Queue", "kafka_handle_error_mode = 'dead_letter_queue'")]
    public void GivenHandleErrorMode_WhenGenerateKafkaTable_ThenRendersNormalizedSetting(string mode, string expected)
    {
        var config = OrdersQueueTestConfig.Create();
        config.Kafka.HandleErrorMode = mode;

        var sql = KafkaTableGenerator.Generate(config, MapColumns(config));

        sql.Should().Contain(expected);
    }

    [Fact]
    public void GivenSkipBrokenMessages_WhenGenerateKafkaTable_ThenRendersSetting()
    {
        var config = OrdersQueueTestConfig.Create();
        config.Kafka.SkipBrokenMessages = 10;

        var sql = KafkaTableGenerator.Generate(config, MapColumns(config));

        sql.Should().Contain("kafka_skip_broken_messages = 10");
        sql.Should().NotContain("kafka_handle_error_mode");
    }

    [Fact]
    public void GivenStreamMode_WhenBuildPlan_ThenAddsErrorsTableAndErrorsView()
    {
        var plan = SchemaGeneratorFactory.Create().BuildPlan(LoadFixture(KafkaHandleErrorModes.Stream));

        var errorsTable = plan.MergeTreeTables.Should()
            .ContainSingle(table => table.Config.TableName == "orders_queue_errors").Subject;
        errorsTable.Origin.Should().Be(PlanOrigin.Auto);
        errorsTable.Config.SourceTable.Should().Be("orders_queue");
        errorsTable.Config.Columns.Select(column => (column.Name, column.Type)).Should().Equal(
            ("_topic", "LowCardinality(String)"),
            ("_partition", "UInt64"),
            ("_offset", "UInt64"),
            ("_raw_message", "String"),
            ("_error", "String"),
            ("received_at", "DateTime"));

        var errorsView = plan.MaterializedViews.Should()
            .ContainSingle(view => view.Config.Name == "orders_queue_errors_mv").Subject;
        errorsView.Origin.Should().Be(PlanOrigin.Auto);
        errorsView.Config.SourceTable.Should().Be("orders_queue");
        errorsView.Config.TargetTable.Should().Be("orders_queue_errors");
        errorsView.Config.Where.Should().Be("length(_error) > 0");
    }

    [Fact]
    public void GivenStreamMode_WhenRenderPipeline_ThenMainViewsSkipBrokenRowsAndErrorsViewKeepsThem()
    {
        var plan = SchemaGeneratorFactory.Create().BuildPlan(LoadFixture(KafkaHandleErrorModes.Stream));

        var statements = SqlStatementSplitter.Split(SchemaPlanRenderer.RenderPipelineSql(plan));

        statements.Should().Contain(statement =>
            statement.StartsWith("CREATE TABLE orders_queue_errors", StringComparison.Ordinal)
            && statement.Contains("ENGINE = MergeTree")
            && statement.Contains("ORDER BY (_topic, _partition, _offset)"));
        Statement(statements, "CREATE MATERIALIZED VIEW orders_mv TO orders AS")
            .Should().Contain("FROM orders_queue" + SqlText.NewLine + "WHERE length(_error) = 0");
        var errorsView = Statement(statements, "CREATE MATERIALIZED VIEW orders_queue_errors_mv TO orders_queue_errors AS");
        errorsView.Should().Contain("FROM orders_queue" + SqlText.NewLine + "WHERE length(_error) > 0");
        errorsView.Should().MatchRegex(@"_raw_message\s+AS _raw_message");
        errorsView.Should().MatchRegex(@"now\(\)\s+AS received_at");
        Statement(statements, "CREATE MATERIALIZED VIEW orders_agg_mv").Should().NotContain("_error");
    }

    [Fact]
    public void GivenStreamModeWithAutoFilledTable_WhenRenderPipeline_ThenAutoViewAlsoSkipsBrokenRows()
    {
        var config = LoadFixture(KafkaHandleErrorModes.Stream);
        config.Pipeline!.MergeTreeTables.Add(new MergeTreeTableConfig
        {
            TableName = "orders_raw",
            SourceTable = "orders_queue",
            OrderBy = "(order_id)"
        });

        var plan = SchemaGeneratorFactory.Create().BuildPlan(config);
        var statements = SqlStatementSplitter.Split(SchemaPlanRenderer.RenderPipelineSql(plan));

        plan.MaterializedViews.Should().Contain(view =>
            view.Origin == PlanOrigin.Auto && view.Config.Name == "orders_raw_mv" && view.Config.Where == "length(_error) = 0");
        Statement(statements, "CREATE MATERIALIZED VIEW orders_raw_mv TO orders_raw AS")
            .Should().EndWith(ValidRowsFilter);
    }

    [Theory]
    [InlineData(KafkaHandleErrorModes.Default)]
    [InlineData(KafkaHandleErrorModes.DeadLetterQueue)]
    public void GivenNonStreamMode_WhenBuildPlan_ThenNoErrorsObjectsAndNoFilters(string mode)
    {
        var plan = SchemaGeneratorFactory.Create().BuildPlan(LoadFixture(mode));

        plan.MergeTreeTables.Should().NotContain(table => table.Config.TableName.EndsWith("_errors", StringComparison.Ordinal));
        plan.MaterializedViews.Should().OnlyContain(view => view.Config.Where == null);
        SchemaPlanRenderer.RenderPipelineSql(plan).Should().NotContain("_error");
    }

    [Fact]
    public void GivenStreamModeOnCluster_WhenRenderPipeline_ThenErrorsTableIsLocalPlusDistributed()
    {
        var config = LoadFixture(KafkaHandleErrorModes.Stream, TestContractsPaths.ClusterCodegenConfigPath);

        var statements = SqlStatementSplitter.Split(
            SchemaPlanRenderer.RenderPipelineSql(SchemaGeneratorFactory.Create().BuildPlan(config)));

        Statement(statements, "CREATE TABLE orders_queue_errors_local ON CLUSTER kafka2ch")
            .Should().Contain("ENGINE = ReplicatedMergeTree(");
        Statement(statements, "CREATE TABLE orders_queue_errors ON CLUSTER kafka2ch AS orders_queue_errors_local")
            .Should().Contain("ENGINE = Distributed('kafka2ch', currentDatabase(), orders_queue_errors_local, rand())");
        Statement(statements, "CREATE MATERIALIZED VIEW orders_queue_errors_mv ON CLUSTER kafka2ch TO orders_queue_errors_local AS")
            .Should().EndWith(ErrorRowsFilter);
        Statement(statements, "CREATE MATERIALIZED VIEW orders_mv ON CLUSTER kafka2ch TO orders_local AS")
            .Should().EndWith(ValidRowsFilter);
    }

    [Fact]
    public void GivenStreamModeInReplicatedDatabase_WhenRenderPipeline_ThenErrorsViewWritesToLocalTable()
    {
        var config = LoadFixture(KafkaHandleErrorModes.Stream, TestContractsPaths.ClusterReplicatedDbCodegenConfigPath);

        var statements = SqlStatementSplitter.Split(
            SchemaPlanRenderer.RenderPipelineSql(SchemaGeneratorFactory.Create().BuildPlan(config)));

        Statement(statements, "CREATE TABLE orders_queue_errors_local")
            .Should().Contain("ENGINE = ReplicatedMergeTree").And.NotContain("ON CLUSTER");
        Statement(statements, "CREATE MATERIALIZED VIEW orders_queue_errors_mv TO orders_queue_errors_local AS")
            .Should().EndWith(ErrorRowsFilter);
    }

    [Theory]
    [InlineData("skip", null, "handleErrorMode")]
    [InlineData(KafkaHandleErrorModes.Default, -1, "SkipBrokenMessages")]
    [InlineData(KafkaHandleErrorModes.Stream, 5, "skipBrokenMessages")]
    [InlineData(KafkaHandleErrorModes.DeadLetterQueue, 5, "skipBrokenMessages")]
    public void GivenInvalidErrorSettings_WhenValidate_ThenFails(string mode, int? skipBroken, string expected)
    {
        var config = LoadFixture(mode);
        config.KafkaTables[0].Kafka.SkipBrokenMessages = skipBroken;

        var result = new CodegenConfigValidator().Validate(config);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(error =>
            error.ErrorMessage.Contains(expected, StringComparison.OrdinalIgnoreCase)
            || error.PropertyName.Contains(expected, StringComparison.OrdinalIgnoreCase));
    }

    [Theory]
    [InlineData(KafkaHandleErrorModes.Default, 0)]
    [InlineData(KafkaHandleErrorModes.Default, 100)]
    [InlineData("STREAM", null)]
    [InlineData(KafkaHandleErrorModes.DeadLetterQueue, null)]
    public void GivenValidErrorSettings_WhenValidate_ThenSucceeds(string mode, int? skipBroken)
    {
        var config = LoadFixture(mode);
        config.KafkaTables[0].Kafka.SkipBrokenMessages = skipBroken;

        new CodegenConfigValidator().Validate(config).IsValid.Should().BeTrue();
    }

    [Fact]
    public void GivenStreamModeWithoutPipeline_WhenValidate_ThenFails()
    {
        var config = LoadFixture(KafkaHandleErrorModes.Stream);
        config.Pipeline = null;

        var result = new CodegenConfigValidator().Validate(config);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(error => error.ErrorMessage.Contains("orders_queue_errors", StringComparison.Ordinal));
    }

    [Fact]
    public void GivenMergeTreeTableNamedLikeErrorsTable_WhenValidate_ThenFailsWithNameConflict()
    {
        var config = LoadFixture(KafkaHandleErrorModes.Stream);
        config.Pipeline!.MergeTreeTables.Add(new MergeTreeTableConfig
        {
            TableName = "orders_queue_errors",
            OrderBy = "(order_id)",
            Columns = [new PipelineColumnConfig { Name = "order_id", Type = "String" }]
        });

        var result = new CodegenConfigValidator().Validate(config);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(error =>
            error.ErrorMessage.Contains("'orders_queue_errors'", StringComparison.Ordinal)
            && error.ErrorMessage.Contains("handleErrorMode", StringComparison.Ordinal));
    }

    [Fact]
    public void GivenViewNamedLikeErrorsView_WhenValidate_ThenFailsWithNameConflict()
    {
        var config = LoadFixture(KafkaHandleErrorModes.Stream);
        config.Pipeline!.MaterializedViews[0].Name = "orders_queue_errors_mv";

        var result = new CodegenConfigValidator().Validate(config);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(error =>
            error.ErrorMessage.Contains("'orders_queue_errors_mv'", StringComparison.Ordinal));
    }

    [Fact]
    public void GivenMergeTreeTableNamedLikeErrorsLocalTable_WhenValidateCluster_ThenFails()
    {
        var config = LoadFixture(KafkaHandleErrorModes.Stream, TestContractsPaths.ClusterCodegenConfigPath);
        config.Pipeline!.MergeTreeTables.Add(new MergeTreeTableConfig
        {
            TableName = "orders_queue_errors_local",
            OrderBy = "(order_id)",
            Columns = [new PipelineColumnConfig { Name = "order_id", Type = "String" }]
        });

        var result = new CodegenConfigValidator().Validate(config);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(error =>
            error.ErrorMessage.Contains("'orders_queue_errors_local'", StringComparison.Ordinal));
    }

    [Fact]
    public void GivenStreamModeEnabled_WhenDiff_ThenRecreatesQueueAddsErrorsTableAndRecreatesViews()
    {
        var generator = SchemaGeneratorFactory.Create();
        var oldPlan = generator.BuildPlan(LoadFixture(KafkaHandleErrorModes.Default));
        var newPlan = generator.BuildPlan(LoadFixture(KafkaHandleErrorModes.Stream));

        var diff = SchemaDiffer.Diff(oldPlan, newPlan);

        diff.Changes.OfType<KafkaTableChanged>().Should().ContainSingle()
            .Which.Reason.Should().Contain("handleErrorMode changed");
        diff.Changes.Should().Contain(new TableAdded("orders_queue_errors", "MergeTree"));
        diff.Changes.OfType<ViewChanged>().Select(change => change.ViewName)
            .Should().BeEquivalentTo("orders_mv", "orders_queue_errors_mv");

        var plan = MigrationPolicy.Apply(diff, oldPlan, newPlan);
        plan.RequiresManualApproval.Should().BeFalse();

        var sql = GenerateMigration(plan, oldPlan, newPlan);
        AssertAscending(sql,
            "DETACH TABLE IF EXISTS orders_queue;",
            "CREATE TABLE IF NOT EXISTS orders_queue_errors",
            "DROP VIEW IF EXISTS orders_mv;",
            "DROP VIEW IF EXISTS orders_queue_errors_mv;",
            "DROP TABLE IF EXISTS orders_queue;",
            "kafka_handle_error_mode = 'stream'",
            "CREATE MATERIALIZED VIEW orders_mv TO orders AS",
            ValidRowsFilter,
            "CREATE MATERIALIZED VIEW orders_queue_errors_mv TO orders_queue_errors AS",
            ErrorRowsFilter);
    }

    [Fact]
    public void GivenSkipBrokenMessagesChanged_WhenDiff_ThenEmitsKafkaTableChanged()
    {
        var generator = SchemaGeneratorFactory.Create();
        var oldPlan = generator.BuildPlan(LoadFixture(KafkaHandleErrorModes.Default));
        var newConfig = LoadFixture(KafkaHandleErrorModes.Default);
        newConfig.KafkaTables[0].Kafka.SkipBrokenMessages = 10;

        var diff = SchemaDiffer.Diff(oldPlan, generator.BuildPlan(newConfig));

        diff.Changes.Should().ContainSingle()
            .Which.Should().Be(new KafkaTableChanged("orders_queue", "skipBrokenMessages changed"));
    }

    [Fact]
    public void GivenModeCaseOnlyChanged_WhenDiff_ThenNoChanges()
    {
        var generator = SchemaGeneratorFactory.Create();
        var oldPlan = generator.BuildPlan(LoadFixture(KafkaHandleErrorModes.Stream));
        var newPlan = generator.BuildPlan(LoadFixture("Stream"));

        SchemaDiffer.Diff(oldPlan, newPlan).Changes.Should().BeEmpty();
    }

    [Fact]
    public void GivenDefaultErrorSettings_WhenSerializeSnapshot_ThenOmitsNewFields()
    {
        var plan = SchemaGeneratorFactory.Create().BuildPlan(LoadFixture(KafkaHandleErrorModes.Default));

        var json = SchemaSnapshotSerializer.SerializeToString(SchemaSnapshotMapper.FromPlan(plan));

        json.Should().NotContain("handleErrorMode");
        json.Should().NotContain("skipBrokenMessages");
        json.Should().NotContain("\"where\"");
    }

    [Fact]
    public void GivenStreamSnapshot_WhenRoundTripToPlanAndDiff_ThenNoChangesAndNoDrift()
    {
        var config = LoadFixture(KafkaHandleErrorModes.Stream);
        config.KafkaTables[0].Kafka.HandleErrorMode = "Stream";
        var plan = SchemaGeneratorFactory.Create().BuildPlan(config);
        var path = Path.Combine(Path.GetTempPath(), $"snapshot-{Guid.NewGuid():N}.json");

        try
        {
            SchemaSnapshotSerializer.Save(path, SchemaSnapshotMapper.FromPlan(plan));
            var json = File.ReadAllText(path);
            json.Should().Contain("\"handleErrorMode\": \"stream\"");
            json.Should().Contain("\"where\": \"length(_error) = 0\"");

            var snapshot = SchemaSnapshotSerializer.Load(path);
            SchemaDiffer.Diff(SchemaSnapshotMapper.ToPlan(snapshot, config), plan).Changes.Should().BeEmpty();
            var act = () => SnapshotDriftChecker.Check(plan, snapshot, TestContractsPaths.CodegenConfigPath);
            act.Should().NotThrow();
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void GivenSnapshotWithoutStreamMode_WhenCheckStreamPlan_ThenReportsDrift()
    {
        var generator = SchemaGeneratorFactory.Create();
        var snapshot = SchemaSnapshotMapper.FromPlan(generator.BuildPlan(LoadFixture(KafkaHandleErrorModes.Default)));
        var plan = generator.BuildPlan(LoadFixture(KafkaHandleErrorModes.Stream));

        var act = () => SnapshotDriftChecker.Check(plan, snapshot, TestContractsPaths.CodegenConfigPath);

        var differences = act.Should().Throw<SchemaDriftException>().Which.Differences;
        differences.Should().Contain(difference => difference.Contains("handleErrorMode changed", StringComparison.Ordinal));
        differences.Should().Contain(difference => difference.Contains("'orders_mv' WHERE changed", StringComparison.Ordinal));
        differences.Should().Contain("MergeTree table 'orders_queue_errors' is missing from snapshot.");
    }

    private static CodegenConfig LoadFixture(string mode, string? path = null)
    {
        var config = CodegenConfigLoader.Load(path ?? TestContractsPaths.CodegenConfigPath);
        config.KafkaTables[0].Kafka.HandleErrorMode = mode;
        return config;
    }

    private static IReadOnlyList<ClickHouseColumn> MapColumns(KafkaTableConfig config) =>
        MappingTestSupport.MapFixture(ProtoToClickHouseMapper.ResolveDescriptor(config.MessageType), config.FieldOverrides);

    private static string Statement(IEnumerable<string> statements, string prefix) =>
        statements.Should().ContainSingle(statement => statement.StartsWith(prefix, StringComparison.Ordinal)).Subject;

    private static string GenerateMigration(MigrationPlan plan, ResolvedSchemaPlan oldPlan, ResolvedSchemaPlan newPlan) =>
        MigrationSqlGenerator.Generate(
            plan,
            oldPlan,
            newPlan,
            "enable_stream",
            parentChecksum: null,
            targetChecksum: "target",
            new DateTimeOffset(2099, 1, 3, 0, 0, 0, TimeSpan.Zero));

    private static void AssertAscending(string sql, params string[] fragments)
    {
        var indexes = new List<int>();
        var from = 0;
        foreach (var fragment in fragments)
        {
            var index = sql.IndexOf(fragment, from, StringComparison.Ordinal);
            index.Should().BeGreaterThanOrEqualTo(0, $"'{fragment}' must follow previous fragments in:{Environment.NewLine}{sql}");
            indexes.Add(index);
            from = index + fragment.Length;
        }
    }
}
