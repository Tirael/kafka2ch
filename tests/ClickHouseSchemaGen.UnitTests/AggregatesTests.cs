namespace ClickHouseSchemaGen.UnitTests;

public sealed class AggregatesTests
{
    private static readonly string LegacyTrailingSql = """
        CREATE TABLE orders_agg_1m
        (
            minute        DateTime,
            category      LowCardinality(String),
            orders_count  UInt64,
            total_amount  Float64,
            total_qty     UInt64
        )
        ENGINE = MergeTree
        ORDER BY (minute, category);

        CREATE MATERIALIZED VIEW orders_agg_mv
        REFRESH EVERY 1 MINUTE
        TO orders_agg_1m AS
        SELECT
            toStartOfMinute(event_time) AS minute,
            category,
            count()                     AS orders_count,
            sum(amount)                 AS total_amount,
            sum(quantity)               AS total_qty
        FROM orders FINAL
        GROUP BY minute, category;

        """.ReplaceLineEndings("\n");

    public static TheoryData<string> DedupConfigs => new()
    {
        TestContractsPaths.DedupCodegenConfigPath,
        TestContractsPaths.ClusterDedupCodegenConfigPath,
        TestContractsPaths.ClusterReplicatedDbDedupCodegenConfigPath
    };

    [Theory]
    [MemberData(nameof(DedupConfigs))]
    public void GivenDedupAggregate_WhenRenderInEveryTopology_ThenPipelineIsByteIdenticalToLegacyTrailingSql(string configPath)
    {
        var generator = SchemaGeneratorFactory.Create();

        var rendered = SchemaPlanRenderer.RenderInitScripts(generator.BuildPlan(CodegenConfigLoader.Load(configPath)));
        var legacy = SchemaPlanRenderer.RenderInitScripts(generator.BuildPlan(LegacyConfig(configPath)));

        rendered.Should().BeEquivalentTo(legacy);
        rendered.Values.Should().Contain(sql =>
            sql.Contains("CREATE MATERIALIZED VIEW orders_agg_mv", StringComparison.Ordinal)
            || sql.Contains("CREATE MATERIALIZED VIEW default.orders_agg_mv", StringComparison.Ordinal));
    }

    [Fact]
    public void GivenSingleNodeAggregate_WhenRender_ThenTargetTableAndRefreshableViewOverSourceFinal()
    {
        var sql = RenderAggregates(TestContractsPaths.DedupCodegenConfigPath);

        sql.TrimEnd().Should().Be(LegacyTrailingSql.TrimEnd());
    }

    [Fact]
    public void GivenOnClusterAggregate_WhenRender_ThenUnreplicatedLocalTargetReadsLocalSourceFinal()
    {
        var sql = RenderAggregates(TestContractsPaths.ClusterDedupCodegenConfigPath, aggregate => aggregate.Engine = "SummingMergeTree");

        sql.TrimEnd().Should().Be("""
            CREATE TABLE orders_agg_1m_local ON CLUSTER kafka2ch
            (
                minute        DateTime,
                category      LowCardinality(String),
                orders_count  UInt64,
                total_amount  Float64,
                total_qty     UInt64
            )
            ENGINE = SummingMergeTree
            ORDER BY (minute, category);

            CREATE TABLE orders_agg_1m ON CLUSTER kafka2ch AS orders_agg_1m_local
            ENGINE = Distributed('kafka2ch', currentDatabase(), orders_agg_1m_local, rand());

            CREATE MATERIALIZED VIEW orders_agg_mv ON CLUSTER kafka2ch
            REFRESH EVERY 1 MINUTE
            TO orders_agg_1m_local AS
            SELECT
                toStartOfMinute(event_time) AS minute,
                category,
                count()                     AS orders_count,
                sum(amount)                 AS total_amount,
                sum(quantity)               AS total_qty
            FROM orders_local FINAL
            GROUP BY minute, category;
            """.ReplaceLineEndings("\n"));
    }

    [Fact]
    public void GivenReplicatedDatabaseAggregate_WhenRender_ThenDefaultDatabaseTargetOnClusterReadsDistributedSourceFinal()
    {
        var sql = RenderAggregates(
            TestContractsPaths.ClusterReplicatedDbDedupCodegenConfigPath,
            aggregate => aggregate.Engine = "SummingMergeTree");

        sql.TrimEnd().Should().Be("""
            CREATE TABLE default.orders_agg_1m ON CLUSTER kafka2ch
            (
                minute        DateTime,
                category      LowCardinality(String),
                orders_count  UInt64,
                total_amount  Float64,
                total_qty     UInt64
            )
            ENGINE = SummingMergeTree
            ORDER BY (minute, category);

            CREATE MATERIALIZED VIEW default.orders_agg_mv ON CLUSTER kafka2ch
            REFRESH EVERY 1 MINUTE
            TO default.orders_agg_1m AS
            SELECT
                toStartOfMinute(event_time) AS minute,
                category,
                count()                     AS orders_count,
                sum(amount)                 AS total_amount,
                sum(quantity)               AS total_qty
            FROM kafka2ch.orders FINAL
            GROUP BY minute, category;
            """.ReplaceLineEndings("\n"));
        sql.Should().NotContain("orders_agg_1m_local");
        sql.Should().NotContain("Distributed");
        sql.Should().NotContain("ReplicatedSummingMergeTree");
    }

    [Fact]
    public void GivenAggregateWithoutTimeBucketOverPlainMergeTree_WhenRender_ThenSkipsFinalAndInfersTypes()
    {
        var config = CodegenConfigLoader.Load(TestContractsPaths.DedupCodegenConfigPath);
        config.Pipeline!.MergeTreeTables[0].Engine = "MergeTree";
        config.Pipeline.Aggregates =
        [
            new AggregateConfig
            {
                Name = "orders_by_category",
                SourceTable = "orders",
                GroupBy = ["category", "currency"],
                Measures =
                [
                    new AggregateMeasureConfig { Function = "avg", Column = "amount", Alias = "avg_amount" },
                    new AggregateMeasureConfig { Function = "max", Column = "quantity", Alias = "max_qty" },
                    new AggregateMeasureConfig { Function = "uniqExact", Column = "order_id", Alias = "orders" },
                    new AggregateMeasureConfig { Function = "sum", Column = "amount", Alias = "total", Type = "Decimal(18, 2)" }
                ],
                Refresh = new AggregateRefreshConfig { Every = "5 MINUTE" }
            }
        ];

        var sql = AggregateGenerator.Generate(
            SchemaGeneratorFactory.Create().BuildPlan(config).Aggregates,
            ClusterDdl.SingleNode);

        sql.TrimEnd().Should().Be("""
            CREATE TABLE orders_by_category
            (
                category    LowCardinality(String),
                currency    LowCardinality(String),
                avg_amount  Float64,
                max_qty     UInt32,
                orders      UInt64,
                total       Decimal(18, 2)
            )
            ENGINE = MergeTree
            ORDER BY (category, currency);

            CREATE MATERIALIZED VIEW orders_by_category_mv
            REFRESH EVERY 5 MINUTE
            TO orders_by_category AS
            SELECT
                category,
                currency,
                avg(amount)         AS avg_amount,
                max(quantity)       AS max_qty,
                uniqExact(order_id) AS orders,
                sum(amount)         AS total
            FROM orders
            GROUP BY category, currency;
            """.ReplaceLineEndings("\n"));
    }

    public static TheoryData<string, string> InvalidAggregates => new()
    {
        { "unknown-source", "unknown source table 'missing'" },
        { "insert-mode", "refresh.mode" },
        { "bad-interval", "refresh.every" },
        { "unknown-function", "'median' is not supported" },
        { "sum-without-column", "'sum' requires a column" },
        { "count-with-column", "count() takes no column" },
        { "duplicate-alias", "duplicate column 'orders_count'" },
        { "name-conflict", "Aggregate 'orders' conflicts" },
        { "final-over-mergetree", "FINAL" },
        { "unknown-bucket", "timeBucket.function 'toStartOfYear'" },
        { "replicated-engine", "must not be Replicated*" },
        { "no-measures", "at least one measure" }
    };

    [Theory]
    [MemberData(nameof(InvalidAggregates))]
    public void GivenInvalidAggregate_WhenValidate_ThenFailsWithMessage(string scenario, string expectedMessage)
    {
        var config = CodegenConfigLoader.Load(TestContractsPaths.DedupCodegenConfigPath);
        var aggregate = config.Pipeline!.Aggregates[0];
        switch (scenario)
        {
            case "unknown-source":
                aggregate.SourceTable = "missing";
                break;
            case "insert-mode":
                aggregate.Refresh.Mode = "insert";
                break;
            case "bad-interval":
                aggregate.Refresh.Every = "soon";
                break;
            case "unknown-function":
                aggregate.Measures[1].Function = "median";
                break;
            case "sum-without-column":
                aggregate.Measures[1].Column = null;
                break;
            case "count-with-column":
                aggregate.Measures[0].Column = "order_id";
                break;
            case "duplicate-alias":
                aggregate.Measures[2].Alias = "orders_count";
                break;
            case "name-conflict":
                aggregate.Name = "orders";
                break;
            case "final-over-mergetree":
                config.Pipeline.MergeTreeTables[0].Engine = "MergeTree";
                aggregate.Final = true;
                break;
            case "unknown-bucket":
                aggregate.TimeBucket!.Function = "toStartOfYear";
                break;
            case "replicated-engine":
                aggregate.Engine = "ReplicatedMergeTree";
                break;
            case "no-measures":
                aggregate.Measures = [];
                break;
        }

        var result = new CodegenConfigValidator().Validate(config);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(error => error.ErrorMessage.Contains(expectedMessage, StringComparison.Ordinal));
    }

    [Fact]
    public void GivenAggregateTargetLocalNameTaken_WhenValidateCluster_ThenFails()
    {
        var config = CodegenConfigLoader.Load(TestContractsPaths.ClusterDedupCodegenConfigPath);
        config.Pipeline!.Aggregates[0].ViewName = "orders_agg_1m_local";

        var result = new CodegenConfigValidator().Validate(config);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(error => error.ErrorMessage.Contains("'orders_agg_1m_local'", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("region", null, "unknown column 'region'")]
    [InlineData(null, "category", "cannot infer the type of sum(category)")]
    public void GivenAggregateReferencingUnusableColumns_WhenBuildPlan_ThenThrows(
        string? groupBy,
        string? sumColumn,
        string expectedMessage)
    {
        var config = CodegenConfigLoader.Load(TestContractsPaths.DedupCodegenConfigPath);
        var aggregate = config.Pipeline!.Aggregates[0];
        if (groupBy is not null)
            aggregate.GroupBy.Add(groupBy);
        if (sumColumn is not null)
            aggregate.Measures[1].Column = sumColumn;

        var act = () => SchemaGeneratorFactory.Create().BuildPlan(config);

        act.Should().Throw<InvalidOperationException>().WithMessage($"*{expectedMessage}*");
    }

    [Fact]
    public void GivenRefreshIntervalChanged_WhenMigrate_ThenRecreatesViewAutomatically()
    {
        var (diff, plan, sql) = Migrate(
            TestContractsPaths.DedupCodegenConfigPath,
            aggregate => aggregate.Refresh.Every = "5 MINUTE");

        diff.Changes.Should().ContainSingle().Which.Should().BeOfType<AggregateChanged>()
            .Which.Should().Match<AggregateChanged>(change =>
                change.Name == "orders_agg_1m"
                && !change.RecreatesTarget
                && change.Reason.Contains("refresh changed", StringComparison.Ordinal));
        plan.RequiresManualApproval.Should().BeFalse();
        plan.ManualChanges.Should().BeEmpty();
        sql.Should().Contain("DROP VIEW IF EXISTS orders_agg_mv;\n");
        sql.Should().Contain("CREATE MATERIALIZED VIEW orders_agg_mv\nREFRESH EVERY 5 MINUTE\nTO orders_agg_1m AS");
        sql.Should().NotContain("CREATE TABLE");
    }

    [Fact]
    public void GivenMeasureFunctionChangedWithSameType_WhenMigrate_ThenOnlyViewIsRecreated()
    {
        var (diff, plan, sql) = Migrate(
            TestContractsPaths.ClusterDedupCodegenConfigPath,
            aggregate => aggregate.Measures[1].Function = "max");

        diff.Changes.Should().ContainSingle().Which.Should().BeOfType<AggregateChanged>()
            .Which.RecreatesTarget.Should().BeFalse();
        plan.RequiresManualApproval.Should().BeFalse();
        sql.Should().Contain("DROP VIEW IF EXISTS orders_agg_mv ON CLUSTER kafka2ch;\n");
        sql.Should().Contain("max(amount)                 AS total_amount");
        sql.Should().Contain("FROM orders_local FINAL");
    }

    [Theory]
    [InlineData("add-measure")]
    [InlineData("add-group-by")]
    [InlineData("engine")]
    public void GivenTargetColumnsOrStorageChanged_WhenMigrate_ThenTargetRecreateIsManual(string scenario)
    {
        var (diff, plan, sql) = Migrate(
            TestContractsPaths.ClusterDedupCodegenConfigPath,
            aggregate =>
            {
                switch (scenario)
                {
                    case "add-measure":
                        aggregate.Measures.Add(new AggregateMeasureConfig { Function = "uniqExact", Column = "order_id", Alias = "orders" });
                        break;
                    case "add-group-by":
                        aggregate.GroupBy.Add("currency");
                        break;
                    case "engine":
                        aggregate.Engine = "SummingMergeTree";
                        break;
                }
            });

        diff.Changes.Should().ContainSingle().Which.Should().BeOfType<AggregateChanged>()
            .Which.RecreatesTarget.Should().BeTrue();
        plan.RequiresManualApproval.Should().BeTrue();
        plan.AutomaticChanges.Should().BeEmpty();
        sql.Should().NotContain("\nDROP VIEW");
        sql.Should().Contain("-- DROP VIEW IF EXISTS orders_agg_mv ON CLUSTER kafka2ch;");
        sql.Should().Contain("-- DROP TABLE IF EXISTS orders_agg_1m ON CLUSTER kafka2ch;");
        sql.Should().Contain("-- DROP TABLE IF EXISTS orders_agg_1m_local ON CLUSTER kafka2ch SYNC;");
        sql.Should().Contain("-- CREATE TABLE orders_agg_1m_local ON CLUSTER kafka2ch");
        sql.Should().Contain("-- CREATE MATERIALIZED VIEW orders_agg_mv ON CLUSTER kafka2ch");
    }

    [Fact]
    public void GivenViewRenamed_WhenMigrate_ThenDropsOldViewAndCreatesNewOne()
    {
        var (diff, plan, sql) = Migrate(
            TestContractsPaths.DedupCodegenConfigPath,
            aggregate => aggregate.ViewName = "orders_agg_1m_mv");

        diff.Changes.Should().ContainSingle().Which.Should().BeOfType<AggregateChanged>()
            .Which.OldViewName.Should().Be("orders_agg_mv");
        plan.RequiresManualApproval.Should().BeFalse();
        sql.Should().Contain("DROP VIEW IF EXISTS orders_agg_mv;\n");
        sql.Should().Contain("CREATE MATERIALIZED VIEW orders_agg_1m_mv\n");
    }

    [Fact]
    public void GivenAggregateAdded_WhenMigrate_ThenCreatesTargetAndViewAutomatically()
    {
        var (diff, plan, sql) = Migrate(
            TestContractsPaths.ClusterReplicatedDbDedupCodegenConfigPath,
            _ => { },
            config =>
            {
                var copy = CodegenConfigLoader.Load(TestContractsPaths.DedupCodegenConfigPath).Pipeline!.Aggregates[0];
                copy.Name = "orders_agg_5m";
                copy.ViewName = null;
                copy.Refresh.Every = "5 MINUTE";
                config.Pipeline!.Aggregates.Add(copy);
            });

        diff.Changes.Should().ContainSingle().Which.Should().Be(new AggregateAdded("orders_agg_5m"));
        plan.RequiresManualApproval.Should().BeFalse();
        sql.Should().Contain("CREATE TABLE IF NOT EXISTS default.orders_agg_5m ON CLUSTER kafka2ch\n");
        sql.Should().Contain("ENGINE = MergeTree\n");
        sql.Should().Contain("CREATE MATERIALIZED VIEW default.orders_agg_5m_mv ON CLUSTER kafka2ch\nREFRESH EVERY 5 MINUTE\nTO default.orders_agg_5m AS");
        sql.Should().Contain("FROM kafka2ch.orders FINAL");
        sql.Should().NotContain("orders_agg_5m_local");
        sql.Should().NotContain("Distributed");
    }

    [Fact]
    public void GivenAggregateRemoved_WhenMigrate_ThenDropIsManual()
    {
        var (diff, plan, sql) = Migrate(
            TestContractsPaths.DedupCodegenConfigPath,
            _ => { },
            config => config.Pipeline!.Aggregates.Clear());

        diff.Changes.Should().ContainSingle().Which.Should().Be(new AggregateRemoved("orders_agg_1m", "orders_agg_mv"));
        plan.RequiresManualApproval.Should().BeTrue();
        sql.Should().Contain("-- DROP VIEW IF EXISTS orders_agg_mv;");
        sql.Should().Contain("-- DROP TABLE IF EXISTS orders_agg_1m;");
    }

    [Theory]
    [MemberData(nameof(DedupConfigs))]
    public void GivenAggregates_WhenSnapshotRoundTrip_ThenNoDriftAndNoDiff(string configPath)
    {
        var config = CodegenConfigLoader.Load(configPath);
        var plan = SchemaGeneratorFactory.Create().BuildPlan(config);
        var path = Path.Combine(Path.GetTempPath(), $"aggregates-snapshot-{Guid.NewGuid():N}.json");
        try
        {
            SchemaSnapshotSerializer.Save(path, SchemaSnapshotMapper.FromPlan(plan));
            var json = File.ReadAllText(path);
            var loaded = SchemaSnapshotSerializer.Load(path, plan);

            json.Should().Contain("\"aggregates\"").And.Contain("\"refreshEvery\": \"1 MINUTE\"");
            json.Should().NotContain("trailingSqlHash");
            loaded.Aggregates.Should().ContainSingle().Which.Measures.Select(m => m.Type)
                .Should().Equal("UInt64", "Float64", "UInt64");
            FluentActions.Invoking(() => SnapshotDriftChecker.Check(plan, loaded, configPath)).Should().NotThrow();
            SchemaDiffer.Diff(SchemaSnapshotMapper.ToPlan(loaded, config), plan).Changes.Should().BeEmpty();
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void GivenNoAggregates_WhenSerializeSnapshot_ThenAggregatesPropertyIsOmitted()
    {
        var plan = SchemaGeneratorFactory.Create().BuildPlan(CodegenConfigLoader.Load(TestContractsPaths.CodegenConfigPath));

        SchemaSnapshotSerializer.SerializeToString(SchemaSnapshotMapper.FromPlan(plan)).Should().NotContain("aggregates");
    }

    [Theory]
    [MemberData(nameof(DedupConfigs))]
    public void GivenLegacyTrailingSqlSnapshot_WhenSameAggregateDeclared_ThenDiffIsAdoptionWithoutDdl(string configPath)
    {
        var generator = SchemaGeneratorFactory.Create();
        var legacySnapshot = SchemaSnapshotMapper.FromPlan(generator.BuildPlan(LegacyConfig(configPath)));
        var config = CodegenConfigLoader.Load(configPath);
        var newPlan = generator.BuildPlan(config);
        var oldPlan = SchemaSnapshotMapper.ToPlan(legacySnapshot, config);

        var diff = SchemaDiffer.Diff(oldPlan, newPlan);
        var plan = MigrationPolicy.Apply(diff, oldPlan, newPlan);

        diff.Changes.Should().ContainSingle().Which.Should().BeOfType<AggregatesAdoptedFromTrailingSql>()
            .Which.Names.Should().Equal("orders_agg_1m");
        plan.RequiresManualApproval.Should().BeFalse();
    }

    [Fact]
    public void GivenLegacyTrailingSqlDiffersFromAggregate_WhenDiff_ThenTrailingSqlChangeStaysManual()
    {
        var generator = SchemaGeneratorFactory.Create();
        var legacy = LegacyConfig(TestContractsPaths.DedupCodegenConfigPath);
        legacy.Pipeline!.TrailingSql = LegacyTrailingSql.Replace("EVERY 1 MINUTE", "EVERY 2 MINUTE", StringComparison.Ordinal);
        var legacySnapshot = SchemaSnapshotMapper.FromPlan(generator.BuildPlan(legacy));
        var config = CodegenConfigLoader.Load(TestContractsPaths.DedupCodegenConfigPath);
        var newPlan = generator.BuildPlan(config);
        var oldPlan = SchemaSnapshotMapper.ToPlan(legacySnapshot, config);

        var diff = SchemaDiffer.Diff(oldPlan, newPlan);

        diff.Changes.Should().ContainSingle(change => change is TrailingSqlChanged);
        diff.Changes.Should().ContainSingle(change => change is AggregateAdded);
        MigrationPolicy.Apply(diff, oldPlan, newPlan).RequiresManualApproval.Should().BeTrue();
    }

    [Fact]
    public void GivenLegacyTrailingSqlSnapshotOnDisk_WhenMigrateToAggregates_ThenSnapshotUpdatedWithoutMigrationFile()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"aggregates-adopt-{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        try
        {
            foreach (var file in Directory.GetFiles(TestContractsPaths.RootDirectory, "*.json"))
                File.Copy(file, Path.Combine(directory, Path.GetFileName(file)));
            var configPath = Path.Combine(directory, Path.GetFileName(TestContractsPaths.DedupCodegenConfigPath));
            var legacyPath = Path.Combine(directory, "clickhouse.codegen.legacy.json");
            File.WriteAllText(legacyPath, new System.Text.Json.Nodes.JsonObject
            {
                ["extends"] = Path.GetFileName(configPath),
                ["pipeline"] = new System.Text.Json.Nodes.JsonObject
                {
                    ["aggregates"] = new System.Text.Json.Nodes.JsonArray(),
                    ["trailingSql"] = LegacyTrailingSql
                }
            }.ToJsonString());
            var config = CodegenConfigLoader.Load(configPath);
            var snapshotPath = CodegenConfigLoader.ResolvePath(configPath, config.Migrations.SnapshotPath);
            var migrationsDirectory = CodegenConfigLoader.ResolvePath(configPath, config.Migrations.MigrationsDirectory);
            var orchestrator = new MigrationOrchestrator();
            orchestrator.Init(legacyPath);
            SchemaSnapshotSerializer.Load(snapshotPath).TrailingSqlHash.Should().NotBeNull();
            orchestrator.Status(configPath).IsDrifted.Should().BeTrue();

            var result = orchestrator.Migrate(configPath, "adopt_aggregates", allowManual: false);

            result.ExitCode.Should().Be(0);
            result.MigrationPath.Should().BeNull();
            result.Message.Should().Contain("adopted");
            Directory.GetFiles(migrationsDirectory, "*.sql").Should().BeEmpty();
            var snapshot = SchemaSnapshotSerializer.Load(snapshotPath);
            snapshot.TrailingSqlHash.Should().BeNull();
            snapshot.Aggregates.Should().ContainSingle(aggregate => aggregate.Name == "orders_agg_1m");
            orchestrator.Status(configPath).IsDrifted.Should().BeFalse();
            orchestrator.Migrate(configPath, "noop", allowManual: false).Message.Should().Be("No schema changes detected.");
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private static CodegenConfig LegacyConfig(string configPath)
    {
        var config = CodegenConfigLoader.Load(configPath);
        config.Pipeline!.Aggregates = [];
        config.Pipeline.TrailingSql = LegacyTrailingSql;
        return config;
    }

    private static string RenderAggregates(string configPath, Action<AggregateConfig>? configure = null)
    {
        var config = CodegenConfigLoader.Load(configPath);
        configure?.Invoke(config.Pipeline!.Aggregates[0]);
        var plan = SchemaGeneratorFactory.Create().BuildPlan(config);
        return AggregateGenerator.Generate(plan.Aggregates, ClusterDdl.For(config));
    }

    private static (SchemaDiff Diff, MigrationPlan Plan, string Sql) Migrate(
        string configPath,
        Action<AggregateConfig> changeAggregate,
        Action<CodegenConfig>? changeConfig = null)
    {
        var generator = SchemaGeneratorFactory.Create();
        var snapshot = SchemaSnapshotMapper.FromPlan(generator.BuildPlan(CodegenConfigLoader.Load(configPath)));
        var config = CodegenConfigLoader.Load(configPath);
        changeAggregate(config.Pipeline!.Aggregates[0]);
        changeConfig?.Invoke(config);
        var newPlan = generator.BuildPlan(config);
        var oldPlan = SchemaSnapshotMapper.ToPlan(snapshot, config);
        var diff = SchemaDiffer.Diff(oldPlan, newPlan);
        var plan = MigrationPolicy.Apply(diff, oldPlan, newPlan);
        var sql = MigrationSqlGenerator.Generate(plan, oldPlan, newPlan, "aggregates", null, "checksum", DateTimeOffset.UnixEpoch);
        return (diff, plan, sql.ReplaceLineEndings("\n"));
    }
}
