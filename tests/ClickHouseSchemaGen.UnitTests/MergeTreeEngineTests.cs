namespace ClickHouseSchemaGen.UnitTests;

public sealed class MergeTreeEngineTests
{
    private const string ReplicatedArgs = "'/clickhouse/tables/{shard}/{database}/{table}', '{replica}'";

    private static readonly ClusterDdl OnCluster = new(new ClusterConfig { Name = "kafka2ch" });

    private static readonly ClusterDdl ReplicatedDatabase = new(new ClusterConfig
    {
        Name = "kafka2ch",
        DdlMode = ClusterDdlModes.ReplicatedDatabase
    });

    public static TheoryData<string?, string, string, string> Engines => new()
    {
        { null, "MergeTree", $"ReplicatedMergeTree({ReplicatedArgs})", "ReplicatedMergeTree" },
        { "MergeTree()", "MergeTree", $"ReplicatedMergeTree({ReplicatedArgs})", "ReplicatedMergeTree" },
        {
            "ReplacingMergeTree",
            "ReplacingMergeTree",
            $"ReplicatedReplacingMergeTree({ReplicatedArgs})",
            "ReplicatedReplacingMergeTree"
        },
        {
            "ReplacingMergeTree(event_time)",
            "ReplacingMergeTree(event_time)",
            $"ReplicatedReplacingMergeTree({ReplicatedArgs}, event_time)",
            "ReplicatedReplacingMergeTree(event_time)"
        },
        {
            "SummingMergeTree((amount))",
            "SummingMergeTree((amount))",
            $"ReplicatedSummingMergeTree({ReplicatedArgs}, (amount))",
            "ReplicatedSummingMergeTree((amount))"
        },
        {
            "AggregatingMergeTree",
            "AggregatingMergeTree",
            $"ReplicatedAggregatingMergeTree({ReplicatedArgs})",
            "ReplicatedAggregatingMergeTree"
        },
        {
            " CollapsingMergeTree( sign ) ",
            "CollapsingMergeTree(sign)",
            $"ReplicatedCollapsingMergeTree({ReplicatedArgs}, sign)",
            "ReplicatedCollapsingMergeTree(sign)"
        }
    };

    [Theory]
    [MemberData(nameof(Engines))]
    public void GivenEngine_WhenGenerateInEveryTopology_ThenEmitsMatchingStorageEngine(
        string? engine,
        string singleNode,
        string onCluster,
        string replicatedDatabase)
    {
        var table = CreateTable(engine);

        MergeTreeTableGenerator.Generate(table).Should()
            .Contain($"{SqlText.NewLine}ENGINE = {singleNode}{SqlText.NewLine}ORDER BY (event_time, order_id);");
        MergeTreeTableGenerator.Generate(table, cluster: OnCluster).Should()
            .Contain($"{SqlText.NewLine}ENGINE = {onCluster}{SqlText.NewLine}ORDER BY (event_time, order_id);")
            .And.Contain("ENGINE = Distributed('kafka2ch', currentDatabase(), orders_local, rand());");
        MergeTreeTableGenerator.Generate(table, cluster: ReplicatedDatabase).Should()
            .Contain($"{SqlText.NewLine}ENGINE = {replicatedDatabase}{SqlText.NewLine}ORDER BY (event_time, order_id);")
            .And.Contain("ENGINE = Distributed('kafka2ch', currentDatabase(), orders_local, rand());");
    }

    [Fact]
    public void GivenEngineChanged_WhenDiffAndApplyPolicy_ThenEngineChangeIsManual()
    {
        var oldPlan = CreatePlan(engine: null);
        var newPlan = CreatePlan("ReplacingMergeTree(event_time)", extraColumn: true);

        var diff = SchemaDiffer.Diff(oldPlan, newPlan);
        var migrationPlan = MigrationPolicy.Apply(diff, oldPlan, newPlan);

        diff.Changes.OfType<EngineChanged>().Should().ContainSingle().Which.Should().Be(
            new EngineChanged("orders", "MergeTree", "ReplacingMergeTree(event_time)"));
        migrationPlan.ManualChanges.OfType<EngineChanged>().Should().ContainSingle();
        migrationPlan.AutomaticChanges.OfType<EngineChanged>().Should().BeEmpty();
        migrationPlan.ManualChanges.OfType<ColumnAdded>().Should().ContainSingle(change => change.ColumnName == "status");
        migrationPlan.RequiresManualApproval.Should().BeTrue();
    }

    [Fact]
    public void GivenEquivalentEngineSpellings_WhenDiff_ThenNoEngineChange()
    {
        var diff = SchemaDiffer.Diff(CreatePlan(engine: null), CreatePlan(" MergeTree() "));

        diff.Changes.Should().BeEmpty();
    }

    [Fact]
    public void GivenEngineChangedOnSingleNode_WhenGenerateMigrationSql_ThenEmitsCommentedCopyAndExchangeTemplate()
    {
        var sql = GenerateEngineChangeSql(clusterConfig: null);

        sql.Should().Contain(SqlText.Normalize(
            """
            -- Manual engine change for orders: 'MergeTree' -> 'ReplacingMergeTree(event_time)'.
            -- Stop writers to orders (DETACH the Kafka queues feeding it) before copying.
            -- CREATE TABLE orders__engine_new AS orders
            -- ENGINE = ReplacingMergeTree(event_time)
            -- ORDER BY (event_time, order_id)
            -- TTL event_time + INTERVAL 30 DAY;
            -- INSERT INTO orders__engine_new SELECT * FROM orders;
            -- EXCHANGE TABLES orders AND orders__engine_new;
            -- DROP TABLE orders__engine_new SYNC;
            """));
        sql.Split(SqlText.NewLine)
            .Where(line => line.Length > 0 && !line.StartsWith("--", StringComparison.Ordinal))
            .Should().BeEmpty();
    }

    [Fact]
    public void GivenEngineChangedOnCluster_WhenGenerateMigrationSql_ThenTemplateTargetsReplicatedLocalTable()
    {
        var sql = GenerateEngineChangeSql(new ClusterConfig { Name = "kafka2ch" });

        sql.Should().Contain("-- CREATE TABLE orders_local__engine_new ON CLUSTER kafka2ch AS orders_local");
        sql.Should().Contain($"-- ENGINE = ReplicatedReplacingMergeTree({ReplicatedArgs}, event_time)");
        sql.Should().Contain("-- INSERT INTO orders_local__engine_new SELECT * FROM orders_local; -- once per shard");
        sql.Should().Contain("-- EXCHANGE TABLES orders_local AND orders_local__engine_new ON CLUSTER kafka2ch;");
        sql.Should().Contain("-- DROP TABLE orders_local__engine_new ON CLUSTER kafka2ch SYNC;");
    }

    [Fact]
    public void GivenEngineChangedInReplicatedDatabase_WhenGenerateMigrationSql_ThenTemplateUsesBareReplicatedEngine()
    {
        var sql = GenerateEngineChangeSql(
            new ClusterConfig { Name = "kafka2ch", DdlMode = ClusterDdlModes.ReplicatedDatabase });

        sql.Should().Contain("-- CREATE TABLE orders_local__engine_new AS orders_local");
        sql.Should().Contain("-- ENGINE = ReplicatedReplacingMergeTree(event_time)");
        sql.Should().Contain("-- EXCHANGE TABLES orders_local AND orders_local__engine_new;");
    }

    [Fact]
    public void GivenDefaultEngine_WhenSerializeSnapshot_ThenOmitsEngineField()
    {
        var snapshot = SchemaSnapshotMapper.FromPlan(CreatePlan(engine: null));

        snapshot.MergeTreeTables.Single().Engine.Should().BeNull();
        SchemaSnapshotSerializer.SerializeToString(snapshot).Should().NotContain("\"engine\"");
    }

    [Fact]
    public void GivenCustomEngine_WhenSnapshotRoundTrips_ThenEngineIsPreservedWithoutDrift()
    {
        var plan = CreatePlan("ReplacingMergeTree(event_time)");
        var path = Path.Combine(Path.GetTempPath(), $"engine-snapshot-{Guid.NewGuid():N}.json");
        try
        {
            SchemaSnapshotSerializer.Save(path, SchemaSnapshotMapper.FromPlan(plan));
            File.ReadAllText(path).Should().Contain("\"engine\": \"ReplacingMergeTree(event_time)\"");

            var loaded = SchemaSnapshotSerializer.Load(path, plan);
            var mapped = SchemaSnapshotMapper.ToPlan(loaded, plan.Config);

            mapped.MergeTreeTables.Single().Config.Engine.Should().Be("ReplacingMergeTree(event_time)");
            SchemaDiffer.Diff(mapped, plan).Changes.Should().BeEmpty();
            FluentActions.Invoking(() => SnapshotDriftChecker.Check(plan, loaded, "clickhouse.codegen.json")).Should().NotThrow();
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void GivenSnapshotWithoutEngineField_WhenLoadedAgainstDefaultEnginePlan_ThenNoDriftAndNoDiff()
    {
        var plan = CreatePlan(engine: null);
        var path = Path.Combine(Path.GetTempPath(), $"legacy-snapshot-{Guid.NewGuid():N}.json");
        try
        {
            File.WriteAllText(path, LegacySnapshotJson);

            var loaded = SchemaSnapshotSerializer.Load(path, plan);
            var mapped = SchemaSnapshotMapper.ToPlan(loaded, plan.Config);

            loaded.MergeTreeTables.Single().Engine.Should().BeNull();
            mapped.MergeTreeTables.Single().Config.Engine.Should().Be(MergeTreeEngines.Default);
            SchemaDiffer.Diff(mapped, plan).Changes.Should().BeEmpty();
            FluentActions.Invoking(() => SnapshotDriftChecker.Check(plan, loaded, "clickhouse.codegen.json")).Should().NotThrow();
            SchemaSnapshotSerializer.SerializeToString(loaded).Should()
                .Be(SchemaSnapshotSerializer.SerializeToString(SchemaSnapshotMapper.FromPlan(plan)));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void GivenSnapshotWithoutEngineField_WhenPlanUsesReplacingEngine_ThenDriftAndManualEngineChange()
    {
        var plan = CreatePlan("ReplacingMergeTree(event_time)");
        var path = Path.Combine(Path.GetTempPath(), $"legacy-snapshot-{Guid.NewGuid():N}.json");
        try
        {
            File.WriteAllText(path, LegacySnapshotJson);

            var loaded = SchemaSnapshotSerializer.Load(path, plan);
            var mapped = SchemaSnapshotMapper.ToPlan(loaded, plan.Config);

            FluentActions.Invoking(() => SnapshotDriftChecker.Check(plan, loaded, "clickhouse.codegen.json"))
                .Should().Throw<SchemaDriftException>()
                .Which.Differences.Should().ContainSingle(difference =>
                    difference.Contains("engine changed", StringComparison.Ordinal));
            var diff = SchemaDiffer.Diff(mapped, plan);
            diff.Changes.Should().ContainSingle().Which.Should().BeOfType<EngineChanged>();
            MigrationPolicy.Apply(diff, mapped, plan).ManualChanges.Should().ContainSingle();
        }
        finally
        {
            File.Delete(path);
        }
    }

    private const string LegacySnapshotJson =
        """
        {
          "version": 1,
          "kafkaTables": [],
          "mergeTreeTables": [
            {
              "tableName": "orders",
              "origin": "explicit",
              "orderBy": "(event_time, order_id)",
              "ttl": "event_time + INTERVAL 30 DAY",
              "columns": [
                { "name": "event_time", "type": "DateTime64(3)", "fieldNumberPath": "2" },
                { "name": "order_id", "type": "String", "fieldNumberPath": "1" }
              ]
            }
          ],
          "materializedViews": []
        }
        """;

    private static string GenerateEngineChangeSql(ClusterConfig? clusterConfig)
    {
        var oldPlan = CreatePlan(engine: null, clusterConfig);
        var newPlan = CreatePlan("ReplacingMergeTree(event_time)", clusterConfig: clusterConfig);

        var migrationPlan = MigrationPolicy.Apply(SchemaDiffer.Diff(oldPlan, newPlan), oldPlan, newPlan);
        return MigrationSqlGenerator.Generate(
            migrationPlan,
            oldPlan,
            newPlan,
            "change_engine",
            parentChecksum: null,
            targetChecksum: "checksum",
            DateTimeOffset.UnixEpoch);
    }

    private static MergeTreeTableConfig CreateTable(string? engine)
    {
        var table = new MergeTreeTableConfig
        {
            TableName = "orders",
            OrderBy = "(event_time, order_id)",
            Columns =
            [
                new PipelineColumnConfig { Name = "order_id", Type = "String" },
                new PipelineColumnConfig { Name = "event_time", Type = "DateTime64(3)" }
            ]
        };
        if (engine is not null)
            table.Engine = engine;
        return table;
    }

    private static ResolvedSchemaPlan CreatePlan(
        string? engine,
        ClusterConfig? clusterConfig = null,
        bool extraColumn = false)
    {
        var table = new MergeTreeTableConfig
        {
            TableName = "orders",
            OrderBy = "(event_time, order_id)",
            Ttl = "event_time + INTERVAL 30 DAY",
            Columns =
            [
                new PipelineColumnConfig { Name = "order_id", Type = "String", FieldNumberPath = "1" },
                new PipelineColumnConfig { Name = "event_time", Type = "DateTime64(3)", FieldNumberPath = "2" },
                .. extraColumn
                    ? [new PipelineColumnConfig { Name = "status", Type = "String", FieldNumberPath = "3" }]
                    : Array.Empty<PipelineColumnConfig>()
            ]
        };
        if (engine is not null)
            table.Engine = engine;

        return new ResolvedSchemaPlan
        {
            Config = new CodegenConfig { KafkaTables = [], Cluster = clusterConfig ?? new ClusterConfig() },
            KafkaTables = [],
            MergeTreeTables = [new MergeTreeTablePlan { Origin = PlanOrigin.Explicit, Config = table }],
            MaterializedViews = []
        };
    }
}
