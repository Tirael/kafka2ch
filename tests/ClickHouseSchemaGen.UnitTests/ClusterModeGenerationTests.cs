namespace ClickHouseSchemaGen.UnitTests;

public sealed class ClusterModeGenerationTests
{
    private static readonly ClusterDdl Cluster = new(new ClusterConfig { Name = "kafka2ch" });

    [Fact]
    public void GivenDisabledCluster_WhenGenerate_ThenOutputMatchesSingleNode()
    {
        var disabled = new ClusterDdl(new ClusterConfig { Name = "  " });
        var table = CreateTable();
        var view = CreateView();

        MergeTreeTableGenerator.Generate(table, cluster: disabled).Should().Be(MergeTreeTableGenerator.Generate(table));
        MaterializedViewGenerator.Generate(view, cluster: disabled).Should().Be(MaterializedViewGenerator.Generate(view));
        disabled.OnCluster.Should().BeEmpty();
        disabled.StorageTable("orders").Should().Be("orders");
    }

    [Fact]
    public void GivenCluster_WhenGenerateMergeTree_ThenEmitsReplicatedLocalAndDistributedTables()
    {
        var table = CreateTable();
        table.Ttl = "event_time + INTERVAL 1 DAY";

        var sql = MergeTreeTableGenerator.Generate(table, cluster: Cluster);

        sql.Should().Be(
            """
            CREATE TABLE orders_local ON CLUSTER kafka2ch
            (
                order_id             String,
                event_time           DateTime64(3)
            )
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')
            ORDER BY (event_time, order_id)
            TTL event_time + INTERVAL 1 DAY;

            CREATE TABLE orders ON CLUSTER kafka2ch AS orders_local
            ENGINE = Distributed('kafka2ch', currentDatabase(), orders_local, rand());


            """.ReplaceLineEndings());
    }

    [Fact]
    public void GivenTableShardingKeyAndIfNotExists_WhenGenerateMergeTree_ThenDistributedUsesKey()
    {
        var table = CreateTable();
        table.ShardingKey = "cityHash64(order_id)";

        var sql = MergeTreeTableGenerator.Generate(table, ifNotExists: true, Cluster);

        sql.Should().Contain("CREATE TABLE IF NOT EXISTS orders_local ON CLUSTER kafka2ch");
        sql.Should().Contain("CREATE TABLE IF NOT EXISTS orders ON CLUSTER kafka2ch AS orders_local");
        sql.Should().Contain("ENGINE = Distributed('kafka2ch', currentDatabase(), orders_local, cityHash64(order_id));");
    }

    [Fact]
    public void GivenCustomPaths_WhenGenerateMergeTree_ThenUsesConfiguredKeeperPathReplicaAndSuffix()
    {
        var cluster = new ClusterDdl(new ClusterConfig
        {
            Name = "prod",
            ReplicatedPath = "/ch/{cluster_id}/{shard}/{uuid}",
            ReplicaName = "{host}",
            LocalTableSuffix = "_shard",
            ShardingKey = "xxHash64(order_id)"
        });

        var sql = MergeTreeTableGenerator.Generate(CreateTable(), cluster: cluster);

        sql.Should().Contain("CREATE TABLE orders_shard ON CLUSTER prod");
        sql.Should().Contain("ENGINE = ReplicatedMergeTree('/ch/{cluster_id}/{shard}/{uuid}', '{host}')");
        sql.Should().Contain("ENGINE = Distributed('prod', currentDatabase(), orders_shard, xxHash64(order_id));");
    }

    [Fact]
    public void GivenCluster_WhenGenerateKafkaTable_ThenQueueIsCreatedOnEveryNodeWithSameGroup()
    {
        var config = OrdersQueueTestConfig.Create();
        var columns = MappingTestSupport.MapFixture(OrderEvent.Descriptor, config.FieldOverrides);

        var sql = KafkaTableGenerator.Generate(config, columns, cluster: Cluster);

        sql.Should().Contain("CREATE TABLE orders_queue ON CLUSTER kafka2ch");
        sql.Should().Contain("ENGINE = Kafka");
        sql.Should().Contain("kafka_group_name = 'clickhouse-orders'");
        sql.Should().NotContain("kafka_keeper_path");
        sql.Should().NotContain("kafka_replica_name");
    }

    [Fact]
    public void GivenCluster_WhenGenerateMaterializedView_ThenWritesToLocalTableAndCreatesFunctionsOnCluster()
    {
        var view = CreateView();
        view.Columns.Add(new PipelineColumnMapping
        {
            Source = "_key",
            Target = "kafka_key.order_id",
            Expression = "CAST(protobufWireBytes(substring(_key, 7), 1) AS String)"
        });

        var sql = MaterializedViewGenerator.Generate(view, cluster: Cluster);

        sql.Should().Contain("CREATE MATERIALIZED VIEW orders_mv ON CLUSTER kafka2ch TO orders_local AS");
        sql.Should().Contain("FROM orders_queue;");
        sql.Should().Contain("CREATE OR REPLACE FUNCTION protobufWireVarint ON CLUSTER kafka2ch AS (m, p) ->");
        sql.Should().Contain("CREATE OR REPLACE FUNCTION protobufWireBytes ON CLUSTER kafka2ch AS (m, field_number) ->");
        sql.Split("CREATE OR REPLACE FUNCTION").Skip(1).Should().OnlyContain(statement => statement.Contains(" ON CLUSTER kafka2ch AS "));
    }

    [Theory]
    [InlineData("MergeTree", "ReplicatedMergeTree('/p', 'r')")]
    [InlineData("SummingMergeTree", "ReplicatedSummingMergeTree('/p', 'r')")]
    [InlineData("ReplacingMergeTree(version)", "ReplicatedReplacingMergeTree('/p', 'r', version)")]
    [InlineData("SummingMergeTree((a, b))", "ReplicatedSummingMergeTree('/p', 'r', (a, b))")]
    [InlineData("ReplicatedMergeTree('/x', 'y')", "ReplicatedMergeTree('/x', 'y')")]
    public void GivenMergeTreeFamilyEngine_WhenReplicated_ThenPrependsKeeperArguments(string engine, string expected)
    {
        ClusterDdl.Replicated(engine, "/p", "r").Should().Be(expected);
    }

    [Fact]
    public void GivenNonMergeTreeEngine_WhenReplicated_ThenThrows()
    {
        var act = () => ClusterDdl.Replicated("Memory", "/p", "r");

        act.Should().Throw<InvalidOperationException>().WithMessage("*not a MergeTree-family engine*");
    }

    [Fact]
    public void GivenRepoClusterConfig_WhenRenderInitScripts_ThenMatchesCommittedClusterInitDirectory()
    {
        var config = CodegenConfigLoader.Load(RepoPaths.ClusterCodegenConfigPath);
        var plan = SchemaGeneratorFactory.Create().BuildPlan(config);

        var scripts = SchemaPlanRenderer.RenderInitScripts(plan);

        foreach (var (outputPath, sql) in scripts)
        {
            var committed = Path.Combine(RepoPaths.ClusterInitDirectory, Path.GetFileName(outputPath));
            File.ReadAllText(committed).ReplaceLineEndings().Should().Be(sql.ReplaceLineEndings(), committed);
        }
    }

    [Fact]
    public void GivenRepoClusterConfig_WhenRenderPipeline_ThenEveryCreateRunsOnClusterAndAggregatesAreReplicated()
    {
        var config = CodegenConfigLoader.Load(RepoPaths.ClusterCodegenConfigPath);
        var plan = SchemaGeneratorFactory.Create().BuildPlan(config);

        var statements = SqlStatementSplitter.Split(SchemaPlanRenderer.RenderPipelineSql(plan))
            .Where(statement => statement.StartsWith("CREATE", StringComparison.Ordinal))
            .ToList();

        statements.Should().OnlyContain(statement => statement.Contains(" ON CLUSTER kafka2ch"));
        statements.Should().NotContain(statement => statement.Contains("ENGINE = MergeTree") || statement.Contains("ENGINE = SummingMergeTree"));
        statements.Should().Contain(statement =>
            statement.StartsWith("CREATE TABLE orders_agg_1m_local ON CLUSTER kafka2ch", StringComparison.Ordinal)
            && statement.Contains("ENGINE = ReplicatedSummingMergeTree("));
        statements.Should().Contain(statement =>
            statement.StartsWith("CREATE MATERIALIZED VIEW orders_agg_mv ON CLUSTER kafka2ch TO orders_agg_1m_local AS", StringComparison.Ordinal)
            && statement.Contains("FROM orders_local"));
    }

    [Fact]
    public void GivenClusterConfig_WhenGenerateMigration_ThenDetachesQueuesAndAltersLocalAndDistributedTablesOnCluster()
    {
        var sql = PersistPartitionMigrationFixture.GenerateSql(RepoPaths.ClusterCodegenConfigPath);

        sql.Should().Contain(
            """
            DETACH TABLE IF EXISTS orders_queue ON CLUSTER kafka2ch;
            -- await:kafka_consumers_empty orders_queue ON CLUSTER kafka2ch
            """.ReplaceLineEndings());
        sql.Should().Contain(
            """
            ALTER TABLE orders_local ON CLUSTER kafka2ch ADD COLUMN IF NOT EXISTS kafka_partition UInt64 DEFAULT 0 AFTER kafka_headers;
            ALTER TABLE orders ON CLUSTER kafka2ch ADD COLUMN IF NOT EXISTS kafka_partition UInt64 DEFAULT 0 AFTER kafka_headers;
            """.ReplaceLineEndings());
        sql.Should().Contain("DROP VIEW IF EXISTS orders_mv ON CLUSTER kafka2ch;");
        sql.Should().Contain("ATTACH TABLE orders_queue ON CLUSTER kafka2ch;");
        sql.Should().Contain("CREATE OR REPLACE FUNCTION protobufWireBytes ON CLUSTER kafka2ch AS");
        sql.Should().Contain("CREATE MATERIALIZED VIEW orders_mv ON CLUSTER kafka2ch TO orders_local AS");
    }

    [Fact]
    public void GivenSingleNodeConfig_WhenGenerateMigration_ThenHasNoClusterClauses()
    {
        var sql = PersistPartitionMigrationFixture.GenerateSql(RepoPaths.CodegenConfigPath);

        sql.Should().Contain("DETACH TABLE IF EXISTS orders_queue;");
        sql.Should().Contain("-- await:kafka_consumers_empty orders_queue" + Environment.NewLine);
        sql.Should().Contain("ALTER TABLE orders ADD COLUMN IF NOT EXISTS kafka_partition UInt64 DEFAULT 0 AFTER kafka_headers;");
        sql.Should().NotContain("ON CLUSTER");
        sql.Should().NotContain("_local");
    }


    [Fact]
    public void GivenWriteThroughDistributed_WhenGenerateMaterializedView_ThenTargetsDistributedTable()
    {
        var cluster = new ClusterDdl(new ClusterConfig
        {
            Name = "kafka2ch",
            MaterializedViewsWriteThroughDistributed = true
        });

        var sql = MaterializedViewGenerator.Generate(CreateView(), cluster: cluster);

        sql.Should().Contain("CREATE MATERIALIZED VIEW orders_mv ON CLUSTER kafka2ch TO orders AS");
        sql.Should().NotContain("TO orders_local");
    }

    [Fact]
    public void GivenPerTableWriteThroughOverride_WhenRenderPipeline_ThenOnlyThatViewWritesThroughDistributed()
    {
        var config = CodegenConfigLoader.Load(RepoPaths.ClusterCodegenConfigPath);
        config.Cluster.MaterializedViewsWriteThroughDistributed = false;
        config.Pipeline!.MergeTreeTables.First(t => t.TableName == "orders")
            .MaterializedViewsWriteThroughDistributed = true;
        config.Pipeline.MergeTreeTables.Add(new MergeTreeTableConfig
        {
            TableName = "orders_archive",
            OrderBy = "(event_time, order_id)",
            Columns =
            [
                new PipelineColumnConfig { Name = "order_id", Type = "String" },
                new PipelineColumnConfig { Name = "event_time", Type = "DateTime64(3)" }
            ]
        });
        config.Pipeline.MaterializedViews.Add(new MaterializedViewConfig
        {
            Name = "orders_archive_mv",
            TargetTable = "orders_archive",
            SourceTable = "orders_queue",
            Columns =
            [
                new PipelineColumnMapping { Source = "order_id", Target = "order_id" },
                new PipelineColumnMapping
                {
                    Source = "event_time.seconds",
                    Target = "event_time",
                    Expression = "toDateTime64(event_time.seconds + event_time.nanos / 1000000000.0, 3)"
                }
            ]
        });

        var sql = SchemaPlanRenderer.RenderPipelineSql(SchemaGeneratorFactory.Create().BuildPlan(config));

        sql.Should().Contain("CREATE MATERIALIZED VIEW orders_mv ON CLUSTER kafka2ch TO orders AS");
        sql.Should().Contain("CREATE MATERIALIZED VIEW orders_archive_mv ON CLUSTER kafka2ch TO orders_archive_local AS");
    }

    [Fact]
    public void GivenReplicatedDatabaseDdlMode_WhenGenerate_ThenOmitsOnClusterAndCreatesReplicatedDatabase()
    {
        var cluster = new ClusterDdl(new ClusterConfig
        {
            Name = "kafka2ch",
            DdlMode = ClusterDdlModes.ReplicatedDatabase,
            ReplicatedDatabaseName = "kafka2ch"
        });

        cluster.OnCluster.Should().BeEmpty();
        cluster.CreateReplicatedDatabaseSql().Should().Be(
            """
            CREATE DATABASE IF NOT EXISTS kafka2ch ON CLUSTER kafka2ch
            ENGINE = Replicated('/clickhouse/databases/{uuid}', '{replica}');

            """.ReplaceLineEndings());

        var tableSql = MergeTreeTableGenerator.Generate(CreateTable(), cluster: cluster);
        tableSql.Should().Contain("CREATE TABLE orders_local\n");
        tableSql.Should().NotContain("ON CLUSTER");
        tableSql.Should().Contain("ENGINE = ReplicatedMergeTree\n");
        tableSql.Should().NotContain("ReplicatedMergeTree('/");
        tableSql.Should().Contain("CREATE TABLE orders AS orders_local");

        var versions = SchemaMigrationsScriptGenerator.Generate([], cluster);
        versions.Should().Contain("CREATE DATABASE IF NOT EXISTS kafka2ch ON CLUSTER kafka2ch");
        versions.Should().Contain("USE kafka2ch;");
        versions.Should().Contain("CREATE TABLE IF NOT EXISTS default.schema_migrations ON CLUSTER kafka2ch");
        versions.Should().Contain("ENGINE = ReplicatedMergeTree('/clickhouse/tables/all/{database}/{table}', '{shard}-{replica}')");
        versions.Should().Contain("USE kafka2ch;");
    }

    [Fact]
    public void GivenRepoReplicatedDbConfig_WhenRenderInitScripts_ThenMatchesCommittedDirectory()
    {
        var config = CodegenConfigLoader.Load(RepoPaths.ClusterReplicatedDbCodegenConfigPath);
        var plan = SchemaGeneratorFactory.Create().BuildPlan(config);
        var scripts = SchemaPlanRenderer.RenderInitScripts(plan);

        config.Cluster.UsesReplicatedDatabase.Should().BeTrue();
        config.Cluster.MaterializedViewsWriteThroughDistributed.Should().BeTrue();
        File.ReadAllText(Path.Combine(RepoPaths.ClusterReplicatedDbInitDirectory, "00_schema_migrations.sql"))
            .ReplaceLineEndings()
            .Should().Be(SchemaMigrationsScriptGenerator.Generate(
                SchemaMigrationsScriptGenerator.ReadFromDirectory(
                    CodegenConfigLoader.ResolvePath(RepoPaths.ClusterReplicatedDbCodegenConfigPath, config.Migrations.MigrationsDirectory)),
                ClusterDdl.For(config)).ReplaceLineEndings());

        foreach (var (outputPath, sql) in scripts)
        {
            var committed = Path.Combine(RepoPaths.ClusterReplicatedDbInitDirectory, Path.GetFileName(outputPath));
            File.ReadAllText(committed).ReplaceLineEndings().Should().Be(sql.ReplaceLineEndings(), committed);
        }

        var pipeline = scripts[config.Pipeline!.OutputPath];
        pipeline.Should().Contain("CREATE MATERIALIZED VIEW orders_mv TO orders AS");
        pipeline.Should().Contain("CREATE MATERIALIZED VIEW orders_agg_mv TO orders_agg_1m AS");
        pipeline.Should().Contain("FROM orders_local");
        pipeline.Should().NotContain("ON CLUSTER");
    }

    private static MergeTreeTableConfig CreateTable() => new()
    {
        TableName = "orders",
        OrderBy = "(event_time, order_id)",
        Columns =
        [
            new PipelineColumnConfig { Name = "order_id", Type = "String" },
            new PipelineColumnConfig { Name = "event_time", Type = "DateTime64(3)" }
        ]
    };

    private static MaterializedViewConfig CreateView() => new()
    {
        Name = "orders_mv",
        TargetTable = "orders",
        SourceTable = "orders_queue",
        Columns = [new PipelineColumnMapping { Source = "order_id", Target = "order_id" }]
    };
}
