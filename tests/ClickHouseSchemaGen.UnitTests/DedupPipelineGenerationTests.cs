namespace ClickHouseSchemaGen.UnitTests;

public sealed class DedupPipelineGenerationTests
{
    [Fact]
    public void GivenDedupConfig_WhenRenderPipeline_ThenOrdersReplaceByKafkaOffsetAndAggregatesRefreshFromFinal()
    {
        var statements = RenderPipeline(TestContractsPaths.DedupCodegenConfigPath);

        Single(statements, "CREATE TABLE orders\n").Should()
            .Contain("kafka_partition      UInt64")
            .And.Contain("kafka_offset         UInt64")
            .And.Contain("ENGINE = ReplacingMergeTree\nORDER BY (event_time, order_id, kafka_partition, kafka_offset)");
        Single(statements, "CREATE MATERIALIZED VIEW orders_mv").Should()
            .Contain("_partition                   AS kafka_partition")
            .And.Contain("_offset                      AS kafka_offset");
        Single(statements, "CREATE TABLE orders_agg_1m").Should().Contain("ENGINE = MergeTree\nORDER BY (minute, category)");
        Single(statements, "CREATE MATERIALIZED VIEW orders_agg_mv").Should()
            .Contain("REFRESH EVERY 1 MINUTE\nTO orders_agg_1m AS")
            .And.Contain("FROM orders FINAL");
    }

    [Fact]
    public void GivenClusterDedupConfig_WhenRenderPipeline_ThenRefreshTargetIsNodeLocalAndUnreplicated()
    {
        var statements = RenderPipeline(TestContractsPaths.ClusterDedupCodegenConfigPath);

        Single(statements, "CREATE TABLE orders_local ON CLUSTER kafka2ch").Should().Contain(
            "ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')\n" +
            "ORDER BY (event_time, order_id, kafka_partition, kafka_offset)");
        Single(statements, "CREATE MATERIALIZED VIEW orders_mv ON CLUSTER kafka2ch TO orders_local AS");
        Single(statements, "CREATE TABLE orders_agg_1m_local ON CLUSTER kafka2ch").Should()
            .Contain("ENGINE = MergeTree\nORDER BY (minute, category)");
        Single(statements, "CREATE TABLE orders_agg_1m ON CLUSTER kafka2ch AS orders_agg_1m_local");
        Single(statements, "CREATE MATERIALIZED VIEW orders_agg_mv ON CLUSTER kafka2ch").Should()
            .Contain("REFRESH EVERY 1 MINUTE\nTO orders_agg_1m_local AS")
            .And.Contain("FROM orders_local FINAL");
    }

    [Fact]
    public void GivenReplicatedDbDedupConfig_WhenRenderPipeline_ThenRefreshableAggregateIsPerNodeInDefaultDatabase()
    {
        var statements = RenderPipeline(TestContractsPaths.ClusterReplicatedDbDedupCodegenConfigPath);

        Single(statements, "CREATE TABLE orders_local\n").Should()
            .Contain("ENGINE = ReplicatedReplacingMergeTree\nORDER BY (event_time, order_id, kafka_partition, kafka_offset)");
        Single(statements, "CREATE MATERIALIZED VIEW orders_mv TO orders_local AS");
        Single(statements, "CREATE TABLE default.orders_agg_1m ON CLUSTER kafka2ch\n").Should()
            .Contain("ENGINE = MergeTree\nORDER BY (minute, category)");
        Single(statements, "CREATE MATERIALIZED VIEW default.orders_agg_mv ON CLUSTER kafka2ch\n").Should()
            .Contain("REFRESH EVERY 1 MINUTE\nTO default.orders_agg_1m AS")
            .And.Contain("FROM kafka2ch.orders FINAL");
        statements.Should().NotContain(statement => statement.Contains("orders_agg_1m_local"));
        statements.Should().NotContain(statement => statement.Contains("ReplicatedMergeTree"));
        statements.Should().OnlyContain(statement =>
            !statement.Contains("ON CLUSTER") || statement.Contains("default.orders_agg_"));
    }

    private static List<string> RenderPipeline(string configPath)
    {
        var plan = SchemaGeneratorFactory.Create().BuildPlan(CodegenConfigLoader.Load(configPath));
        return SqlStatementSplitter.Split(SchemaPlanRenderer.RenderPipelineSql(plan).ReplaceLineEndings("\n"))
            .Select(statement => statement.ReplaceLineEndings("\n"))
            .ToList();
    }

    private static string Single(IEnumerable<string> statements, string prefix) =>
        statements.Should().ContainSingle(statement => statement.StartsWith(prefix, StringComparison.Ordinal)).Subject;
}
