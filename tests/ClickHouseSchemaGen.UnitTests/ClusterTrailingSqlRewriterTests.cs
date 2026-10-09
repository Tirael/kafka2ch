namespace ClickHouseSchemaGen.UnitTests;

public sealed class ClusterTrailingSqlRewriterTests
{
    private static readonly ClusterDdl Cluster = new(new ClusterConfig { Name = "kafka2ch" });

    [Fact]
    public void GivenSingleNode_WhenRewrite_ThenReturnsSqlUnchanged()
    {
        const string sql = "CREATE TABLE t (a UInt8) ENGINE = SummingMergeTree ORDER BY a;";

        ClusterTrailingSqlRewriter.Rewrite(sql, ClusterDdl.SingleNode, []).Should().Be(sql);
    }

    [Fact]
    public void GivenAggregateTableAndView_WhenRewrite_ThenReplicatesTableAndPointsViewAtLocalTables()
    {
        const string sql = """
            CREATE TABLE orders_agg_1m
            (
                minute        DateTime,
                orders_count  UInt64
            )
            ENGINE = SummingMergeTree
            ORDER BY minute;

            CREATE MATERIALIZED VIEW orders_agg_mv TO orders_agg_1m AS
            SELECT toStartOfMinute(event_time) AS minute, count() AS orders_count
            FROM orders
            GROUP BY minute;
            """;

        var rewritten = ClusterTrailingSqlRewriter.Rewrite(sql, Cluster, ["orders"]);

        rewritten.ReplaceLineEndings().Should().Be(
            """
            CREATE TABLE orders_agg_1m_local ON CLUSTER kafka2ch
            (
                minute        DateTime,
                orders_count  UInt64
            )
            ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')
            ORDER BY minute;

            CREATE TABLE orders_agg_1m ON CLUSTER kafka2ch AS orders_agg_1m_local
            ENGINE = Distributed('kafka2ch', currentDatabase(), orders_agg_1m_local, rand());

            CREATE MATERIALIZED VIEW orders_agg_mv ON CLUSTER kafka2ch TO orders_agg_1m_local AS
            SELECT toStartOfMinute(event_time) AS minute, count() AS orders_count
            FROM orders_local
            GROUP BY minute;


            """.ReplaceLineEndings());
    }

    [Fact]
    public void GivenEngineWithArgumentsAndIfNotExists_WhenRewrite_ThenKeepsArgumentsAfterKeeperPath()
    {
        const string sql = "CREATE TABLE IF NOT EXISTS latest (id String, v UInt64) ENGINE = ReplacingMergeTree(v) ORDER BY id";

        var rewritten = ClusterTrailingSqlRewriter.Rewrite(sql, Cluster, []);

        rewritten.Should().Contain(
            "CREATE TABLE IF NOT EXISTS latest_local ON CLUSTER kafka2ch (id String, v UInt64) " +
            "ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}', v) ORDER BY id;");
        rewritten.Should().Contain("CREATE TABLE IF NOT EXISTS latest ON CLUSTER kafka2ch AS latest_local");
    }

    [Fact]
    public void GivenViewJoiningLocalAndForeignTables_WhenRewrite_ThenOnlyKnownStorageTablesBecomeLocal()
    {
        const string sql = """
            CREATE MATERIALIZED VIEW enriched_mv TO enriched AS
            SELECT o.order_id, d.name
            FROM orders AS o
            LEFT JOIN dict_table AS d ON d.id = o.order_id
            """;

        var rewritten = ClusterTrailingSqlRewriter.Rewrite(sql, Cluster, ["orders", "enriched"]);

        rewritten.Should().Contain("CREATE MATERIALIZED VIEW enriched_mv ON CLUSTER kafka2ch TO enriched_local AS");
        rewritten.Should().Contain("FROM orders_local AS o");
        rewritten.Should().Contain("LEFT JOIN dict_table AS d");
    }

    [Fact]
    public void GivenNonMergeTreeTableViewAndFunction_WhenRewrite_ThenOnlyAddsOnCluster()
    {
        const string sql = """
            CREATE TABLE lookup (id UInt8) ENGINE = Memory;
            CREATE VIEW recent AS SELECT * FROM orders;
            CREATE OR REPLACE FUNCTION double AS (x) -> x * 2;
            INSERT INTO lookup VALUES (1);
            """;

        var rewritten = ClusterTrailingSqlRewriter.Rewrite(sql, Cluster, ["orders"]);

        rewritten.Should().Contain("CREATE TABLE lookup ON CLUSTER kafka2ch (id UInt8) ENGINE = Memory;");
        rewritten.Should().Contain("CREATE VIEW recent ON CLUSTER kafka2ch AS SELECT * FROM orders;");
        rewritten.Should().Contain("CREATE OR REPLACE FUNCTION double ON CLUSTER kafka2ch AS (x) -> x * 2;");
        rewritten.Should().Contain("INSERT INTO lookup VALUES (1);");
        rewritten.Should().NotContain("lookup_local");
    }

    [Theory]
    [InlineData("ALTER TABLE orders ADD COLUMN x UInt8")]
    [InlineData("DROP TABLE orders")]
    public void GivenUnsupportedStatement_WhenRewrite_ThenThrows(string sql)
    {
        var act = () => ClusterTrailingSqlRewriter.Rewrite(sql, Cluster, ["orders"]);

        act.Should().Throw<InvalidOperationException>().WithMessage("*not supported in cluster mode*");
    }
}
