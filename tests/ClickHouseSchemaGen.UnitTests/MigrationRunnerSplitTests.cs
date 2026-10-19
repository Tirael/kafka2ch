namespace ClickHouseSchemaGen.UnitTests;

public sealed class MigrationRunnerSplitTests
{
    [Fact]
    public void GivenSqlWithCommentsAndQuotes_WhenSplit_ThenKeepsStatementsIntact()
    {
        var sql = """
            -- header
            DETACH TABLE IF EXISTS orders_queue;
            -- await:kafka_consumers_empty orders_queue
            ALTER TABLE orders ADD COLUMN IF NOT EXISTS note String DEFAULT '';
            INSERT INTO t VALUES ('a;b');
            """;

        var statements = MigrationRunner.SplitStatements(sql).ToList();

        statements.Should().Contain(s => s.Contains("DETACH TABLE", StringComparison.Ordinal));
        statements.Should().Contain(s => s.Contains("await:kafka_consumers_empty", StringComparison.Ordinal));
        statements.Should().Contain(s => s.Contains("ALTER TABLE", StringComparison.Ordinal));
        statements.Should().Contain(s => s.Contains("'a;b'", StringComparison.Ordinal));
    }

    [Fact]
    public void GivenCommentsBeforeStatements_WhenSplit_ThenEmitsCommentsSeparately()
    {
        var sql = """
            -- header
            DETACH TABLE IF EXISTS orders_queue;
            -- await:kafka_consumers_empty orders_queue

            ALTER TABLE orders
                -- inline note
                ADD COLUMN IF NOT EXISTS note String DEFAULT '';
            """;

        var statements = MigrationRunner.SplitStatements(sql)
            .Select(SqlText.Normalize)
            .ToList();

        statements.Should().Equal(
            "-- header",
            "DETACH TABLE IF EXISTS orders_queue",
            "-- await:kafka_consumers_empty orders_queue",
            "ALTER TABLE orders\n    -- inline note\n    ADD COLUMN IF NOT EXISTS note String DEFAULT ''");
    }

    [Fact]
    public void GivenDetachWithoutAttach_WhenTrackLifecycle_ThenQueueStaysPendingForRecovery()
    {
        var detached = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);

        MigrationRunner.TrackQueueLifecycle("DETACH TABLE IF EXISTS orders_queue ON CLUSTER kafka2ch", detached);
        MigrationRunner.TrackQueueLifecycle("ALTER TABLE orders ADD COLUMN IF NOT EXISTS note String", detached);

        detached.Should().ContainKey("orders_queue");
        detached["orders_queue"].Should().Be("kafka2ch");
    }

    [Fact]
    public void GivenDetachThenAttach_WhenTrackLifecycle_ThenQueueIsCleared()
    {
        var detached = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);

        MigrationRunner.TrackQueueLifecycle("DETACH TABLE IF EXISTS orders_queue", detached);
        MigrationRunner.TrackQueueLifecycle("ATTACH TABLE orders_queue", detached);

        detached.Should().BeEmpty();
    }

    [Fact]
    public void GivenDetachThenDrop_WhenTrackLifecycle_ThenQueueMovesFromDetachedToDroppedUntilRecreated()
    {
        var detached = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        var dropped = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);

        MigrationRunner.TrackQueueLifecycle("DETACH TABLE IF EXISTS orders_queue PERMANENTLY", detached, dropped);
        MigrationRunner.TrackQueueLifecycle("DROP TABLE IF EXISTS orders_queue ON CLUSTER kafka2ch", detached, dropped);

        detached.Should().BeEmpty();
        dropped.Should().ContainKey("orders_queue");
        dropped["orders_queue"].Should().Be("kafka2ch");

        MigrationRunner.TrackQueueLifecycle("CREATE TABLE IF NOT EXISTS orders_queue (id String) ENGINE = Kafka", detached, dropped);

        dropped.Should().BeEmpty();
    }

    [Fact]
    public void GivenDropViewThenCreateView_WhenTrackViewLifecycle_ThenViewIsTrackedUntilRecreated()
    {
        var droppedViews = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);

        MigrationRunner.TrackViewLifecycle("DROP VIEW IF EXISTS orders_mv ON CLUSTER kafka2ch", droppedViews);

        droppedViews.Should().ContainKey("orders_mv");
        droppedViews["orders_mv"].Should().Be("kafka2ch");

        MigrationRunner.TrackViewLifecycle(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS orders_mv ON CLUSTER kafka2ch TO orders AS SELECT * FROM orders_queue",
            droppedViews);

        droppedViews.Should().BeEmpty();
    }

    [Fact]
    public void GivenDetachOrDropStatement_WhenExtractTargetObjectForBackup_ThenReturnsObjectName()
    {
        MigrationRunner.ExtractTargetObjectForBackup("DETACH TABLE IF EXISTS orders_queue PERMANENTLY")
            .Should().Be("orders_queue");
        MigrationRunner.ExtractTargetObjectForBackup("DROP TABLE IF EXISTS orders_queue ON CLUSTER kafka2ch")
            .Should().Be("orders_queue");
        MigrationRunner.ExtractTargetObjectForBackup("DROP VIEW IF EXISTS orders_mv ON CLUSTER kafka2ch")
            .Should().Be("orders_mv");
        MigrationRunner.ExtractTargetObjectForBackup("ALTER TABLE orders ADD COLUMN IF NOT EXISTS note String")
            .Should().BeNull();
    }

    [Fact]
    public void GivenShowCreateTableOutputAndCluster_WhenWithClusterClause_ThenInjectsOnCluster()
    {
        var tableSql = "CREATE TABLE default.orders_queue (`order_id` String) ENGINE = Kafka";
        var viewSql = "CREATE MATERIALIZED VIEW default.orders_mv TO default.orders AS SELECT order_id FROM default.orders_queue";

        MigrationRunner.WithClusterClause(tableSql, "kafka2ch")
            .Should().Be("CREATE TABLE default.orders_queue ON CLUSTER kafka2ch (`order_id` String) ENGINE = Kafka");
        MigrationRunner.WithClusterClause(viewSql, "kafka2ch")
            .Should().Be("CREATE MATERIALIZED VIEW default.orders_mv ON CLUSTER kafka2ch TO default.orders AS SELECT order_id FROM default.orders_queue");
        MigrationRunner.WithClusterClause(tableSql, null)
            .Should().Be(tableSql);
    }
}
