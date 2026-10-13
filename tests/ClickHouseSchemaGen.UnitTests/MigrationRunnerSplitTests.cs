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
            .Select(statement => statement.ReplaceLineEndings("\n"))
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
    public void GivenDetachThenDrop_WhenTrackLifecycle_ThenQueueIsCleared()
    {
        var detached = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);

        MigrationRunner.TrackQueueLifecycle("DETACH TABLE IF EXISTS orders_queue PERMANENTLY", detached);
        MigrationRunner.TrackQueueLifecycle("DROP TABLE IF EXISTS orders_queue", detached);

        detached.Should().BeEmpty();
    }
}
