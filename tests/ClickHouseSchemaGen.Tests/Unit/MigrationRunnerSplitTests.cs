using ClickHouseSchemaGen.Migrator;

namespace ClickHouseSchemaGen.Tests.Unit;

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
}
