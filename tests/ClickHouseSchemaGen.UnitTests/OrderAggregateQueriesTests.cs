using Sandbox.Observability;

namespace ClickHouseSchemaGen.UnitTests;

public sealed class OrderAggregateQueriesTests
{
    [Fact]
    public void AggregatesWindow_ReadsOrdersAggNotRawOrders()
    {
        var sql = OrderAggregateQueries.AggregatesWindow(database: null, windowMinutes: 10);

        sql.Should().Contain($"FROM {OrderAggregateQueries.AggregatesTable}");
        sql.Should().Contain("GROUP BY minute, category");
        sql.Should().NotContain("FROM orders ");
        sql.Should().NotContain("FROM orders\n");
        ForbidRawOrdersWithoutFinal(sql);
    }

    [Fact]
    public void AggregatesWindow_WithDatabase_QualifiesAggregatesTableOnly()
    {
        var sql = OrderAggregateQueries.AggregatesWindow("default", windowMinutes: 5);

        sql.Should().Contain("FROM default.orders_agg_1m");
        sql.Should().Contain("INTERVAL 5 MINUTE");
        ForbidRawOrdersWithoutFinal(sql);
    }

    [Fact]
    public void LastRefreshSuccess_TargetsOrdersAggMv()
    {
        var sql = OrderAggregateQueries.LastRefreshSuccess(database: null);

        sql.Should().Contain("FROM system.view_refreshes");
        sql.Should().Contain($"view = '{OrderAggregateQueries.AggregatesView}'");
        sql.Should().Contain("database = currentDatabase()");
        ForbidRawOrdersWithoutFinal(sql);
    }

    [Fact]
    public void OrdersExactCount_AlwaysUsesFinal()
    {
        var sql = OrderAggregateQueries.OrdersExactCount();

        sql.Should().Contain("FROM orders FINAL");
        ForbidRawOrdersWithoutFinal(sql);
    }

    [Fact]
    public void ReadAggregatesWorkerSource_UsesOrderAggregateQueriesOnly()
    {
        var source = File.ReadAllText(Path.Combine(
            FindRepositoryRoot(),
            "src", "Sandbox.App", "Features", "ReadAggregates", "ReadAggregatesWorker.cs"));

        source.Should().Contain("OrderAggregateQueries.AggregatesWindow");
        source.Should().Contain("OrderAggregateQueries.LastRefreshSuccess");
        // Worker must not embed SQL against raw/agg tables; comments may mention the contract.
        source.Should().NotMatchRegex(@"CommandText\s*=\s*\$?""");
        source.Should().NotContain("orders_agg_1m");
    }

    [Theory]
    [InlineData("orders; DROP TABLE x")]
    [InlineData("")]
    [InlineData("1orders")]
    public void InvalidIdentifiers_Throw(string identifier)
    {
        var aggregates = () => OrderAggregateQueries.AggregatesWindow(identifier, 10);
        var refresh = () => OrderAggregateQueries.LastRefreshSuccess(identifier);
        var count = () => OrderAggregateQueries.OrdersExactCount(identifier);

        aggregates.Should().Throw<ArgumentException>();
        refresh.Should().Throw<ArgumentException>();
        count.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void NonPositiveWindow_Throws()
    {
        var act = () => OrderAggregateQueries.AggregatesWindow(null, 0);

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    private static void ForbidRawOrdersWithoutFinal(string sql)
    {
        // Allow "orders FINAL" / "orders_agg_*" / comments mentioning orders; reject bare FROM orders.
        var withoutFinal = System.Text.RegularExpressions.Regex.Replace(
            sql,
            @"\borders\b\s+FINAL\b",
            "orders_FINAL",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        withoutFinal = System.Text.RegularExpressions.Regex.Replace(
            withoutFinal,
            @"\borders_agg_\w+\b",
            "orders_agg_SAFE",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);

        withoutFinal.Should().NotMatchRegex(
            @"(?i)\bFROM\s+orders\b",
            "raw orders without FINAL is forbidden on the production read path");
    }

    private static string FindRepositoryRoot()
    {
        for (var directory = new DirectoryInfo(AppContext.BaseDirectory); directory is not null; directory = directory.Parent)
        {
            if (File.Exists(Path.Combine(directory.FullName, "kafka2ch.sln")))
            {
                return directory.FullName;
            }
        }

        throw new InvalidOperationException("Repository root with kafka2ch.sln was not found.");
    }
}
