namespace ClickHouseSchemaGen.Tests.Unit;

public sealed class MergeTreeTableGeneratorTests
{
    [Fact]
    public void GivenTableWithoutTtl_WhenGenerate_ThenEndsWithOrderBy()
    {
        var config = CreateTable(ttl: null);

        var sql = MergeTreeTableGenerator.Generate(config);

        sql.Should().Contain("ENGINE = MergeTree");
        sql.Should().Contain("ORDER BY (event_time, order_id);");
        sql.Should().NotContain("TTL");
    }

    [Fact]
    public void GivenTableWithTtl_WhenGenerate_ThenEmitsTtlClauseAfterOrderBy()
    {
        var config = CreateTable(ttl: "event_time + INTERVAL 90 DAY");

        var sql = MergeTreeTableGenerator.Generate(config);

        sql.Should().Contain(
            """
            ENGINE = MergeTree
            ORDER BY (event_time, order_id)
            TTL event_time + INTERVAL 90 DAY;
            """);
    }

    [Fact]
    public void GivenTableWithWhitespaceTtl_WhenGenerate_ThenOmitsTtlClause()
    {
        var config = CreateTable(ttl: "   ");

        var sql = MergeTreeTableGenerator.Generate(config);

        sql.Should().Contain("ORDER BY (event_time, order_id);");
        sql.Should().NotContain("TTL");
    }

    private static MergeTreeTableConfig CreateTable(string? ttl) => new()
    {
        TableName = "orders",
        OrderBy = "(event_time, order_id)",
        Ttl = ttl,
        Columns =
        [
            new PipelineColumnConfig { Name = "order_id", Type = "String" },
            new PipelineColumnConfig { Name = "event_time", Type = "DateTime64(3)" }
        ]
    };
}
