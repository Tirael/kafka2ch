namespace ClickHouseSchemaGen.UnitTests;

public sealed class SqlTextTests
{
    [Fact]
    public void GivenCrLfInput_WhenNormalize_ThenEmitsUnixNewlinesOnly()
    {
        SqlText.Normalize("a\r\nb\rc\n").Should().Be("a\nb\nc\n");
    }

    [Fact]
    public void GivenMergeTreeGenerate_WhenCalled_ThenDoesNotEmitCarriageReturns()
    {
        var sql = MergeTreeTableGenerator.Generate(new MergeTreeTableConfig
        {
            TableName = "orders",
            OrderBy = "(order_id)",
            Columns = [new PipelineColumnConfig { Name = "order_id", Type = "String" }]
        });

        sql.Should().NotContain("\r");
        sql.Should().Contain("CREATE TABLE orders\n(");
    }
}
