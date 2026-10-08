namespace ClickHouseSchemaGen.UnitTests;

public sealed class PipelineColumnExpanderTests
{
    private static readonly IReadOnlyList<ClickHouseColumn> OrdersQueueColumns =
    [
        ClickHouseColumn.Create("order_id", "String", MappingStrategy.Direct),
        ClickHouseColumn.Create("price.amount", "Float64", MappingStrategy.Flatten, "nested message"),
        ClickHouseColumn.Create("status", "Enum8('A' = 0)", MappingStrategy.Direct)
    ];

    private static readonly IReadOnlyDictionary<string, IReadOnlyList<ClickHouseColumn>> QueueColumns =
        new Dictionary<string, IReadOnlyList<ClickHouseColumn>>(StringComparer.OrdinalIgnoreCase)
        {
            ["orders_queue"] = OrdersQueueColumns
        };

    [Fact]
    public void GivenEmptyMergeTreeColumns_WhenExpand_ThenCopiesAllQueueColumns()
    {
        var table = new MergeTreeTableConfig
        {
            TableName = "orders_raw",
            SourceTable = "orders_queue",
            OrderBy = "(order_id)",
            Columns = []
        };

        var resolved = PipelineColumnExpander.ExpandMergeTreeTable(table, QueueColumns);

        resolved.Columns.Should().HaveCount(3);
        resolved.Columns.Select(column => column.Name).Should().Equal(
            "order_id",
            "price.amount",
            "status");
        resolved.Columns.Select(column => column.Type).Should().Equal(
            "String",
            "Float64",
            "Enum8('A' = 0)");
    }

    [Fact]
    public void GivenExplicitMergeTreeColumns_WhenExpand_ThenKeepsExplicitList()
    {
        var table = new MergeTreeTableConfig
        {
            TableName = "orders",
            SourceTable = "orders_queue",
            OrderBy = "(order_id)",
            Columns =
            [
                new PipelineColumnConfig { Name = "order_id", Type = "String" }
            ]
        };

        var resolved = PipelineColumnExpander.ExpandMergeTreeTable(table, QueueColumns);

        resolved.Columns.Should().HaveCount(1);
        resolved.Columns[0].Name.Should().Be("order_id");
    }

    [Fact]
    public void GivenEmptyMaterializedViewColumns_WhenExpand_ThenMapsAllQueueColumnsOneToOne()
    {
        var view = new MaterializedViewConfig
        {
            Name = "orders_raw_mv",
            SourceTable = "orders_queue",
            TargetTable = "orders_raw",
            Columns = []
        };

        var resolved = PipelineColumnExpander.ExpandMaterializedView(view, QueueColumns);

        resolved.Columns.Should().HaveCount(3);
        resolved.Columns.Should().OnlyContain(mapping =>
            mapping.Source == mapping.Target && mapping.Expression == null);
        resolved.Columns.Select(mapping => mapping.Source).Should().Equal(
            "order_id",
            "price.amount",
            "status");
    }

    [Fact]
    public void GivenEmptyMergeTreeColumnsWithoutSourceTable_WhenExpand_ThenThrows()
    {
        var table = new MergeTreeTableConfig
        {
            TableName = "orders_raw",
            OrderBy = "(order_id)",
            Columns = []
        };

        var act = () => PipelineColumnExpander.ExpandMergeTreeTable(table, QueueColumns);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*no sourceTable*");
    }
}
