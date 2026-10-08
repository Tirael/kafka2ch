namespace ClickHouseSchemaGen.UnitTests;

public sealed class MaterializedViewAutoGeneratorTests
{
    [Fact]
    public void GivenAutoFilledMergeTreeWithoutView_WhenCreate_ThenAddsOneToOneView()
    {
        var original = AutoTable("orders_raw", "orders_queue");
        var expanded = WithColumns(original, "order_id", "price.amount");

        var created = MaterializedViewAutoGenerator.CreateForAutoColumns(
            [original],
            [expanded],
            []);

        created.Should().ContainSingle();
        created[0].Name.Should().Be("orders_raw_mv");
        created[0].SourceTable.Should().Be("orders_queue");
        created[0].TargetTable.Should().Be("orders_raw");
        created[0].Columns.Select(column => (column.Source, column.Target)).Should().Equal(
            ("order_id", "order_id"),
            ("price.amount", "price.amount"));
    }

    [Fact]
    public void GivenExplicitViewForSameTarget_WhenCreate_ThenSkipsAutoView()
    {
        var original = AutoTable("orders_raw", "orders_queue");
        var expanded = WithColumns(original, "order_id");
        var explicitView = new MaterializedViewConfig
        {
            Name = "custom_orders_mv",
            SourceTable = "orders_queue",
            TargetTable = "orders_raw",
            Columns = [new PipelineColumnMapping { Source = "order_id", Target = "order_id" }]
        };

        var created = MaterializedViewAutoGenerator.CreateForAutoColumns(
            [original],
            [expanded],
            [explicitView]);

        created.Should().BeEmpty();
    }

    [Fact]
    public void GivenExplicitColumns_WhenCreate_ThenSkipsAutoView()
    {
        var table = new MergeTreeTableConfig
        {
            TableName = "orders",
            SourceTable = "orders_queue",
            OrderBy = "(order_id)",
            Columns = [new PipelineColumnConfig { Name = "order_id", Type = "String" }]
        };

        var created = MaterializedViewAutoGenerator.CreateForAutoColumns([table], [table], []);

        created.Should().BeEmpty();
    }

    [Fact]
    public void GivenAutoViewNameAlreadyUsed_WhenCreate_ThenThrows()
    {
        var original = AutoTable("orders_raw", "orders_queue");
        var expanded = WithColumns(original, "order_id");
        var otherView = new MaterializedViewConfig
        {
            Name = "orders_raw_mv",
            SourceTable = "orders_queue",
            TargetTable = "orders_other",
            Columns = [new PipelineColumnMapping { Source = "order_id", Target = "order_id" }]
        };

        var act = () => MaterializedViewAutoGenerator.CreateForAutoColumns(
            [original],
            [expanded],
            [otherView]);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*orders_raw_mv*already used*");
    }

    private static MergeTreeTableConfig AutoTable(string tableName, string sourceTable) => new()
    {
        TableName = tableName,
        SourceTable = sourceTable,
        OrderBy = "(order_id)",
        Columns = []
    };

    private static MergeTreeTableConfig WithColumns(MergeTreeTableConfig table, params string[] names) => new()
    {
        TableName = table.TableName,
        SourceTable = table.SourceTable,
        OrderBy = table.OrderBy,
        Columns = names
            .Select(name => new PipelineColumnConfig { Name = name, Type = "String" })
            .ToList()
    };
}
