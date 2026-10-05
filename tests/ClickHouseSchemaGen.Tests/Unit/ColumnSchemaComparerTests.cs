namespace ClickHouseSchemaGen.Tests.Unit;

public sealed class ColumnSchemaComparerTests
{
    [Fact]
    public void GivenAddedAndRemovedColumns_WhenCompared_ThenDiffContainsAddAndDrop()
    {
        var from = new[]
        {
            ClickHouseColumn.Create("id", "String", MappingStrategy.Direct),
            ClickHouseColumn.Create("legacy", "Int32", MappingStrategy.Direct)
        };
        var to = new[]
        {
            ClickHouseColumn.Create("id", "String", MappingStrategy.Direct),
            ClickHouseColumn.Create("note", "Nullable(String)", MappingStrategy.Optional)
        };

        var diff = ColumnSchemaComparer.Compare(from, to);

        diff.Changes.Should().BeEquivalentTo([
            new SchemaChange
            {
                Kind = SchemaChangeKind.AddColumn,
                ColumnName = "note",
                NewType = "Nullable(String)"
            },
            new SchemaChange
            {
                Kind = SchemaChangeKind.DropColumn,
                ColumnName = "legacy",
                OldType = "Int32"
            }
        ]);
        diff.Unchanged.Select(column => column.Name).Should().Equal("id");
    }

    [Fact]
    public void GivenTypeChange_WhenCompared_ThenDiffContainsModify()
    {
        var from = new[]
        {
            ClickHouseColumn.Create("status", "Int32", MappingStrategy.Direct)
        };
        var to = new[]
        {
            ClickHouseColumn.Create(
                "status",
                "Enum8('SAMPLE_STATUS_UNSPECIFIED' = 0, 'SAMPLE_STATUS_ACTIVE' = 1)",
                MappingStrategy.Direct)
        };

        var diff = ColumnSchemaComparer.Compare(from, to);

        diff.Changes.Should().ContainSingle()
            .Which.Should().BeEquivalentTo(new SchemaChange
            {
                Kind = SchemaChangeKind.ModifyColumn,
                ColumnName = "status",
                OldType = "Int32",
                NewType = "Enum8('SAMPLE_STATUS_UNSPECIFIED' = 0, 'SAMPLE_STATUS_ACTIVE' = 1)"
            });
        diff.Unchanged.Should().BeEmpty();
    }

    [Fact]
    public void GivenIdenticalSchemas_WhenCompared_ThenDiffIsEmpty()
    {
        var columns = new[]
        {
            ClickHouseColumn.Create("id", "String", MappingStrategy.Direct),
            ClickHouseColumn.Create("price.amount", "Float64", MappingStrategy.Flatten)
        };

        var diff = ColumnSchemaComparer.Compare(columns, columns);

        diff.IsEmpty.Should().BeTrue();
        diff.Unchanged.Select(column => column.Name).Should().Equal("id", "price.amount");
    }
}
