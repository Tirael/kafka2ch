namespace ClickHouseSchemaGen.Tests.Unit;

public sealed class AlterTableMigrationGeneratorTests
{
    [Fact]
    public void GivenAddAndModify_WhenGenerated_ThenEmitsSingleAlterTable()
    {
        var diff = new SchemaDiff
        {
            Changes =
            [
                new SchemaChange
                {
                    Kind = SchemaChangeKind.AddColumn,
                    ColumnName = "note",
                    NewType = "Nullable(String)"
                },
                new SchemaChange
                {
                    Kind = SchemaChangeKind.ModifyColumn,
                    ColumnName = "status",
                    OldType = "Int32",
                    NewType = "Enum8('A' = 0)"
                },
                new SchemaChange
                {
                    Kind = SchemaChangeKind.AddColumn,
                    ColumnName = "price.amount",
                    NewType = "Float64"
                }
            ],
            Unchanged = []
        };

        var sql = AlterTableMigrationGenerator.Generate("orders", diff);

        sql.Should().Contain("ALTER TABLE orders");
        sql.Should().Contain("ADD COLUMN IF NOT EXISTS note Nullable(String)");
        sql.Should().Contain("MODIFY COLUMN status Enum8('A' = 0)");
        sql.Should().Contain("ADD COLUMN IF NOT EXISTS `price.amount` Float64");
        sql.Should().NotContain("DROP COLUMN");
    }

    [Fact]
    public void GivenRemovedColumn_WhenDropDisabled_ThenOnlySkippedComment()
    {
        var diff = new SchemaDiff
        {
            Changes =
            [
                new SchemaChange
                {
                    Kind = SchemaChangeKind.DropColumn,
                    ColumnName = "legacy",
                    OldType = "Int32"
                }
            ],
            Unchanged = []
        };

        var sql = AlterTableMigrationGenerator.Generate(
            "orders",
            diff,
            new AlterTableMigrationOptions { DropRemovedColumns = false });

        sql.Should().Contain("-- SKIPPED DROP COLUMN legacy");
        sql.Should().Contain("-- No ALTER TABLE mutations for orders.");
        sql.Should().NotContain("ALTER TABLE orders");
    }

    [Fact]
    public void GivenRemovedColumn_WhenDropEnabled_ThenEmitsDropColumn()
    {
        var diff = new SchemaDiff
        {
            Changes =
            [
                new SchemaChange
                {
                    Kind = SchemaChangeKind.DropColumn,
                    ColumnName = "legacy",
                    OldType = "Int32"
                }
            ],
            Unchanged = []
        };

        var sql = AlterTableMigrationGenerator.Generate(
            "orders",
            diff,
            new AlterTableMigrationOptions { DropRemovedColumns = true });

        sql.Should().Contain("ALTER TABLE orders");
        sql.Should().Contain("DROP COLUMN IF EXISTS legacy;");
    }
}
