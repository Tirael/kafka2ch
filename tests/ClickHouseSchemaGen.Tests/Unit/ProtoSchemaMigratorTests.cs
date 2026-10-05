using Sandbox.Contracts.TestFixtures;

namespace ClickHouseSchemaGen.Tests.Unit;

public sealed class ProtoSchemaMigratorTests
{
    private readonly ProtoSchemaMigrator _sut = new();

    [Fact]
    public void GivenBaselineAndEvolvedProtos_WhenDiffed_ThenAddsNewColumnsAndDropsRemoved()
    {
        var diff = _sut.Diff(
            MigrationBaseline.Descriptor,
            MigrationEvolved.Descriptor,
            OrdersQueueTestConfig.Defaults);

        diff.Changes.Where(change => change.Kind == SchemaChangeKind.AddColumn)
            .Select(change => (change.ColumnName, change.NewType))
            .Should().BeEquivalentTo([
                ("note", "Nullable(String)"),
                ("created_at.seconds", "Int64"),
                ("created_at.nanos", "Int32"),
                ("labels", "Map(String, String)")
            ]);

        diff.Changes.Should().ContainSingle(change => change.Kind == SchemaChangeKind.DropColumn)
            .Which.ColumnName.Should().Be("count");

        diff.Unchanged.Select(column => column.Name)
            .Should().BeEquivalentTo(["id", "name", "status"]);
    }

    [Fact]
    public void GivenBaselineAndEvolvedProtos_WhenAlterGenerated_ThenAddsColumnsAndSkipsDropByDefault()
    {
        var sql = _sut.GenerateAlterTableSql(
            "orders_raw",
            MigrationBaseline.Descriptor,
            MigrationEvolved.Descriptor,
            OrdersQueueTestConfig.Defaults);

        sql.Should().Contain("ALTER TABLE orders_raw");
        sql.Should().Contain("ADD COLUMN IF NOT EXISTS note Nullable(String)");
        sql.Should().Contain("ADD COLUMN IF NOT EXISTS `created_at.seconds` Int64");
        sql.Should().Contain("ADD COLUMN IF NOT EXISTS `created_at.nanos` Int32");
        sql.Should().Contain("ADD COLUMN IF NOT EXISTS labels Map(String, String)");
        sql.Should().Contain("-- SKIPPED DROP COLUMN count");
        sql.Should().NotContain("DROP COLUMN IF EXISTS count");
    }

    [Fact]
    public void GivenClrTypeNames_WhenAlterGeneratedWithDropRemoved_ThenEmitsDrop()
    {
        var sql = _sut.GenerateAlterTableSql(
            "orders_raw",
            "Sandbox.Contracts.TestFixtures.MigrationBaseline, Sandbox.Contracts",
            "Sandbox.Contracts.TestFixtures.MigrationEvolved, Sandbox.Contracts",
            OrdersQueueTestConfig.Defaults,
            options: new AlterTableMigrationOptions { DropRemovedColumns = true });

        sql.Should().Contain("DROP COLUMN IF EXISTS count");
    }
}
