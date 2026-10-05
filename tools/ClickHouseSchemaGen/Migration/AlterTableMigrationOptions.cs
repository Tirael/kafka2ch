namespace ClickHouseSchemaGen.Migration;

public sealed class AlterTableMigrationOptions
{
    /// <summary>
    /// When true, emit <c>DROP COLUMN</c> for columns present in the old schema but missing in the new one.
    /// Default is false: protobuf field removal should not silently delete ClickHouse history.
    /// </summary>
    public bool DropRemovedColumns { get; init; }

    /// <summary>
    /// When true, emit <c>MODIFY COLUMN</c> for columns whose ClickHouse type changed.
    /// </summary>
    public bool ModifyChangedTypes { get; init; } = true;

    /// <summary>
    /// Prefer <c>IF NOT EXISTS</c> / <c>IF EXISTS</c> guards on ADD/DROP.
    /// </summary>
    public bool UseIfExistsGuards { get; init; } = true;
}
