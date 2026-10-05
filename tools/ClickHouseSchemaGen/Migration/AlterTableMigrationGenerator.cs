namespace ClickHouseSchemaGen.Migration;

/// <summary>
/// Emits ClickHouse <c>ALTER TABLE</c> statements from a <see cref="SchemaDiff"/>.
/// Intended for MergeTree (and other Mutable) tables — Kafka engine does not support ADD/MODIFY COLUMN.
/// </summary>
public static class AlterTableMigrationGenerator
{
    public static string Generate(
        string tableName,
        SchemaDiff diff,
        AlterTableMigrationOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);
        ArgumentNullException.ThrowIfNull(diff);
        options ??= new AlterTableMigrationOptions();

        var mutations = new List<string>();
        var skippedComments = new List<string>();

        foreach (var change in diff.Changes)
        {
            switch (change.Kind)
            {
                case SchemaChangeKind.AddColumn:
                    mutations.Add(FormatAddColumn(change, options));
                    break;

                case SchemaChangeKind.ModifyColumn when options.ModifyChangedTypes:
                    mutations.Add(FormatModifyColumn(change));
                    break;

                case SchemaChangeKind.DropColumn when options.DropRemovedColumns:
                    mutations.Add(FormatDropColumn(change, options));
                    break;

                case SchemaChangeKind.ModifyColumn:
                case SchemaChangeKind.DropColumn:
                    skippedComments.Add(FormatSkippedComment(change));
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(change), change.Kind, "Unknown schema change kind.");
            }
        }

        var builder = new StringBuilder()
            .AppendLine(SqlScriptWriter.GeneratedHeader)
            .AppendLine()
            .AppendLine($"-- Proto schema migration for {tableName}");

        foreach (var comment in skippedComments)
            builder.AppendLine(comment);

        if (mutations.Count == 0)
        {
            builder.AppendLine($"-- No ALTER TABLE mutations for {tableName}.");
            builder.AppendLine();
            return builder.ToString();
        }

        builder.AppendLine($"ALTER TABLE {tableName}");

        for (var i = 0; i < mutations.Count; i++)
        {
            var suffix = i < mutations.Count - 1 ? "," : ";";
            builder.AppendLine($"    {mutations[i]}{suffix}");
        }

        builder.AppendLine();
        return builder.ToString();
    }

    private static string FormatAddColumn(SchemaChange change, AlterTableMigrationOptions options)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(change.NewType);
        var guard = options.UseIfExistsGuards ? " IF NOT EXISTS" : string.Empty;
        return $"ADD COLUMN{guard} {SqlColumnFormatter.FormatBareDefinition(change.ColumnName, change.NewType)}";
    }

    private static string FormatModifyColumn(SchemaChange change)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(change.NewType);
        return $"MODIFY COLUMN {SqlColumnFormatter.FormatBareDefinition(change.ColumnName, change.NewType)}";
    }

    private static string FormatDropColumn(SchemaChange change, AlterTableMigrationOptions options)
    {
        var guard = options.UseIfExistsGuards ? " IF EXISTS" : string.Empty;
        return $"DROP COLUMN{guard} {SqlColumnFormatter.FormatColumnName(change.ColumnName)}";
    }

    private static string FormatSkippedComment(SchemaChange change) =>
        change.Kind switch
        {
            SchemaChangeKind.DropColumn =>
                $"-- SKIPPED DROP COLUMN {SqlColumnFormatter.FormatColumnName(change.ColumnName)}" +
                $" ({change.OldType}) — enable DropRemovedColumns to emit",
            SchemaChangeKind.ModifyColumn =>
                $"-- SKIPPED MODIFY COLUMN {SqlColumnFormatter.FormatColumnName(change.ColumnName)}" +
                $" from {change.OldType} to {change.NewType} — enable ModifyChangedTypes to emit",
            _ => $"-- SKIPPED {change.Kind} {change.ColumnName}"
        };
}
