namespace ClickHouseSchemaGen.Migration;

/// <summary>
/// Compares two ClickHouse column lists (typically produced by mapping two protobuf messages)
/// and returns additive / destructive / type-change operations.
/// </summary>
public static class ColumnSchemaComparer
{
    public static SchemaDiff Compare(
        IReadOnlyList<ClickHouseColumn> fromColumns,
        IReadOnlyList<ClickHouseColumn> toColumns)
    {
        ArgumentNullException.ThrowIfNull(fromColumns);
        ArgumentNullException.ThrowIfNull(toColumns);

        var fromByName = IndexByName(fromColumns);
        var toByName = IndexByName(toColumns);

        var changes = new List<SchemaChange>();
        var unchanged = new List<ClickHouseColumn>();

        foreach (var (name, toColumn) in toByName.OrderBy(pair => pair.Key, StringComparer.OrdinalIgnoreCase))
        {
            if (!fromByName.TryGetValue(name, out var fromColumn))
            {
                changes.Add(new SchemaChange
                {
                    Kind = SchemaChangeKind.AddColumn,
                    ColumnName = toColumn.Name,
                    NewType = toColumn.Type,
                    Comment = toColumn.Comment
                });
                continue;
            }

            if (TypesEqual(fromColumn.Type, toColumn.Type))
            {
                unchanged.Add(toColumn);
                continue;
            }

            changes.Add(new SchemaChange
            {
                Kind = SchemaChangeKind.ModifyColumn,
                ColumnName = toColumn.Name,
                OldType = fromColumn.Type,
                NewType = toColumn.Type,
                Comment = toColumn.Comment
            });
        }

        foreach (var (name, fromColumn) in fromByName.OrderBy(pair => pair.Key, StringComparer.OrdinalIgnoreCase))
        {
            if (toByName.ContainsKey(name))
                continue;

            changes.Add(new SchemaChange
            {
                Kind = SchemaChangeKind.DropColumn,
                ColumnName = fromColumn.Name,
                OldType = fromColumn.Type,
                Comment = fromColumn.Comment
            });
        }

        return new SchemaDiff
        {
            Changes = changes,
            Unchanged = unchanged
        };
    }

    private static Dictionary<string, ClickHouseColumn> IndexByName(IReadOnlyList<ClickHouseColumn> columns)
    {
        var index = new Dictionary<string, ClickHouseColumn>(StringComparer.OrdinalIgnoreCase);

        foreach (var column in columns)
        {
            if (string.IsNullOrWhiteSpace(column.Name))
                throw new InvalidOperationException("ClickHouse column name cannot be empty.");

            if (!index.TryAdd(column.Name, column))
                throw new InvalidOperationException($"Duplicate column name '{column.Name}' in schema snapshot.");
        }

        return index;
    }

    private static bool TypesEqual(string left, string right) =>
        string.Equals(NormalizeType(left), NormalizeType(right), StringComparison.Ordinal);

    private static string NormalizeType(string type) => type.Trim();
}
