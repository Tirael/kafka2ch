namespace ClickHouseSchemaGen.Migration;

public static class MigrationPolicy
{
    public static MigrationPlan Apply(SchemaDiff diff, ResolvedSchemaPlan oldPlan, ResolvedSchemaPlan newPlan)
    {
        ArgumentNullException.ThrowIfNull(diff);
        ArgumentNullException.ThrowIfNull(oldPlan);
        ArgumentNullException.ThrowIfNull(newPlan);

        List<SchemaChange> automatic = [];
        List<SchemaChange> manual = [];

        var manualTables = CollectManualTables(diff, oldPlan, newPlan);

        foreach (var change in diff.Changes)
        {
            if (ShouldBeManual(change, manualTables))
                manual.Add(change);
            else
                automatic.Add(change);
        }

        return new MigrationPlan
        {
            AutomaticChanges = automatic,
            ManualChanges = manual,
            Warnings = diff.Warnings
        };
    }

    private static HashSet<string> CollectManualTables(
        SchemaDiff diff,
        ResolvedSchemaPlan oldPlan,
        ResolvedSchemaPlan newPlan)
    {
        HashSet<string> manualTables = new(StringComparer.OrdinalIgnoreCase);

        foreach (var change in diff.Changes)
        {
            switch (change)
            {
                case OriginChanged origin:
                    manualTables.Add(origin.TableName);
                    break;
                case OrderByOrTtlChanged order:
                    manualTables.Add(order.TableName);
                    break;
                case ColumnTypeChanged typeChanged when typeChanged.Kind is TypeChangeKind.Manual or TypeChangeKind.Destructive:
                    manualTables.Add(typeChanged.TableName);
                    break;
            }
        }

        foreach (var tableName in manualTables.ToList())
        {
            if (TouchesOrderByColumn(tableName, diff, oldPlan, newPlan))
                manualTables.Add(tableName);
        }

        return manualTables;
    }

    private static bool ShouldBeManual(SchemaChange change, HashSet<string> manualTables) =>
        change switch
        {
            ColumnRemoved => true,
            ColumnTypeChanged { Kind: TypeChangeKind.Destructive or TypeChangeKind.Manual } => true,
            OrderByOrTtlChanged => true,
            OriginChanged => true,
            TrailingSqlChanged => true,
            TableRemoved => true,
            ColumnAdded added when manualTables.Contains(added.TableName) => true,
            ColumnRenamed renamed when manualTables.Contains(renamed.TableName) => true,
            ColumnTypeChanged changed when manualTables.Contains(changed.TableName) => true,
            KafkaTableChanged => false,
            ViewChanged => false,
            TableAdded => false,
            ColumnAdded => false,
            ColumnRenamed => false,
            ColumnTypeChanged { Kind: TypeChangeKind.Safe or TypeChangeKind.Rewrite } => false,
            _ => MigrationPlan.IsManualOrDestructive(change)
        };

    private static bool TouchesOrderByColumn(
        string tableName,
        SchemaDiff diff,
        ResolvedSchemaPlan oldPlan,
        ResolvedSchemaPlan newPlan)
    {
        var newTable = newPlan.MergeTreeTables.FirstOrDefault(t =>
            string.Equals(t.Config.TableName, tableName, StringComparison.OrdinalIgnoreCase));
        if (newTable is null)
            return false;

        var orderByColumns = ParseOrderByColumns(newTable.Config.OrderBy);
        return diff.Changes.Any(change => change switch
        {
            ColumnRenamed renamed when string.Equals(renamed.TableName, tableName, StringComparison.OrdinalIgnoreCase)
                => orderByColumns.Contains(renamed.OldName, StringComparer.OrdinalIgnoreCase),
            ColumnRemoved removed when string.Equals(removed.TableName, tableName, StringComparison.OrdinalIgnoreCase)
                => orderByColumns.Contains(removed.ColumnName, StringComparer.OrdinalIgnoreCase),
            ColumnTypeChanged typeChanged when string.Equals(typeChanged.TableName, tableName, StringComparison.OrdinalIgnoreCase)
                => orderByColumns.Contains(typeChanged.ColumnName, StringComparer.OrdinalIgnoreCase),
            _ => false
        });
    }

    private static HashSet<string> ParseOrderByColumns(string orderBy) =>
        orderBy.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(token => token.Split('(', 2)[0].Trim())
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
}
