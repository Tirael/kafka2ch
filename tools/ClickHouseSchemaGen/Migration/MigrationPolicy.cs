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
        var autoRewrite = newPlan.Config.Migrations.AutoRewrite;

        foreach (var change in diff.Changes)
        {
            if (ShouldBeManual(change, manualTables, autoRewrite))
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
                case EngineChanged engine:
                    manualTables.Add(engine.TableName);
                    break;
                case ColumnTypeChanged { Kind: TypeChangeKind.Manual or TypeChangeKind.Destructive } typeChanged:
                    manualTables.Add(typeChanged.TableName);
                    break;
            }
        }

        var allTableNames = oldPlan.MergeTreeTables
            .Select(t => t.Config.TableName)
            .Concat(newPlan.MergeTreeTables.Select(t => t.Config.TableName))
            .Distinct(StringComparer.OrdinalIgnoreCase);

        foreach (var tableName in allTableNames)
        {
            if (TouchesOrderByColumn(tableName, diff, oldPlan, newPlan))
                manualTables.Add(tableName);
        }

        return manualTables;
    }

    private static bool ShouldBeManual(SchemaChange change, HashSet<string> manualTables, bool autoRewrite) =>
        change switch
        {
            ColumnRemoved => true,
            ColumnTypeChanged { Kind: TypeChangeKind.Destructive or TypeChangeKind.Manual } => true,
            ColumnTypeChanged { Kind: TypeChangeKind.Rewrite } when !autoRewrite => true,
            OrderByOrTtlChanged => true,
            EngineChanged => true,
            OriginChanged => true,
            ClusterTopologyChanged => true,
            TrailingSqlChanged => true,
            TableRemoved => true,
            AggregateRemoved => true,
            AggregateChanged { RecreatesTarget: true } => true,
            ColumnAdded added when manualTables.Contains(added.TableName) => true,
            ColumnRenamed renamed when manualTables.Contains(renamed.TableName) => true,
            ColumnTypeChanged changed when manualTables.Contains(changed.TableName) => true,
            KafkaTableChanged => false,
            ViewChanged => false,
            TableAdded => false,
            SourceTableChanged => false,
            AggregateAdded => false,
            AggregateChanged => false,
            AggregatesAdoptedFromTrailingSql => false,
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
        var oldTable = oldPlan.MergeTreeTables.FirstOrDefault(t =>
            string.Equals(t.Config.TableName, tableName, StringComparison.OrdinalIgnoreCase));
        var newTable = newPlan.MergeTreeTables.FirstOrDefault(t =>
            string.Equals(t.Config.TableName, tableName, StringComparison.OrdinalIgnoreCase));
        if (oldTable is null && newTable is null)
            return false;

        var orderByColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (oldTable is not null)
            orderByColumns.UnionWith(ParseOrderByColumns(oldTable.Config.OrderBy));
        if (newTable is not null)
            orderByColumns.UnionWith(ParseOrderByColumns(newTable.Config.OrderBy));

        return diff.Changes.Any(change => change switch
        {
            ColumnRenamed renamed when string.Equals(renamed.TableName, tableName, StringComparison.OrdinalIgnoreCase)
                => orderByColumns.Contains(renamed.OldName) || orderByColumns.Contains(renamed.NewName),
            ColumnRemoved removed when string.Equals(removed.TableName, tableName, StringComparison.OrdinalIgnoreCase)
                => orderByColumns.Contains(removed.ColumnName),
            ColumnTypeChanged typeChanged when string.Equals(typeChanged.TableName, tableName, StringComparison.OrdinalIgnoreCase)
                => orderByColumns.Contains(typeChanged.ColumnName),
            _ => false
        });
    }

    private static HashSet<string> ParseOrderByColumns(string orderBy) =>
        ValidationRules.ExtractColumnCandidates(orderBy)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
}
