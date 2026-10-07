namespace ClickHouseSchemaGen.Migration;

public static class SchemaDiffer
{
    public static SchemaDiff Diff(ResolvedSchemaPlan oldPlan, ResolvedSchemaPlan newPlan)
    {
        ArgumentNullException.ThrowIfNull(oldPlan);
        ArgumentNullException.ThrowIfNull(newPlan);

        List<SchemaChange> changes = [];
        List<string> warnings = [];

        DiffTrailingSql(oldPlan, newPlan, changes);
        DiffKafkaTables(oldPlan, newPlan, changes, warnings);
        DiffMergeTreeTables(oldPlan, newPlan, changes);
        DiffMaterializedViews(oldPlan, newPlan, changes);

        return new SchemaDiff { Changes = changes, Warnings = warnings };
    }

    private static void DiffTrailingSql(
        ResolvedSchemaPlan oldPlan,
        ResolvedSchemaPlan newPlan,
        List<SchemaChange> changes)
    {
        var oldHash = SchemaSnapshotMapper.ComputeTrailingSqlHash(oldPlan.TrailingSql);
        var newHash = SchemaSnapshotMapper.ComputeTrailingSqlHash(newPlan.TrailingSql);
        if (!string.Equals(oldHash, newHash, StringComparison.OrdinalIgnoreCase))
            changes.Add(new TrailingSqlChanged(oldHash, newHash));
    }

    private static void DiffKafkaTables(
        ResolvedSchemaPlan oldPlan,
        ResolvedSchemaPlan newPlan,
        List<SchemaChange> changes,
        List<string> warnings)
    {
        var oldByName = oldPlan.KafkaTables.ToDictionary(t => t.Config.TableName, StringComparer.OrdinalIgnoreCase);
        var newByName = newPlan.KafkaTables.ToDictionary(t => t.Config.TableName, StringComparer.OrdinalIgnoreCase);

        foreach (var name in newByName.Keys.Except(oldByName.Keys, StringComparer.OrdinalIgnoreCase))
            changes.Add(new TableAdded(name, "Kafka"));

        foreach (var name in oldByName.Keys.Except(newByName.Keys, StringComparer.OrdinalIgnoreCase))
            changes.Add(new TableRemoved(name, "Kafka"));

        foreach (var (name, oldTable) in oldByName)
        {
            if (!newByName.TryGetValue(name, out var newTable))
                continue;

            var reasons = CollectKafkaChangeReasons(oldTable, newTable);
            if (reasons.Count > 0)
                changes.Add(new KafkaTableChanged(name, string.Join("; ", reasons)));

            if (!string.Equals(oldTable.Config.Kafka.GroupName, newTable.Config.Kafka.GroupName, StringComparison.Ordinal))
            {
                warnings.Add(
                    $"Kafka table '{name}' groupName changed; consumer offsets will reset for the new group.");
            }
        }
    }

    private static List<string> CollectKafkaChangeReasons(KafkaTablePlan oldTable, KafkaTablePlan newTable)
    {
        List<string> reasons = [];

        if (!string.Equals(oldTable.Config.ProtoFile, newTable.Config.ProtoFile, StringComparison.Ordinal))
            reasons.Add("protoFile changed");

        if (!string.Equals(oldTable.Config.MessageName, newTable.Config.MessageName, StringComparison.Ordinal))
            reasons.Add("messageName changed");

        var oldKafka = oldTable.Config.Kafka;
        var newKafka = newTable.Config.Kafka;

        if (!string.Equals(oldKafka.BrokerList, newKafka.BrokerList, StringComparison.Ordinal))
            reasons.Add("brokerList changed");

        if (!string.Equals(oldKafka.Topic, newKafka.Topic, StringComparison.Ordinal))
            reasons.Add("topic changed");

        if (!string.Equals(oldKafka.GroupName, newKafka.GroupName, StringComparison.Ordinal))
            reasons.Add("groupName changed");

        if (oldKafka.SkipBytes != newKafka.SkipBytes)
            reasons.Add("skipBytes changed");

        if (oldKafka.NumConsumers != newKafka.NumConsumers)
            reasons.Add("numConsumers changed");

        if (oldKafka.FlattenNested != newKafka.FlattenNested)
            reasons.Add("flattenNested changed");

        if (oldKafka.ProtobufOneofPresence != newKafka.ProtobufOneofPresence)
            reasons.Add("protobufOneofPresence changed");

        if (oldKafka.ProtobufFlattenGoogleWrappers != newKafka.ProtobufFlattenGoogleWrappers)
            reasons.Add("protobufFlattenGoogleWrappers changed");

        if (HasColumnDiff(oldTable.Columns, newTable.Columns))
            reasons.Add("proto columns changed");

        return reasons;
    }

    private static void DiffMergeTreeTables(ResolvedSchemaPlan oldPlan, ResolvedSchemaPlan newPlan, List<SchemaChange> changes)
    {
        var oldByName = oldPlan.MergeTreeTables.ToDictionary(t => t.Config.TableName, StringComparer.OrdinalIgnoreCase);
        var newByName = newPlan.MergeTreeTables.ToDictionary(t => t.Config.TableName, StringComparer.OrdinalIgnoreCase);

        foreach (var name in newByName.Keys.Except(oldByName.Keys, StringComparer.OrdinalIgnoreCase))
            changes.Add(new TableAdded(name, "MergeTree"));

        foreach (var name in oldByName.Keys.Except(newByName.Keys, StringComparer.OrdinalIgnoreCase))
            changes.Add(new TableRemoved(name, "MergeTree"));

        foreach (var (name, oldTable) in oldByName)
        {
            if (!newByName.TryGetValue(name, out var newTable))
                continue;

            if (oldTable.Origin != newTable.Origin)
                changes.Add(new OriginChanged(name, oldTable.Origin, newTable.Origin));

            if (!string.Equals(oldTable.Config.OrderBy, newTable.Config.OrderBy, StringComparison.Ordinal)
                || !string.Equals(oldTable.Config.Ttl?.Trim(), newTable.Config.Ttl?.Trim(), StringComparison.Ordinal))
            {
                changes.Add(new OrderByOrTtlChanged(
                    name,
                    oldTable.Config.OrderBy,
                    newTable.Config.OrderBy,
                    oldTable.Config.Ttl,
                    newTable.Config.Ttl));
            }

            DiffColumns(name, oldTable.Config.Columns, newTable.Config.Columns, changes, newTable.Config.OrderBy);
        }
    }

    private static void DiffMaterializedViews(
        ResolvedSchemaPlan oldPlan,
        ResolvedSchemaPlan newPlan,
        List<SchemaChange> changes)
    {
        var oldByName = oldPlan.MaterializedViews.ToDictionary(v => v.Config.Name, StringComparer.OrdinalIgnoreCase);
        var newByName = newPlan.MaterializedViews.ToDictionary(v => v.Config.Name, StringComparer.OrdinalIgnoreCase);

        foreach (var name in newByName.Keys.Except(oldByName.Keys, StringComparer.OrdinalIgnoreCase))
        {
            var view = newByName[name];
            changes.Add(new ViewChanged(view.Config.Name, view.Config.SourceTable, view.Config.TargetTable));
        }

        foreach (var name in oldByName.Keys.Except(newByName.Keys, StringComparer.OrdinalIgnoreCase))
        {
            var view = oldByName[name];
            changes.Add(new ViewChanged(view.Config.Name, view.Config.SourceTable, view.Config.TargetTable));
        }

        foreach (var (name, oldView) in oldByName)
        {
            if (!newByName.TryGetValue(name, out var newView))
                continue;

            if (MappingsEqual(oldView.Config.Columns, newView.Config.Columns)
                && string.Equals(oldView.Config.SourceTable, newView.Config.SourceTable, StringComparison.OrdinalIgnoreCase)
                && string.Equals(oldView.Config.TargetTable, newView.Config.TargetTable, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            changes.Add(new ViewChanged(newView.Config.Name, newView.Config.SourceTable, newView.Config.TargetTable));
        }
    }

    private static void DiffColumns(
        string tableName,
        IReadOnlyList<PipelineColumnConfig> oldColumns,
        IReadOnlyList<PipelineColumnConfig> newColumns,
        List<SchemaChange> changes,
        string orderBy)
    {
        var oldByKey = oldColumns.ToDictionary(ColumnKey, StringComparer.Ordinal);
        var newByKey = newColumns.ToDictionary(ColumnKey, StringComparer.Ordinal);

        var orderByColumns = ParseOrderByColumns(orderBy);

        foreach (var key in newByKey.Keys.Except(oldByKey.Keys, StringComparer.Ordinal))
        {
            var column = newByKey[key];
            var after = FindPreviousColumnName(newColumns, column);
            changes.Add(new ColumnAdded(tableName, column.Name, column.Type, key, after));
        }

        foreach (var key in oldByKey.Keys.Except(newByKey.Keys, StringComparer.Ordinal))
        {
            var column = oldByKey[key];
            changes.Add(new ColumnRemoved(tableName, column.Name, key));
        }

        foreach (var (key, oldColumn) in oldByKey)
        {
            if (!newByKey.TryGetValue(key, out var newColumn))
                continue;

            if (!string.Equals(oldColumn.Name, newColumn.Name, StringComparison.Ordinal))
                changes.Add(new ColumnRenamed(tableName, oldColumn.Name, newColumn.Name, key));

            if (!string.Equals(oldColumn.Type, newColumn.Type, StringComparison.Ordinal))
            {
                var kind = TypeCompatibility.Classify(oldColumn.Type, newColumn.Type);
                if (orderByColumns.Contains(oldColumn.Name, StringComparer.OrdinalIgnoreCase)
                    && kind is TypeChangeKind.Safe or TypeChangeKind.Rewrite)
                {
                    kind = TypeChangeKind.Manual;
                }

                changes.Add(new ColumnTypeChanged(
                    tableName,
                    newColumn.Name,
                    oldColumn.Type,
                    newColumn.Type,
                    key,
                    kind));
            }
        }
    }

    private static bool HasColumnDiff(
        IReadOnlyList<ClickHouseColumn> oldColumns,
        IReadOnlyList<ClickHouseColumn> newColumns)
    {
        var oldByKey = oldColumns.ToDictionary(ColumnKey, StringComparer.Ordinal);
        var newByKey = newColumns.ToDictionary(ColumnKey, StringComparer.Ordinal);

        if (!oldByKey.Keys.ToHashSet(StringComparer.Ordinal).SetEquals(newByKey.Keys))
            return true;

        foreach (var (key, oldColumn) in oldByKey)
        {
            var newColumn = newByKey[key];
            if (!string.Equals(oldColumn.Name, newColumn.Name, StringComparison.Ordinal)
                || !string.Equals(oldColumn.Type, newColumn.Type, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private static bool MappingsEqual(
        IReadOnlyList<PipelineColumnMapping> oldMappings,
        IReadOnlyList<PipelineColumnMapping> newMappings)
    {
        if (oldMappings.Count != newMappings.Count)
            return false;

        var oldByTarget = oldMappings.ToDictionary(m => m.Target, StringComparer.Ordinal);
        var newByTarget = newMappings.ToDictionary(m => m.Target, StringComparer.Ordinal);

        if (!oldByTarget.Keys.ToHashSet(StringComparer.Ordinal).SetEquals(newByTarget.Keys))
            return false;

        foreach (var (target, oldMapping) in oldByTarget)
        {
            var newMapping = newByTarget[target];
            var oldExpression = oldMapping.Expression ?? oldMapping.Source;
            var newExpression = newMapping.Expression ?? newMapping.Source;

            if (!string.Equals(oldMapping.Source, newMapping.Source, StringComparison.Ordinal)
                || !string.Equals(oldExpression, newExpression, StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }

    private static string ColumnKey(ClickHouseColumn column) =>
        string.IsNullOrWhiteSpace(column.FieldNumberPath) ? column.Name : column.FieldNumberPath;

    private static string ColumnKey(PipelineColumnConfig column) =>
        string.IsNullOrWhiteSpace(column.FieldNumberPath) ? column.Name : column.FieldNumberPath!;

    private static string? FindPreviousColumnName(IReadOnlyList<PipelineColumnConfig> columns, PipelineColumnConfig column)
    {
        for (var i = 0; i < columns.Count; i++)
        {
            if (!string.Equals(columns[i].Name, column.Name, StringComparison.Ordinal))
                continue;

            return i == 0 ? null : columns[i - 1].Name;
        }

        return columns.Count == 0 ? null : columns[^1].Name;
    }

    private static HashSet<string> ParseOrderByColumns(string orderBy) =>
        orderBy.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(token => token.Split('(', 2)[0].Trim())
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
}
