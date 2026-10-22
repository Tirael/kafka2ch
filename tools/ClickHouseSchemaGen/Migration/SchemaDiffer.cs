namespace ClickHouseSchemaGen.Migration;

public static class SchemaDiffer
{
    public static SchemaDiff Diff(ResolvedSchemaPlan oldPlan, ResolvedSchemaPlan newPlan)
    {
        ArgumentNullException.ThrowIfNull(oldPlan);
        ArgumentNullException.ThrowIfNull(newPlan);

        List<SchemaChange> changes = [];
        List<string> warnings = [];

        var adopted = DiffTrailingSql(oldPlan, newPlan, changes);
        DiffCluster(oldPlan, newPlan, changes);
        DiffKafkaTables(oldPlan, newPlan, changes, warnings);
        DiffMergeTreeTables(oldPlan, newPlan, changes);
        DiffMaterializedViews(oldPlan, newPlan, changes);
        DiffKafkaDependentViews(oldPlan, newPlan, changes);
        DiffAggregates(oldPlan, newPlan, adopted, changes);

        return new SchemaDiff { Changes = changes, Warnings = warnings };
    }

    private static HashSet<string> DiffTrailingSql(
        ResolvedSchemaPlan oldPlan,
        ResolvedSchemaPlan newPlan,
        List<SchemaChange> changes)
    {
        HashSet<string> adopted = new(StringComparer.OrdinalIgnoreCase);
        var oldHash = oldPlan.TrailingSqlHash ?? SchemaSnapshotMapper.ComputeTrailingSqlHash(oldPlan.TrailingSql);
        var newHash = newPlan.TrailingSqlHash ?? SchemaSnapshotMapper.ComputeTrailingSqlHash(newPlan.TrailingSql);
        if (string.Equals(oldHash, newHash, StringComparison.OrdinalIgnoreCase))
            return adopted;

        var oldAggregates = oldPlan.Aggregates.Select(aggregate => aggregate.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
        var added = newPlan.Aggregates.Where(aggregate => !oldAggregates.Contains(aggregate.Name)).ToList();
        if (oldHash is not null
            && newHash is null
            && added.Count > 0
            && string.Equals(
                oldHash,
                SchemaSnapshotMapper.ComputeTrailingSqlHash(AggregateGenerator.Generate(added, ClusterDdl.SingleNode)),
                StringComparison.OrdinalIgnoreCase))
        {
            adopted.UnionWith(added.Select(aggregate => aggregate.Name));
            changes.Add(new AggregatesAdoptedFromTrailingSql(added.Select(aggregate => aggregate.Name).ToList(), oldHash));
            return adopted;
        }

        changes.Add(new TrailingSqlChanged(oldHash, newHash));
        return adopted;
    }

    private static void DiffAggregates(
        ResolvedSchemaPlan oldPlan,
        ResolvedSchemaPlan newPlan,
        HashSet<string> adopted,
        List<SchemaChange> changes)
    {
        var oldByName = oldPlan.Aggregates.ToDictionary(aggregate => aggregate.Name, StringComparer.OrdinalIgnoreCase);
        var newByName = newPlan.Aggregates.ToDictionary(aggregate => aggregate.Name, StringComparer.OrdinalIgnoreCase);

        foreach (var aggregate in newPlan.Aggregates.Where(a => !oldByName.ContainsKey(a.Name) && !adopted.Contains(a.Name)))
            changes.Add(new AggregateAdded(aggregate.Name));

        foreach (var aggregate in oldPlan.Aggregates.Where(a => !newByName.ContainsKey(a.Name)))
            changes.Add(new AggregateRemoved(aggregate.Name, aggregate.ViewName));

        foreach (var (name, oldAggregate) in oldByName)
        {
            if (!newByName.TryGetValue(name, out var newAggregate))
                continue;

            var targetReasons = CollectAggregateTargetReasons(oldAggregate, newAggregate);
            var reasons = targetReasons.Concat(CollectAggregateViewReasons(oldAggregate, newAggregate)).ToList();
            if (reasons.Count > 0)
            {
                changes.Add(new AggregateChanged(
                    newAggregate.Name,
                    oldAggregate.ViewName,
                    string.Join("; ", reasons),
                    RecreatesTarget: targetReasons.Count > 0));
            }
        }
    }

    private static List<string> CollectAggregateTargetReasons(AggregatePlan oldAggregate, AggregatePlan newAggregate)
    {
        List<string> reasons = [];
        if (!string.Equals(oldAggregate.Engine, newAggregate.Engine, StringComparison.Ordinal))
            reasons.Add($"engine changed ('{oldAggregate.Engine}' -> '{newAggregate.Engine}')");

        if (!string.Equals(oldAggregate.OrderBy, newAggregate.OrderBy, StringComparison.Ordinal))
            reasons.Add($"ORDER BY changed ('{oldAggregate.OrderBy}' -> '{newAggregate.OrderBy}')");

        if (!AggregateColumns(oldAggregate).SequenceEqual(AggregateColumns(newAggregate), StringComparer.Ordinal))
        {
            reasons.Add(
                $"target columns changed ({string.Join(", ", AggregateColumns(oldAggregate))} -> "
                    + $"{string.Join(", ", AggregateColumns(newAggregate))})");
        }

        return reasons;
    }

    private static List<string> CollectAggregateViewReasons(AggregatePlan oldAggregate, AggregatePlan newAggregate)
    {
        List<string> reasons = [];
        if (!string.Equals(oldAggregate.ViewName, newAggregate.ViewName, StringComparison.Ordinal))
            reasons.Add($"viewName changed ('{oldAggregate.ViewName}' -> '{newAggregate.ViewName}')");

        if (!string.Equals(oldAggregate.SourceTable, newAggregate.SourceTable, StringComparison.Ordinal))
            reasons.Add($"sourceTable changed ('{oldAggregate.SourceTable}' -> '{newAggregate.SourceTable}')");

        if (oldAggregate.Final != newAggregate.Final)
            reasons.Add($"FINAL changed ({oldAggregate.Final} -> {newAggregate.Final})");

        if (!string.Equals(oldAggregate.RefreshMode, newAggregate.RefreshMode, StringComparison.Ordinal)
            || !string.Equals(oldAggregate.RefreshEvery, newAggregate.RefreshEvery, StringComparison.Ordinal))
        {
            reasons.Add(
                $"refresh changed ('{oldAggregate.RefreshMode} EVERY {oldAggregate.RefreshEvery}' -> "
                    + $"'{newAggregate.RefreshMode} EVERY {newAggregate.RefreshEvery}')");
        }

        if (!AggregateExpressions(oldAggregate).SequenceEqual(AggregateExpressions(newAggregate), StringComparer.Ordinal))
        {
            reasons.Add(
                $"select expressions changed ({string.Join(", ", AggregateExpressions(oldAggregate))} -> "
                    + $"{string.Join(", ", AggregateExpressions(newAggregate))})");
        }

        return reasons;
    }

    private static IEnumerable<string> AggregateColumns(AggregatePlan aggregate) =>
        aggregate.TargetColumns.Select(column => $"{column.Name} {column.Type}");

    private static IEnumerable<string> AggregateExpressions(AggregatePlan aggregate) =>
        (aggregate.TimeBucket is null ? [] : new[] { aggregate.TimeBucket.Expression })
            .Concat(aggregate.Measures.Select(measure => measure.Expression));

    private static void DiffCluster(
        ResolvedSchemaPlan oldPlan,
        ResolvedSchemaPlan newPlan,
        List<SchemaChange> changes)
    {
        var reasons = ClusterTopology.Compare(
            SchemaSnapshotMapper.ToClusterSnapshot(oldPlan.Config.Cluster),
            SchemaSnapshotMapper.ToClusterSnapshot(newPlan.Config.Cluster));
        if (reasons.Count > 0)
            changes.Add(new ClusterTopologyChanged("cluster", string.Join("; ", reasons)));
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

        if (!string.Equals(oldKafka.ResolvedBrokerList, newKafka.ResolvedBrokerList, StringComparison.Ordinal))
            reasons.Add("brokerList changed");

        if (!string.Equals(oldKafka.ResolvedNamedCollection, newKafka.ResolvedNamedCollection, StringComparison.Ordinal))
            reasons.Add("namedCollection changed");

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

        if (oldKafka.UsesDefaultHandleErrorMode != newKafka.UsesDefaultHandleErrorMode
            || (!newKafka.UsesDefaultHandleErrorMode
                && !string.Equals(oldKafka.HandleErrorMode, newKafka.HandleErrorMode, StringComparison.OrdinalIgnoreCase)))
        {
            reasons.Add("handleErrorMode changed");
        }

        if (oldKafka.SkipBrokenMessages != newKafka.SkipBrokenMessages)
            reasons.Add("skipBrokenMessages changed");

        reasons.AddRange(KafkaKeyComparison.Compare(
            SchemaSnapshotMapper.ToKeySnapshot(oldTable.Config.Key),
            SchemaSnapshotMapper.ToKeySnapshot(newTable.Config.Key)));

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

            if (!string.Equals(oldTable.Config.SourceTable, newTable.Config.SourceTable, StringComparison.OrdinalIgnoreCase))
                changes.Add(new SourceTableChanged(name, oldTable.Config.SourceTable, newTable.Config.SourceTable));

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

            var topologyReasons = ClusterTopology.CompareTable(
                oldTable.Config.ShardingKey,
                newTable.Config.ShardingKey,
                oldTable.Config.MaterializedViewsWriteThroughDistributed,
                newTable.Config.MaterializedViewsWriteThroughDistributed);
            if (topologyReasons.Count > 0)
                changes.Add(new ClusterTopologyChanged(name, string.Join("; ", topologyReasons)));

            var oldEngine = MergeTreeEngines.Normalize(oldTable.Config.Engine);
            var newEngine = MergeTreeEngines.Normalize(newTable.Config.Engine);
            if (!string.Equals(oldEngine, newEngine, StringComparison.Ordinal))
                changes.Add(new EngineChanged(name, oldEngine, newEngine));

            DiffColumns(
                name,
                oldTable.Config.Columns,
                newTable.Config.Columns,
                changes,
                oldTable.Config.OrderBy,
                newTable.Config.OrderBy);
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

            if (oldView.Origin == newView.Origin
                && MappingsEqual(oldView.Config.Columns, newView.Config.Columns)
                && string.Equals(oldView.Config.Where, newView.Config.Where, StringComparison.Ordinal)
                && string.Equals(oldView.Config.SourceTable, newView.Config.SourceTable, StringComparison.OrdinalIgnoreCase)
                && string.Equals(oldView.Config.TargetTable, newView.Config.TargetTable, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            changes.Add(new ViewChanged(newView.Config.Name, newView.Config.SourceTable, newView.Config.TargetTable));
        }
    }

    private static void DiffKafkaDependentViews(
        ResolvedSchemaPlan oldPlan,
        ResolvedSchemaPlan newPlan,
        List<SchemaChange> changes)
    {
        var oldByName = oldPlan.KafkaTables.ToDictionary(t => t.Config.TableName, StringComparer.OrdinalIgnoreCase);

        foreach (var newTable in newPlan.KafkaTables)
        {
            var name = newTable.Config.TableName;
            if (!oldByName.TryGetValue(name, out var oldTable))
                continue;

            var keyChanged = KafkaKeyComparison.Compare(
                SchemaSnapshotMapper.ToKeySnapshot(oldTable.Config.Key),
                SchemaSnapshotMapper.ToKeySnapshot(newTable.Config.Key)).Count > 0;
            var metaChanged = !MetaEquals(ResolveMeta(oldPlan, oldTable), ResolveMeta(newPlan, newTable));
            if (!keyChanged && !metaChanged)
                continue;

            var views = newPlan.MaterializedViews
                .Where(v => string.Equals(v.Config.SourceTable, name, StringComparison.OrdinalIgnoreCase))
                .ToList();
            var changedViews = changes.OfType<ViewChanged>()
                .Select(v => v.ViewName)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            if (!keyChanged)
            {
                var alreadyCovered = views.Any(v => changedViews.Contains(v.Config.Name))
                    || changes.OfType<KafkaTableChanged>().Any(c =>
                        string.Equals(c.TableName, name, StringComparison.OrdinalIgnoreCase));
                if (alreadyCovered)
                    continue;

                if (views.Count == 0)
                {
                    changes.Add(new KafkaTableChanged(name, "persistKafkaMeta changed"));
                    continue;
                }
            }

            foreach (var view in views.Where(v => !changedViews.Contains(v.Config.Name)))
                changes.Add(new ViewChanged(view.Config.Name, view.Config.SourceTable, view.Config.TargetTable));
        }
    }

    private static PersistKafkaMetaConfig ResolveMeta(ResolvedSchemaPlan plan, KafkaTablePlan table) =>
        KafkaMetaColumnFactory.Resolve(
            plan.Config.Defaults.PersistKafkaMeta,
            table.Config.PersistKafkaMeta,
            includeKafkaMeta: null);

    private static bool MetaEquals(PersistKafkaMetaConfig left, PersistKafkaMetaConfig right) =>
        left.Key == right.Key
        && left.Headers == right.Headers
        && left.Topic == right.Topic
        && left.Partition == right.Partition
        && left.Offset == right.Offset
        && left.TimestampMs == right.TimestampMs;

    private static void DiffColumns(
        string tableName,
        IReadOnlyList<PipelineColumnConfig> oldColumns,
        IReadOnlyList<PipelineColumnConfig> newColumns,
        List<SchemaChange> changes,
        string oldOrderBy,
        string newOrderBy)
    {
        var oldByKey = oldColumns.ToDictionary(ColumnKey, StringComparer.Ordinal);
        var newByKey = newColumns.ToDictionary(ColumnKey, StringComparer.Ordinal);

        var orderByColumns = ParseOrderByColumns(oldOrderBy);
        orderByColumns.UnionWith(ParseOrderByColumns(newOrderBy));

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
                if ((orderByColumns.Contains(oldColumn.Name) || orderByColumns.Contains(newColumn.Name))
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
        ValidationRules.ExtractColumnCandidates(orderBy)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
}
