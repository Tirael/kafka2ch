namespace ClickHouseSchemaGen.Snapshot;

public static class SnapshotDriftChecker
{
    public static void Check(ResolvedSchemaPlan plan, SchemaSnapshot snapshot, string configPath)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentException.ThrowIfNullOrWhiteSpace(configPath);

        var expected = SchemaSnapshotMapper.FromPlan(plan);
        var differences = CollectDifferences(expected, snapshot);
        if (differences.Count > 0)
            throw new SchemaDriftException(differences, configPath);
    }

    internal static IReadOnlyList<string> CollectDifferences(SchemaSnapshot expected, SchemaSnapshot actual)
    {
        List<string> differences = [];

        CompareTrailingSql(expected.TrailingSqlHash, actual.TrailingSqlHash, differences);
        CompareKafkaTables(expected.KafkaTables, actual.KafkaTables, differences);
        CompareMergeTreeTables(expected.MergeTreeTables, actual.MergeTreeTables, differences);
        CompareMaterializedViews(expected.MaterializedViews, actual.MaterializedViews, differences);

        return differences;
    }

    private static void CompareTrailingSql(string? expectedHash, string? actualHash, List<string> differences)
    {
        if (!string.Equals(expectedHash, actualHash, StringComparison.OrdinalIgnoreCase))
            differences.Add($"trailingSql hash changed (expected {expectedHash ?? "<empty>"}, actual {actualHash ?? "<empty>"}).");
    }

    private static void CompareKafkaTables(
        IReadOnlyList<KafkaTableSnapshot> expected,
        IReadOnlyList<KafkaTableSnapshot> actual,
        List<string> differences)
    {
        var expectedByName = expected.ToDictionary(t => t.TableName, StringComparer.OrdinalIgnoreCase);
        var actualByName = actual.ToDictionary(t => t.TableName, StringComparer.OrdinalIgnoreCase);

        foreach (var name in expectedByName.Keys.Except(actualByName.Keys, StringComparer.OrdinalIgnoreCase))
            differences.Add($"Kafka table '{name}' is missing from snapshot.");

        foreach (var name in actualByName.Keys.Except(expectedByName.Keys, StringComparer.OrdinalIgnoreCase))
            differences.Add($"Kafka table '{name}' exists in snapshot but not in current plan.");

        foreach (var (name, expectedTable) in expectedByName)
        {
            if (!actualByName.TryGetValue(name, out var actualTable))
                continue;

            CompareKafkaSettings(name, expectedTable.Kafka, actualTable.Kafka, differences);
            ComparePersistKafkaMeta(name, expectedTable.PersistKafkaMeta, actualTable.PersistKafkaMeta, differences);
            CompareColumns($"Kafka table '{name}'", expectedTable.Columns, actualTable.Columns, differences);
        }
    }

    private static void CompareMergeTreeTables(
        IReadOnlyList<MergeTreeTableSnapshot> expected,
        IReadOnlyList<MergeTreeTableSnapshot> actual,
        List<string> differences)
    {
        var expectedByName = expected.ToDictionary(t => t.TableName, StringComparer.OrdinalIgnoreCase);
        var actualByName = actual.ToDictionary(t => t.TableName, StringComparer.OrdinalIgnoreCase);

        foreach (var name in expectedByName.Keys.Except(actualByName.Keys, StringComparer.OrdinalIgnoreCase))
            differences.Add($"MergeTree table '{name}' is missing from snapshot.");

        foreach (var name in actualByName.Keys.Except(expectedByName.Keys, StringComparer.OrdinalIgnoreCase))
            differences.Add($"MergeTree table '{name}' exists in snapshot but not in current plan.");

        foreach (var (name, expectedTable) in expectedByName)
        {
            if (!actualByName.TryGetValue(name, out var actualTable))
                continue;

            if (expectedTable.Origin != actualTable.Origin)
                differences.Add($"MergeTree table '{name}' origin changed ({expectedTable.Origin} -> {actualTable.Origin}).");

            if (!string.Equals(expectedTable.SourceTable, actualTable.SourceTable, StringComparison.OrdinalIgnoreCase))
            {
                differences.Add(
                    $"MergeTree table '{name}' sourceTable changed ('{expectedTable.SourceTable}' -> '{actualTable.SourceTable}').");
            }

            if (!string.Equals(expectedTable.OrderBy, actualTable.OrderBy, StringComparison.Ordinal))
                differences.Add($"MergeTree table '{name}' ORDER BY changed.");

            if (!string.Equals(expectedTable.Ttl?.Trim(), actualTable.Ttl?.Trim(), StringComparison.Ordinal))
                differences.Add($"MergeTree table '{name}' TTL changed.");

            CompareColumns($"MergeTree table '{name}'", expectedTable.Columns, actualTable.Columns, differences);
        }
    }

    private static void CompareMaterializedViews(
        IReadOnlyList<MaterializedViewSnapshot> expected,
        IReadOnlyList<MaterializedViewSnapshot> actual,
        List<string> differences)
    {
        var expectedByName = expected.ToDictionary(v => v.Name, StringComparer.OrdinalIgnoreCase);
        var actualByName = actual.ToDictionary(v => v.Name, StringComparer.OrdinalIgnoreCase);

        foreach (var name in expectedByName.Keys.Except(actualByName.Keys, StringComparer.OrdinalIgnoreCase))
            differences.Add($"Materialized view '{name}' is missing from snapshot.");

        foreach (var name in actualByName.Keys.Except(expectedByName.Keys, StringComparer.OrdinalIgnoreCase))
            differences.Add($"Materialized view '{name}' exists in snapshot but not in current plan.");

        foreach (var (name, expectedView) in expectedByName)
        {
            if (!actualByName.TryGetValue(name, out var actualView))
                continue;

            if (expectedView.Origin != actualView.Origin)
                differences.Add($"Materialized view '{name}' origin changed ({expectedView.Origin} -> {actualView.Origin}).");

            if (!string.Equals(expectedView.SourceTable, actualView.SourceTable, StringComparison.OrdinalIgnoreCase))
                differences.Add($"Materialized view '{name}' sourceTable changed.");

            if (!string.Equals(expectedView.TargetTable, actualView.TargetTable, StringComparison.OrdinalIgnoreCase))
                differences.Add($"Materialized view '{name}' targetTable changed.");

            CompareViewMappings(name, expectedView.Columns, actualView.Columns, differences);
        }
    }

    private static void CompareKafkaSettings(
        string tableName,
        KafkaSettingsSnapshot expected,
        KafkaSettingsSnapshot actual,
        List<string> differences)
    {
        if (!string.Equals(expected.BrokerList, actual.BrokerList, StringComparison.Ordinal))
            differences.Add($"Kafka table '{tableName}' brokerList changed.");

        if (!string.Equals(expected.Topic, actual.Topic, StringComparison.Ordinal))
            differences.Add($"Kafka table '{tableName}' topic changed.");

        if (!string.Equals(expected.GroupName, actual.GroupName, StringComparison.Ordinal))
            differences.Add($"Kafka table '{tableName}' groupName changed.");

        if (expected.SkipBytes != actual.SkipBytes)
            differences.Add($"Kafka table '{tableName}' skipBytes changed.");

        if (expected.NumConsumers != actual.NumConsumers)
            differences.Add($"Kafka table '{tableName}' numConsumers changed.");

        if (expected.FlattenNested != actual.FlattenNested)
            differences.Add($"Kafka table '{tableName}' flattenNested changed.");

        if (expected.ProtobufOneofPresence != actual.ProtobufOneofPresence)
            differences.Add($"Kafka table '{tableName}' protobufOneofPresence changed.");

        if (expected.ProtobufFlattenGoogleWrappers != actual.ProtobufFlattenGoogleWrappers)
            differences.Add($"Kafka table '{tableName}' protobufFlattenGoogleWrappers changed.");
    }

    private static void ComparePersistKafkaMeta(
        string tableName,
        PersistKafkaMetaConfig expected,
        PersistKafkaMetaConfig actual,
        List<string> differences)
    {
        if (expected.Key != actual.Key
            || expected.Headers != actual.Headers
            || expected.Topic != actual.Topic
            || expected.Partition != actual.Partition
            || expected.Offset != actual.Offset
            || expected.TimestampMs != actual.TimestampMs)
        {
            differences.Add($"Kafka table '{tableName}' persistKafkaMeta flags changed.");
        }
    }

    private static void CompareColumns(
        string scope,
        IReadOnlyList<SnapshotColumn> expected,
        IReadOnlyList<SnapshotColumn> actual,
        List<string> differences)
    {
        var expectedByKey = expected.ToDictionary(ColumnKey, StringComparer.Ordinal);
        var actualByKey = actual.ToDictionary(ColumnKey, StringComparer.Ordinal);

        foreach (var key in expectedByKey.Keys.Except(actualByKey.Keys, StringComparer.Ordinal))
        {
            var column = expectedByKey[key];
            differences.Add($"{scope}: column '{column.Name}' ({key}) is missing from snapshot.");
        }

        foreach (var key in actualByKey.Keys.Except(expectedByKey.Keys, StringComparer.Ordinal))
        {
            var column = actualByKey[key];
            differences.Add($"{scope}: column '{column.Name}' ({key}) exists in snapshot but not in current plan.");
        }

        foreach (var (key, expectedColumn) in expectedByKey)
        {
            if (!actualByKey.TryGetValue(key, out var actualColumn))
                continue;

            if (!string.Equals(expectedColumn.Name, actualColumn.Name, StringComparison.Ordinal))
            {
                differences.Add(
                    $"{scope}: column '{key}' renamed ({expectedColumn.Name} -> {actualColumn.Name}).");
            }

            if (!string.Equals(expectedColumn.Type, actualColumn.Type, StringComparison.Ordinal))
            {
                differences.Add(
                    $"{scope}: column '{expectedColumn.Name}' type changed ({expectedColumn.Type} -> {actualColumn.Type}).");
            }
        }
    }

    private static void CompareViewMappings(
        string viewName,
        IReadOnlyList<SnapshotColumnMapping> expected,
        IReadOnlyList<SnapshotColumnMapping> actual,
        List<string> differences)
    {
        var expectedByTarget = expected.ToDictionary(m => m.Target, StringComparer.Ordinal);
        var actualByTarget = actual.ToDictionary(m => m.Target, StringComparer.Ordinal);

        foreach (var target in expectedByTarget.Keys.Except(actualByTarget.Keys, StringComparer.Ordinal))
            differences.Add($"Materialized view '{viewName}': mapping target '{target}' is missing from snapshot.");

        foreach (var target in actualByTarget.Keys.Except(expectedByTarget.Keys, StringComparer.Ordinal))
            differences.Add($"Materialized view '{viewName}': mapping target '{target}' exists in snapshot but not in plan.");

        foreach (var (target, expectedMapping) in expectedByTarget)
        {
            if (!actualByTarget.TryGetValue(target, out var actualMapping))
                continue;

            if (!string.Equals(expectedMapping.Source, actualMapping.Source, StringComparison.Ordinal))
            {
                differences.Add(
                    $"Materialized view '{viewName}': mapping '{target}' source changed ({expectedMapping.Source} -> {actualMapping.Source}).");
            }

            var expectedExpression = expectedMapping.Expression ?? expectedMapping.Source;
            var actualExpression = actualMapping.Expression ?? actualMapping.Source;
            if (!string.Equals(expectedExpression, actualExpression, StringComparison.Ordinal))
            {
                differences.Add($"Materialized view '{viewName}': mapping '{target}' expression changed.");
            }
        }
    }

    private static string ColumnKey(SnapshotColumn column) =>
        string.IsNullOrWhiteSpace(column.FieldNumberPath) ? column.Name : column.FieldNumberPath;
}
