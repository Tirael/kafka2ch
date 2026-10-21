namespace ClickHouseSchemaGen.Snapshot;

public static class SchemaSnapshotUpgrader
{
    public static bool NeedsUpgrade(SchemaSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        EnsureSupported(snapshot);
        return snapshot.Version < SchemaSnapshot.CurrentVersion;
    }

    public static SchemaSnapshot Upgrade(SchemaSnapshot snapshot, ResolvedSchemaPlan currentPlan)
    {
        ArgumentNullException.ThrowIfNull(currentPlan);
        if (!NeedsUpgrade(snapshot))
            return snapshot;

        var current = SchemaSnapshotMapper.FromPlan(currentPlan);
        var kafkaByName = current.KafkaTables.ToDictionary(t => t.TableName, StringComparer.OrdinalIgnoreCase);
        var mergeTreeByName = current.MergeTreeTables.ToDictionary(t => t.TableName, StringComparer.OrdinalIgnoreCase);

        return snapshot with
        {
            Version = SchemaSnapshot.CurrentVersion,
            Cluster = current.Cluster,
            KafkaTables = snapshot.KafkaTables
                .Select(table => table with
                {
                    Key = kafkaByName.GetValueOrDefault(table.TableName)?.Key ?? new KafkaKeySnapshot()
                })
                .ToList(),
            MergeTreeTables = snapshot.MergeTreeTables
                .Select(table => mergeTreeByName.TryGetValue(table.TableName, out var currentTable)
                    ? table with
                    {
                        ShardingKey = currentTable.ShardingKey,
                        MaterializedViewsWriteThroughDistributed = currentTable.MaterializedViewsWriteThroughDistributed
                    }
                    : table)
                .ToList()
        };
    }

    private static void EnsureSupported(SchemaSnapshot snapshot)
    {
        if (snapshot.Version is < 1 or > SchemaSnapshot.CurrentVersion)
        {
            throw new InvalidOperationException(
                $"Snapshot version {snapshot.Version} is not supported "
                    + $"(supported: 1..{SchemaSnapshot.CurrentVersion}). Update ClickHouseSchemaGen.");
        }
    }
}
