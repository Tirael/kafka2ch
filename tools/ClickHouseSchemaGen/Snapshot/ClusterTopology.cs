namespace ClickHouseSchemaGen.Snapshot;

public static class ClusterTopology
{
    public static IReadOnlyList<string> Compare(ClusterSnapshot? oldCluster, ClusterSnapshot? newCluster)
    {
        if (oldCluster is null && newCluster is null)
            return [];

        if (oldCluster is null)
            return [$"enabled (single-node -> cluster '{newCluster!.Name}', ddlMode '{newCluster.DdlMode}')"];

        if (newCluster is null)
            return [$"disabled (cluster '{oldCluster.Name}', ddlMode '{oldCluster.DdlMode}' -> single-node)"];

        List<string> reasons = [];

        if (!string.Equals(oldCluster.Name, newCluster.Name, StringComparison.Ordinal))
            reasons.Add($"name changed ('{oldCluster.Name}' -> '{newCluster.Name}')");

        if (!string.Equals(oldCluster.DdlMode, newCluster.DdlMode, StringComparison.OrdinalIgnoreCase))
            reasons.Add($"ddlMode changed ('{oldCluster.DdlMode}' -> '{newCluster.DdlMode}')");

        if (!string.Equals(oldCluster.LocalTableSuffix, newCluster.LocalTableSuffix, StringComparison.Ordinal))
            reasons.Add($"localTableSuffix changed ('{oldCluster.LocalTableSuffix}' -> '{newCluster.LocalTableSuffix}')");

        if (!string.Equals(oldCluster.ShardingKey, newCluster.ShardingKey, StringComparison.Ordinal))
            reasons.Add($"shardingKey changed ('{oldCluster.ShardingKey}' -> '{newCluster.ShardingKey}')");

        if (oldCluster.MaterializedViewsWriteThroughDistributed != newCluster.MaterializedViewsWriteThroughDistributed)
        {
            reasons.Add(
                "materializedViewsWriteThroughDistributed changed "
                    + $"({Format(oldCluster.MaterializedViewsWriteThroughDistributed)} -> "
                    + $"{Format(newCluster.MaterializedViewsWriteThroughDistributed)})");
        }

        return reasons;
    }

    public static IReadOnlyList<string> CompareTable(
        string? oldShardingKey,
        string? newShardingKey,
        bool? oldWriteThroughDistributed,
        bool? newWriteThroughDistributed)
    {
        List<string> reasons = [];

        if (!string.Equals(oldShardingKey?.Trim(), newShardingKey?.Trim(), StringComparison.Ordinal))
        {
            reasons.Add(
                $"shardingKey changed ('{oldShardingKey ?? "<cluster default>"}' -> "
                    + $"'{newShardingKey ?? "<cluster default>"}')");
        }

        if (oldWriteThroughDistributed != newWriteThroughDistributed)
        {
            reasons.Add(
                "materializedViewsWriteThroughDistributed changed "
                    + $"({Format(oldWriteThroughDistributed)} -> {Format(newWriteThroughDistributed)})");
        }

        return reasons;
    }

    private static string Format(bool? value) =>
        value switch
        {
            true => "true",
            false => "false",
            null => "<cluster default>"
        };
}
