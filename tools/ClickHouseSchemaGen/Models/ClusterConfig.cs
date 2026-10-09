namespace ClickHouseSchemaGen.Models;

public static class ClusterDdlModes
{
    public const string OnCluster = "onCluster";

    public const string ReplicatedDatabase = "replicatedDatabase";

    public static readonly string[] All = [OnCluster, ReplicatedDatabase];
}

public sealed class ClusterConfig
{
    public const string DefaultReplicatedPath = "/clickhouse/tables/{shard}/{database}/{table}";

    public const string DefaultReplicaName = "{replica}";

    public const string DefaultHistoryReplicatedPath = "/clickhouse/tables/all/{database}/{table}";

    public const string DefaultHistoryReplicaName = "{shard}-{replica}";

    public const string DefaultLocalTableSuffix = "_local";

    public const string DefaultShardingKey = "rand()";

    public const string DefaultReplicatedDatabaseName = "kafka2ch";

    public const string DefaultReplicatedDatabasePath = "/clickhouse/databases/{uuid}";

    public string? Name { get; set; }

    public string DdlMode { get; set; } = ClusterDdlModes.OnCluster;

    public string ReplicatedPath { get; set; } = DefaultReplicatedPath;

    public string ReplicaName { get; set; } = DefaultReplicaName;

    public string HistoryReplicatedPath { get; set; } = DefaultHistoryReplicatedPath;

    public string HistoryReplicaName { get; set; } = DefaultHistoryReplicaName;

    public string LocalTableSuffix { get; set; } = DefaultLocalTableSuffix;

    public string ShardingKey { get; set; } = DefaultShardingKey;

    public bool MaterializedViewsWriteThroughDistributed { get; set; }

    public string ReplicatedDatabaseName { get; set; } = DefaultReplicatedDatabaseName;

    public string ReplicatedDatabasePath { get; set; } = DefaultReplicatedDatabasePath;

    public string ReplicatedDatabaseReplicaName { get; set; } = DefaultReplicaName;

    [System.Text.Json.Serialization.JsonIgnore]
    public bool Enabled => !string.IsNullOrWhiteSpace(Name);

    [System.Text.Json.Serialization.JsonIgnore]
    public bool UsesOnCluster =>
        Enabled && string.Equals(DdlMode, ClusterDdlModes.OnCluster, StringComparison.OrdinalIgnoreCase);

    [System.Text.Json.Serialization.JsonIgnore]
    public bool UsesReplicatedDatabase =>
        Enabled && string.Equals(DdlMode, ClusterDdlModes.ReplicatedDatabase, StringComparison.OrdinalIgnoreCase);
}
