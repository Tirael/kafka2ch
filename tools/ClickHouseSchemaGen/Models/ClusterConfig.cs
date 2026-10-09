namespace ClickHouseSchemaGen.Models;

/// <summary>
/// Clustered ClickHouse deployment. Disabled while <see cref="Name"/> is empty: DDL stays single-node.
/// </summary>
public sealed class ClusterConfig
{
    public const string DefaultReplicatedPath = "/clickhouse/tables/{shard}/{database}/{table}";

    public const string DefaultReplicaName = "{replica}";

    public const string DefaultHistoryReplicatedPath = "/clickhouse/tables/all/{database}/{table}";

    public const string DefaultHistoryReplicaName = "{shard}-{replica}";

    public const string DefaultLocalTableSuffix = "_local";

    public const string DefaultShardingKey = "rand()";

    /// <summary>
    /// Cluster name from <c>remote_servers</c>; used in <c>ON CLUSTER</c> and <c>Distributed</c> engines.
    /// </summary>
    public string? Name { get; set; }

    /// <summary>
    /// Keeper path of <c>Replicated*MergeTree</c> storage tables (one replica set per shard).
    /// </summary>
    public string ReplicatedPath { get; set; } = DefaultReplicatedPath;

    public string ReplicaName { get; set; } = DefaultReplicaName;

    /// <summary>
    /// Keeper path of <c>schema_migrations</c>. It must not contain <c>{shard}</c>: every node keeps the full history.
    /// </summary>
    public string HistoryReplicatedPath { get; set; } = DefaultHistoryReplicatedPath;

    /// <summary>
    /// Replica name of <c>schema_migrations</c>; unique across the whole cluster, not just within a shard.
    /// </summary>
    public string HistoryReplicaName { get; set; } = DefaultHistoryReplicaName;

    /// <summary>
    /// Suffix of per-node storage tables; the configured table name becomes the <c>Distributed</c> table.
    /// </summary>
    public string LocalTableSuffix { get; set; } = DefaultLocalTableSuffix;

    /// <summary>
    /// Sharding key of <c>Distributed</c> tables without their own <see cref="MergeTreeTableConfig.ShardingKey"/>.
    /// </summary>
    public string ShardingKey { get; set; } = DefaultShardingKey;

    [System.Text.Json.Serialization.JsonIgnore]
    public bool Enabled => !string.IsNullOrWhiteSpace(Name);
}
