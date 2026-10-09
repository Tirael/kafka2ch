using System.Text.RegularExpressions;

namespace ClickHouseSchemaGen.Generation;

/// <summary>
/// Renders the topology-dependent parts of DDL. In cluster mode every statement runs <c>ON CLUSTER</c>,
/// a configured MergeTree table <c>t</c> becomes a per-node <c>Replicated*MergeTree</c> table <c>t_local</c>
/// plus a <c>Distributed</c> table <c>t</c>, and Kafka queues / materialized views exist on every node.
/// Single-node rendering is the identity.
/// </summary>
public sealed partial class ClusterDdl
{
    private readonly ClusterConfig? _config;

    public ClusterDdl(ClusterConfig? config)
    {
        _config = config?.Enabled == true ? config : null;
    }

    public static ClusterDdl SingleNode { get; } = new(null);

    public static ClusterDdl For(CodegenConfig config) => new(config.Cluster);

    public bool Enabled => _config is not null;

    public string? Name => _config?.Name?.Trim();

    /// <summary>Clause to append after the object name, with a leading space; empty on a single node.</summary>
    public string OnCluster => Enabled ? $" ON CLUSTER {Name}" : string.Empty;

    /// <summary>Name of the table that physically stores rows of the configured table.</summary>
    public string StorageTable(string tableName) =>
        Enabled ? tableName + _config!.LocalTableSuffix : tableName;

    public string StorageEngine(string engine) =>
        Enabled ? Replicated(engine, _config!.ReplicatedPath, _config.ReplicaName) : engine;

    public string HistoryEngine(string engine) =>
        Enabled ? Replicated(engine, _config!.HistoryReplicatedPath, _config.HistoryReplicaName) : engine;

    public string DistributedEngine(string tableName, string? shardingKey)
    {
        EnsureEnabled();
        var key = string.IsNullOrWhiteSpace(shardingKey) ? _config!.ShardingKey : shardingKey.Trim();
        return $"Distributed('{EscapeLiteral(Name!)}', currentDatabase(), {StorageTable(tableName)}, {key})";
    }

    /// <summary>
    /// <c>CREATE TABLE t ON CLUSTER c AS t_local ENGINE = Distributed(...)</c>; empty on a single node.
    /// </summary>
    public string DistributedTableStatement(string tableName, string? shardingKey, bool ifNotExists)
    {
        if (!Enabled)
            return string.Empty;

        var create = ifNotExists ? "CREATE TABLE IF NOT EXISTS" : "CREATE TABLE";
        return $"{create} {tableName}{OnCluster} AS {StorageTable(tableName)}" + Environment.NewLine +
               $"ENGINE = {DistributedEngine(tableName, shardingKey)};" + Environment.NewLine;
    }

    /// <summary>Marker the migrator turns into a wait for detached Kafka consumers (on every replica in cluster mode).</summary>
    public string AwaitKafkaConsumersEmpty(string queueName) =>
        $"-- await:kafka_consumers_empty {queueName}{OnCluster}";

    public string ProtobufWireFunctionDefinitions() =>
        Enabled
            ? CreateFunctionRegex().Replace(ProtobufWireSqlFunctions.Definitions, match => match.Value + OnCluster)
            : ProtobufWireSqlFunctions.Definitions;

    /// <summary>
    /// <c>XMergeTree[(args)]</c> → <c>ReplicatedXMergeTree('path', 'replica'[, args])</c>. Already replicated engines are kept.
    /// </summary>
    public static string Replicated(string engine, string path, string replicaName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(engine);

        var trimmed = engine.Trim();
        var open = trimmed.IndexOf('(');
        var name = (open < 0 ? trimmed : trimmed[..open]).Trim();
        if (name.StartsWith("Replicated", StringComparison.Ordinal))
            return trimmed;

        if (!name.EndsWith("MergeTree", StringComparison.Ordinal))
            throw new InvalidOperationException($"Engine '{engine}' is not a MergeTree-family engine.");

        var arguments = open < 0 ? string.Empty : trimmed[(open + 1)..trimmed.LastIndexOf(')')].Trim();
        var replicated = $"'{EscapeLiteral(path)}', '{EscapeLiteral(replicaName)}'";
        return arguments.Length == 0
            ? $"Replicated{name}({replicated})"
            : $"Replicated{name}({replicated}, {arguments})";
    }

    private void EnsureEnabled()
    {
        if (!Enabled)
            throw new InvalidOperationException("Cluster mode is disabled.");
    }

    private static string EscapeLiteral(string value) => value.Replace("'", "''", StringComparison.Ordinal);

    [GeneratedRegex(@"(?m)^\s*CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+[A-Za-z_][A-Za-z0-9_]*", RegexOptions.CultureInvariant)]
    private static partial Regex CreateFunctionRegex();
}
