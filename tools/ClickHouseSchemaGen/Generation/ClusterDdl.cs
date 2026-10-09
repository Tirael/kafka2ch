using System.Text.RegularExpressions;

namespace ClickHouseSchemaGen.Generation;

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

    public bool UsesOnCluster => _config?.UsesOnCluster == true;

    public bool UsesReplicatedDatabase => _config?.UsesReplicatedDatabase == true;

    public bool MaterializedViewsWriteThroughDistributed =>
        _config?.MaterializedViewsWriteThroughDistributed == true;

    public string? ReplicatedDatabaseName =>
        UsesReplicatedDatabase ? _config!.ReplicatedDatabaseName.Trim() : null;

    public string OnCluster => UsesOnCluster ? $" ON CLUSTER {Name}" : string.Empty;

    public string HistoryOnCluster => Enabled ? $" ON CLUSTER {Name}" : string.Empty;

    public string HistoryTableName =>
        UsesReplicatedDatabase ? $"default.{SchemaMigrationsTable.TableName}" : SchemaMigrationsTable.TableName;

    public string StorageTable(string tableName) =>
        Enabled ? tableName + _config!.LocalTableSuffix : tableName;

    public string MaterializedViewTarget(string tableName, bool? writeThroughDistributed = null)
    {
        if (!Enabled)
            return tableName;

        var through = writeThroughDistributed ?? _config!.MaterializedViewsWriteThroughDistributed;
        return through ? tableName : StorageTable(tableName);
    }

    public string StorageEngine(string engine) =>
        !Enabled
            ? engine
            : UsesReplicatedDatabase
                ? BareReplicated(engine)
                : Replicated(engine, _config!.ReplicatedPath, _config.ReplicaName);

    public string HistoryEngine(string engine) =>
        !Enabled
            ? engine
            : Replicated(engine, _config!.HistoryReplicatedPath, _config.HistoryReplicaName);

    public string DistributedEngine(string tableName, string? shardingKey)
    {
        EnsureEnabled();
        var key = string.IsNullOrWhiteSpace(shardingKey) ? _config!.ShardingKey : shardingKey.Trim();
        return $"Distributed('{EscapeLiteral(Name!)}', currentDatabase(), {StorageTable(tableName)}, {key})";
    }

    public string DistributedTableStatement(string tableName, string? shardingKey, bool ifNotExists)
    {
        if (!Enabled)
            return string.Empty;

        var create = ifNotExists ? "CREATE TABLE IF NOT EXISTS" : "CREATE TABLE";
        return $"{create} {tableName}{OnCluster} AS {StorageTable(tableName)}" + Environment.NewLine +
               $"ENGINE = {DistributedEngine(tableName, shardingKey)};" + Environment.NewLine;
    }

    public string CreateReplicatedDatabaseSql()
    {
        if (!UsesReplicatedDatabase)
            return string.Empty;

        var database = _config!.ReplicatedDatabaseName.Trim();
        var path = _config.ReplicatedDatabasePath.Trim();
        var replica = _config.ReplicatedDatabaseReplicaName.Trim();
        return $"CREATE DATABASE IF NOT EXISTS {database} ON CLUSTER {Name}" + Environment.NewLine +
               $"ENGINE = Replicated('{EscapeLiteral(path)}', '{EscapeLiteral(replica)}');" + Environment.NewLine;
    }

    public string UseReplicatedDatabaseSql() =>
        UsesReplicatedDatabase ? $"USE {ReplicatedDatabaseName};" + Environment.NewLine : string.Empty;

    public string WithDatabaseContext(string sql) =>
        UsesReplicatedDatabase ? UseReplicatedDatabaseSql() + Environment.NewLine + sql : sql;

    public string DetachTableSql(string tableName) =>
        UsesReplicatedDatabase
            ? $"DETACH TABLE IF EXISTS {tableName} PERMANENTLY;"
            : $"DETACH TABLE IF EXISTS {tableName}{OnCluster};";

    public string AttachTableSql(string tableName) =>
        $"ATTACH TABLE {tableName}{OnCluster};";

    public string AwaitKafkaConsumersEmpty(string queueName)
    {
        var clusterSuffix = Enabled && Name is not null ? $" ON CLUSTER {Name}" : string.Empty;
        return $"-- await:kafka_consumers_empty {queueName}{clusterSuffix}";
    }

    public string ProtobufWireFunctionDefinitions() =>
        UsesOnCluster
            ? CreateFunctionRegex().Replace(ProtobufWireSqlFunctions.Definitions, match => match.Value + OnCluster)
            : ProtobufWireSqlFunctions.Definitions;

    public static string Replicated(string engine, string path, string replicaName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(engine);

        var (name, arguments) = ParseMergeTreeEngine(engine);
        if (name.StartsWith("Replicated", StringComparison.Ordinal))
            return engine.Trim();

        var replicated = $"'{EscapeLiteral(path)}', '{EscapeLiteral(replicaName)}'";
        return arguments.Length == 0
            ? $"Replicated{name}({replicated})"
            : $"Replicated{name}({replicated}, {arguments})";
    }

    public static string BareReplicated(string engine)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(engine);

        var (name, arguments) = ParseMergeTreeEngine(engine);
        if (name.StartsWith("Replicated", StringComparison.Ordinal))
        {
            var bareName = name;
            return arguments.Length == 0 ? bareName : $"{bareName}({arguments})";
        }

        return arguments.Length == 0 ? $"Replicated{name}" : $"Replicated{name}({arguments})";
    }

    private static (string Name, string Arguments) ParseMergeTreeEngine(string engine)
    {
        var trimmed = engine.Trim();
        var open = trimmed.IndexOf('(');
        var name = (open < 0 ? trimmed : trimmed[..open]).Trim();
        if (!name.EndsWith("MergeTree", StringComparison.Ordinal)
            && !name.StartsWith("Replicated", StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"Engine '{engine}' is not a MergeTree-family engine.");
        }

        var arguments = open < 0 ? string.Empty : trimmed[(open + 1)..trimmed.LastIndexOf(')')].Trim();
        return (name, arguments);
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
