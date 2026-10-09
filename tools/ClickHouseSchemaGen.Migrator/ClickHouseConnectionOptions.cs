using ClickHouseSchemaGen.Models;

namespace ClickHouseSchemaGen.Migrator;

public sealed class ClickHouseConnectionOptions
{
    public string Host { get; init; } = Environment.GetEnvironmentVariable("ClickHouse__Host") ?? "localhost";

    public int Port { get; init; } = int.TryParse(Environment.GetEnvironmentVariable("ClickHouse__Port"), out var port)
        ? port
        : 8123;

    public string Database { get; init; } =
        Environment.GetEnvironmentVariable("ClickHouse__Database") ?? "default";

    public string Username { get; init; } =
        Environment.GetEnvironmentVariable("ClickHouse__Username") ?? "default";

    public string Password { get; init; } =
        Environment.GetEnvironmentVariable("ClickHouse__Password") ?? "";

    public string? Cluster { get; init; } = Environment.GetEnvironmentVariable("ClickHouse__Cluster");

    public string DdlMode { get; init; } =
        Environment.GetEnvironmentVariable("ClickHouse__DdlMode") is { Length: > 0 } mode
            ? mode
            : ClusterDdlModes.OnCluster;

    public string HistoryReplicatedPath { get; init; } =
        Environment.GetEnvironmentVariable("ClickHouse__HistoryReplicatedPath") is { Length: > 0 } path
            ? path
            : ClusterConfig.DefaultHistoryReplicatedPath;

    public string HistoryReplicaName { get; init; } =
        Environment.GetEnvironmentVariable("ClickHouse__HistoryReplicaName") is { Length: > 0 } replica
            ? replica
            : ClusterConfig.DefaultHistoryReplicaName;

    public ClusterConfig ToClusterConfig() => new()
    {
        Name = Cluster,
        DdlMode = string.IsNullOrWhiteSpace(Cluster) ? ClusterDdlModes.OnCluster : DdlMode,
        HistoryReplicatedPath = HistoryReplicatedPath,
        HistoryReplicaName = HistoryReplicaName,
        ReplicatedDatabaseName = Database
    };

    public string ConnectionString
    {
        get
        {
            var connection = $"Host={Host};Port={Port};Username={Username};";
            if (!string.IsNullOrEmpty(Password))
                connection += $"Password={Password};";
            return connection + $"Database={Database}";
        }
    }
}
