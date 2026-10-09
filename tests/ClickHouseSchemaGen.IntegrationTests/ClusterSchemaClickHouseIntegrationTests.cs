using ClickHouseSchemaGen.Migrator;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Microsoft.Extensions.Logging.Abstractions;

namespace ClickHouseSchemaGen.IntegrationTests;

/// <summary>
/// Two shards x one replica + Keeper: committed cluster init scripts, Distributed reads/writes and a generated
/// ON CLUSTER migration applied by the migrator. Replica-level data replication is covered by the compose stack.
/// </summary>
public sealed class ClusterSchemaClickHouseIntegrationTests : IAsyncLifetime
{
    private const string Image = "clickhouse/clickhouse-server:25.11";
    private const string ClusterName = "kafka2ch";

    private readonly INetwork _network = new NetworkBuilder().Build();
    private readonly IContainer _keeper;
    private readonly ClickHouseContainer _shard1;
    private readonly ClickHouseContainer _shard2;
    private readonly string _migrationsDirectory =
        Path.Combine(Path.GetTempPath(), $"kafka2ch-cluster-migrations-{Guid.NewGuid():N}");

    public ClusterSchemaClickHouseIntegrationTests()
    {
        _keeper = new ContainerBuilder(Image)
            .WithNetwork(_network)
            .WithNetworkAliases("clickhouse-keeper")
            .WithEntrypoint("clickhouse", "keeper", "--config-file=/etc/clickhouse-keeper/keeper.xml")
            .WithResourceMapping(
                File.ReadAllBytes(Path.Combine(RepoPaths.RepositoryRoot, "docker", "clickhouse-cluster", "config", "keeper.xml")),
                "/etc/clickhouse-keeper/keeper.xml")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(9181))
            .Build();
        _shard1 = CreateNode("ch1", shard: "01");
        _shard2 = CreateNode("ch2", shard: "02");
    }

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_migrationsDirectory);
        await _network.CreateAsync();
        await _keeper.StartAsync();
        await Task.WhenAll(_shard1.StartAsync(), _shard2.StartAsync());
    }

    public async Task DisposeAsync()
    {
        await _shard1.DisposeAsync();
        await _shard2.DisposeAsync();
        await _keeper.DisposeAsync();
        await _network.DisposeAsync();
        Directory.Delete(_migrationsDirectory, recursive: true);
    }

    [Fact]
    public async Task GivenCommittedClusterInitScripts_WhenBootstrappedAndMigrated_ThenSchemaAndHistoryExistOnEveryNode()
    {
        // Arrange
        foreach (var script in Directory.GetFiles(RepoPaths.ClusterInitDirectory, "*.sql").Order(StringComparer.Ordinal))
        {
            var execResult = await _shard1.ExecScriptAsync(await File.ReadAllTextAsync(script));
            execResult.ExitCode.Should().Be(0, $"{Path.GetFileName(script)}: {execResult.Stderr}");
        }

        await File.WriteAllTextAsync(
            Path.Combine(_migrationsDirectory, PersistPartitionMigrationFixture.FileName),
            PersistPartitionMigrationFixture.GenerateSql(RepoPaths.ClusterCodegenConfigPath));

        await using var connection = new ClickHouseConnection(_shard2.GetConnectionString());
        await connection.OpenAsync();
        await ExecuteAsync(connection,
            "INSERT INTO orders (order_id, category, amount, quantity, event_time) " +
            "SELECT toString(number), 'books', 1.5, 2, now64(3) FROM numbers(200) " +
            "SETTINGS distributed_foreground_insert = 1");

        // Act
        await CreateRunner().ApplyAsync(_shard2.GetConnectionString(), _migrationsDirectory, CancellationToken.None);
        await CreateRunner().ApplyAsync(_shard1.GetConnectionString(), _migrationsDirectory, CancellationToken.None);

        // Assert
        (await QueryRowsAsync(connection,
                "SELECT getMacro('replica'), name, engine FROM clusterAllReplicas('kafka2ch', system.tables) " +
                "WHERE database = currentDatabase() AND name IN ('orders_local', 'orders', 'orders_queue', 'orders_mv', " +
                "'orders_agg_1m_local', 'orders_agg_1m', 'schema_migrations') ORDER BY 1, 2"))
            .Select(row => $"{row[0]}:{row[1]}:{row[2]}")
            .Should().Equal(
                new[] { "ch1", "ch2" }.SelectMany(host => new[]
                {
                    $"{host}:orders:Distributed",
                    $"{host}:orders_agg_1m:Distributed",
                    $"{host}:orders_agg_1m_local:ReplicatedSummingMergeTree",
                    $"{host}:orders_local:ReplicatedMergeTree",
                    $"{host}:orders_mv:MaterializedView",
                    $"{host}:orders_queue:Kafka",
                    $"{host}:schema_migrations:ReplicatedMergeTree"
                }));

        (await QueryRowsAsync(connection,
                "SELECT getMacro('replica'), count() FROM clusterAllReplicas('kafka2ch', currentDatabase(), orders_local) GROUP BY 1 ORDER BY 1"))
            .Should().HaveCount(2).And.OnlyContain(row => Convert.ToInt64(row[1]) > 0);
        (await ScalarAsync(connection, "SELECT count() FROM orders")).Should().Be(200);
        (await ScalarAsync(connection, "SELECT sum(orders_count) FROM orders_agg_1m")).Should().Be(200);

        (await ScalarAsync(connection,
                "SELECT count() FROM clusterAllReplicas('kafka2ch', system.columns) WHERE database = currentDatabase() " +
                $"AND table IN ('orders_local', 'orders') AND name = '{PersistPartitionMigrationFixture.AddedColumn}'"))
            .Should().Be(4);

        (await QueryRowsAsync(connection,
                "SELECT getMacro('replica'), count(), countIf(kind = 'init'), countIf(version = '" + PersistPartitionMigrationFixture.Version + "') " +
                "FROM clusterAllReplicas('kafka2ch', currentDatabase(), schema_migrations) GROUP BY 1 ORDER BY 1"))
            .Select(row => $"{row[0]}:{row[1]}:{row[2]}:{row[3]}")
            .Should().Equal("ch1:4:3:1", "ch2:4:3:1");
    }

    private ClickHouseContainer CreateNode(string host, string shard) =>
        new ClickHouseBuilder(Image)
            .WithNetwork(_network)
            .WithNetworkAliases(host)
            .WithEnvironment("CLICKHOUSE_SHARD", shard)
            .WithEnvironment("CLICKHOUSE_REPLICA", host)
            .WithBindMount(RepoPaths.FormatSchemasDirectory, "/var/lib/clickhouse/format_schemas")
            .WithResourceMapping(Encoding.UTF8.GetBytes(ClusterXml), "/etc/clickhouse-server/config.d/cluster.xml")
            .Build();

    private static string ClusterXml => $"""
        <clickhouse>
            <remote_servers>
                <{ClusterName}>
                    <shard>
                        <replica><host>ch1</host><port>9000</port><user>{ClickHouseBuilder.DefaultUsername}</user><password>{ClickHouseBuilder.DefaultPassword}</password></replica>
                    </shard>
                    <shard>
                        <replica><host>ch2</host><port>9000</port><user>{ClickHouseBuilder.DefaultUsername}</user><password>{ClickHouseBuilder.DefaultPassword}</password></replica>
                    </shard>
                </{ClusterName}>
            </remote_servers>
            <zookeeper><node><host>clickhouse-keeper</host><port>9181</port></node></zookeeper>
            <macros><shard from_env="CLICKHOUSE_SHARD"/><replica from_env="CLICKHOUSE_REPLICA"/></macros>
            <interserver_http_host from_env="CLICKHOUSE_REPLICA"/>
        </clickhouse>
        """;

    private static MigrationRunner CreateRunner() =>
        new(NullLogger.Instance, TimeProvider.System, new ClusterConfig { Name = ClusterName });

    private static async Task ExecuteAsync(ClickHouseConnection connection, string sql)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<long> ScalarAsync(ClickHouseConnection connection, string sql)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(await command.ExecuteScalarAsync());
    }

    private static async Task<List<object[]>> QueryRowsAsync(ClickHouseConnection connection, string sql)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<object[]>();
        while (await reader.ReadAsync())
        {
            var row = new object[reader.FieldCount];
            reader.GetValues(row);
            rows.Add(row);
        }

        return rows;
    }
}
