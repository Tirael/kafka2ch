using ClickHouseSchemaGen.Migrator;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Microsoft.Extensions.Logging.Abstractions;

namespace ClickHouseSchemaGen.IntegrationTests;

public sealed class ClusterReplicatedDatabaseIntegrationTests : IAsyncLifetime
{
    private const string Image = "clickhouse/clickhouse-server:25.11";
    private const string ClusterName = "kafka2ch";
    private const string DatabaseName = "kafka2ch";

    private readonly INetwork _network = new NetworkBuilder().Build();
    private readonly IContainer _keeper;
    private readonly ClickHouseContainer _shard1;
    private readonly ClickHouseContainer _shard2;
    private readonly string _migrationsDirectory =
        Path.Combine(Path.GetTempPath(), $"kafka2ch-rdb-migrations-{Guid.NewGuid():N}");

    public ClusterReplicatedDatabaseIntegrationTests()
    {
        _keeper = new ContainerBuilder(Image)
            .WithNetwork(_network)
            .WithNetworkAliases("clickhouse-keeper")
            .WithEntrypoint("clickhouse", "keeper", "--config-file=/etc/clickhouse-keeper/keeper.xml")
            .WithResourceMapping(File.ReadAllBytes(RepoPaths.KeeperTestConfigPath), "/etc/clickhouse-keeper/keeper.xml")
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
    public async Task GivenReplicatedDatabaseInit_WhenBootstrappedAndMigrated_ThenDdlReplicatesWithoutOnCluster()
    {
        foreach (var script in Directory.GetFiles(RepoPaths.ClusterReplicatedDbInitDirectory, "*.sql")
                     .Order(StringComparer.Ordinal))
        {
            var execResult = await _shard1.ExecScriptAsync(await File.ReadAllTextAsync(script));
            execResult.ExitCode.Should().Be(0, $"{Path.GetFileName(script)}: {execResult.Stderr}");
        }

        var migrationSql = PersistPartitionMigrationFixture.GenerateSql(RepoPaths.ClusterReplicatedDbCodegenConfigPath);
        migrationSql.Should().Contain("-- await:kafka_consumers_empty orders_queue ON CLUSTER kafka2ch");
        migrationSql.Should().Contain("DETACH TABLE IF EXISTS orders_queue PERMANENTLY;");
        migrationSql.Should().NotContain("DETACH TABLE IF EXISTS orders_queue ON CLUSTER");
        migrationSql.Should().NotContain("ALTER TABLE orders_local ON CLUSTER");
        migrationSql.Should().Contain("CREATE MATERIALIZED VIEW orders_mv TO orders AS");
        await File.WriteAllTextAsync(
            Path.Combine(_migrationsDirectory, PersistPartitionMigrationFixture.FileName),
            migrationSql);

        await using var connection = new ClickHouseConnection(ConnectionString(_shard2));
        await connection.OpenAsync();
        await ExecuteAsync(connection,
            "INSERT INTO orders (order_id, category, amount, quantity, event_time) " +
            "SELECT toString(number), 'books', 1.5, 2, now64(3) FROM numbers(200) " +
            "SETTINGS distributed_foreground_insert = 1");

        await CreateRunner().ApplyAsync(ConnectionString(_shard1), _migrationsDirectory, CancellationToken.None);
        await CreateRunner().ApplyAsync(ConnectionString(_shard2), _migrationsDirectory, CancellationToken.None);

        (await ScalarAsync(connection, "SELECT count() FROM orders")).Should().Be(200);
        (await ScalarAsync(connection, "SELECT sum(orders_count) FROM orders_agg_1m")).Should().Be(200);

        (await QueryRowsAsync(connection,
                "SELECT getMacro('replica'), engine FROM clusterAllReplicas('kafka2ch', system.tables) " +
                $"WHERE database = '{DatabaseName}' AND name = 'orders_mv' ORDER BY 1"))
            .Select(row => $"{row[0]}:{row[1]}")
            .Should().Equal("ch1:MaterializedView", "ch2:MaterializedView");

        (await ScalarAsync(connection,
                "SELECT count() FROM clusterAllReplicas('kafka2ch', system.columns) " +
                $"WHERE database = '{DatabaseName}' AND table IN ('orders_local', 'orders') " +
                $"AND name = '{PersistPartitionMigrationFixture.AddedColumn}'"))
            .Should().Be(4);

        (await QueryRowsAsync(connection,
                "SELECT getMacro('replica'), countIf(kind = 'init'), countIf(version = '" +
                PersistPartitionMigrationFixture.Version + "') " +
                "FROM clusterAllReplicas('kafka2ch', default.schema_migrations) GROUP BY 1 ORDER BY 1"))
            .Select(row => $"{row[0]}:{row[1]}:{row[2]}")
            .Should().Equal("ch1:3:1", "ch2:3:1");
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

    private static string ConnectionString(ClickHouseContainer container) =>
        container.GetConnectionString() + $";Database={DatabaseName}";

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
        new(NullLogger.Instance, TimeProvider.System, new ClusterConfig
        {
            Name = ClusterName,
            DdlMode = ClusterDdlModes.ReplicatedDatabase,
            ReplicatedDatabaseName = DatabaseName
        });

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
