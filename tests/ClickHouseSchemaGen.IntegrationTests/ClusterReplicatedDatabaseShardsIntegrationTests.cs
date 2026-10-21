namespace ClickHouseSchemaGen.IntegrationTests;

public sealed class ClusterReplicatedDatabaseShardsIntegrationTests : IAsyncLifetime
{
    private const string ClusterName = "kafka2ch";
    private const string DatabaseName = "kafka2ch";

    private readonly INetwork _network = new NetworkBuilder().Build();
    private readonly IContainer _keeper;
    private readonly ClickHouseContainer _shard1Replica1;
    private readonly ClickHouseContainer _shard1Replica2;
    private readonly ClickHouseContainer _shard2Replica1;
    private readonly ClickHouseContainer _shard2Replica2;

    public ClusterReplicatedDatabaseShardsIntegrationTests()
    {
        TestContainersEnv.SetVariables();
        _keeper = new ContainerBuilder(TestContainersEnv.FullClickHouseImageName)
            .WithNetwork(_network)
            .WithNetworkAliases("clickhouse-keeper")
            .WithEntrypoint("clickhouse", "keeper", "--config-file=/etc/clickhouse-keeper/keeper.xml")
            .WithResourceMapping(File.ReadAllBytes(TestContractsPaths.KeeperTestConfigPath), "/etc/clickhouse-keeper/keeper.xml")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(9181))
            .Build();
        _shard1Replica1 = CreateNode("ch1", shard: "01");
        _shard1Replica2 = CreateNode("ch2", shard: "01");
        _shard2Replica1 = CreateNode("ch3", shard: "02");
        _shard2Replica2 = CreateNode("ch4", shard: "02");
    }

    private IEnumerable<ClickHouseContainer> Nodes =>
        [_shard1Replica1, _shard1Replica2, _shard2Replica1, _shard2Replica2];

    public async Task InitializeAsync()
    {
        await _network.CreateAsync();
        await _keeper.StartAsync();
        await Task.WhenAll(Nodes.Select(node => node.StartAsync()));
    }

    public async Task DisposeAsync()
    {
        await Task.WhenAll(Nodes.Select(node => node.DisposeAsync().AsTask()));
        await _keeper.DisposeAsync();
        await _network.DisposeAsync();
    }

    [Fact]
    public async Task GivenTwoShardsWithTwoReplicas_WhenRowsInsertedIntoLocalTable_ThenReplicasOfSameShardShareData()
    {
        foreach (var (fileName, sql) in InitScriptsFixture.Generate(TestContractsPaths.ClusterReplicatedDbCodegenConfigPath))
        {
            var execResult = await _shard1Replica1.ExecScriptAsync(sql);
            execResult.ExitCode.Should().Be(0, $"{fileName}: {execResult.Stderr}");
        }

        await using var shard1Replica1 = await OpenAsync(_shard1Replica1);
        await using var shard1Replica2 = await OpenAsync(_shard1Replica2);
        await using var shard2Replica1 = await OpenAsync(_shard2Replica1);
        await using var shard2Replica2 = await OpenAsync(_shard2Replica2);

        await InsertLocalAsync(shard1Replica1, rows: 100, offset: 0);
        await InsertLocalAsync(shard2Replica1, rows: 50, offset: 1000);

        foreach (var replica in new[] { shard1Replica2, shard2Replica2 })
        {
            await ExecuteAsync(replica, $"SYSTEM SYNC DATABASE REPLICA {DatabaseName}");
            await ExecuteAsync(replica, "SYSTEM SYNC REPLICA orders_local");
        }

        (await ScalarAsync(shard1Replica2, "SELECT count() FROM orders_local"))
            .Should().Be(100, "replica 2 of shard 1 must receive rows inserted on replica 1 of shard 1");
        (await ScalarAsync(shard2Replica2, "SELECT count() FROM orders_local"))
            .Should().Be(50, "replica 2 of shard 2 must receive rows inserted on replica 1 of shard 2");

        foreach (var connection in new[] { shard1Replica1, shard1Replica2, shard2Replica1, shard2Replica2 })
        {
            (await ScalarAsync(connection, "SELECT count() FROM orders")).Should().Be(150);
        }

        await ExecuteAsync(shard1Replica1, "ALTER TABLE orders_local ADD COLUMN probe UInt8 DEFAULT 1");
        await ExecuteAsync(shard1Replica1, "ALTER TABLE orders ADD COLUMN probe UInt8 DEFAULT 1");
        (await QueryRowsAsync(shard1Replica1,
                "SELECT getMacro('replica'), table FROM clusterAllReplicas('kafka2ch', system.columns) " +
                $"WHERE database = '{DatabaseName}' AND table IN ('orders_local', 'orders') AND name = 'probe' " +
                "ORDER BY 1, 2"))
            .Select(row => $"{row[0]}:{row[1]}")
            .Should().Equal(
                "ch1:orders", "ch1:orders_local", "ch2:orders", "ch2:orders_local",
                "ch3:orders", "ch3:orders_local", "ch4:orders", "ch4:orders_local");

        (await QueryRowsAsync(shard1Replica1,
                "SELECT count(DISTINCT uuid) FROM clusterAllReplicas('kafka2ch', system.databases) " +
                $"WHERE name = '{DatabaseName}'"))
            .Select(row => Convert.ToInt64(row[0]))
            .Should().Equal(1);

        (await QueryRowsAsync(shard1Replica1,
                "SELECT arrayStringConcat(arraySort(groupArray(replica_name)), ','), " +
                "any(total_replicas), any(active_replicas) " +
                "FROM clusterAllReplicas('kafka2ch', system.replicas) " +
                $"WHERE database = '{DatabaseName}' AND table = 'orders_local' " +
                "GROUP BY zookeeper_path ORDER BY 1"))
            .Select(row => $"{row[0]}:{row[1]}:{row[2]}")
            .Should().Equal("ch1,ch2:2:2", "ch3,ch4:2:2");

        (await QueryRowsAsync(shard1Replica1,
                "SELECT shard_name, replica_name " +
                "FROM clusterAllReplicas('kafka2ch', system.database_replicas) " +
                $"WHERE database = '{DatabaseName}' ORDER BY shard_name, replica_name"))
            .Select(row => $"{row[0]}|{row[1]}")
            .Should().Equal("01|ch1", "01|ch2", "02|ch3", "02|ch4");
    }

    [Fact]
    public async Task GivenDedupPipelineOnTwoShards_WhenMessagesStoredTwice_ThenEveryNodeAggregatesEachMessageOnce()
    {
        foreach (var (fileName, sql) in InitScriptsFixture.Generate(TestContractsPaths.ClusterReplicatedDbDedupCodegenConfigPath))
        {
            var execResult = await _shard1Replica1.ExecScriptAsync(sql);
            execResult.ExitCode.Should().Be(0, $"{fileName}: {execResult.Stderr}");
        }

        await using var shard1Replica1 = await OpenAsync(_shard1Replica1);
        await using var shard1Replica2 = await OpenAsync(_shard1Replica2);
        await using var shard2Replica1 = await OpenAsync(_shard2Replica1);
        await using var shard2Replica2 = await OpenAsync(_shard2Replica2);
        ClickHouseConnection[] connections = [shard1Replica1, shard1Replica2, shard2Replica1, shard2Replica2];

        foreach (var connection in connections)
        {
            await ExecuteAsync(connection, $"SYSTEM SYNC DATABASE REPLICA {DatabaseName}");
            await ExecuteAsync(connection, "SYSTEM STOP MERGES orders_local");
        }

        await InsertKafkaMessagesAsync(shard1Replica1, rows: 100, offset: 0);
        await InsertKafkaMessagesAsync(shard2Replica1, rows: 50, offset: 1000);
        foreach (var connection in new[] { shard1Replica1, shard2Replica1 })
            await ExecuteAsync(connection, "INSERT INTO orders_local SELECT * FROM orders_local SETTINGS insert_deduplicate = 0");

        foreach (var connection in connections)
            await ExecuteAsync(connection, "SYSTEM SYNC REPLICA orders_local");

        foreach (var connection in connections)
        {
            (await ScalarAsync(connection, "SELECT count() FROM orders")).Should().Be(300);
            (await ScalarAsync(connection, "SELECT count() FROM orders FINAL")).Should().Be(150);
        }

        foreach (var connection in connections)
        {
            await ExecuteAsync(connection, "SYSTEM REFRESH VIEW default.orders_agg_mv");
            await ExecuteAsync(connection, "SYSTEM WAIT VIEW default.orders_agg_mv");
        }

        (await QueryRowsAsync(shard1Replica1,
                "SELECT getMacro('replica'), sum(orders_count) " +
                "FROM clusterAllReplicas('kafka2ch', default.orders_agg_1m) GROUP BY 1 ORDER BY 1"))
            .Select(row => $"{row[0]}:{row[1]}")
            .Should().Equal("ch1:150", "ch2:150", "ch3:150", "ch4:150");
        foreach (var connection in connections)
            (await ScalarAsync(connection, "SELECT sum(orders_count) FROM default.orders_agg_1m")).Should().Be(150);
    }

    private static Task InsertKafkaMessagesAsync(ClickHouseConnection connection, int rows, int offset) =>
        ExecuteAsync(connection,
            "INSERT INTO orders_local (order_id, category, amount, quantity, event_time, kafka_partition, kafka_offset) " +
            $"SELECT toString(number + {offset}), 'books', 1.5, 2, now64(3), number % 2, intDiv(number, 2) + {offset} " +
            $"FROM numbers({rows})");

    private ClickHouseContainer CreateNode(string host, string shard) =>
        new ClickHouseBuilder(TestContainersEnv.FullClickHouseImageName)
            .WithNetwork(_network)
            .WithNetworkAliases(host)
            .WithEnvironment("CLICKHOUSE_SHARD", shard)
            .WithEnvironment("CLICKHOUSE_REPLICA", host)
            .WithResourceMapping(FormatSchemas.Directory, "/var/lib/clickhouse/format_schemas")
            .WithResourceMapping(Encoding.UTF8.GetBytes(ClusterXml), "/etc/clickhouse-server/config.d/cluster.xml")
            .Build();

    private static async Task<ClickHouseConnection> OpenAsync(ClickHouseContainer container)
    {
        var connection = new ClickHouseConnection(container.GetConnectionString() + $";Database={DatabaseName}");
        await connection.OpenAsync();
        return connection;
    }

    private static Task InsertLocalAsync(ClickHouseConnection connection, int rows, int offset) =>
        ExecuteAsync(connection,
            "INSERT INTO orders_local (order_id, category, amount, quantity, event_time) " +
            $"SELECT toString(number + {offset}), 'books', 1.5, 2, now64(3) FROM numbers({rows})");

    private static string Replica(string host) =>
        $"<replica><host>{host}</host><port>9000</port><user>{ClickHouseBuilder.DefaultUsername}</user><password>{ClickHouseBuilder.DefaultPassword}</password></replica>";

    private static string ClusterXml => $"""
        <clickhouse>
            <remote_servers>
                <{ClusterName}>
                    <shard>
                        <internal_replication>true</internal_replication>
                        {Replica("ch1")}
                        {Replica("ch2")}
                    </shard>
                    <shard>
                        <internal_replication>true</internal_replication>
                        {Replica("ch3")}
                        {Replica("ch4")}
                    </shard>
                </{ClusterName}>
            </remote_servers>
            <zookeeper><node><host>clickhouse-keeper</host><port>9181</port></node></zookeeper>
            <macros><shard from_env="CLICKHOUSE_SHARD"/><replica from_env="CLICKHOUSE_REPLICA"/></macros>
            <interserver_http_host from_env="CLICKHOUSE_REPLICA"/>
        </clickhouse>
        """;

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
