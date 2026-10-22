namespace ClickHouseSchemaGen.IntegrationTests;

public sealed class ClusterSchemaClickHouseIntegrationTests : IAsyncLifetime
{
    private const string ClusterName = "kafka2ch";

    private readonly INetwork _network = new NetworkBuilder().Build();
    private readonly IContainer _keeper;
    private readonly ClickHouseContainer _shard1;
    private readonly ClickHouseContainer _shard2;
    private readonly string _migrationsDirectory =
        Path.Combine(Path.GetTempPath(), $"kafka2ch-cluster-migrations-{Guid.NewGuid():N}");

    public ClusterSchemaClickHouseIntegrationTests()
    {
        TestContainersEnv.SetVariables();
        _keeper = new ContainerBuilder(TestContainersEnv.FullClickHouseImageName)
            .WithNetwork(_network)
            .WithNetworkAliases("clickhouse-keeper")
            .WithEntrypoint("clickhouse", "keeper", "--config-file=/etc/clickhouse-keeper/keeper.xml")
            .WithResourceMapping(File.ReadAllBytes(TestContractsPaths.KeeperTestConfigPath), "/etc/clickhouse-keeper/keeper.xml")
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
    public async Task GivenGeneratedClusterInitScripts_WhenBootstrappedAndMigrated_ThenSchemaAndHistoryExistOnEveryNode()
    {
        foreach (var (fileName, sql) in InitScriptsFixture.Generate(TestContractsPaths.ClusterCodegenConfigPath))
        {
            var execResult = await _shard1.ExecScriptAsync(sql);
            execResult.ExitCode.Should().Be(0, $"{fileName}: {execResult.Stderr}");
        }

        await File.WriteAllTextAsync(
            Path.Combine(_migrationsDirectory, PersistPartitionMigrationFixture.FileName),
            PersistPartitionMigrationFixture.GenerateSql(TestContractsPaths.ClusterCodegenConfigPath));

        await using var connection = new ClickHouseConnection(_shard2.GetConnectionString());
        await connection.OpenAsync();
        await ExecuteAsync(connection,
            "INSERT INTO orders (order_id, category, amount, quantity, event_time) " +
            "SELECT toString(number), 'books', 1.5, 2, now64(3) FROM numbers(200) " +
            "SETTINGS distributed_foreground_insert = 1");

        await CreateRunner().ApplyAsync(_shard2.GetConnectionString(), _migrationsDirectory, CancellationToken.None);
        await CreateRunner().ApplyAsync(_shard1.GetConnectionString(), _migrationsDirectory, CancellationToken.None);

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
            .Should().Equal("ch1:3:2:1", "ch2:3:2:1");
    }

    [Fact]
    public async Task GivenReplacingMergeTreeEngine_WhenGeneratedOnCluster_ThenReplicatedReplacingTableDeduplicatesOnEveryNode()
    {
        var cluster = ClusterDdl.For(CodegenConfigLoader.Load(TestContractsPaths.ClusterCodegenConfigPath));
        var table = new MergeTreeTableConfig
        {
            TableName = "orders_dedup",
            Engine = "ReplacingMergeTree(version)",
            OrderBy = "(order_id)",
            ShardingKey = "cityHash64(order_id)",
            Columns =
            [
                new PipelineColumnConfig { Name = "order_id", Type = "String" },
                new PipelineColumnConfig { Name = "version", Type = "UInt64" }
            ]
        };

        var execResult = await _shard1.ExecScriptAsync(MergeTreeTableGenerator.Generate(table, cluster: cluster));
        execResult.ExitCode.Should().Be(0, execResult.Stderr);

        await using var connection = new ClickHouseConnection(_shard2.GetConnectionString());
        await connection.OpenAsync();

        (await QueryRowsAsync(connection,
                "SELECT getMacro('replica'), name, engine FROM clusterAllReplicas('kafka2ch', system.tables) " +
                "WHERE database = currentDatabase() AND name IN ('orders_dedup', 'orders_dedup_local') ORDER BY 1, 2"))
            .Select(row => $"{row[0]}:{row[1]}:{row[2]}")
            .Should().Equal(
                "ch1:orders_dedup:Distributed",
                "ch1:orders_dedup_local:ReplicatedReplacingMergeTree",
                "ch2:orders_dedup:Distributed",
                "ch2:orders_dedup_local:ReplicatedReplacingMergeTree");

        foreach (var version in new[] { 1, 2 })
        {
            await ExecuteAsync(connection,
                $"INSERT INTO orders_dedup (order_id, version) SELECT toString(number), {version} FROM numbers(50) " +
                "SETTINGS distributed_foreground_insert = 1");
        }

        (await ScalarAsync(connection, "SELECT count() FROM orders_dedup FINAL")).Should().Be(50);
        (await ScalarAsync(connection, "SELECT min(version) FROM orders_dedup FINAL")).Should().Be(2);
    }

    private ClickHouseContainer CreateNode(string host, string shard) =>
        new ClickHouseBuilder(TestContainersEnv.FullClickHouseImageName)
            .WithNetwork(_network)
            .WithNetworkAliases(host)
            .WithEnvironment("CLICKHOUSE_SHARD", shard)
            .WithEnvironment("CLICKHOUSE_REPLICA", host)
            .WithResourceMapping(FormatSchemas.Directory, "/var/lib/clickhouse/format_schemas")
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
