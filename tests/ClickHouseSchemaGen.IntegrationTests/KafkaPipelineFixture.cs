using System.Collections.Concurrent;
using System.Text.Json.Nodes;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Testcontainers.Kafka;

namespace ClickHouseSchemaGen.IntegrationTests;

public sealed class KafkaPipelineFixture : IAsyncLifetime
{
    public const string Topic = "orders";

    public const int TopicPartitions = 2;

    public const string NamedCollection = "kafka_orders";
    private const string KafkaListener = "kafka:19092";
    private const string BrokerListEnvironmentVariable = "KAFKA_BROKER_LIST";
    private const string SchemaRegistryAlias = "schema-registry";
    private const ushort SchemaRegistryPort = 8081;

    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(500);

    private readonly string _workDirectory = Path.Combine(Path.GetTempPath(), $"kafka2ch-e2e-{Guid.NewGuid():N}");
    private readonly INetwork _network;
    private readonly KafkaContainer _kafka;
    private readonly IContainer _schemaRegistry;
    private readonly ClickHouseContainer _clickHouse;
    private CachedSchemaRegistryClient? _schemaRegistryClient;

    public KafkaPipelineFixture()
    {
        TestContainersEnv.SetVariables();
        _network = new NetworkBuilder().Build();
        _kafka = new KafkaBuilder(KafkaTestImages.FullKafkaImageName)
            .WithKRaft()
            .WithNetwork(_network)
            .WithListener(KafkaListener)
            .Build();
        _schemaRegistry = new ContainerBuilder(KafkaTestImages.FullSchemaRegistryImageName)
            .WithNetwork(_network)
            .WithNetworkAliases(SchemaRegistryAlias)
            .WithPortBinding(SchemaRegistryPort, true)
            .WithEnvironment("SCHEMA_REGISTRY_HOST_NAME", SchemaRegistryAlias)
            .WithEnvironment("SCHEMA_REGISTRY_LISTENERS", $"http://0.0.0.0:{SchemaRegistryPort}")
            .WithEnvironment("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", $"PLAINTEXT://{KafkaListener}")
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilHttpRequestIsSucceeded(request => request.ForPort(SchemaRegistryPort).ForPath("/subjects")))
            .Build();
        _clickHouse = new ClickHouseBuilder(TestContainersEnv.FullClickHouseImageName)
            .WithNetwork(_network)
            .WithResourceMapping(FormatSchemas.Directory, "/var/lib/clickhouse/format_schemas")
            .WithEnvironment(BrokerListEnvironmentVariable, KafkaListener)
            .WithResourceMapping(Encoding.UTF8.GetBytes(NamedCollectionXml), "/etc/clickhouse-server/config.d/kafka.xml")
            .WithResourceMapping(Encoding.UTF8.GetBytes(NamedCollectionUsageXml), "/etc/clickhouse-server/users.d/named-collections.xml")
            .Build();
    }

    public string ConfigPath => Path.Combine(_workDirectory, "clickhouse.codegen.json");

    public string MigrationsDirectory => Path.Combine(_workDirectory, "migrations");

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(MigrationsDirectory);
        WriteConfig();

        await _network.CreateAsync();
        await _kafka.StartAsync();
        await Task.WhenAll(_schemaRegistry.StartAsync(), _clickHouse.StartAsync());

        await CreateTopicAsync();
        _schemaRegistryClient = new CachedSchemaRegistryClient(new SchemaRegistryConfig
        {
            Url = $"http://{_schemaRegistry.Hostname}:{_schemaRegistry.GetMappedPublicPort(SchemaRegistryPort)}"
        });

        foreach (var (fileName, sql) in InitScriptsFixture.Generate(ConfigPath))
        {
            var execResult = await _clickHouse.ExecScriptAsync(sql);
            execResult.ExitCode.Should().Be(0, $"{fileName}: {execResult.Stderr}");
        }
    }

    public async Task DisposeAsync()
    {
        _schemaRegistryClient?.Dispose();
        await _clickHouse.DisposeAsync();
        await _schemaRegistry.DisposeAsync();
        await _kafka.DisposeAsync();
        await _network.DisposeAsync();
        Directory.Delete(_workDirectory, recursive: true);
    }

    public IProducer<OrderKey, OrderEvent> CreateProducer()
    {
        var serializerConfig = new ProtobufSerializerConfig
        {
            AutoRegisterSchemas = true,
            SkipKnownTypes = true
        };

        return new ProducerBuilder<OrderKey, OrderEvent>(new ProducerConfig
        {
            BootstrapServers = _kafka.GetBootstrapAddress(),
            Acks = Acks.All,
            EnableIdempotence = true
        })
            .SetKeySerializer(new ProtobufSerializer<OrderKey>(_schemaRegistryClient!, serializerConfig))
            .SetValueSerializer(new ProtobufSerializer<OrderEvent>(_schemaRegistryClient!, serializerConfig))
            .Build();
    }

    public async Task<TopicPartitionOffset> PublishRawAsync(byte[] value, int partition)
    {
        using var producer = new ProducerBuilder<Null, byte[]>(new ProducerConfig
        {
            BootstrapServers = _kafka.GetBootstrapAddress(),
            Acks = Acks.All
        }).Build();

        var result = await producer.ProduceAsync(
            new TopicPartition(Topic, new Partition(partition)),
            new Message<Null, byte[]> { Value = value });
        return result.TopicPartitionOffset;
    }

    public static async Task PublishAsync(
        IProducer<OrderKey, OrderEvent> producer,
        IReadOnlyList<(OrderKey Key, OrderEvent Value)> orders,
        ConcurrentQueue<string> published)
    {
        await Task.WhenAll(orders.Select(order => producer.ProduceAsync(
            Topic,
            new Message<OrderKey, OrderEvent> { Key = order.Key, Value = order.Value })));

        foreach (var order in orders)
            published.Enqueue(order.Value.OrderId);
    }

    public async Task ApplyMigrationAsync(string fileName, string sql)
    {
        await File.WriteAllTextAsync(Path.Combine(MigrationsDirectory, fileName), sql);
        await new MigrationRunner(NullLogger.Instance, TimeProvider.System)
            .ApplyAsync(_clickHouse.GetConnectionString(), MigrationsDirectory, CancellationToken.None);
    }

    public async Task<ulong> WaitForCountAsync(string sql, ulong expected, TimeSpan timeout)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;
        while (true)
        {
            var value = Convert.ToUInt64(await ScalarAsync(sql));
            if (value >= expected || DateTimeOffset.UtcNow >= deadline)
                return value;

            await Task.Delay(PollInterval);
        }
    }

    public async Task ExecuteAsync(string sql)
    {
        await using var connection = new ClickHouseConnection(_clickHouse.GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    public async Task RefreshAggregatesAsync()
    {
        await ExecuteAsync("SYSTEM REFRESH VIEW orders_agg_mv");
        await ExecuteAsync("SYSTEM WAIT VIEW orders_agg_mv");
    }

    public async Task<object?> ScalarAsync(string sql)
    {
        await using var connection = new ClickHouseConnection(_clickHouse.GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return await command.ExecuteScalarAsync();
    }

    public async Task<IReadOnlyList<string>> QueryStringsAsync(string sql)
    {
        await using var connection = new ClickHouseConnection(_clickHouse.GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await using var reader = await command.ExecuteReaderAsync();

        var values = new List<string>();
        while (await reader.ReadAsync())
            values.Add(reader.GetString(0));

        return values;
    }

    private void WriteConfig()
    {
        var config = JsonNode.Parse(File.ReadAllText(TestContractsPaths.DedupCodegenConfigPath))!;
        config["extends"] = Path.Combine(TestContractsPaths.RootDirectory, config["extends"]!.GetValue<string>());

        var kafkaTables = JsonNode.Parse(File.ReadAllText(TestContractsPaths.CodegenConfigPath))!["kafkaTables"]!.AsArray();
        foreach (var table in kafkaTables)
        {
            var kafka = table!["kafka"]!.AsObject();
            kafka.Remove("brokerList");
            kafka["namedCollection"] = NamedCollection;
        }
        config["kafkaTables"] = kafkaTables.DeepClone();

        File.WriteAllText(ConfigPath, config.ToJsonString());
    }

    private static string NamedCollectionXml => $"""
        <clickhouse>
            <named_collections>
                <{NamedCollection}>
                    <kafka_broker_list from_env="{BrokerListEnvironmentVariable}"/>
                </{NamedCollection}>
            </named_collections>
        </clickhouse>
        """;

    private static string NamedCollectionUsageXml => $"""
        <clickhouse>
            <users>
                <{ClickHouseBuilder.DefaultUsername}>
                    <named_collection_control>1</named_collection_control>
                </{ClickHouseBuilder.DefaultUsername}>
            </users>
        </clickhouse>
        """;

    private async Task CreateTopicAsync()
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _kafka.GetBootstrapAddress() })
            .Build();
        await admin.CreateTopicsAsync(
        [
            new TopicSpecification { Name = Topic, NumPartitions = TopicPartitions, ReplicationFactor = 1 }
        ]);
    }
}
