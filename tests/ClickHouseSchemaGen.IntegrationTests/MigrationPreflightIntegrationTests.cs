namespace ClickHouseSchemaGen.IntegrationTests;

public sealed class MigrationPreflightIntegrationTests : IAsyncLifetime
{
    private const string FormatSchemaPath = "/var/lib/clickhouse/user_files/format_schemas/";

    private const string WrongChecksum = "0000000000000000000000000000000000000000000000000000000000000000";

    private readonly ClickHouseContainer _clickHouse;
    private readonly string _migrationsDirectory =
        Path.Combine(Path.GetTempPath(), $"kafka2ch-preflight-migrations-{Guid.NewGuid():N}");

    public MigrationPreflightIntegrationTests()
    {
        TestContainersEnv.SetVariables();
        _clickHouse = new ClickHouseBuilder(TestContainersEnv.FullClickHouseImageName)
            .WithResourceMapping(FormatSchemas.Directory, FormatSchemaPath.TrimEnd('/'))
            .WithResourceMapping(
                Encoding.UTF8.GetBytes($"<clickhouse><format_schema_path>{FormatSchemaPath}</format_schema_path></clickhouse>"),
                "/etc/clickhouse-server/config.d/format_schemas.xml")
            .Build();
    }

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_migrationsDirectory);
        await _clickHouse.StartAsync();
        foreach (var (fileName, sql) in InitScriptsFixture.Generate(TestContractsPaths.CodegenConfigPath))
        {
            var execResult = await _clickHouse.ExecScriptAsync(sql);
            execResult.ExitCode.Should().Be(0, $"{fileName}: {execResult.Stderr}");
        }
    }

    public async Task DisposeAsync()
    {
        await _clickHouse.DisposeAsync();
        Directory.Delete(_migrationsDirectory, recursive: true);
    }

    [Fact]
    public async Task GivenFormatSchemaHashMismatch_WhenApplied_ThenMigrationDoesNotStartAndQueueIsUntouched()
    {
        var queueBefore = await ScalarAsync("SHOW CREATE TABLE orders_queue");
        await WriteRecreateMigrationAsync(new Dictionary<string, string>
        {
            [KafkaRecreateMigrationFixture.FormatSchemaFile] = WrongChecksum
        });

        var act = () => CreateRunner().ApplyAsync(_clickHouse.GetConnectionString(), _migrationsDirectory, CancellationToken.None);

        (await act.Should().ThrowAsync<MigrationPreflightException>())
            .WithMessage($"*{KafkaRecreateMigrationFixture.FormatSchemaFile}*{WrongChecksum}*");
        await AssertQueueUntouchedAsync(queueBefore);
    }

    [Fact]
    public async Task GivenMissingFormatSchema_WhenApplied_ThenMigrationDoesNotStartAndQueueIsUntouched()
    {
        var queueBefore = await ScalarAsync("SHOW CREATE TABLE orders_queue");
        var sql = KafkaRecreateMigrationFixture.GenerateSql(
            TestContractsPaths.CodegenConfigPath,
            KafkaRecreateMigrationFixture.FormatSchemaChecksums(FormatSchemas.Directory));
        await File.WriteAllTextAsync(
            Path.Combine(_migrationsDirectory, KafkaRecreateMigrationFixture.FileName),
            new FormatSchemaRequirement("missing_event.proto", WrongChecksum).ToMarker() + SqlText.NewLine + sql);

        var act = () => CreateRunner().ApplyAsync(_clickHouse.GetConnectionString(), _migrationsDirectory, CancellationToken.None);

        (await act.Should().ThrowAsync<MigrationPreflightException>()).WithMessage("*missing_event.proto*not found*");
        await AssertQueueUntouchedAsync(queueBefore);
    }

    [Fact]
    public async Task GivenInvalidQueueDdl_WhenApplied_ThenDryRunFailsBeforeDetachAndLeavesNoPreflightDatabase()
    {
        var queueBefore = await ScalarAsync("SHOW CREATE TABLE orders_queue");
        await File.WriteAllTextAsync(
            Path.Combine(_migrationsDirectory, "20990105000000_broken_queue.sql"),
            """
            DETACH TABLE IF EXISTS orders_queue;
            -- await:kafka_consumers_empty orders_queue
            DROP VIEW IF EXISTS orders_mv;
            ATTACH TABLE IF NOT EXISTS orders_queue;
            DROP TABLE IF EXISTS orders_queue;
            CREATE TABLE orders_queue (broken NoSuchType) ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'orders',
                kafka_group_name = 'clickhouse-orders', kafka_format = 'ProtobufSingle';
            """);

        var act = () => CreateRunner().ApplyAsync(_clickHouse.GetConnectionString(), _migrationsDirectory, CancellationToken.None);

        (await act.Should().ThrowAsync<MigrationPreflightException>()).WithMessage("*orders_queue*");
        await AssertQueueUntouchedAsync(queueBefore);
        Convert.ToInt32(await ScalarAsync("SELECT count() FROM system.databases WHERE name LIKE 'kafka2ch_preflight_%'"))
            .Should().Be(0);
    }

    [Fact]
    public async Task GivenMaterializedViewReferencingMissingQueueColumn_WhenApplied_ThenDryRunFailsBeforeDetach()
    {
        var queueBefore = await ScalarAsync("SHOW CREATE TABLE orders_queue");
        await File.WriteAllTextAsync(
            Path.Combine(_migrationsDirectory, "20990106000000_broken_view.sql"),
            """
            DETACH TABLE IF EXISTS orders_queue;
            -- await:kafka_consumers_empty orders_queue
            DROP VIEW IF EXISTS orders_mv;
            ATTACH TABLE IF NOT EXISTS orders_queue;
            DROP TABLE IF EXISTS orders_queue;
            CREATE TABLE orders_queue (order_id String) ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'orders',
                kafka_group_name = 'clickhouse-orders', kafka_format = 'ProtobufSingle',
                kafka_schema = 'order_event:OrderEvent';
            CREATE MATERIALIZED VIEW orders_mv TO orders AS
            SELECT order_id AS order_id, no_such_column AS category
            FROM orders_queue;
            """);

        var act = () => CreateRunner().ApplyAsync(_clickHouse.GetConnectionString(), _migrationsDirectory, CancellationToken.None);

        (await act.Should().ThrowAsync<MigrationPreflightException>()).WithMessage("*orders_mv*");
        await AssertQueueUntouchedAsync(queueBefore);
    }

    [Fact]
    public async Task GivenMatchingFormatSchema_WhenApplied_ThenQueueIsRecreatedWithoutLeftovers()
    {
        await WriteRecreateMigrationAsync(KafkaRecreateMigrationFixture.FormatSchemaChecksums(FormatSchemas.Directory));

        await CreateRunner().ApplyAsync(_clickHouse.GetConnectionString(), _migrationsDirectory, CancellationToken.None);

        Convert.ToString(await ScalarAsync("SHOW CREATE TABLE orders_queue"))
            .Should().Contain($"kafka_num_consumers = {KafkaRecreateMigrationFixture.NewNumConsumers}");
        Convert.ToInt32(await ScalarAsync("EXISTS TABLE orders_mv")).Should().Be(1);
        Convert.ToInt32(await ScalarAsync("SELECT count() FROM system.databases WHERE name LIKE 'kafka2ch_preflight_%'"))
            .Should().Be(0);
    }

    [Fact]
    public async Task GivenHashMismatchAndSkipPreflight_WhenApplied_ThenMigrationIsApplied()
    {
        await WriteRecreateMigrationAsync(new Dictionary<string, string>
        {
            [KafkaRecreateMigrationFixture.FormatSchemaFile] = WrongChecksum
        });

        await CreateRunner(skipPreflight: true)
            .ApplyAsync(_clickHouse.GetConnectionString(), _migrationsDirectory, CancellationToken.None);

        Convert.ToString(await ScalarAsync("SHOW CREATE TABLE orders_queue"))
            .Should().Contain($"kafka_num_consumers = {KafkaRecreateMigrationFixture.NewNumConsumers}");
    }

    private Task WriteRecreateMigrationAsync(IReadOnlyDictionary<string, string> checksums) =>
        File.WriteAllTextAsync(
            Path.Combine(_migrationsDirectory, KafkaRecreateMigrationFixture.FileName),
            KafkaRecreateMigrationFixture.GenerateSql(TestContractsPaths.CodegenConfigPath, checksums));

    private async Task AssertQueueUntouchedAsync(object? queueBefore)
    {
        Convert.ToInt32(await ScalarAsync("EXISTS TABLE orders_queue")).Should().Be(1);
        Convert.ToInt32(await ScalarAsync("EXISTS TABLE orders_mv")).Should().Be(1);
        (await ScalarAsync("SHOW CREATE TABLE orders_queue")).Should().Be(queueBefore);
        Convert.ToInt32(await ScalarAsync(
                "SELECT count() FROM schema_migrations WHERE kind = 'migration' AND version LIKE '2099%'"))
            .Should().Be(0);

        await ScalarAsync("SYSTEM FLUSH LOGS");
        Convert.ToInt32(await ScalarAsync(
                "SELECT count() FROM system.query_log WHERE type = 'QueryStart' " +
                "AND query ILIKE 'DETACH TABLE%orders_queue%'"))
            .Should().Be(0, "preflight must fail before the queue is detached");
    }

    private static MigrationRunner CreateRunner(bool skipPreflight = false) =>
        new(NullLogger.Instance, TimeProvider.System, skipPreflight: skipPreflight);

    private async Task<object?> ScalarAsync(string sql)
    {
        await using var connection = new ClickHouseConnection(_clickHouse.GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return await command.ExecuteScalarAsync();
    }
}
