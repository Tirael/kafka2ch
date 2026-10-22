namespace ClickHouseSchemaGen.IntegrationTests;

public sealed class MigrationQueueLifecycleIntegrationTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _clickHouse;
    private readonly string _migrationsDirectory =
        Path.Combine(Path.GetTempPath(), $"kafka2ch-migrations-{Guid.NewGuid():N}");

    public MigrationQueueLifecycleIntegrationTests()
    {
        TestContainersEnv.SetVariables();
        _clickHouse = new ClickHouseBuilder(TestContainersEnv.FullClickHouseImageName)
            .WithResourceMapping(FormatSchemas.Directory, "/var/lib/clickhouse/format_schemas")
            .Build();
    }

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_migrationsDirectory);
        await _clickHouse.StartAsync();
        await ApplyInitScriptsAsync();
    }

    public async Task DisposeAsync()
    {
        await _clickHouse.DisposeAsync();
        Directory.Delete(_migrationsDirectory, recursive: true);
    }

    [Fact]
    public async Task GivenKafkaSettingsChanged_WhenApplied_ThenQueueIsRecreatedAttachedAndPipelineRestored()
    {
        await File.WriteAllTextAsync(
            Path.Combine(_migrationsDirectory, KafkaRecreateMigrationFixture.FileName),
            KafkaRecreateMigrationFixture.GenerateSql(TestContractsPaths.CodegenConfigPath));

        await CreateRunner().ApplyAsync(_clickHouse.GetConnectionString(), _migrationsDirectory, CancellationToken.None);
        await CreateRunner().ApplyAsync(_clickHouse.GetConnectionString(), _migrationsDirectory, CancellationToken.None);

        Convert.ToInt32(await ScalarAsync("EXISTS TABLE orders_queue")).Should().Be(1);
        Convert.ToInt32(await ScalarAsync("EXISTS TABLE orders_mv")).Should().Be(1);
        Convert.ToString(await ScalarAsync("SHOW CREATE TABLE orders_queue"))
            .Should().Contain($"kafka_num_consumers = {KafkaRecreateMigrationFixture.NewNumConsumers}");
        Convert.ToInt32(await ScalarAsync(
                $"SELECT count() FROM schema_migrations WHERE kind = 'migration' AND version = '{KafkaRecreateMigrationFixture.Version}'"))
            .Should().Be(1);
    }

    [Fact]
    public async Task GivenMetaOnlyChange_WhenApplied_ThenQueueIsReattachedAndViewRecreated()
    {
        await File.WriteAllTextAsync(
            Path.Combine(_migrationsDirectory, PersistPartitionMigrationFixture.FileName),
            PersistPartitionMigrationFixture.GenerateSql(TestContractsPaths.CodegenConfigPath));

        await CreateRunner().ApplyAsync(_clickHouse.GetConnectionString(), _migrationsDirectory, CancellationToken.None);
        await CreateRunner().ApplyAsync(_clickHouse.GetConnectionString(), _migrationsDirectory, CancellationToken.None);

        Convert.ToInt32(await ScalarAsync("EXISTS TABLE orders_queue")).Should().Be(1);
        Convert.ToString(await ScalarAsync("SHOW CREATE TABLE orders_mv"))
            .Should().Contain(PersistPartitionMigrationFixture.AddedColumn);
        Convert.ToInt32(await ScalarAsync(
                $"SELECT count() FROM schema_migrations WHERE kind = 'migration' AND version = '{PersistPartitionMigrationFixture.Version}'"))
            .Should().Be(1);
    }

    [Fact]
    public async Task GivenMigrationFailsAfterDetach_WhenApplied_ThenQueueIsReattachedAndMigrationNotRecorded()
    {
        const string version = "20990103000000";
        await File.WriteAllTextAsync(
            Path.Combine(_migrationsDirectory, $"{version}_broken.sql"),
            """
            DETACH TABLE IF EXISTS orders_queue;
            -- await:kafka_consumers_empty orders_queue
            ALTER TABLE orders ADD COLUMN IF NOT EXISTS broken NoSuchType;
            ATTACH TABLE IF NOT EXISTS orders_queue;
            """);

        var act = () => CreateRunner().ApplyAsync(_clickHouse.GetConnectionString(), _migrationsDirectory, CancellationToken.None);

        await act.Should().ThrowAsync<Exception>();
        Convert.ToInt32(await ScalarAsync("EXISTS TABLE orders_queue"))
            .Should().Be(1, "queue must be re-attached so Kafka consumption resumes without a server restart");
        Convert.ToInt32(await ScalarAsync($"SELECT count() FROM schema_migrations WHERE kind = 'migration' AND version = '{version}'"))
            .Should().Be(0);
    }

    [Fact]
    public async Task GivenMigrationFailsAfterDropQueueAndView_WhenApplied_ThenQueueAndViewAreRestoredAndMigrationNotRecorded()
    {
        const string version = "20990104000000";
        await File.WriteAllTextAsync(
            Path.Combine(_migrationsDirectory, $"{version}_broken_recreate.sql"),
            """
            DETACH TABLE IF EXISTS orders_queue;
            -- await:kafka_consumers_empty orders_queue
            DROP VIEW IF EXISTS orders_mv;
            ATTACH TABLE IF NOT EXISTS orders_queue;
            DROP TABLE IF EXISTS orders_queue;
            CREATE TABLE IF NOT EXISTS orders_queue (broken NoSuchType) ENGINE = Kafka;
            """);

        var act = () => CreateRunner().ApplyAsync(_clickHouse.GetConnectionString(), _migrationsDirectory, CancellationToken.None);

        var failure = (await act.Should().ThrowAsync<MigrationFailedException>()).Which;
        failure.Report.Restored.Should().Equal("orders_queue", "orders_mv");
        failure.Report.NotRestored.Should().BeEmpty();
        failure.Message.Should().Contain("restored: orders_queue, orders_mv; not restored: none");
        Convert.ToInt32(await ScalarAsync("EXISTS TABLE orders_queue"))
            .Should().Be(1, "dropped queue must be restored from its pre-migration DDL when recreate fails");
        Convert.ToInt32(await ScalarAsync("EXISTS TABLE orders_mv"))
            .Should().Be(1, "dropped materialized view must be restored when recreate fails");
        Convert.ToInt32(await ScalarAsync($"SELECT count() FROM schema_migrations WHERE kind = 'migration' AND version = '{version}'"))
            .Should().Be(0);
    }

    [Fact]
    public async Task GivenQueueRecreatedWithOtherSchemaBeforeFailure_WhenApplied_ThenViewIsNotRestoredAndReported()
    {
        const string version = "20990105000000";
        await File.WriteAllTextAsync(
            Path.Combine(_migrationsDirectory, $"{version}_broken_after_recreate.sql"),
            """
            DETACH TABLE IF EXISTS orders_queue;
            -- await:kafka_consumers_empty orders_queue
            DROP VIEW IF EXISTS orders_mv;
            ATTACH TABLE IF NOT EXISTS orders_queue;
            DROP TABLE IF EXISTS orders_queue;
            CREATE TABLE orders_queue (unexpected String) ENGINE = Null;
            ALTER TABLE orders ADD COLUMN IF NOT EXISTS broken NoSuchType;
            """);

        var act = () => CreateRunner().ApplyAsync(_clickHouse.GetConnectionString(), _migrationsDirectory, CancellationToken.None);

        var failure = (await act.Should().ThrowAsync<MigrationFailedException>()).Which;
        failure.Report.Restored.Should().BeEmpty();
        failure.Report.NotRestored.Should().ContainSingle(f => f.ObjectName == "orders_mv")
            .Which.Reason.Should().Contain("source 'orders_queue' schema changed").And.Contain("unexpected String");
        Convert.ToInt32(await ScalarAsync("EXISTS TABLE orders_mv"))
            .Should().Be(0, "an old view must not be attached to a queue whose schema no longer matches");
        Convert.ToInt32(await ScalarAsync($"SELECT count() FROM schema_migrations WHERE kind = 'migration' AND version = '{version}'"))
            .Should().Be(0);
    }

    [Fact]
    public async Task GivenModifyColumnWithMutationsMarker_WhenApplied_ThenTypeIsChangedAndNoMutationIsPending()
    {
        const string version = "20990106000000";
        await File.WriteAllTextAsync(
            Path.Combine(_migrationsDirectory, $"{version}_modify_column.sql"),
            """
            CREATE TABLE IF NOT EXISTS mutation_probe (id UInt64, ts DateTime) ENGINE = MergeTree ORDER BY id;
            INSERT INTO mutation_probe SELECT number, now() FROM numbers(10000);
            ALTER TABLE mutation_probe MODIFY COLUMN ts DateTime64(3) SETTINGS mutations_sync = 2;
            -- await:mutations_done mutation_probe
            """);

        await CreateRunner().ApplyAsync(_clickHouse.GetConnectionString(), _migrationsDirectory, CancellationToken.None);

        Convert.ToString(await ScalarAsync(
                "SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'mutation_probe' AND name = 'ts'"))
            .Should().Be("DateTime64(3)");
        Convert.ToInt64(await ScalarAsync(
                "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 'mutation_probe' AND is_done = 0"))
            .Should().Be(0);
        Convert.ToInt32(await ScalarAsync($"SELECT count() FROM schema_migrations WHERE kind = 'migration' AND version = '{version}'"))
            .Should().Be(1);
    }

    private async Task ApplyInitScriptsAsync()
    {
        foreach (var (fileName, sql) in InitScriptsFixture.Generate(TestContractsPaths.CodegenConfigPath))
        {
            var execResult = await _clickHouse.ExecScriptAsync(sql);
            execResult.ExitCode.Should().Be(0, $"{fileName}: {execResult.Stderr}");
        }
    }

    private static MigrationRunner CreateRunner() => new(NullLogger.Instance, TimeProvider.System);

    private async Task<object?> ScalarAsync(string sql)
    {
        await using var connection = new ClickHouseConnection(_clickHouse.GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return await command.ExecuteScalarAsync();
    }
}
