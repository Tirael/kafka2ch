namespace ClickHouseSchemaGen.IntegrationTests;

public sealed class SchemaMigrationsHistoryIntegrationTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _clickHouse;
    private readonly string _migrationsDirectory =
        Path.Combine(Path.GetTempPath(), $"kafka2ch-migrations-{Guid.NewGuid():N}");

    public SchemaMigrationsHistoryIntegrationTests()
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
    }

    public async Task DisposeAsync()
    {
        await _clickHouse.DisposeAsync();
        Directory.Delete(_migrationsDirectory, recursive: true);
    }

    [Fact]
    public async Task GivenGeneratedInitScripts_WhenBootstrappedAndMigrated_ThenHistoryHasInitAndMigrationRows()
    {
        foreach (var (fileName, sql) in InitScriptsFixture.Generate(TestContractsPaths.CodegenConfigPath))
        {
            var execResult = await _clickHouse.ExecScriptAsync(sql);
            execResult.ExitCode.Should().Be(0, $"{fileName}: {execResult.Stderr}");
        }

        await File.WriteAllTextAsync(
            Path.Combine(_migrationsDirectory, "20990101000000_add_note.sql"),
            "ALTER TABLE orders ADD COLUMN IF NOT EXISTS migration_note String DEFAULT '';\n");

        await CreateRunner().ApplyAsync(_clickHouse.GetConnectionString(), _migrationsDirectory, CancellationToken.None);
        await CreateRunner().ApplyAsync(_clickHouse.GetConnectionString(), _migrationsDirectory, CancellationToken.None);

        var rows = await ReadHistoryAsync();
        rows.Should().Equal(
            ("01", "01_orders_queue.sql", "init"),
            ("02", "02_pipeline.sql", "init"),
            ("20990101000000", "20990101000000_add_note.sql", "migration"),
            ("20990101000000", "20990101000000_add_note.sql", "started"));
    }

    [Fact]
    public async Task GivenTwoParallelMigrators_WhenApply_ThenMigrationIsAppliedOnce()
    {
        await ExecuteAsync("CREATE TABLE lock_probe (applied_by String) ENGINE = MergeTree ORDER BY applied_by");
        await File.WriteAllTextAsync(
            Path.Combine(_migrationsDirectory, "20990101000002_lock_probe.sql"),
            "SELECT sleep(2);\n\nINSERT INTO lock_probe VALUES ('migration');\n");

        await Task.WhenAll(
            CreateRunner().ApplyAsync(_clickHouse.GetConnectionString(), _migrationsDirectory, CancellationToken.None),
            CreateRunner().ApplyAsync(_clickHouse.GetConnectionString(), _migrationsDirectory, CancellationToken.None));

        Convert.ToInt64(await ScalarAsync("SELECT count() FROM lock_probe")).Should().Be(1);
        (await ReadHistoryAsync()).Should().Equal(
            ("20990101000002", "20990101000002_lock_probe.sql", "migration"),
            ("20990101000002", "20990101000002_lock_probe.sql", "started"));
        Convert.ToInt64(await ScalarAsync("SELECT count() FROM schema_migrations WHERE kind = 'lock'"))
            .Should().Be(0, "the lock is released after apply");
    }

    [Fact]
    public async Task GivenLockHeldByAnotherMigrator_WhenApplyWithShortTimeout_ThenTimesOutWithoutApplying()
    {
        await ExecuteAsync(SchemaMigrationsTable.CreateTableSql);
        await File.WriteAllTextAsync(
            Path.Combine(_migrationsDirectory, "20990101000003_blocked.sql"),
            "CREATE TABLE IF NOT EXISTS blocked_t (id String) ENGINE = MergeTree ORDER BY id;\n");

        await using (await AcquireLockAsync(new MigrationLockOptions { Owner = "holder" }))
        {
            var apply = () => CreateRunner(new MigrationLockOptions { Timeout = TimeSpan.FromSeconds(2) })
                .ApplyAsync(_clickHouse.GetConnectionString(), _migrationsDirectory, CancellationToken.None);

            (await apply.Should().ThrowAsync<TimeoutException>()).WithMessage("*'holder'*");
            Convert.ToInt32(await ScalarAsync("EXISTS TABLE blocked_t")).Should().Be(0);
        }

        await CreateRunner().ApplyAsync(_clickHouse.GetConnectionString(), _migrationsDirectory, CancellationToken.None);

        Convert.ToInt32(await ScalarAsync("EXISTS TABLE blocked_t")).Should().Be(1);
    }

    [Fact]
    public async Task GivenAbandonedLock_WhenTtlExpires_ThenNextMigratorTakesOver()
    {
        await ExecuteAsync(SchemaMigrationsTable.CreateTableSql);
        await File.WriteAllTextAsync(
            Path.Combine(_migrationsDirectory, "20990101000004_after_crash.sql"),
            "CREATE TABLE IF NOT EXISTS after_crash_t (id String) ENGINE = MergeTree ORDER BY id;\n");
        await AcquireLockAsync(new MigrationLockOptions { Owner = "crashed", Ttl = TimeSpan.FromSeconds(3) });

        await CreateRunner(new MigrationLockOptions { Timeout = TimeSpan.FromSeconds(30) })
            .ApplyAsync(_clickHouse.GetConnectionString(), _migrationsDirectory, CancellationToken.None);

        Convert.ToInt32(await ScalarAsync("EXISTS TABLE after_crash_t")).Should().Be(1);
        Convert.ToInt64(await ScalarAsync("SELECT count() FROM schema_migrations WHERE kind = 'lock'")).Should().Be(0);
    }

    [Fact]
    public async Task GivenStartedWithoutMigrationRecord_WhenApply_ThenWarnsAndReapplies()
    {
        await ExecuteAsync(SchemaMigrationsTable.CreateTableSql);
        await ExecuteAsync(SchemaMigrationsTable.InsertSql(
            "20990101000005", "20990101000005_interrupted.sql", "abc", SchemaMigrationsTable.StartedKind));
        await File.WriteAllTextAsync(
            Path.Combine(_migrationsDirectory, "20990101000005_interrupted.sql"),
            "CREATE TABLE IF NOT EXISTS interrupted_t (id String) ENGINE = MergeTree ORDER BY id;\n");
        var logger = new RecordingLogger();

        await new MigrationRunner(logger, TimeProvider.System)
            .ApplyAsync(_clickHouse.GetConnectionString(), _migrationsDirectory, CancellationToken.None);

        logger.Warnings.Should().ContainSingle(message =>
            message.Contains("20990101000005_interrupted.sql", StringComparison.Ordinal)
            && message.Contains("interrupted", StringComparison.Ordinal));
        Convert.ToInt32(await ScalarAsync("EXISTS TABLE interrupted_t")).Should().Be(1);
        (await ReadHistoryAsync()).Should().Equal(
            ("20990101000005", "20990101000005_interrupted.sql", "migration"),
            ("20990101000005", "20990101000005_interrupted.sql", "started"),
            ("20990101000005", "20990101000005_interrupted.sql", "started"));
    }

    [Fact]
    public async Task GivenLegacyTableWithoutKind_WhenApply_ThenUpgradesTableAndKeepsAppliedMigrations()
    {
        const string migrationSql = "CREATE TABLE IF NOT EXISTS legacy_t (id String) ENGINE = MergeTree ORDER BY id;\n";
        var migrationPath = Path.Combine(_migrationsDirectory, "20260101000000_legacy.sql");
        await File.WriteAllTextAsync(migrationPath, migrationSql);
        var checksum = SqlChecksum.Compute(migrationSql);

        var legacySetup = $"""
            CREATE TABLE schema_migrations
            (
                version String,
                name String,
                checksum String,
                applied_at DateTime
            )
            ENGINE = MergeTree
            ORDER BY version;
            INSERT INTO schema_migrations VALUES ('20260101000000', '20260101000000_legacy.sql', '{checksum}', now());
            """;
        var execResult = await _clickHouse.ExecScriptAsync(legacySetup);
        execResult.ExitCode.Should().Be(0, execResult.Stderr);

        await CreateRunner().ApplyAsync(_clickHouse.GetConnectionString(), _migrationsDirectory, CancellationToken.None);

        var rows = await ReadHistoryAsync();
        rows.Should().Equal(("20260101000000", "20260101000000_legacy.sql", "migration"));
        Convert.ToInt32(await ScalarAsync("EXISTS TABLE legacy_t"))
            .Should().Be(0, "already applied migration must not rerun");
    }

    [Fact]
    public async Task GivenAppliedMigration_WhenFileCheckedOutWithCrLfAndBom_ThenNoChecksumMismatch()
    {
        const string migrationSql =
            "CREATE TABLE IF NOT EXISTS eol_t (id String) ENGINE = MergeTree ORDER BY id;\n\n"
            + "ALTER TABLE eol_t ADD COLUMN IF NOT EXISTS note String DEFAULT '';\n";
        var migrationPath = Path.Combine(_migrationsDirectory, "20990101000001_eol_note.sql");
        await File.WriteAllTextAsync(migrationPath, migrationSql);
        await CreateRunner().ApplyAsync(_clickHouse.GetConnectionString(), _migrationsDirectory, CancellationToken.None);

        await File.WriteAllTextAsync(
            migrationPath,
            migrationSql.ReplaceLineEndings("\r\n"),
            new UTF8Encoding(encoderShouldEmitUTF8Identifier: true));
        var reapply = () => CreateRunner().ApplyAsync(
            _clickHouse.GetConnectionString(),
            _migrationsDirectory,
            CancellationToken.None);

        await reapply.Should().NotThrowAsync();
        Convert.ToString(await ScalarAsync(
                "SELECT checksum FROM schema_migrations WHERE version = '20990101000001'"))
            .Should().Be(SqlChecksum.Compute(migrationSql));
    }

    private static MigrationRunner CreateRunner(MigrationLockOptions? lockOptions = null) =>
        new(NullLogger.Instance, TimeProvider.System, lockOptions: lockOptions);

    private async Task<MigrationLock> AcquireLockAsync(MigrationLockOptions options)
    {
        var connection = new ClickHouseConnection(_clickHouse.GetConnectionString());
        await connection.OpenAsync();
        return await MigrationLock.AcquireAsync(
            connection,
            ClusterDdl.SingleNode,
            options,
            NullLogger.Instance,
            TimeProvider.System,
            CancellationToken.None);
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var connection = new ClickHouseConnection(_clickHouse.GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    private async Task<List<(string Version, string Name, string Kind)>> ReadHistoryAsync()
    {
        await using var connection = new ClickHouseConnection(_clickHouse.GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            "SELECT version, name, kind FROM schema_migrations WHERE kind != 'lock' ORDER BY version, kind";
        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<(string, string, string)>();
        while (await reader.ReadAsync())
            rows.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2)));
        return rows;
    }

    private async Task<object?> ScalarAsync(string sql)
    {
        await using var connection = new ClickHouseConnection(_clickHouse.GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return await command.ExecuteScalarAsync();
    }
}
