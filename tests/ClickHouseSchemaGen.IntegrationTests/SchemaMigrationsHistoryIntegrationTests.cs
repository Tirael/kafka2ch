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
            ("20990101000000", "20990101000000_add_note.sql", "migration"));
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

    private static MigrationRunner CreateRunner() => new(NullLogger.Instance, TimeProvider.System);

    private async Task<List<(string Version, string Name, string Kind)>> ReadHistoryAsync()
    {
        await using var connection = new ClickHouseConnection(_clickHouse.GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT version, name, kind FROM schema_migrations ORDER BY version";
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
