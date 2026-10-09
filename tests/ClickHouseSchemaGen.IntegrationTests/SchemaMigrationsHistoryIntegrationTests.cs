using ClickHouseSchemaGen.Migrator;
using Microsoft.Extensions.Logging.Abstractions;

namespace ClickHouseSchemaGen.IntegrationTests;

public sealed class SchemaMigrationsHistoryIntegrationTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _clickHouse = new ClickHouseBuilder("clickhouse/clickhouse-server:25.11")
        .WithBindMount(RepoPaths.FormatSchemasDirectory, "/var/lib/clickhouse/format_schemas")
        .Build();

    private readonly string _migrationsDirectory =
        Path.Combine(Path.GetTempPath(), $"kafka2ch-migrations-{Guid.NewGuid():N}");

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
    public async Task GivenCommittedInitScripts_WhenBootstrappedAndMigrated_ThenHistoryHasInitAndMigrationRows()
    {

        var initDirectory = Path.Combine(RepoPaths.RepositoryRoot, "docker", "clickhouse", "init");
        var initScripts = Directory.GetFiles(initDirectory, "*.sql")
            .OrderBy(path => Path.GetFileName(path), StringComparer.Ordinal)
            .ToList();
        foreach (var script in initScripts)
        {
            var execResult = await _clickHouse.ExecScriptAsync(await File.ReadAllTextAsync(script));
            execResult.ExitCode.Should().Be(0, $"{Path.GetFileName(script)}: {execResult.Stderr}");
        }

        await File.WriteAllTextAsync(
            Path.Combine(_migrationsDirectory, "20990101000000_add_note.sql"),
            "ALTER TABLE orders ADD COLUMN IF NOT EXISTS migration_note String DEFAULT '';\n");


        await CreateRunner().ApplyAsync(_clickHouse.GetConnectionString(), _migrationsDirectory, CancellationToken.None);
        await CreateRunner().ApplyAsync(_clickHouse.GetConnectionString(), _migrationsDirectory, CancellationToken.None);


        var committedMigrations = SchemaMigrationsScriptGenerator
            .ReadFromDirectory(Path.Combine(RepoPaths.RepositoryRoot, "docker", "clickhouse", "migrations"))
            .Select(migration => (migration.Version, migration.Name, "migration"));
        var rows = await ReadHistoryAsync();
        rows.Should().Equal(
            new[]
            {
                ("01", "01_orders_queue.sql", "init"),
                ("02", "02_shipments_queue.sql", "init"),
                ("03", "03_pipeline.sql", "init")
            }
            .Concat(committedMigrations)
            .Append(("20990101000000", "20990101000000_add_note.sql", "migration")));
    }

    [Fact]
    public async Task GivenLegacyTableWithoutKind_WhenApply_ThenUpgradesTableAndKeepsAppliedMigrations()
    {

        const string migrationSql = "CREATE TABLE IF NOT EXISTS legacy_t (id String) ENGINE = MergeTree ORDER BY id;\n";
        var migrationPath = Path.Combine(_migrationsDirectory, "20260101000000_legacy.sql");
        await File.WriteAllTextAsync(migrationPath, migrationSql);
        var checksum = SchemaSnapshotSerializer.ComputeChecksum(migrationSql);

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
