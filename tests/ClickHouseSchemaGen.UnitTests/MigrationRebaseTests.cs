namespace ClickHouseSchemaGen.UnitTests;

public sealed class MigrationRebaseTests : IDisposable
{
    private const string ParentChecksum = "1111111111111111111111111111111111111111111111111111111111111111";

    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"kafka2ch-rebase-{Guid.NewGuid():N}");
    private readonly FixedTimeProvider _clock = new(new DateTimeOffset(2026, 10, 21, 12, 0, 0, TimeSpan.Zero));

    public MigrationRebaseTests()
    {
        Directory.CreateDirectory(MigrationsDirectory);
        File.WriteAllText(ConfigPath, """
            {
              "kafkaTables": [],
              "migrations": {
                "snapshotPath": "init/schema.snapshot.json",
                "migrationsDirectory": "migrations",
                "versionsOutputPath": "init/00_schema_migrations.sql"
              }
            }
            """);
        SchemaSnapshotSerializer.Save(SnapshotPath, new SchemaSnapshot { ParentChecksum = ParentChecksum });
    }

    private string ConfigPath => Path.Combine(_directory, "codegen.json");

    private string MigrationsDirectory => Path.Combine(_directory, "migrations");

    private string SnapshotPath => Path.Combine(_directory, "init", "schema.snapshot.json");

    private string VersionsPath => Path.Combine(_directory, "init", "00_schema_migrations.sql");

    public void Dispose() => Directory.Delete(_directory, recursive: true);

    [Fact]
    public void GivenStaleMigration_WhenRebase_ThenRenamesToCurrentTimestampAndRebuildsVersionsScript()
    {
        WriteMigration("20261020000000_from_main.sql", "SELECT 1;\n");
        WriteMigration("20261001000000_feature.sql", "SELECT 2;\n");

        var result = new MigrationOrchestrator().Rebase(ConfigPath, "20261001000000_feature.sql", timeProvider: _clock);

        result.Version.Should().Be("20261021120000");
        result.NewPath.Should().Be(Path.Combine(MigrationsDirectory, "20261021120000_feature.sql"));
        File.Exists(result.OldPath).Should().BeFalse();
        File.ReadAllText(result.NewPath).Should().Be("SELECT 2;\n");
        var versions = File.ReadAllText(VersionsPath);
        versions.Should().Contain("VALUES ('20261021120000', 'feature', ");
        versions.Should().Contain("VALUES ('20261020000000', 'from_main', ");
        versions.Should().NotContain("20261001000000");
    }

    [Fact]
    public void GivenMigrationNameWithoutVersion_WhenRebase_ThenResolvesFile()
    {
        WriteMigration("20261001000000_feature.sql", "SELECT 2;\n");

        var result = new MigrationOrchestrator().Rebase(ConfigPath, "feature", timeProvider: _clock);

        result.OldPath.Should().Be(Path.Combine(MigrationsDirectory, "20261001000000_feature.sql"));
        File.Exists(result.NewPath).Should().BeTrue();
    }

    [Fact]
    public void GivenClockNotAfterLatestVersion_WhenRebase_ThenUsesNextSecondAfterLatest()
    {
        WriteMigration("20991231235959_from_main.sql", "SELECT 1;\n");
        WriteMigration("20261001000000_feature.sql", "SELECT 2;\n");

        var result = new MigrationOrchestrator().Rebase(ConfigPath, "20261001000000_feature", timeProvider: _clock);

        result.Version.Should().Be("21000101000000");
    }

    [Fact]
    public void GivenExplicitVersion_WhenRebase_ThenUsesIt()
    {
        WriteMigration("20261020000000_from_main.sql", "SELECT 1;\n");
        WriteMigration("20261001000000_feature.sql", "SELECT 2;\n");

        var result = new MigrationOrchestrator().Rebase(ConfigPath, "feature", version: "20261020000001");

        result.NewPath.Should().Be(Path.Combine(MigrationsDirectory, "20261020000001_feature.sql"));
    }

    [Theory]
    [InlineData("20261020000000")]
    [InlineData("20261019000000")]
    [InlineData("2026102")]
    public void GivenExplicitVersionNotAfterLatestOrMalformed_WhenRebase_ThenThrowsAndKeepsFiles(string version)
    {
        WriteMigration("20261020000000_from_main.sql", "SELECT 1;\n");
        WriteMigration("20261001000000_feature.sql", "SELECT 2;\n");

        var act = () => new MigrationOrchestrator().Rebase(ConfigPath, "feature", version: version);

        act.Should().Throw<InvalidOperationException>().WithMessage("*version*");
        File.Exists(Path.Combine(MigrationsDirectory, "20261001000000_feature.sql")).Should().BeTrue();
        File.Exists(VersionsPath).Should().BeFalse();
    }

    [Fact]
    public void GivenUnknownMigration_WhenRebase_ThenThrows()
    {
        WriteMigration("20261001000000_feature.sql", "SELECT 2;\n");

        var act = () => new MigrationOrchestrator().Rebase(ConfigPath, "missing", timeProvider: _clock);

        act.Should().Throw<InvalidOperationException>().WithMessage("*'missing'*not found*");
    }

    [Fact]
    public void GivenAmbiguousMigrationName_WhenRebase_ThenThrows()
    {
        WriteMigration("20261001000000_feature.sql", "SELECT 1;\n");
        WriteMigration("20261002000000_feature.sql", "SELECT 2;\n");

        var act = () => new MigrationOrchestrator().Rebase(ConfigPath, "feature", timeProvider: _clock);

        act.Should().Throw<InvalidOperationException>().WithMessage("*ambiguous*");
    }

    [Fact]
    public void GivenSnapshotWithoutParentChecksum_WhenRebase_ThenThrowsAndKeepsFile()
    {
        SchemaSnapshotSerializer.Save(SnapshotPath, new SchemaSnapshot());
        WriteMigration("20261001000000_feature.sql", "SELECT 2;\n");

        var act = () => new MigrationOrchestrator().Rebase(ConfigPath, "feature", timeProvider: _clock);

        act.Should().Throw<InvalidOperationException>().WithMessage("*parentChecksum*");
        File.Exists(Path.Combine(MigrationsDirectory, "20261001000000_feature.sql")).Should().BeTrue();
    }

    [Fact]
    public void GivenParentSnapshotMatchingParentChecksum_WhenRebase_ThenSucceeds()
    {
        var parentPath = Path.Combine(_directory, "main.snapshot.json");
        SchemaSnapshotSerializer.Save(parentPath, new SchemaSnapshot { TrailingSqlHash = "main" });
        var mainChecksum = SchemaSnapshotSerializer.ComputeChecksum(
            SchemaSnapshotSerializer.SerializeToString(SchemaSnapshotSerializer.Load(parentPath)));
        SchemaSnapshotSerializer.Save(SnapshotPath, new SchemaSnapshot { ParentChecksum = mainChecksum });
        WriteMigration("20261001000000_feature.sql", "SELECT 2;\n");

        var result = new MigrationOrchestrator().Rebase(
            ConfigPath,
            "feature",
            parentSnapshotPath: parentPath,
            timeProvider: _clock);

        File.Exists(result.NewPath).Should().BeTrue();
    }

    [Fact]
    public void GivenParentSnapshotNotMatchingParentChecksum_WhenRebase_ThenThrowsAndKeepsFile()
    {
        var parentPath = Path.Combine(_directory, "main.snapshot.json");
        SchemaSnapshotSerializer.Save(parentPath, new SchemaSnapshot { TrailingSqlHash = "main" });
        WriteMigration("20261001000000_feature.sql", "SELECT 2;\n");

        var act = () => new MigrationOrchestrator().Rebase(
            ConfigPath,
            "feature",
            parentSnapshotPath: parentPath,
            timeProvider: _clock);

        act.Should().Throw<InvalidOperationException>().WithMessage("*not generated over*regenerate*");
        File.Exists(Path.Combine(MigrationsDirectory, "20261001000000_feature.sql")).Should().BeTrue();
    }

    [Fact]
    public void GivenHeaderMatchingSnapshotChain_WhenRebase_ThenRewritesMigrationHeader()
    {
        var targetChecksum = CurrentSnapshotChecksum();
        WriteMigration("20261001000000_feature.sql", Header("20261001000000_feature", ParentChecksum, targetChecksum));

        var result = new MigrationOrchestrator().Rebase(ConfigPath, "feature", timeProvider: _clock);

        File.ReadAllText(result.NewPath).Should().Be(Header("20261021120000_feature", ParentChecksum, targetChecksum));
    }

    [Fact]
    public void GivenHeaderTargetNotMatchingSnapshot_WhenRebase_ThenThrowsAndKeepsFile()
    {
        WriteMigration(
            "20261001000000_feature.sql",
            Header("20261001000000_feature", ParentChecksum, new string('2', 64)));

        var act = () => new MigrationOrchestrator().Rebase(ConfigPath, "feature", timeProvider: _clock);

        act.Should().Throw<InvalidOperationException>().WithMessage("*Target checksum*");
        File.Exists(Path.Combine(MigrationsDirectory, "20261001000000_feature.sql")).Should().BeTrue();
    }

    [Fact]
    public void GivenHeaderParentNotMatchingSnapshot_WhenRebase_ThenThrows()
    {
        WriteMigration(
            "20261001000000_feature.sql",
            Header("20261001000000_feature", new string('3', 64), CurrentSnapshotChecksum()));

        var act = () => new MigrationOrchestrator().Rebase(ConfigPath, "feature", timeProvider: _clock);

        act.Should().Throw<InvalidOperationException>().WithMessage("*Parent checksum*");
    }

    private void WriteMigration(string fileName, string content) =>
        File.WriteAllText(Path.Combine(MigrationsDirectory, fileName), content);

    private string CurrentSnapshotChecksum() =>
        SchemaSnapshotSerializer.ComputeChecksum(
            SchemaSnapshotSerializer.SerializeToString(SchemaSnapshotSerializer.Load(SnapshotPath)));

    private static string Header(string migration, string parent, string target) =>
        $"""
        -- Generated by ClickHouseSchemaGen. Do not edit manually.
        -- Migration: {migration}
        -- Generated at: 2026-10-01T00:00:00.0000000+00:00
        -- Parent checksum: {parent}
        -- Target checksum: {target}

        SELECT 2;

        """.ReplaceLineEndings("\n");

    private sealed class FixedTimeProvider(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }
}
