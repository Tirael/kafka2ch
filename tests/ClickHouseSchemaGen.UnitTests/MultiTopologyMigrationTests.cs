namespace ClickHouseSchemaGen.UnitTests;

public sealed class MultiTopologyMigrationTests : IDisposable
{
    private const string Version = "20261021120000";

    private static readonly TimeProvider Clock =
        new FixedTimeProvider(new DateTimeOffset(2026, 10, 21, 12, 0, 0, TimeSpan.Zero));

    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"multi-topology-{Guid.NewGuid():N}");
    private readonly MigrationOrchestrator _orchestrator = new(SchemaGeneratorFactory.Create());

    public MultiTopologyMigrationTests()
    {
        Directory.CreateDirectory(_directory);
        foreach (var file in Directory.GetFiles(TestContractsPaths.RootDirectory, "*.json"))
            File.Copy(file, Path.Combine(_directory, Path.GetFileName(file)));
        foreach (var configPath in ConfigPaths)
            _orchestrator.Init(configPath);
    }

    private string SingleNodeConfig => Path.Combine(_directory, "clickhouse.codegen.json");

    private string ClusterConfig => Path.Combine(_directory, "clickhouse.codegen.cluster.json");

    private string ReplicatedDbConfig => Path.Combine(_directory, "clickhouse.codegen.cluster.replicated-db.json");

    private IReadOnlyList<string> ConfigPaths => [SingleNodeConfig, ClusterConfig, ReplicatedDbConfig];

    [Fact]
    public void GivenChangeInSharedConfig_WhenMigrateAllTopologies_ThenWritesOneVersionAndNameIntoEachDirectory()
    {
        ReplaceInFile(SingleNodeConfig, "\"numConsumers\": 1", "\"numConsumers\": 2");

        var result = _orchestrator.Migrate(ConfigPaths, "bump_consumers", allowManual: false, Clock);

        result.ExitCode.Should().Be(0);
        result.MigrationPaths.Should().HaveCount(3);
        foreach (var configPath in ConfigPaths)
        {
            var paths = PathsOf(configPath);
            Directory.GetFiles(paths.MigrationsDirectory, "*.sql").Select(Path.GetFileName)
                .Should().Equal($"{Version}_bump_consumers.sql");
            File.ReadAllText(Path.Combine(paths.MigrationsDirectory, $"{Version}_bump_consumers.sql"))
                .Should().Contain("orders_queue");
            File.ReadAllText(paths.VersionsOutputPath).Should().Contain($"'{Version}'");
            _orchestrator.Status(configPath).IsDrifted.Should().BeFalse();
        }

        _orchestrator.Verify(ConfigPaths, baselineVersion: null).IsConsistent.Should().BeTrue();
    }

    [Fact]
    public void GivenChangeInOneTopologyOnly_WhenMigrateAllTopologies_ThenOtherTopologiesGetNoOpMigrationWithSameVersion()
    {
        ReplaceInFile(
            ReplicatedDbConfig,
            "\"materializedViewsWriteThroughDistributed\": true",
            "\"materializedViewsWriteThroughDistributed\": false");
        var singleSnapshotBefore = File.ReadAllText(PathsOf(SingleNodeConfig).SnapshotPath);

        var result = _orchestrator.Migrate(ConfigPaths, "replicated_db_local_writes", allowManual: true, Clock);

        result.ExitCode.Should().Be(0);
        foreach (var configPath in ConfigPaths)
        {
            Directory.GetFiles(PathsOf(configPath).MigrationsDirectory, "*.sql").Select(Path.GetFileName)
                .Should().Equal($"{Version}_replicated_db_local_writes.sql");
            _orchestrator.Status(configPath).IsDrifted.Should().BeFalse();
        }

        File.ReadAllText(Path.Combine(PathsOf(SingleNodeConfig).MigrationsDirectory, $"{Version}_replicated_db_local_writes.sql"))
            .Should().Contain("No schema changes");
        File.ReadAllText(PathsOf(SingleNodeConfig).SnapshotPath).Should().Be(singleSnapshotBefore);
        _orchestrator.Verify(ConfigPaths, baselineVersion: null).IsConsistent.Should().BeTrue();
    }

    [Fact]
    public void GivenNoChangesInAnyTopology_WhenMigrateAllTopologies_ThenWritesNothing()
    {
        var before = CaptureFiles();

        var result = _orchestrator.Migrate(ConfigPaths, "noop", allowManual: false, Clock);

        result.ExitCode.Should().Be(0);
        result.MigrationPaths.Should().BeEmpty();
        result.Message.Should().Be("No schema changes detected.");
        CaptureFiles().Should().BeEquivalentTo(before);
    }

    [Fact]
    public void GivenSecondTopologyFailsWhileCommitting_WhenMigrateAllTopologies_ThenRollsBackEveryTopology()
    {
        ReplaceInFile(SingleNodeConfig, "\"numConsumers\": 1", "\"numConsumers\": 2");
        var clusterVersions = PathsOf(ClusterConfig).VersionsOutputPath;
        File.Delete(clusterVersions);
        Directory.CreateDirectory(clusterVersions);
        var before = CaptureFiles();

        var act = () => _orchestrator.Migrate(ConfigPaths, "bump_consumers", allowManual: false, Clock);

        act.Should().Throw<Exception>();
        CaptureFiles().Should().BeEquivalentTo(before);
        foreach (var configPath in ConfigPaths)
            Directory.Exists(PathsOf(configPath).MigrationsDirectory).Should().BeTrue();
        Directory.EnumerateFileSystemEntries(_directory, "*", SearchOption.AllDirectories)
            .Should().NotContain(path => path.Contains(".tmp-", StringComparison.Ordinal)
                || path.Contains(".bak-", StringComparison.Ordinal));
    }

    [Fact]
    public void GivenSecondTopologyHasNoSnapshot_WhenMigrateAllTopologies_ThenFirstTopologyIsUntouched()
    {
        ReplaceInFile(SingleNodeConfig, "\"numConsumers\": 1", "\"numConsumers\": 2");
        File.Delete(PathsOf(ClusterConfig).SnapshotPath);
        var before = CaptureFiles();

        var act = () => _orchestrator.Migrate(ConfigPaths, "bump_consumers", allowManual: false, Clock);

        act.Should().Throw<InvalidOperationException>().WithMessage("*Snapshot not found*");
        CaptureFiles().Should().BeEquivalentTo(before);
    }

    [Fact]
    public void GivenManualChangeWithoutApproval_WhenMigrateAllTopologies_ThenWritesSqlForAllButUpdatesNoSnapshot()
    {
        ReplaceInFile(
            ReplicatedDbConfig,
            "\"materializedViewsWriteThroughDistributed\": true",
            "\"materializedViewsWriteThroughDistributed\": false");
        var snapshotsBefore = ConfigPaths.ToDictionary(path => path, path => File.ReadAllText(PathsOf(path).SnapshotPath));

        var result = _orchestrator.Migrate(ConfigPaths, "manual_change", allowManual: false, Clock);

        result.ExitCode.Should().Be(2);
        result.MigrationPaths.Should().HaveCount(3);
        foreach (var configPath in ConfigPaths)
            File.ReadAllText(PathsOf(configPath).SnapshotPath).Should().Be(snapshotsBefore[configPath]);
    }

    [Fact]
    public void GivenTwoConfigsSharingMigrationsDirectory_WhenMigrate_ThenThrows()
    {
        var act = () => _orchestrator.Migrate([SingleNodeConfig, SingleNodeConfig], "dup", allowManual: false, Clock);

        act.Should().Throw<InvalidOperationException>().WithMessage("*same migrations directory*");
    }

    [Fact]
    public void GivenDivergenceAfterBaseline_WhenVerify_ThenFailsAndReportsMissingVersions()
    {
        AddMigration(SingleNodeConfig, "20261001000000_only_single");
        AddMigration(SingleNodeConfig, "20261101000000_after_baseline");
        AddMigration(ClusterConfig, "20261101000000_after_baseline");
        AddMigration(ReplicatedDbConfig, "20261101000001_after_baseline");

        var result = _orchestrator.Verify(ConfigPaths, baselineVersion: "20261015000000");

        result.IsConsistent.Should().BeFalse();
        result.Gaps.Should().BeEquivalentTo(new[]
        {
            new MigrationVersionGap(PathsOf(SingleNodeConfig).MigrationsDirectory, ["20261101000001"]),
            new MigrationVersionGap(PathsOf(ClusterConfig).MigrationsDirectory, ["20261101000001"]),
            new MigrationVersionGap(PathsOf(ReplicatedDbConfig).MigrationsDirectory, ["20261101000000"])
        });
    }

    [Fact]
    public void GivenDivergenceOnlyUpToBaseline_WhenVerify_ThenPasses()
    {
        AddMigration(SingleNodeConfig, "20261008115350_persist_protobuf_kafka_key");
        AddMigration(SingleNodeConfig, "20261021205602_dedup");
        AddMigration(ClusterConfig, "20261021205602_dedup");
        AddMigration(ReplicatedDbConfig, "20261021205603_dedup");
        foreach (var configPath in ConfigPaths)
            AddMigration(configPath, "20261101000000_after_baseline");

        _orchestrator.Verify(ConfigPaths, baselineVersion: "20261021205603").IsConsistent.Should().BeTrue();
        _orchestrator.Verify(ConfigPaths, baselineVersion: null).IsConsistent.Should().BeFalse();
    }

    [Fact]
    public void GivenTopologiesConfig_WhenLoaded_ThenResolvesConfigPathsAndBaselineRelativeToIt()
    {
        var rootConfig = Path.Combine(_directory, "clickhouse.topologies.json");
        File.WriteAllText(rootConfig, """
            {
              // comment
              "topologies": ["clickhouse.codegen.json", "clickhouse.codegen.cluster.json"],
              "migrationsBaseline": "20261021205603"
            }
            """);

        var topologies = TopologiesConfigLoader.Load(rootConfig);

        topologies.ConfigPaths.Should().Equal(SingleNodeConfig, ClusterConfig);
        topologies.MigrationsBaseline.Should().Be("20261021205603");
    }

    [Fact]
    public void GivenRepositoryTopologiesConfig_WhenVerify_ThenCommittedMigrationHistoryIsConsistent()
    {
        var rootConfig = Path.Combine(FindRepositoryRoot(), "src", "Sandbox.Contracts", "clickhouse.topologies.json");
        var topologies = TopologiesConfigLoader.Load(rootConfig);

        topologies.ConfigPaths.Should().HaveCount(3).And.OnlyContain(path => File.Exists(path));
        topologies.MigrationsBaseline.Should().NotBeNullOrWhiteSpace();
        var result = _orchestrator.Verify(topologies.ConfigPaths, topologies.MigrationsBaseline);

        result.Gaps.Should().BeEmpty();
    }

    public void Dispose()
    {
        if (Directory.Exists(_directory))
            Directory.Delete(_directory, recursive: true);
    }

    private MigrationPaths PathsOf(string configPath)
    {
        var config = CodegenConfigLoader.Load(configPath);
        return new MigrationPaths(
            CodegenConfigLoader.ResolvePath(configPath, config.Migrations.SnapshotPath),
            CodegenConfigLoader.ResolvePath(configPath, config.Migrations.MigrationsDirectory),
            CodegenConfigLoader.ResolvePath(configPath, config.Migrations.VersionsOutputPath));
    }

    private void AddMigration(string configPath, string fileName) =>
        File.WriteAllText(Path.Combine(PathsOf(configPath).MigrationsDirectory, $"{fileName}.sql"), "SELECT 1;\n");

    private Dictionary<string, string> CaptureFiles() =>
        Directory.EnumerateFiles(_directory, "*", SearchOption.AllDirectories)
            .ToDictionary(path => path, File.ReadAllText);

    private static void ReplaceInFile(string path, string oldValue, string newValue)
    {
        var text = File.ReadAllText(path);
        text.Should().Contain(oldValue);
        File.WriteAllText(path, text.Replace(oldValue, newValue, StringComparison.Ordinal));
    }

    private static string FindRepositoryRoot()
    {
        for (var directory = new DirectoryInfo(AppContext.BaseDirectory); directory is not null; directory = directory.Parent)
        {
            if (File.Exists(Path.Combine(directory.FullName, "kafka2ch.sln")))
                return directory.FullName;
        }

        throw new InvalidOperationException("Repository root with kafka2ch.sln was not found.");
    }

    private sealed class FixedTimeProvider(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }
}
