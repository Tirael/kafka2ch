namespace ClickHouseSchemaGen.Migration;

public sealed class MigrationOrchestrator(ClickHouseSchemaGenerator generator, TimeProvider? defaultTimeProvider = null)
{
    private readonly TimeProvider _defaultTimeProvider = defaultTimeProvider ?? TimeProvider.System;

    public MigrationOrchestrator()
        : this(new ClickHouseSchemaGenerator(new DenormalizationPlanner()))
    {
    }

    public void Init(string configPath)
    {
        var (config, paths) = LoadContext(configPath);
        var plan = generator.BuildPlan(config);
        var snapshot = SchemaSnapshotMapper.FromPlan(plan);

        Directory.CreateDirectory(paths.MigrationsDirectory);
        SchemaSnapshotSerializer.Save(paths.SnapshotPath, snapshot);
        File.WriteAllText(
            paths.VersionsOutputPath,
            SqlText.Normalize(SchemaMigrationsScriptGenerator.Generate([], ClusterDdl.For(config))));
    }

    public MigrationStatusResult Status(string configPath)
    {
        var (config, paths) = LoadContext(configPath);
        var plan = generator.BuildPlan(config);

        if (!File.Exists(paths.SnapshotPath))
        {
            return new MigrationStatusResult(
                HasSnapshot: false,
                IsDrifted: false,
                Differences: [],
                SnapshotPath: paths.SnapshotPath);
        }

        var snapshot = SchemaSnapshotSerializer.Load(paths.SnapshotPath, plan);
        var differences = SnapshotDriftChecker.CollectDifferences(SchemaSnapshotMapper.FromPlan(plan), snapshot);
        return new MigrationStatusResult(
            HasSnapshot: true,
            IsDrifted: differences.Count > 0,
            Differences: differences,
            SnapshotPath: paths.SnapshotPath);
    }

    public MigrationRunResult Migrate(
        string configPath,
        string name,
        bool allowManual,
        TimeProvider? timeProvider = null) =>
        Migrate([configPath], name, allowManual, timeProvider);

    public MigrationRunResult Migrate(
        IReadOnlyList<string> configPaths,
        string name,
        bool allowManual,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(configPaths);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        if (configPaths.Count == 0)
            throw new ArgumentException("At least one codegen config is required.", nameof(configPaths));

        var clock = timeProvider ?? _defaultTimeProvider;
        var topologies = configPaths.Select(PlanTopology).ToList();
        EnsureDistinctTargets(topologies);

        if (topologies.All(topology => topology.Diff.Changes.Count == 0))
        {
            return new MigrationRunResult(
                ExitCode: 0,
                MigrationPaths: [],
                Message: "No schema changes detected.");
        }

        var generatedAt = clock.GetUtcNow();
        var version = generatedAt.ToString("yyyyMMddHHmmss");
        var fileName = $"{version}_{SanitizeMigrationName(name)}.sql";
        var requiresManualApproval = topologies.Any(topology => topology.MigrationPlan.RequiresManualApproval);
        var updateSnapshots = !requiresManualApproval || allowManual;

        var files = new AtomicFileSet();
        var migrationPaths = new List<string>();
        foreach (var topology in topologies)
        {
            var migrationPath = Path.Combine(topology.Paths.MigrationsDirectory, fileName);
            var sql = SqlText.Normalize(topology.Diff.Changes.Count == 0
                ? NoOpMigrationSql
                : MigrationSqlGenerator.Generate(
                    topology.MigrationPlan,
                    topology.OldPlan,
                    topology.NewPlan,
                    $"{version}_{name}",
                    topology.ParentChecksum,
                    topology.TargetChecksum,
                    generatedAt));
            files.Add(migrationPath, sql);
            migrationPaths.Add(migrationPath);

            if (!updateSnapshots)
                continue;

            if (topology.Diff.Changes.Count > 0)
                files.Add(topology.Paths.SnapshotPath, SchemaSnapshotSerializer.SerializeToString(topology.NewSnapshot));

            var migrations = SchemaMigrationsScriptGenerator.ReadFromDirectory(topology.Paths.MigrationsDirectory)
                .Append(new MigrationFileInfo(
                    version,
                    Path.GetFileNameWithoutExtension(fileName)[(version.Length + 1)..],
                    SqlChecksum.Compute(sql),
                    migrationPath))
                .ToList();
            files.Add(
                topology.Paths.VersionsOutputPath,
                SqlText.Normalize(SchemaMigrationsScriptGenerator.Generate(migrations, ClusterDdl.For(topology.Config))));
        }

        files.Commit();

        var written = string.Join(", ", migrationPaths.Select(path => $"'{path}'"));
        if (!updateSnapshots)
        {
            return new MigrationRunResult(
                ExitCode: 2,
                MigrationPaths: migrationPaths,
                Message: "Migration SQL was written, but manual/destructive changes "
                    + "require --allow-manual before updating the snapshot.");
        }

        return new MigrationRunResult(
            ExitCode: 0,
            MigrationPaths: migrationPaths,
            Message: migrationPaths.Count == 1
                ? $"Migration written to {written} and snapshot updated."
                : $"Migration written to {written} and snapshots updated.");
    }

    public MigrationHistoryVerification Verify(IReadOnlyList<string> configPaths, string? baselineVersion)
    {
        ArgumentNullException.ThrowIfNull(configPaths);
        return MigrationHistoryVerifier.Verify(
            configPaths.Select(configPath => LoadContext(configPath).Paths.MigrationsDirectory).ToList(),
            baselineVersion);
    }

    private const string NoOpMigrationSql =
        "-- No schema changes for this topology; keeps migration versions aligned across topologies.\n";

    private TopologyMigration PlanTopology(string configPath)
    {
        var (config, paths) = LoadContext(configPath);
        if (!File.Exists(paths.SnapshotPath))
        {
            throw new InvalidOperationException(
                $"Snapshot not found at '{paths.SnapshotPath}'. Run migrate init first.");
        }

        var newPlan = generator.BuildPlan(config);
        var oldSnapshot = SchemaSnapshotSerializer.Load(paths.SnapshotPath, newPlan);
        var oldPlan = SchemaSnapshotMapper.ToPlan(oldSnapshot, config);

        ProtoCompatibilityValidator.Validate(oldPlan, newPlan);

        var diff = SchemaDiffer.Diff(oldPlan, newPlan);
        var migrationPlan = MigrationPolicy.Apply(diff, oldPlan, newPlan);
        var parentChecksum = SchemaSnapshotSerializer.ComputeChecksum(
            SchemaSnapshotSerializer.SerializeToString(oldSnapshot));
        var newSnapshot = SchemaSnapshotMapper.FromPlan(newPlan, parentChecksum);
        var targetChecksum = SchemaSnapshotSerializer.ComputeChecksum(
            SchemaSnapshotSerializer.SerializeToString(newSnapshot));

        return new TopologyMigration(
            config,
            paths,
            oldPlan,
            newPlan,
            diff,
            migrationPlan,
            newSnapshot,
            parentChecksum,
            targetChecksum);
    }

    private static void EnsureDistinctTargets(IReadOnlyList<TopologyMigration> topologies)
    {
        var duplicate = topologies
            .SelectMany(topology => new[]
            {
                topology.Paths.MigrationsDirectory,
                topology.Paths.SnapshotPath,
                topology.Paths.VersionsOutputPath
            })
            .GroupBy(path => path, StringComparer.Ordinal)
            .FirstOrDefault(group => group.Count() > 1);
        if (duplicate is not null)
        {
            throw new InvalidOperationException(
                $"Several codegen configs share the same migrations directory, snapshot or versions script '{duplicate.Key}'.");
        }
    }

    public SnapshotUpgradeResult UpgradeSnapshot(string configPath)
    {
        var (config, paths) = LoadContext(configPath);
        if (!File.Exists(paths.SnapshotPath))
        {
            throw new InvalidOperationException(
                $"Snapshot not found at '{paths.SnapshotPath}'. Run migrate init first.");
        }

        var snapshot = SchemaSnapshotSerializer.Load(paths.SnapshotPath);
        var upgraded = SchemaSnapshotUpgrader.Upgrade(snapshot, generator.BuildPlan(config));
        SchemaSnapshotSerializer.Save(paths.SnapshotPath, upgraded);
        return new SnapshotUpgradeResult(paths.SnapshotPath, snapshot.Version, upgraded.Version);
    }

    public string WriteVersionsScript(string configPath)
    {
        var (config, paths) = LoadContext(configPath);
        WriteVersionsScript(config, paths);
        return paths.VersionsOutputPath;
    }

    private static void WriteVersionsScript(CodegenConfig config, MigrationPaths paths)
    {
        var migrations = SchemaMigrationsScriptGenerator.ReadFromDirectory(paths.MigrationsDirectory);
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(paths.VersionsOutputPath))!);
        File.WriteAllText(
            paths.VersionsOutputPath,
            SqlText.Normalize(SchemaMigrationsScriptGenerator.Generate(migrations, ClusterDdl.For(config))));
    }

    private static (CodegenConfig Config, MigrationPaths Paths) LoadContext(string configPath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(configPath);

        var config = CodegenConfigLoader.Load(configPath);
        return (config, new MigrationPaths(
            SnapshotPath: CodegenConfigLoader.ResolvePath(configPath, config.Migrations.SnapshotPath),
            MigrationsDirectory: CodegenConfigLoader.ResolvePath(configPath, config.Migrations.MigrationsDirectory),
            VersionsOutputPath: CodegenConfigLoader.ResolvePath(configPath, config.Migrations.VersionsOutputPath)));
    }

    private static string SanitizeMigrationName(string name) =>
        string.Join('_', name.Split(Path.GetInvalidFileNameChars(), StringSplitOptions.RemoveEmptyEntries))
            .Replace(" ", "_", StringComparison.Ordinal);
}

public sealed record MigrationPaths(
    string SnapshotPath,
    string MigrationsDirectory,
    string VersionsOutputPath);

public sealed record MigrationStatusResult(
    bool HasSnapshot,
    bool IsDrifted,
    IReadOnlyList<string> Differences,
    string SnapshotPath);

public sealed record SnapshotUpgradeResult(
    string SnapshotPath,
    int FromVersion,
    int ToVersion);

public sealed record MigrationRunResult(
    int ExitCode,
    IReadOnlyList<string> MigrationPaths,
    string Message)
{
    public string? MigrationPath => MigrationPaths.Count > 0 ? MigrationPaths[0] : null;
}

internal sealed record TopologyMigration(
    CodegenConfig Config,
    MigrationPaths Paths,
    ResolvedSchemaPlan OldPlan,
    ResolvedSchemaPlan NewPlan,
    SchemaDiff Diff,
    MigrationPlan MigrationPlan,
    SchemaSnapshot NewSnapshot,
    string ParentChecksum,
    string TargetChecksum);
