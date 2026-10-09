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
            SchemaMigrationsScriptGenerator.Generate([], ClusterDdl.For(config)));
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

        var snapshot = SchemaSnapshotSerializer.Load(paths.SnapshotPath);
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
        TimeProvider? timeProvider = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        var clock = timeProvider ?? _defaultTimeProvider;

        var (config, paths) = LoadContext(configPath);
        if (!File.Exists(paths.SnapshotPath))
            throw new InvalidOperationException($"Snapshot not found at '{paths.SnapshotPath}'. Run migrate init first.");

        var oldSnapshot = SchemaSnapshotSerializer.Load(paths.SnapshotPath);
        var oldPlan = SchemaSnapshotMapper.ToPlan(oldSnapshot, config);
        var newPlan = generator.BuildPlan(config);

        ProtoCompatibilityValidator.Validate(oldPlan, newPlan);

        var diff = SchemaDiffer.Diff(oldPlan, newPlan);
        if (diff.Changes.Count == 0)
        {
            return new MigrationRunResult(
                ExitCode: 0,
                MigrationPath: null,
                Message: "No schema changes detected.");
        }

        var migrationPlan = MigrationPolicy.Apply(diff, oldPlan, newPlan);
        var parentChecksum = SchemaSnapshotSerializer.ComputeChecksum(
            SchemaSnapshotSerializer.SerializeToString(oldSnapshot));
        var newSnapshot = SchemaSnapshotMapper.FromPlan(newPlan, parentChecksum);
        var targetChecksum = SchemaSnapshotSerializer.ComputeChecksum(
            SchemaSnapshotSerializer.SerializeToString(newSnapshot));

        var version = clock.GetUtcNow().ToString("yyyyMMddHHmmss");
        Directory.CreateDirectory(paths.MigrationsDirectory);
        var migrationPath = Path.Combine(paths.MigrationsDirectory, $"{version}_{SanitizeMigrationName(name)}.sql");
        var sql = MigrationSqlGenerator.Generate(
            migrationPlan,
            oldPlan,
            newPlan,
            $"{version}_{name}",
            parentChecksum,
            targetChecksum,
            clock.GetUtcNow());

        File.WriteAllText(migrationPath, sql);

        if (migrationPlan.RequiresManualApproval && !allowManual)
        {
            return new MigrationRunResult(
                ExitCode: 2,
                MigrationPath: migrationPath,
                Message: "Migration SQL was written, but manual/destructive changes require --allow-manual before updating the snapshot.");
        }

        SchemaSnapshotSerializer.Save(paths.SnapshotPath, newSnapshot);

        var migrations = SchemaMigrationsScriptGenerator.ReadFromDirectory(paths.MigrationsDirectory);
        File.WriteAllText(
            paths.VersionsOutputPath,
            SchemaMigrationsScriptGenerator.Generate(migrations, ClusterDdl.For(config)));

        return new MigrationRunResult(
            ExitCode: 0,
            MigrationPath: migrationPath,
            Message: $"Migration written to '{migrationPath}' and snapshot updated.");
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

public sealed record MigrationRunResult(
    int ExitCode,
    string? MigrationPath,
    string Message);
