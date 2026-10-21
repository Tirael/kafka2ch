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
            var adoptionMessages = topologies
                .Select(topology => topology.AdoptionMessage)
                .Where(message => !string.IsNullOrEmpty(message))
                .ToList();
            return new MigrationRunResult(
                ExitCode: 0,
                MigrationPaths: [],
                Message: adoptionMessages.Count > 0
                    ? string.Join("; ", adoptionMessages)
                    : "No schema changes detected.");
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
                    generatedAt,
                    topology.Paths.FormatSchemasDirectory is null
                        ? null
                        : FormatSchemaRequirement.ComputeChecksums(
                            topology.Config.KafkaTables,
                            topology.Paths.FormatSchemasDirectory)));
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
        if (diff.Changes.Count > 0 && diff.Changes.All(change => change is AggregatesAdoptedFromTrailingSql))
        {
            SchemaSnapshotSerializer.Save(paths.SnapshotPath, newSnapshot);
            var names = diff.Changes.OfType<AggregatesAdoptedFromTrailingSql>().SelectMany(change => change.Names);
            var targetChecksum = SchemaSnapshotSerializer.ComputeChecksum(
                SchemaSnapshotSerializer.SerializeToString(newSnapshot));
            return new TopologyMigration(
                config,
                paths,
                oldPlan,
                newPlan,
                Diff: new SchemaDiff(),
                MigrationPlan: MigrationPolicy.Apply(new SchemaDiff(), oldPlan, newPlan),
                newSnapshot,
                parentChecksum,
                targetChecksum,
                AdoptionMessage: $"trailingSql adopted as aggregates {string.Join(", ", names)} with identical DDL; "
                    + "snapshot updated, no migration needed.");
        }

        var targetChecksumAfterDiff = SchemaSnapshotSerializer.ComputeChecksum(
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
            targetChecksumAfterDiff);
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

    public MigrationRebaseResult Rebase(
        string configPath,
        string migration,
        string? version = null,
        string? parentSnapshotPath = null,
        TimeProvider? timeProvider = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(migration);
        var clock = timeProvider ?? _defaultTimeProvider;

        var (config, paths) = LoadContext(configPath);
        var migrations = SchemaMigrationsScriptGenerator.ReadFromDirectory(paths.MigrationsDirectory);
        var source = FindMigration(migrations, migration, paths.MigrationsDirectory);
        var latest = migrations
            .Where(m => !ReferenceEquals(m, source))
            .Select(m => m.Version)
            .OrderBy(v => v, StringComparer.Ordinal)
            .LastOrDefault();

        var newVersion = version is null
            ? MigrationVersion.Next(latest, clock.GetUtcNow())
            : ValidateExplicitVersion(version, latest);

        var sql = File.ReadAllText(source.Path);
        ValidateSnapshotChain(paths.SnapshotPath, parentSnapshotPath, source, sql);

        var newPath = Path.Combine(Path.GetDirectoryName(source.Path)!, $"{newVersion}_{source.Name}.sql");
        if (File.Exists(newPath))
            throw new InvalidOperationException($"Migration file '{newPath}' already exists.");

        if (MigrationVersion.ReadHeader(sql, MigrationHeaderField.Migration) is null)
        {
            File.Move(source.Path, newPath);
        }
        else
        {
            File.WriteAllText(newPath, MigrationVersion.RewriteMigrationHeader(sql, $"{newVersion}_{source.Name}"));
            File.Delete(source.Path);
        }

        WriteVersionsScript(config, paths);
        return new MigrationRebaseResult(source.Path, newPath, newVersion);
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
            VersionsOutputPath: CodegenConfigLoader.ResolvePath(configPath, config.Migrations.VersionsOutputPath),
            FormatSchemasDirectory: string.IsNullOrWhiteSpace(config.Migrations.FormatSchemasDirectory)
                ? null
                : CodegenConfigLoader.ResolvePath(configPath, config.Migrations.FormatSchemasDirectory)));
    }

    private static MigrationFileInfo FindMigration(
        IReadOnlyList<MigrationFileInfo> migrations,
        string migration,
        string migrationsDirectory)
    {
        var key = Path.GetFileName(migration);
        if (key.EndsWith(".sql", StringComparison.OrdinalIgnoreCase))
            key = key[..^".sql".Length];

        var matches = migrations
            .Where(m => string.Equals($"{m.Version}_{m.Name}", key, StringComparison.Ordinal)
                || string.Equals(m.Name, key, StringComparison.Ordinal))
            .ToList();

        return matches.Count switch
        {
            0 => throw new InvalidOperationException(
                $"Migration '{migration}' not found in '{migrationsDirectory}'."),
            1 => matches[0],
            _ => throw new InvalidOperationException(
                $"Migration name '{migration}' is ambiguous: "
                + string.Join(", ", matches.Select(m => Path.GetFileName(m.Path)))
                + ". Pass the full file name.")
        };
    }

    private static string ValidateExplicitVersion(string version, string? latest)
    {
        if (version.Length != MigrationVersion.Format.Length || !MigrationVersion.TryParse(version, out _))
        {
            throw new InvalidOperationException(
                $"Invalid version '{version}'; expected {MigrationVersion.Format} (UTC).");
        }

        if (latest is not null && string.CompareOrdinal(version, latest) <= 0)
        {
            throw new InvalidOperationException(
                $"Version '{version}' must be greater than the latest migration version '{latest}'.");
        }

        return version;
    }

    private static void ValidateSnapshotChain(
        string snapshotPath,
        string? parentSnapshotPath,
        MigrationFileInfo migration,
        string sql)
    {
        if (!File.Exists(snapshotPath))
        {
            throw new InvalidOperationException(
                $"Snapshot not found at '{snapshotPath}'. Run migrate init first.");
        }

        var snapshot = SchemaSnapshotSerializer.Load(snapshotPath);
        if (string.IsNullOrWhiteSpace(snapshot.ParentChecksum))
        {
            throw new InvalidOperationException(
                $"Snapshot '{snapshotPath}' has no parentChecksum, so it was not produced by a migration. "
                + "Restore the snapshot from the base branch and regenerate the migration with migrate --name.");
        }

        if (parentSnapshotPath is not null)
        {
            var expectedParent = SnapshotChecksum(SchemaSnapshotSerializer.Load(parentSnapshotPath));
            if (!string.Equals(snapshot.ParentChecksum, expectedParent, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException(
                    $"Snapshot '{snapshotPath}' was not generated over '{parentSnapshotPath}' "
                    + $"(parentChecksum {snapshot.ParentChecksum}, expected {expectedParent}). "
                    + "Restore the snapshot from the base branch and regenerate the migration with migrate --name.");
            }
        }

        var fileName = Path.GetFileName(migration.Path);
        var headerTarget = MigrationVersion.ReadHeader(sql, MigrationHeaderField.TargetChecksum);
        var snapshotChecksum = SnapshotChecksum(snapshot);
        if (headerTarget is not null
            && !string.Equals(headerTarget, snapshotChecksum, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"Migration '{fileName}' Target checksum {headerTarget} does not match snapshot '{snapshotPath}' "
                + $"({snapshotChecksum}); only the migration that produced the current snapshot can be rebased.");
        }

        var headerParent = MigrationVersion.ReadHeader(sql, MigrationHeaderField.ParentChecksum);
        if (headerParent is not null
            && !string.Equals(headerParent, snapshot.ParentChecksum, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"Migration '{fileName}' Parent checksum {headerParent} does not match snapshot parentChecksum "
                + $"{snapshot.ParentChecksum} in '{snapshotPath}'.");
        }
    }

    private static string SnapshotChecksum(SchemaSnapshot snapshot) =>
        SchemaSnapshotSerializer.ComputeChecksum(SchemaSnapshotSerializer.SerializeToString(snapshot));

    private static string SanitizeMigrationName(string name) =>
        string.Join('_', name.Split(Path.GetInvalidFileNameChars(), StringSplitOptions.RemoveEmptyEntries))
            .Replace(" ", "_", StringComparison.Ordinal);
}

public sealed record MigrationPaths(
    string SnapshotPath,
    string MigrationsDirectory,
    string VersionsOutputPath,
    string? FormatSchemasDirectory = null);

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
    string TargetChecksum,
    string? AdoptionMessage = null);

public sealed record MigrationRebaseResult(
    string OldPath,
    string NewPath,
    string Version);
