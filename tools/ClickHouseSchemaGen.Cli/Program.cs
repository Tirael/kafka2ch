using System.Reflection;

if (args.Length == 0 || args.Contains("--help") || args.Contains("-h"))
{
    PrintUsage();
    return args.Length == 0 ? 1 : 0;
}

var command = args[0];
try
{
    return command switch
    {
        "generate" => RunGenerate(args.AsSpan(1).ToArray()),
        "migrate" => RunMigrate(args.AsSpan(1).ToArray()),
        _ when args.Contains("--config") => RunGenerate(args),
        _ => UnknownCommand(command)
    };
}
catch (ValidationException exception)
{
    Console.Error.WriteLine("Invalid codegen config:");
    foreach (var error in exception.Errors)
        Console.Error.WriteLine($"  {error.PropertyName}: {error.ErrorMessage}");
    return 1;
}
catch (Exception exception)
{
    Console.Error.WriteLine(exception.Message);
    return 1;
}

static int RunGenerate(string[] args)
{
    var generator = CreateGenerator(args);
    var configPath = RequireConfig(args);
    generator.GenerateFromConfigFile(configPath);
    Console.WriteLine($"Generated ClickHouse DDL from '{configPath}'.");
    return 0;
}

static int RunMigrate(string[] args)
{
    if (args.Length == 0)
    {
        Console.Error.WriteLine("Missing migrate subcommand (init|status|versions|snapshot-upgrade|verify|rebase|--name).");
        return 1;
    }

    var orchestrator = new MigrationOrchestrator(CreateGenerator(args));
    if (args[0] == "init")
    {
        var configPath = RequireConfig(args.AsSpan(1).ToArray());
        orchestrator.Init(configPath);
        Console.WriteLine($"Initialized schema snapshot for '{configPath}'.");
        return 0;
    }

    if (args[0] == "versions")
    {
        var configPath = RequireConfig(args.AsSpan(1).ToArray());
        var versionsPath = orchestrator.WriteVersionsScript(configPath);
        Console.WriteLine($"Regenerated migration baseline '{versionsPath}'.");
        return 0;
    }

    if (args[0] == "snapshot-upgrade")
    {
        LoadMessageAssemblies(args.AsSpan(1).ToArray());
        var configPath = RequireConfig(args.AsSpan(1).ToArray());
        var upgrade = orchestrator.UpgradeSnapshot(configPath);
        Console.WriteLine(upgrade.FromVersion == upgrade.ToVersion
            ? $"Snapshot '{upgrade.SnapshotPath}' is already version {upgrade.ToVersion}; rewritten without changes."
            : $"Upgraded snapshot '{upgrade.SnapshotPath}' from version {upgrade.FromVersion} to {upgrade.ToVersion}.");
        return 0;
    }

    if (args[0] == "rebase")
    {
        var rebaseArgs = args.AsSpan(1).ToArray();
        var configPath = RequireConfig(rebaseArgs);
        var migration = OptionalArg(rebaseArgs, "--name")
            ?? throw new InvalidOperationException("Missing required --name for migrate rebase.");
        var rebase = orchestrator.Rebase(
            configPath,
            migration,
            OptionalArg(rebaseArgs, "--version"),
            OptionalArg(rebaseArgs, "--parent-snapshot"));
        Console.WriteLine($"Renamed '{rebase.OldPath}' -> '{rebase.NewPath}' and regenerated the migration baseline.");
        return 0;
    }

    if (args[0] == "status")
    {
        var configPath = RequireConfig(args.AsSpan(1).ToArray());
        var status = orchestrator.Status(configPath);
        if (!status.HasSnapshot)
        {
            Console.WriteLine($"No snapshot at '{status.SnapshotPath}'. Run: migrate init --config ...");
            return 0;
        }

        if (!status.IsDrifted)
        {
            Console.WriteLine("Schema matches snapshot.");
            return 0;
        }

        Console.WriteLine("Schema drifted from snapshot:");
        foreach (var difference in status.Differences)
            Console.WriteLine($"  - {difference}");
        return 1;
    }

    if (args[0] == "verify")
    {
        var (verifyConfigs, rootBaseline) = RequireTopologies(args.AsSpan(1).ToArray());
        var baselineIndex = Array.IndexOf(args, "--baseline");
        var baseline = baselineIndex >= 0 && baselineIndex + 1 < args.Length ? args[baselineIndex + 1] : rootBaseline;
        var verification = orchestrator.Verify(verifyConfigs, baseline);
        var scope = baseline is null ? "all migrations" : $"migrations after baseline {baseline}";
        if (verification.IsConsistent)
        {
            Console.WriteLine($"Migration versions are identical across {verifyConfigs.Count} topologies ({scope}).");
            return 0;
        }

        Console.Error.WriteLine($"Migration versions diverge across topologies ({scope}):");
        foreach (var gap in verification.Gaps)
            Console.Error.WriteLine($"  {gap.MigrationsDirectory} is missing {string.Join(", ", gap.MissingVersions)}");
        Console.Error.WriteLine("Generate migrations for all topologies at once: migrate --all-topologies <path> --name <name>.");
        return 1;
    }

    var migrateArgs = args;
    var nameIndex = Array.IndexOf(migrateArgs, "--name");
    if (nameIndex < 0 || nameIndex + 1 >= migrateArgs.Length)
    {
        Console.Error.WriteLine("Missing required --name for migrate.");
        return 1;
    }

    var name = migrateArgs[nameIndex + 1];
    var allowManual = migrateArgs.Contains("--allow-manual");
    var (configPaths, _) = RequireTopologies(migrateArgs);
    var result = orchestrator.Migrate(configPaths, name, allowManual);
    Console.WriteLine(result.Message);
    foreach (var migrationPath in result.MigrationPaths)
        Console.WriteLine($"Migration file: {migrationPath}");
    return result.ExitCode;
}

static (IReadOnlyList<string> ConfigPaths, string? Baseline) RequireTopologies(string[] args)
{
    var rootIndex = Array.IndexOf(args, "--all-topologies");
    if (rootIndex >= 0)
    {
        if (rootIndex + 1 >= args.Length)
            throw new InvalidOperationException("Missing path after --all-topologies.");
        var topologies = TopologiesConfigLoader.Load(args[rootIndex + 1]);
        return (topologies.ConfigPaths, topologies.MigrationsBaseline);
    }

    var configPaths = args
        .Select((arg, index) => (arg, index))
        .Where(entry => entry.arg == "--config" && entry.index + 1 < args.Length)
        .Select(entry => args[entry.index + 1])
        .ToList();
    if (configPaths.Count == 0)
        throw new InvalidOperationException("Missing required --config or --all-topologies argument.");
    return (configPaths, null);
}

static ClickHouseSchemaGenerator CreateGenerator(string[] args)
{
    var descriptorSetPath = OptionValue(args, "--descriptor-set");
    if (descriptorSetPath is not null && OptionValue(args, "--assemblies") is not null)
        throw new InvalidOperationException("Use either --assemblies or --descriptor-set, not both.");

    LoadMessageAssemblies(args);
    return new ClickHouseSchemaGenerator(
        new DenormalizationPlanner(),
        descriptorSource: descriptorSetPath is null ? null : DescriptorSetMessageDescriptorSource.Load(descriptorSetPath));
}

static string? OptionValue(string[] args, string option)
{
    var index = Array.IndexOf(args, option);
    return index < 0 || index + 1 >= args.Length ? null : args[index + 1];
}

static void LoadMessageAssemblies(string[] args)
{
    var assemblies = OptionValue(args, "--assemblies");
    if (assemblies is null)
        return;

    foreach (var path in assemblies.Split(
                 ';',
                 StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
    {
        if (!File.Exists(path))
            throw new FileNotFoundException($"Message assembly not found: {path}");
        Assembly.LoadFrom(Path.GetFullPath(path));
    }
}

static string RequireConfig(string[] args) =>
    OptionalArg(args, "--config")
        ?? throw new InvalidOperationException("Missing required --config argument.");

static string? OptionalArg(string[] args, string name)
{
    var index = Array.IndexOf(args, name);
    return index < 0 || index + 1 >= args.Length ? null : args[index + 1];
}

static int UnknownCommand(string command)
{
    Console.Error.WriteLine($"Unknown command '{command}'.");
    PrintUsage();
    return 1;
}

static void PrintUsage()
    => Console.WriteLine("""
        Usage:
          clickhouse-schema-gen generate --config <path> [--assemblies <dll[;dll…]> | --descriptor-set <file>]
          clickhouse-schema-gen migrate init --config <path> [--assemblies <dll[;dll…]> | --descriptor-set <file>]
          clickhouse-schema-gen migrate status --config <path> [--assemblies <dll[;dll…]> | --descriptor-set <file>]
          clickhouse-schema-gen migrate versions --config <path>   (rewrite versionsOutputPath from migrations)
          clickhouse-schema-gen migrate snapshot-upgrade --config <path> [--assemblies <dll[;dll…]> | --descriptor-set <file>]
                                                                   (rewrite schema.snapshot.json in the current format)
          clickhouse-schema-gen migrate rebase --config <path> --name <file> [--version <yyyyMMddHHmmss>] [--parent-snapshot <path>]
                                (rename an unapplied migration to a version after all others and rewrite versionsOutputPath)
          clickhouse-schema-gen migrate --config <path> [--config <path> …] --name <name> [--assemblies <dll[;dll…]> | --descriptor-set <file>] [--allow-manual]
          clickhouse-schema-gen migrate --all-topologies <topologies.json> --name <name> [--assemblies <dll[;dll…]> | --descriptor-set <file>] [--allow-manual]
                                                                   (one version for every topology; snapshots updated all or none)
          clickhouse-schema-gen migrate verify --all-topologies <topologies.json> [--baseline <version>]
          clickhouse-schema-gen migrate verify --config <path> --config <path> … [--baseline <version>]
                                                                   (fail unless migration versions after the baseline match)
          clickhouse-schema-gen --config <path> [--assemblies <dll[;dll…]> | --descriptor-set <file>]   (alias for generate)

        --assemblies      Semicolon-separated paths to assemblies that define protobuf message types
                          referenced by messageType in the codegen config (required when the tool
                          package does not already reference those contracts).
        --descriptor-set  FileDescriptorSet produced by `protoc --include_imports --descriptor_set_out=<file>`
                          or `buf build -o <file>`, used instead of assemblies (overrides descriptorSetPath
                          in the codegen config). messageType may be the protobuf full name or the C# type name.
        --all-topologies  Root config listing codegen configs ("topologies") and the history
                          baseline ("migrationsBaseline") accepted as divergent.
        """);
