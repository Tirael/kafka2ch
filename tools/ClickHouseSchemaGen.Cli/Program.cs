using ClickHouseSchemaGen;
using ClickHouseSchemaGen.Mapping;
using ClickHouseSchemaGen.Migration;
using ClickHouseSchemaGen.Validation;

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
    var configPath = RequireConfig(args);
    new ClickHouseSchemaGenerator(new DenormalizationPlanner()).GenerateFromConfigFile(configPath);
    Console.WriteLine($"Generated ClickHouse DDL from '{configPath}'.");
    return 0;
}

static int RunMigrate(string[] args)
{
    if (args.Length == 0)
    {
        Console.Error.WriteLine("Missing migrate subcommand (init|status|--name).");
        return 1;
    }

    var orchestrator = new MigrationOrchestrator();
    if (args[0] == "init")
    {
        var configPath = RequireConfig(args.AsSpan(1).ToArray());
        orchestrator.Init(configPath);
        Console.WriteLine($"Initialized schema snapshot for '{configPath}'.");
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

    var migrateArgs = args;
    var nameIndex = Array.IndexOf(migrateArgs, "--name");
    if (nameIndex < 0 || nameIndex + 1 >= migrateArgs.Length)
    {
        Console.Error.WriteLine("Missing required --name for migrate.");
        return 1;
    }

    var name = migrateArgs[nameIndex + 1];
    var allowManual = migrateArgs.Contains("--allow-manual");
    var configPathMigrate = RequireConfig(migrateArgs);
    var result = orchestrator.Migrate(configPathMigrate, name, allowManual);
    Console.WriteLine(result.Message);
    if (result.MigrationPath is not null)
        Console.WriteLine($"Migration file: {result.MigrationPath}");
    return result.ExitCode;
}

static string RequireConfig(string[] args)
{
    var configIndex = Array.IndexOf(args, "--config");
    if (configIndex < 0 || configIndex + 1 >= args.Length)
        throw new InvalidOperationException("Missing required --config argument.");
    return args[configIndex + 1];
}

static int UnknownCommand(string command)
{
    Console.Error.WriteLine($"Unknown command '{command}'.");
    PrintUsage();
    return 1;
}

static void PrintUsage()
{
    Console.WriteLine("""
        Usage:
          ClickHouseSchemaGen.Cli generate --config <path>
          ClickHouseSchemaGen.Cli migrate init --config <path>
          ClickHouseSchemaGen.Cli migrate status --config <path>
          ClickHouseSchemaGen.Cli migrate --config <path> --name <name> [--allow-manual]
          ClickHouseSchemaGen.Cli --config <path>   (alias for generate)
        """);
}
