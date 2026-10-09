using ClickHouseSchemaGen.Migrator;
using Microsoft.Extensions.Logging;

if (args.Contains("--help") || args.Contains("-h") || args.Length == 0)
{
    Console.WriteLine("""
        Usage: ClickHouseSchemaGen.Migrator --migrations <dir> [--cluster <name>]
          --cluster <name>   apply to a ClickHouse cluster (env ClickHouse__Cluster); schema_migrations
                             is created ON CLUSTER as a ReplicatedMergeTree spanning every node
                             (Keeper path: env ClickHouse__HistoryReplicatedPath, replica: ClickHouse__HistoryReplicaName)
        """);
    return args.Length == 0 ? 1 : 0;
}

var migrationsIndex = Array.IndexOf(args, "--migrations");
if (migrationsIndex < 0 || migrationsIndex + 1 >= args.Length)
{
    Console.Error.WriteLine("Missing required --migrations argument.");
    return 1;
}

var migrationsDirectory = args[migrationsIndex + 1];
var options = new ClickHouseConnectionOptions();
var clusterIndex = Array.IndexOf(args, "--cluster");
if (clusterIndex >= 0)
{
    if (clusterIndex + 1 >= args.Length)
    {
        Console.Error.WriteLine("Missing value for --cluster.");
        return 1;
    }

    options = new ClickHouseConnectionOptions { Cluster = args[clusterIndex + 1] };
}

using var loggerFactory = LoggerFactory.Create(builder => builder.AddSimpleConsole(simpleConsole =>
{
    simpleConsole.SingleLine = true;
    simpleConsole.TimestampFormat = "HH:mm:ss ";
}));
var logger = loggerFactory.CreateLogger("Migrator");
var runner = new MigrationRunner(logger, TimeProvider.System, options.ToClusterConfig());

try
{
    await runner.ApplyAsync(options.ConnectionString, migrationsDirectory, CancellationToken.None);
    logger.LogInformation(
        "Migrations applied from {Directory}{Cluster}",
        migrationsDirectory,
        string.IsNullOrWhiteSpace(options.Cluster) ? string.Empty : $" on cluster {options.Cluster}");
    return 0;
}
catch (Exception exception)
{
    logger.LogError(exception, "Migration apply failed");
    return 1;
}
