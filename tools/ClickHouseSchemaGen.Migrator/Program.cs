if (args.Contains("--help") || args.Contains("-h") || args.Length == 0)
{
    Console.WriteLine("""
        Usage: ClickHouseSchemaGen.Migrator --migrations <dir> [--cluster <name>] [--ddl-mode onCluster|replicatedDatabase]
          --cluster <name>     ClickHouse cluster (env ClickHouse__Cluster)
          --ddl-mode <mode>    onCluster (default): schema_migrations ON CLUSTER;
                               replicatedDatabase: DDL without ON CLUSTER in a Replicated database
                               (env ClickHouse__DdlMode). History Keeper path/replica:
                               ClickHouse__HistoryReplicatedPath / ClickHouse__HistoryReplicaName
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
string? cluster = null;
string? ddlMode = null;

for (var i = 0; i < args.Length; i++)
{
    if (args[i] == "--cluster")
    {
        if (i + 1 >= args.Length)
        {
            Console.Error.WriteLine("Missing value for --cluster.");
            return 1;
        }

        cluster = args[++i];
    }
    else if (args[i] == "--ddl-mode")
    {
        if (i + 1 >= args.Length)
        {
            Console.Error.WriteLine("Missing value for --ddl-mode.");
            return 1;
        }

        ddlMode = args[++i];
    }
}

if (ddlMode is not null
    && !ClusterDdlModes.All.Contains(ddlMode, StringComparer.OrdinalIgnoreCase))
{
    Console.Error.WriteLine($"Invalid --ddl-mode '{ddlMode}'. Expected: {string.Join(", ", ClusterDdlModes.All)}.");
    return 1;
}

var options = new ClickHouseConnectionOptions
{
    Cluster = cluster ?? new ClickHouseConnectionOptions().Cluster,
    DdlMode = ddlMode ?? new ClickHouseConnectionOptions().DdlMode
};

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
