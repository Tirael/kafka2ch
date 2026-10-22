if (args.Contains("--help") || args.Contains("-h") || args.Length == 0)
{
    Console.WriteLine("""
        Usage: ClickHouseSchemaGen.Migrator --migrations <dir> [--cluster <name>] [--ddl-mode onCluster|replicatedDatabase]
                                            [--lock-timeout <duration>] [--lock-ttl <duration>] [--skip-preflight]
          --cluster <name>     ClickHouse cluster (env ClickHouse__Cluster)
          --skip-preflight     do not verify '-- require:format_schema' markers on every replica via file()
                               (needs format_schema_path under user_files_path) and do not dry-run
                               Kafka queue / materialized view DDL in a temporary database before applying
          --ddl-mode <mode>    onCluster (default): schema_migrations ON CLUSTER;
                               replicatedDatabase: DDL without ON CLUSTER in a Replicated database
                               (env ClickHouse__DdlMode). History Keeper path/replica:
                               ClickHouse__HistoryReplicatedPath / ClickHouse__HistoryReplicaName
          --lock-timeout <d>   how long to wait for another migrator's lock, seconds or hh:mm:ss (default 300)
          --lock-ttl <d>       lease of the acquired lock; an expired lock is taken over (default 1800)
        """);
    return args.Length == 0 ? 1 : 0;
}

if (!MigratorArguments.TryParse(args, out var arguments, out var error))
{
    Console.Error.WriteLine(error);
    return 1;
}

var options = new ClickHouseConnectionOptions
{
    Cluster = arguments.Cluster ?? new ClickHouseConnectionOptions().Cluster,
    DdlMode = arguments.DdlMode ?? new ClickHouseConnectionOptions().DdlMode
};

using var loggerFactory = LoggerFactory.Create(builder => builder.AddSimpleConsole(simpleConsole =>
{
    simpleConsole.SingleLine = true;
    simpleConsole.TimestampFormat = "HH:mm:ss ";
}));
var logger = loggerFactory.CreateLogger("Migrator");
var lockOptions = new MigrationLockOptions { Timeout = arguments.LockTimeout, Ttl = arguments.LockTtl };
var runner = new MigrationRunner(
    logger,
    TimeProvider.System,
    options.ToClusterConfig(),
    lockOptions,
    skipPreflight: args.Contains("--skip-preflight"));

try
{
    await runner.ApplyAsync(options.ConnectionString, arguments.MigrationsDirectory, CancellationToken.None);
    logger.LogInformation(
        "Migrations applied from {Directory}{Cluster}",
        arguments.MigrationsDirectory,
        string.IsNullOrWhiteSpace(options.Cluster) ? string.Empty : $" on cluster {options.Cluster}");
    return 0;
}
catch (Exception exception)
{
    logger.LogError(exception, "Migration apply failed");
    return 1;
}
