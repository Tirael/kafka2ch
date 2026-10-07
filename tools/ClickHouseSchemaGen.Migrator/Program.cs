using ClickHouseSchemaGen.Migrator;
using Microsoft.Extensions.Logging;

if (args.Contains("--help") || args.Contains("-h") || args.Length == 0)
{
    Console.WriteLine("Usage: ClickHouseSchemaGen.Migrator --migrations <dir>");
    return args.Length == 0 ? 1 : 0;
}

var migrationsIndex = Array.IndexOf(args, "--migrations");
if (migrationsIndex < 0 || migrationsIndex + 1 >= args.Length)
{
    Console.Error.WriteLine("Missing required --migrations argument.");
    return 1;
}

var migrationsDirectory = args[migrationsIndex + 1];
using var loggerFactory = LoggerFactory.Create(builder => builder.AddSimpleConsole(options =>
{
    options.SingleLine = true;
    options.TimestampFormat = "HH:mm:ss ";
}));
var logger = loggerFactory.CreateLogger("Migrator");
var options = new ClickHouseConnectionOptions();
var runner = new MigrationRunner(logger, TimeProvider.System);

try
{
    await runner.ApplyAsync(options.ConnectionString, migrationsDirectory, CancellationToken.None);
    logger.LogInformation("Migrations applied from {Directory}", migrationsDirectory);
    return 0;
}
catch (Exception exception)
{
    logger.LogError(exception, "Migration apply failed");
    return 1;
}
