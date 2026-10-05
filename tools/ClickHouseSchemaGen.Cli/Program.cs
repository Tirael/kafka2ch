if (args.Length == 0 || args.Contains("--help") || args.Contains("-h"))
{
    PrintUsage();
    return args.Length == 0 ? 1 : 0;
}

if (args.Contains("--migrate"))
    return RunMigrate(args);

return RunGenerate(args);

static int RunGenerate(string[] args)
{
    var configIndex = Array.IndexOf(args, "--config");
    if (configIndex < 0 || configIndex + 1 >= args.Length)
    {
        Console.Error.WriteLine("Missing required --config argument.");
        PrintUsage();
        return 1;
    }

    var configPath = args[configIndex + 1];

    try
    {
        new ClickHouseSchemaGenerator(new DenormalizationPlanner()).GenerateFromConfigFile(configPath);
    }
    catch (ValidationException exception)
    {
        Console.Error.WriteLine($"Invalid codegen config '{configPath}':");
        foreach (var error in exception.Errors)
            Console.Error.WriteLine($"  {error.PropertyName}: {error.ErrorMessage}");

        return 1;
    }

    Console.WriteLine($"Generated ClickHouse DDL from '{configPath}'.");
    return 0;
}

static int RunMigrate(string[] args)
{
    var fromType = RequireArg(args, "--from");
    var toType = RequireArg(args, "--to");
    var tableName = RequireArg(args, "--table");
    if (fromType is null || toType is null || tableName is null)
    {
        PrintUsage();
        return 1;
    }

    var outputPath = OptionalArg(args, "--output");
    var dropRemoved = args.Contains("--drop-removed");
    var noModify = args.Contains("--no-modify");

    try
    {
        var migrator = new ProtoSchemaMigrator();
        var sql = migrator.GenerateAlterTableSql(
            tableName,
            fromType,
            toType,
            options: new AlterTableMigrationOptions
            {
                DropRemovedColumns = dropRemoved,
                ModifyChangedTypes = !noModify
            });

        if (string.IsNullOrWhiteSpace(outputPath))
        {
            Console.Write(sql);
        }
        else
        {
            var fullPath = Path.GetFullPath(outputPath);
            Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);
            File.WriteAllText(fullPath, sql);
            Console.WriteLine($"Wrote ALTER TABLE migration for '{tableName}' to '{fullPath}'.");
        }

        return 0;
    }
    catch (Exception exception)
    {
        Console.Error.WriteLine($"Migration failed: {exception.Message}");
        return 1;
    }
}

static string? RequireArg(string[] args, string name)
{
    var index = Array.IndexOf(args, name);
    if (index < 0 || index + 1 >= args.Length)
    {
        Console.Error.WriteLine($"Missing required {name} argument.");
        return null;
    }

    return args[index + 1];
}

static string? OptionalArg(string[] args, string name)
{
    var index = Array.IndexOf(args, name);
    if (index < 0 || index + 1 >= args.Length)
        return null;

    return args[index + 1];
}

static void PrintUsage()
{
    Console.WriteLine(
        """
        Usage:
          ClickHouseSchemaGen.Cli --config <path-to-clickhouse.codegen.json>
          ClickHouseSchemaGen.Cli --migrate --from <ClrType, Assembly> --to <ClrType, Assembly> --table <name> [--output <file.sql>] [--drop-removed] [--no-modify]

        --migrate compares two protobuf message types (mapped with the same ClickHouseSchemaGen rules)
        and emits ALTER TABLE for a MergeTree (or other mutable) table.

        Notes:
          - Kafka engine tables do not support ADD/MODIFY COLUMN; recreate the queue table separately.
          - Removed columns are skipped unless --drop-removed is set.
        """);
}
