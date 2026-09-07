if (args.Length == 0 || args.Contains("--help") || args.Contains("-h"))
{
    Console.WriteLine("Usage: ClickHouseSchemaGen.Cli --config <path-to-clickhouse.codegen.json>");
    return args.Length == 0 ? 1 : 0;
}

var configIndex = Array.IndexOf(args, "--config");
if (configIndex < 0 || configIndex + 1 >= args.Length)
{
    Console.Error.WriteLine("Missing required --config argument.");
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
