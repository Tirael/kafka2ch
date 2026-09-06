using ClickHouseSchemaGen;

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
var generator = new ClickHouseSchemaGenerator(new ProtoToClickHouseMapper(), new KafkaTableGenerator());
generator.GenerateFromConfigFile(configPath);
Console.WriteLine($"Generated ClickHouse DDL from '{configPath}'.");
return 0;
