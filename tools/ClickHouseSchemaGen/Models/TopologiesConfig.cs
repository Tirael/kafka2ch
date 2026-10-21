namespace ClickHouseSchemaGen.Models;

public sealed class TopologiesConfig
{
    public List<string> Topologies { get; set; } = [];

    public string? MigrationsBaseline { get; set; }
}
