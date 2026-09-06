namespace ClickHouseSchemaGen.Models;

public sealed record ClickHouseColumn(string Name, string Type, string? Comment = null);
