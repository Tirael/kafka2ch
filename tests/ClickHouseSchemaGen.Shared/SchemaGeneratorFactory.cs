namespace ClickHouseSchemaGen.Shared;

public static class SchemaGeneratorFactory
{
    public static ClickHouseSchemaGenerator Create() => new(new DenormalizationPlanner());
}
