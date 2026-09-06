namespace ClickHouseSchemaGen.Tests.Support;

internal static class SchemaGeneratorFactory
{
    public static ClickHouseSchemaGenerator Create() => new(new DenormalizationPlanner());
}
