namespace ClickHouseSchemaGen.Models;

public enum MappingStrategy
{
    Direct,
    Optional,
    Flatten,
    Repeat,
    Nested,
    Map,
    Oneof,
    WellKnownType,
    JsonFallback,
    Tuple
}
