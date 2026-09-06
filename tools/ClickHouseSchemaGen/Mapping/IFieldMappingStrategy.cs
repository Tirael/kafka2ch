namespace ClickHouseSchemaGen.Mapping;

public interface IFieldMappingStrategy
{
    bool CanMap(FieldMappingRequest request);

    IEnumerable<ClickHouseColumn> Map(FieldMappingRequest request);
}
