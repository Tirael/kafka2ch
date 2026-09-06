namespace ClickHouseSchemaGen.Mapping;

public interface IFieldMappingStrategy
{
    bool CanMap(FieldDescriptor field, MappingContext context);

    IEnumerable<ClickHouseColumn> Map(FieldDescriptor field, string columnPath, MappingContext context);
}
