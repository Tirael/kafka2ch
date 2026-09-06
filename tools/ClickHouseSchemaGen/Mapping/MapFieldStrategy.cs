namespace ClickHouseSchemaGen.Mapping;

public sealed class MapFieldStrategy : IFieldMappingStrategy
{
    public bool CanMap(FieldDescriptor field, MappingContext context) => field.IsMap;

    public IEnumerable<ClickHouseColumn> Map(FieldDescriptor field, string columnPath, MappingContext context)
    {
        var fieldOverride = context.GetOverride(columnPath);
        if (!string.IsNullOrWhiteSpace(fieldOverride?.Type))
        {
            yield return new ClickHouseColumn(columnPath, fieldOverride.Type, "proto map", columnPath, MappingStrategy.Map);
            yield break;
        }

        var keyField = field.MessageType.FindFieldByNumber(1)
            ?? throw new InvalidOperationException($"Map field '{columnPath}' is missing key definition.");
        var valueField = field.MessageType.FindFieldByNumber(2)
            ?? throw new InvalidOperationException($"Map field '{columnPath}' is missing value definition.");

        var keyType = ClickHouseTypeResolver.ResolveScalar(keyField, $"{columnPath}.key", context);
        var valueType = ClickHouseTypeResolver.ResolveScalar(valueField, $"{columnPath}.value", context);

        yield return new ClickHouseColumn(
            columnPath,
            $"Map({keyType}, {valueType})",
            "proto map",
            columnPath,
            MappingStrategy.Map);
    }
}
