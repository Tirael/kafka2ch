namespace ClickHouseSchemaGen.Mapping;

public sealed class MapFieldStrategy : IFieldMappingStrategy
{
    public bool CanMap(FieldMappingRequest request) => request.Field.IsMap;

    public IEnumerable<ClickHouseColumn> Map(FieldMappingRequest request) =>
        FieldMappingHelpers.TryCreateFromTypeOverride(request, MappingStrategy.Map, "proto map")
        ?? [CreateMapColumn(request)];

    private static ClickHouseColumn CreateMapColumn(FieldMappingRequest request)
    {
        var keyField = request.Field.MessageType.FindFieldByNumber(1)
            ?? throw new InvalidOperationException($"Map field '{request.ColumnPath}' is missing key definition.");
        var valueField = request.Field.MessageType.FindFieldByNumber(2)
            ?? throw new InvalidOperationException($"Map field '{request.ColumnPath}' is missing value definition.");

        var keyType = ClickHouseTypeResolver.ResolveScalar(
            request.WithColumnPath($"{request.ColumnPath}.key") with { Field = keyField });
        var valueType = ClickHouseTypeResolver.ResolveScalar(
            request.WithColumnPath($"{request.ColumnPath}.value") with { Field = valueField });

        return ClickHouseColumn.Create(
            request.ColumnPath,
            $"Map({keyType}, {valueType})",
            MappingStrategy.Map,
            "proto map");
    }
}
