namespace ClickHouseSchemaGen.Mapping;

public sealed class MapFieldStrategy(DenormalizationPlanner planner) : IFieldMappingStrategy
{
    public bool CanMap(FieldMappingRequest request) => request.Field.IsMap;

    public IEnumerable<ClickHouseColumn> Map(FieldMappingRequest request) =>
        FieldMappingHelpers.TryCreateFromTypeOverride(request, MappingStrategy.Map, "proto map")
        ?? [CreateMapColumn(request)];

    private ClickHouseColumn CreateMapColumn(FieldMappingRequest request)
    {
        var keyField = request.Field.MessageType.FindFieldByNumber(1)
            ?? throw new InvalidOperationException($"Map field '{request.ColumnPath}' is missing key definition.");
        var valueField = request.Field.MessageType.FindFieldByNumber(2)
            ?? throw new InvalidOperationException($"Map field '{request.ColumnPath}' is missing value definition.");

        var keyType = ClickHouseTypeResolver.ResolveScalar(
            request.WithColumnPath($"{request.ColumnPath}.key") with { Field = keyField });
        var (valueType, flattensGoogleWrapper) = ResolveMapValueType(request, valueField);

        return ClickHouseColumn.Create(
            request.ColumnPath,
            $"Map({keyType}, {valueType})",
            MappingStrategy.Map,
            "proto map",
            flattensGoogleWrapper,
            request.Field.FieldNumber.ToString());
    }

    private (string Type, bool FlattensGoogleWrapper) ResolveMapValueType(
        FieldMappingRequest request,
        FieldDescriptor valueField)
    {
        var valueRequest = request.WithColumnPath($"{request.ColumnPath}.value") with { Field = valueField };

        if (valueField.FieldType != FieldType.Message)
            return (ClickHouseTypeResolver.ResolveScalar(valueRequest), false);

        var wellKnownType = WellKnownTypeRegistry.MapMessageType(valueField.MessageType);
        if (wellKnownType is not null)
        {
            return (
                valueRequest.OverrideType ?? wellKnownType,
                WellKnownTypeRegistry.IsWrapper(valueField.MessageType));
        }

        if (valueRequest.OverrideType is not null)
            return (valueRequest.OverrideType, false);

        var innerColumns = planner.MapNestedFields(
            valueField.MessageType,
            request.Context,
            $"{request.ColumnPath}.value").ToArray();

        return (
            DenormalizationPlanner.BuildTupleType(innerColumns),
            innerColumns.Any(column => column.FlattensGoogleWrapper));
    }
}
