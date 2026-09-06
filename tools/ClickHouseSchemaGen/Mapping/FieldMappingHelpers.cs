namespace ClickHouseSchemaGen.Mapping;

internal static class FieldMappingHelpers
{
    public static ClickHouseColumn[]? TryCreateFromTypeOverride(
        FieldMappingRequest request,
        MappingStrategy strategy,
        string comment) =>
        request.OverrideType is null
            ? null
            : [ClickHouseColumn.Create(request.ColumnPath, request.OverrideType, strategy, comment)];
}
