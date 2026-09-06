namespace ClickHouseSchemaGen.Mapping;

public sealed class RepeatedFieldStrategy(DenormalizationPlanner planner) : IFieldMappingStrategy
{
    public bool CanMap(FieldMappingRequest request) => request.Field.IsRepeated;

    public IEnumerable<ClickHouseColumn> Map(FieldMappingRequest request) =>
        FieldMappingHelpers.TryCreateFromTypeOverride(request, MappingStrategy.Repeat, "proto repeated")
        ?? (request.Field.FieldType == FieldType.Message
            ? MapRepeatedMessage(request)
            : [CreateRepeatedScalarColumn(request)]);

    private static ClickHouseColumn CreateRepeatedScalarColumn(FieldMappingRequest request) =>
        ClickHouseColumn.Create(
            request.ColumnPath,
            $"Array({ClickHouseTypeResolver.ResolveScalar(request)})",
            MappingStrategy.Repeat,
            "proto repeated");

    private IEnumerable<ClickHouseColumn> MapRepeatedMessage(FieldMappingRequest request)
    {
        var innerColumns = planner.MapNestedFields(request.Field.MessageType, request.Context).ToArray();

        var strategy = request.Context.Defaults.RepeatedMessageStrategy.ToLowerInvariant();
        var nestedType = strategy switch
        {
            "arraytuple" => $"Array({DenormalizationPlanner.BuildTupleType(innerColumns)})",
            "flatten" => throw new NotSupportedException(
                $"Repeated message '{request.ColumnPath}' cannot use flatten strategy."),
            _ => DenormalizationPlanner.BuildNestedType(innerColumns)
        };

        return
        [
            ClickHouseColumn.Create(
                request.ColumnPath,
                nestedType,
                MappingStrategy.Nested,
                "proto repeated message")
        ];
    }
}
