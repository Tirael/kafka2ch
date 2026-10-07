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
            "proto repeated",
            fieldNumberPath: request.Field.FieldNumber.ToString());

    private IEnumerable<ClickHouseColumn> MapRepeatedMessage(FieldMappingRequest request)
    {
        var innerColumns = planner.MapNestedFields(
            request.Field.MessageType,
            request.Context,
            request.ColumnPath).ToArray();

        var strategy = request.Context.Defaults.RepeatedMessageStrategy.ToLowerInvariant();
        if (strategy == "flatten")
        {
            throw new NotSupportedException(
                $"Repeated message '{request.ColumnPath}' cannot use flatten strategy.");
        }

        var repeatedType = strategy == "arraytuple"
            ? $"Array({DenormalizationPlanner.BuildTupleType(innerColumns)})"
            : DenormalizationPlanner.BuildNestedType(innerColumns);

        return
        [
            ClickHouseColumn.Create(
                request.ColumnPath,
                repeatedType,
                MappingStrategy.Nested,
                "proto repeated message",
                innerColumns.Any(column => column.FlattensGoogleWrapper),
                request.Field.FieldNumber.ToString())
        ];
    }
}
