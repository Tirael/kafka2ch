namespace ClickHouseSchemaGen.Mapping;

public sealed class MessageFieldStrategy(DenormalizationPlanner planner) : IFieldMappingStrategy
{
    public bool CanMap(FieldMappingRequest request) =>
        request.Field.FieldType == FieldType.Message && !request.Field.IsRepeatedOrMap();

    public IEnumerable<ClickHouseColumn> Map(FieldMappingRequest request) =>
        FieldMappingHelpers.TryCreateFromTypeOverride(request, MappingStrategy.WellKnownType, "message override")
        ?? MapMessageField(request);

    private IEnumerable<ClickHouseColumn> MapMessageField(FieldMappingRequest request)
    {
        var wellKnownType = WellKnownTypeRegistry.MapMessageType(request.Field.MessageType);
        if (wellKnownType is not null)
            return CreateSingleColumn(request.ColumnPath, wellKnownType, MappingStrategy.WellKnownType, "well-known type");

        var fieldOverride = request.Context.GetOverride(request.ColumnPath);
        var maxDepth = fieldOverride?.MaxDepth ?? request.Context.Defaults.MaxFlattenDepth;
        if (request.Context.Depth >= maxDepth)
        {
            var innerColumns = planner.MapNestedFields(request.Field.MessageType, request.Context).ToArray();
            return CreateSingleColumn(
                request.ColumnPath,
                DenormalizationPlanner.BuildTupleType(innerColumns),
                MappingStrategy.Tuple,
                "max flatten depth");
        }

        return FlattenNestedColumns(request);
    }

    private static ClickHouseColumn[] CreateSingleColumn(
        string columnPath,
        string type,
        MappingStrategy strategy,
        string comment) =>
        [ClickHouseColumn.Create(columnPath, type, strategy, comment)];

    private IEnumerable<ClickHouseColumn> FlattenNestedColumns(FieldMappingRequest request)
    {
        List<ClickHouseColumn> columns = [];

        foreach (var nestedColumn in planner.MapNestedFields(request.Field.MessageType, request.Context))
        {
            var nestedPath = $"{request.ColumnPath}.{nestedColumn.Name}";
            columns.Add(nestedColumn with
            {
                Name = nestedPath,
                SourceFieldPath = nestedPath,
                Strategy = MappingStrategy.Flatten,
                Comment = nestedColumn.Comment ?? "nested message"
            });
        }

        return columns;
    }
}
