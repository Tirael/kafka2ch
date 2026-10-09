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
        {
            return CreateSingleColumn(
                request.ColumnPath,
                wellKnownType,
                MappingStrategy.WellKnownType,
                "well-known type",
                WellKnownTypeRegistry.IsWrapper(request.Field.MessageType),
                request.Field.FieldNumber.ToString());
        }

        var fieldOverride = request.Context.GetOverride(request.ColumnPath);
        var maxDepth = fieldOverride?.MaxDepth ?? request.Context.Defaults.MaxFlattenDepth;
        if (request.Context.Depth >= maxDepth)
        {
            var innerColumns = planner.MapNestedFields(
                request.Field.MessageType,
                request.Context,
                request.ColumnPath).ToArray();
            return CreateSingleColumn(
                request.ColumnPath,
                DenormalizationPlanner.BuildTupleType(innerColumns),
                MappingStrategy.Tuple,
                "max flatten depth",
                innerColumns.Any(column => column.FlattensGoogleWrapper),
                request.Field.FieldNumber.ToString());
        }

        var flattened = FlattenNestedColumns(request).ToArray();


        if (flattened.Count(column => IsRepeatedColumn(column.Type)) > 1)
        {
            return CreateStructTuple(request, maxDepth);
        }

        return flattened;
    }

    private IEnumerable<ClickHouseColumn> CreateStructTuple(FieldMappingRequest request, int maxDepth)
    {
        var innerColumns = planner.MapNestedFields(
            request.Field.MessageType,
            request.Context with { Depth = maxDepth },
            request.ColumnPath).ToArray();

        return CreateSingleColumn(
            request.ColumnPath,
            DenormalizationPlanner.BuildTupleType(innerColumns),
            MappingStrategy.Tuple,
            "nested message",
            innerColumns.Any(column => column.FlattensGoogleWrapper),
            request.Field.FieldNumber.ToString());
    }

    private static bool IsRepeatedColumn(string type) =>
        type.StartsWith("Array(", StringComparison.Ordinal)
        || type.StartsWith("Nested(", StringComparison.Ordinal);

    private static ClickHouseColumn[] CreateSingleColumn(
        string columnPath,
        string type,
        MappingStrategy strategy,
        string comment,
        bool flattensGoogleWrapper = false,
        string fieldNumberPath = "") =>
        [ClickHouseColumn.Create(columnPath, type, strategy, comment, flattensGoogleWrapper, fieldNumberPath)];

    private IEnumerable<ClickHouseColumn> FlattenNestedColumns(FieldMappingRequest request)
    {
        List<ClickHouseColumn> columns = [];

        foreach (var nestedColumn in planner.MapNestedFields(
            request.Field.MessageType,
            request.Context,
            request.ColumnPath))
        {
            var nestedPath = $"{request.ColumnPath}.{nestedColumn.Name}";
            var nestedNumberPath = string.IsNullOrEmpty(nestedColumn.FieldNumberPath)
                ? request.Field.FieldNumber.ToString()
                : $"{request.Field.FieldNumber}.{nestedColumn.FieldNumberPath}";
            columns.Add(nestedColumn with
            {
                Name = nestedPath,
                SourceFieldPath = nestedPath,
                FieldNumberPath = nestedNumberPath,
                Type = DenormalizationPlanner.PromoteEmbeddedNested(nestedColumn.Type),
                Strategy = MappingStrategy.Flatten,
                Comment = nestedColumn.Comment ?? "nested message"
            });
        }

        return columns;
    }
}
