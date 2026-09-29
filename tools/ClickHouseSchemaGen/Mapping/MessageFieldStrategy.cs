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
                WellKnownTypeRegistry.IsWrapper(request.Field.MessageType));
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
                innerColumns.Any(column => column.FlattensGoogleWrapper));
        }

        var flattened = FlattenNestedColumns(request).ToArray();
        // ClickHouse treats array columns that share a dotted prefix as one Nested structure
        // and requires equal array sizes. Independent repeated fields must stay inside one Tuple.
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
            innerColumns.Any(column => column.FlattensGoogleWrapper));
    }

    private static bool IsRepeatedColumn(string type) =>
        type.StartsWith("Array(", StringComparison.Ordinal)
        || type.StartsWith("Nested(", StringComparison.Ordinal);

    private static ClickHouseColumn[] CreateSingleColumn(
        string columnPath,
        string type,
        MappingStrategy strategy,
        string comment,
        bool flattensGoogleWrapper = false) =>
        [ClickHouseColumn.Create(columnPath, type, strategy, comment, flattensGoogleWrapper)];

    private IEnumerable<ClickHouseColumn> FlattenNestedColumns(FieldMappingRequest request)
    {
        List<ClickHouseColumn> columns = [];

        foreach (var nestedColumn in planner.MapNestedFields(
            request.Field.MessageType,
            request.Context,
            request.ColumnPath))
        {
            var nestedPath = $"{request.ColumnPath}.{nestedColumn.Name}";
            columns.Add(nestedColumn with
            {
                Name = nestedPath,
                SourceFieldPath = nestedPath,
                Type = DenormalizationPlanner.PromoteEmbeddedNested(nestedColumn.Type),
                Strategy = MappingStrategy.Flatten,
                Comment = nestedColumn.Comment ?? "nested message"
            });
        }

        return columns;
    }
}
