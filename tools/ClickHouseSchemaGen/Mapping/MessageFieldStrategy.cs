namespace ClickHouseSchemaGen.Mapping;

public sealed class MessageFieldStrategy(DenormalizationPlanner planner) : IFieldMappingStrategy
{
    public bool CanMap(FieldDescriptor field, MappingContext context) =>
        field.FieldType == FieldType.Message && !field.IsRepeated && !field.IsMap;

    public IEnumerable<ClickHouseColumn> Map(FieldDescriptor field, string columnPath, MappingContext context)
    {
        var fieldOverride = context.GetOverride(columnPath);
        if (!string.IsNullOrWhiteSpace(fieldOverride?.Type))
        {
            yield return new ClickHouseColumn(
                columnPath,
                fieldOverride.Type,
                "message override",
                columnPath,
                MappingStrategy.WellKnownType);
            yield break;
        }

        var wellKnownType = WellKnownTypeRegistry.MapMessageType(field.MessageType);
        if (wellKnownType is not null)
        {
            yield return new ClickHouseColumn(
                columnPath,
                wellKnownType,
                "well-known type",
                columnPath,
                MappingStrategy.WellKnownType);
            yield break;
        }

        var maxDepth = fieldOverride?.MaxDepth ?? context.Defaults.MaxFlattenDepth;
        if (context.Depth >= maxDepth)
        {
            var innerColumns = planner.MapNestedFields(field.MessageType, context).ToArray();

            yield return new ClickHouseColumn(
                columnPath,
                DenormalizationPlanner.BuildTupleType(innerColumns),
                "max flatten depth",
                columnPath,
                MappingStrategy.Tuple);
            yield break;
        }

        foreach (var nestedColumn in planner.MapNestedFields(field.MessageType, context))
        {
            var nestedPath = $"{columnPath}.{nestedColumn.Name}";
            yield return nestedColumn with
            {
                Name = nestedPath,
                SourceFieldPath = nestedPath,
                Strategy = MappingStrategy.Flatten,
                Comment = nestedColumn.Comment ?? "nested message"
            };
        }
    }
}
