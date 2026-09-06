namespace ClickHouseSchemaGen.Mapping;

public sealed class RepeatedFieldStrategy(DenormalizationPlanner planner) : IFieldMappingStrategy
{
    public bool CanMap(FieldDescriptor field, MappingContext context) => field.IsRepeated;

    public IEnumerable<ClickHouseColumn> Map(FieldDescriptor field, string columnPath, MappingContext context)
    {
        var fieldOverride = context.GetOverride(columnPath);
        if (!string.IsNullOrWhiteSpace(fieldOverride?.Type))
        {
            yield return new ClickHouseColumn(columnPath, fieldOverride.Type, "proto repeated", columnPath, MappingStrategy.Repeat);
            yield break;
        }

        if (field.FieldType == FieldType.Message)
        {
            foreach (var column in MapRepeatedMessage(field, columnPath, context))
                yield return column;

            yield break;
        }

        var elementType = ClickHouseTypeResolver.ResolveScalar(field, columnPath, context);
        yield return new ClickHouseColumn(
            columnPath,
            $"Array({elementType})",
            "proto repeated",
            columnPath,
            MappingStrategy.Repeat);
    }

    private IEnumerable<ClickHouseColumn> MapRepeatedMessage(
        FieldDescriptor field,
        string columnPath,
        MappingContext context)
    {
        var innerColumns = planner.MapNestedFields(field.MessageType, context).ToArray();

        var strategy = context.Defaults.RepeatedMessageStrategy.ToLowerInvariant();
        var nestedType = strategy switch
        {
            "arraytuple" => $"Array({DenormalizationPlanner.BuildTupleType(innerColumns)})",
            "flatten" => throw new NotSupportedException(
                $"Repeated message '{columnPath}' cannot use flatten strategy."),
            _ => DenormalizationPlanner.BuildNestedType(innerColumns)
        };

        yield return new ClickHouseColumn(
            columnPath,
            nestedType,
            "proto repeated message",
            columnPath,
            MappingStrategy.Nested);
    }
}
