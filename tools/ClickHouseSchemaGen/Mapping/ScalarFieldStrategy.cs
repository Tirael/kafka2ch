namespace ClickHouseSchemaGen.Mapping;

public sealed class ScalarFieldStrategy : IFieldMappingStrategy
{
    public bool CanMap(FieldDescriptor field, MappingContext context) =>
        !field.IsRepeated && !field.IsMap && field.FieldType != FieldType.Message;

    public IEnumerable<ClickHouseColumn> Map(FieldDescriptor field, string columnPath, MappingContext context)
    {
        var strategy = ResolveStrategy(field, context);

        yield return new ClickHouseColumn(
            columnPath,
            ClickHouseTypeResolver.ResolveScalar(field, columnPath, context),
            ResolveComment(field.FieldType, strategy),
            columnPath,
            strategy);
    }

    private static MappingStrategy ResolveStrategy(FieldDescriptor field, MappingContext context)
    {
        if (field.FieldType == FieldType.Enum)
            return MappingStrategy.Direct;

        if (IsOptionalScalar(field, context))
            return MappingStrategy.Optional;

        return MappingStrategy.Direct;
    }

    private static bool IsOptionalScalar(FieldDescriptor field, MappingContext context)
    {
        if (field.ContainingOneof?.IsSynthetic == true)
            return true;

        return field.HasPresence && context.Defaults.OptionalAsNullable;
    }

    private static string? ResolveComment(FieldType fieldType, MappingStrategy strategy)
    {
        if (fieldType == FieldType.Enum)
            return "proto enum";

        if (strategy == MappingStrategy.Optional)
            return "proto optional";

        return null;
    }
}
