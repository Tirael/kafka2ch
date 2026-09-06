namespace ClickHouseSchemaGen.Mapping;

public sealed class ScalarFieldStrategy : IFieldMappingStrategy
{
    public bool CanMap(FieldDescriptor field, MappingContext context) =>
        !field.IsRepeated && !field.IsMap && field.FieldType != FieldType.Message;

    public IEnumerable<ClickHouseColumn> Map(FieldDescriptor field, string columnPath, MappingContext context)
    {
        var strategy = field.FieldType == FieldType.Enum
            ? MappingStrategy.Direct
            : field.ContainingOneof?.IsSynthetic == true || (field.HasPresence && context.Defaults.OptionalAsNullable)
                ? MappingStrategy.Optional
                : MappingStrategy.Direct;

        var comment = field.FieldType switch
        {
            FieldType.Enum => "proto enum",
            _ when strategy == MappingStrategy.Optional => "proto optional",
            _ => null
        };

        yield return new ClickHouseColumn(
            columnPath,
            ClickHouseTypeResolver.ResolveScalar(field, columnPath, context),
            comment,
            columnPath,
            strategy);
    }
}
