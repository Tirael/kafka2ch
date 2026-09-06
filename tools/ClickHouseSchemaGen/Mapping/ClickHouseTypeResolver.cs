namespace ClickHouseSchemaGen.Mapping;

public static class ClickHouseTypeResolver
{
    public static string ResolveScalar(
        FieldDescriptor field,
        string columnPath,
        MappingContext context,
        bool forceNullable = false)
    {
        var fieldOverride = context.GetOverride(columnPath);
        if (!string.IsNullOrWhiteSpace(fieldOverride?.Type))
            return fieldOverride.Type;

        var baseType = field.FieldType switch
        {
            FieldType.String or FieldType.Bytes => "String",
            FieldType.Int32 or FieldType.SInt32 or FieldType.SFixed32 => "Int32",
            FieldType.Int64 or FieldType.SInt64 or FieldType.SFixed64 => "Int64",
            FieldType.UInt32 or FieldType.Fixed32 => "UInt32",
            FieldType.UInt64 or FieldType.Fixed64 => "UInt64",
            FieldType.Float => "Float32",
            FieldType.Double => "Float64",
            FieldType.Bool => "UInt8",
            FieldType.Enum => ResolveEnum(field.EnumType, fieldOverride, context.Defaults),
            _ => throw new NotSupportedException(
                $"Unsupported protobuf field type {field.FieldType} for {columnPath}")
        };

        var shouldNullable = forceNullable
            || fieldOverride?.Nullable == true
            || (context.Defaults.OptionalAsNullable && field.HasPresence && field.ContainingOneof is null)
            || (context.Defaults.OptionalAsNullable && field.ContainingOneof?.IsSynthetic == true);

        return shouldNullable ? $"Nullable({baseType})" : baseType;
    }

    public static string ResolveEnum(
        EnumDescriptor enumDescriptor,
        FieldOverrideConfig? fieldOverride,
        CodegenDefaults defaults)
    {
        if (fieldOverride?.Enum8 == true || enumDescriptor.Values.Count <= defaults.EnumMaxValuesForEnum8)
            return BuildEnum("Enum8", enumDescriptor);

        return BuildEnum("Enum16", enumDescriptor);
    }

    private static string BuildEnum(string enumType, EnumDescriptor enumDescriptor)
    {
        var values = enumDescriptor.Values
            .Select(value => $"'{value.Name}' = {value.Number}")
            .ToArray();

        return $"{enumType}({string.Join(", ", values)})";
    }
}
