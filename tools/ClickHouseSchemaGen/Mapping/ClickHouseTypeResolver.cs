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

        var baseType = ResolveBaseType(field, columnPath, fieldOverride, context.Defaults);
        return ShouldBeNullable(field, context, fieldOverride, forceNullable)
            ? $"Nullable({baseType})"
            : baseType;
    }

    public static string ResolveEnum(
        EnumDescriptor enumDescriptor,
        FieldOverrideConfig? fieldOverride,
        CodegenDefaults defaults)
    {
        if (fieldOverride?.Enum8 == true)
            return BuildEnum("Enum8", enumDescriptor);

        if (enumDescriptor.Values.Count <= defaults.EnumMaxValuesForEnum8)
            return BuildEnum("Enum8", enumDescriptor);

        return BuildEnum("Enum16", enumDescriptor);
    }

    private static string ResolveBaseType(
        FieldDescriptor field,
        string columnPath,
        FieldOverrideConfig? fieldOverride,
        CodegenDefaults defaults) =>
        field.FieldType switch
        {
            FieldType.String or FieldType.Bytes => "String",
            FieldType.Int32 or FieldType.SInt32 or FieldType.SFixed32 => "Int32",
            FieldType.Int64 or FieldType.SInt64 or FieldType.SFixed64 => "Int64",
            FieldType.UInt32 or FieldType.Fixed32 => "UInt32",
            FieldType.UInt64 or FieldType.Fixed64 => "UInt64",
            FieldType.Float => "Float32",
            FieldType.Double => "Float64",
            FieldType.Bool => "UInt8",
            FieldType.Enum => ResolveEnum(field.EnumType, fieldOverride, defaults),
            _ => throw new NotSupportedException(
                $"Unsupported protobuf field type {field.FieldType} for {columnPath}")
        };

    private static bool ShouldBeNullable(
        FieldDescriptor field,
        MappingContext context,
        FieldOverrideConfig? fieldOverride,
        bool forceNullable)
    {
        if (forceNullable)
            return true;

        if (fieldOverride?.Nullable == true)
            return true;

        if (!context.Defaults.OptionalAsNullable)
            return false;

        if (field.ContainingOneof?.IsSynthetic == true)
            return true;

        return field.HasPresence && field.ContainingOneof is null;
    }

    private static string BuildEnum(string enumType, EnumDescriptor enumDescriptor)
    {
        var values = enumDescriptor.Values
            .Select(value => $"'{value.Name}' = {value.Number}")
            .ToArray();

        return $"{enumType}({string.Join(", ", values)})";
    }
}
