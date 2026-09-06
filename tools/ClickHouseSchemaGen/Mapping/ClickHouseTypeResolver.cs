namespace ClickHouseSchemaGen.Mapping;

public static class ClickHouseTypeResolver
{
    public static string ResolveScalar(FieldMappingRequest request)
    {
        var fieldOverride = request.Context.GetOverride(request.ColumnPath);
        var baseType = ResolveBaseType(request.Field, request.ColumnPath, fieldOverride, request.Context.Defaults);

        return request.OverrideType ?? (ShouldBeNullable(request, fieldOverride)
            ? $"Nullable({baseType})"
            : baseType);
    }

    public static string ResolveEnum(
        EnumDescriptor enumDescriptor,
        FieldOverrideConfig? fieldOverride,
        CodegenDefaults defaults)
    {
        var enumType = fieldOverride?.Enum8 == true || enumDescriptor.Values.Count <= defaults.EnumMaxValuesForEnum8
            ? "Enum8"
            : "Enum16";

        return BuildEnum(enumType, enumDescriptor);
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

    private static bool ShouldBeNullable(FieldMappingRequest request, FieldOverrideConfig? fieldOverride) =>
        request.ForceNullable
        || fieldOverride?.Nullable == true
        || (request.Context.Defaults.OptionalAsNullable
            && (FieldPresence.HasSyntheticOneof(request.Field)
                || FieldPresence.HasExplicitPresence(request.Field)));

    private static string BuildEnum(string enumType, EnumDescriptor enumDescriptor) =>
        $"{enumType}({ClickHouseEnumFormatter.JoinValues(
            enumDescriptor.Values.Select(value => (value.Name, value.Number)))})";
}
