using ClickHouseSchemaGen.Models;
using Google.Protobuf;
using Google.Protobuf.Reflection;

namespace ClickHouseSchemaGen;

public sealed class ProtoToClickHouseMapper
{
    public IReadOnlyList<ClickHouseColumn> MapMessage(
        MessageDescriptor descriptor,
        IReadOnlyDictionary<string, FieldOverrideConfig> overrides) =>
        MapMessage(descriptor, prefix: null, overrides);

    private IReadOnlyList<ClickHouseColumn> MapMessage(
        MessageDescriptor descriptor,
        string? prefix,
        IReadOnlyDictionary<string, FieldOverrideConfig> overrides)
    {
        var columns = new List<ClickHouseColumn>();

        foreach (var field in descriptor.Fields.InDeclarationOrder())
        {
            var columnPath = prefix is null ? field.Name : $"{prefix}.{field.Name}";

            if (field.IsMap)
                throw new NotSupportedException($"Map fields are not supported yet: {columnPath}");

            if (field.IsRepeated)
            {
                if (field.FieldType == FieldType.Message)
                    throw new NotSupportedException($"Repeated message fields are not supported yet: {columnPath}");

                if (overrides.TryGetValue(columnPath, out var repeatedOverride)
                    && !string.IsNullOrWhiteSpace(repeatedOverride.Type))
                {
                    columns.Add(new ClickHouseColumn(columnPath, repeatedOverride.Type, Comment: "proto repeated"));
                    continue;
                }

                columns.Add(new ClickHouseColumn(
                    columnPath,
                    $"Array({ResolveType(field, columnPath, overrides)})",
                    Comment: "proto repeated"));
                continue;
            }

            if (field.FieldType == FieldType.Message)
            {
                columns.AddRange(MapMessage(field.MessageType, columnPath, overrides));
                continue;
            }

            columns.Add(new ClickHouseColumn(
                columnPath,
                ResolveType(field, columnPath, overrides),
                Comment: field.FieldType == FieldType.Enum ? "proto enum" : null));
        }

        return columns;
    }

    private static string ResolveType(
        FieldDescriptor field,
        string columnPath,
        IReadOnlyDictionary<string, FieldOverrideConfig> overrides)
    {
        if (overrides.TryGetValue(columnPath, out var fieldOverride))
        {
            if (!string.IsNullOrWhiteSpace(fieldOverride.Type))
                return fieldOverride.Type;

            if (fieldOverride.Enum8 && field.FieldType == FieldType.Enum)
                return BuildEnum8(field.EnumType);
        }

        return field.FieldType switch
        {
            FieldType.String or FieldType.Bytes => "String",
            FieldType.Int32 or FieldType.SInt32 or FieldType.SFixed32 => "Int32",
            FieldType.Int64 or FieldType.SInt64 or FieldType.SFixed64 => "Int64",
            FieldType.UInt32 or FieldType.Fixed32 => "UInt32",
            FieldType.UInt64 or FieldType.Fixed64 => "UInt64",
            FieldType.Float => "Float32",
            FieldType.Double => "Float64",
            FieldType.Bool => "UInt8",
            FieldType.Enum => BuildEnum8(field.EnumType),
            _ => throw new NotSupportedException($"Unsupported protobuf field type {field.FieldType} for {columnPath}")
        };
    }

    private static string BuildEnum8(EnumDescriptor enumDescriptor)
    {
        var values = enumDescriptor.Values
            .Select(value => $"'{value.Name}' = {value.Number}")
            .ToArray();

        return $"Enum8({string.Join(", ", values)})";
    }

    public static MessageDescriptor ResolveDescriptor(string messageType)
    {
        var type = Type.GetType(messageType, throwOnError: true)
            ?? throw new InvalidOperationException($"Message type '{messageType}' was not found.");

        if (!typeof(IMessage).IsAssignableFrom(type))
            throw new InvalidOperationException($"Type '{messageType}' is not a protobuf message.");

        var descriptorProperty = type.GetProperty("Descriptor")
            ?? throw new InvalidOperationException($"Type '{messageType}' has no Descriptor property.");

        return (MessageDescriptor)descriptorProperty.GetValue(null)!;
    }
}
