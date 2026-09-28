namespace ClickHouseSchemaGen.Generation;

public static class ProtobufKeyFieldMapper
{
    public static IReadOnlyList<ProtobufKeyField> MapFields(MessageDescriptor descriptor)
    {
        List<ProtobufKeyField> fields = [];

        foreach (var field in descriptor.Fields.InFieldNumberOrder())
        {
            if (field.IsRepeated || field.IsMap)
            {
                throw new InvalidOperationException(
                    $"Kafka key field '{field.Name}' cannot be repeated or a map.");
            }

            if (field.FieldType == FieldType.Message)
            {
                throw new InvalidOperationException(
                    $"Kafka key field '{field.Name}' cannot be a nested message.");
            }

            if (!ClickHouseSchemaGen.Validation.ValidationRules.IsSqlIdentifier(field.Name))
                throw new InvalidOperationException($"Kafka key field '{field.Name}' is not a valid column name.");

            fields.Add(MapField(field));
        }

        if (fields.Count == 0)
            throw new InvalidOperationException($"Kafka key message '{descriptor.Name}' has no fields.");

        return fields;
    }

    public static bool IsSingleStringKey(IReadOnlyList<ProtobufKeyField> fields) =>
        fields is [{ ClickHouseType: "String" }];

    private static ProtobufKeyField MapField(FieldDescriptor field)
    {
        var number = field.FieldNumber;
        var bytes = $"f[{number}]";
        var varint = $"sandbox_vi({bytes}, 1).1";

        return field.FieldType switch
        {
            FieldType.String or FieldType.Bytes => new(
                field.Name,
                number,
                "String",
                $"if(mapContains(f, {number}), {bytes}, '')"),

            FieldType.Bool => new(
                field.Name,
                number,
                "Bool",
                $"if(mapContains(f, {number}), {varint} != 0, false)"),

            FieldType.Int32 or FieldType.Enum => new(
                field.Name,
                number,
                "Int32",
                $"if(mapContains(f, {number}), toInt32({varint}), toInt32(0))"),

            FieldType.UInt32 => new(
                field.Name,
                number,
                "UInt32",
                $"if(mapContains(f, {number}), toUInt32({varint}), toUInt32(0))"),

            FieldType.Int64 => new(
                field.Name,
                number,
                "Int64",
                $"if(mapContains(f, {number}), reinterpretAsInt64(reinterpretAsUInt64({varint})), toInt64(0))"),

            FieldType.UInt64 => new(
                field.Name,
                number,
                "UInt64",
                $"if(mapContains(f, {number}), {varint}, toUInt64(0))"),

            FieldType.SInt32 => new(
                field.Name,
                number,
                "Int32",
                $"if(mapContains(f, {number}), CAST(bitXor(bitShiftRight({varint}, 1), 0 - bitAnd({varint}, 1)), 'Int32'), toInt32(0))"),

            FieldType.SInt64 => new(
                field.Name,
                number,
                "Int64",
                $"if(mapContains(f, {number}), reinterpretAsInt64(reinterpretAsUInt64(bitXor(bitShiftRight({varint}, 1), 0 - bitAnd({varint}, 1)))), toInt64(0))"),

            FieldType.Fixed32 => new(
                field.Name,
                number,
                "UInt32",
                $"if(mapContains(f, {number}) AND length({bytes}) = 4, reinterpretAsUInt32({bytes}), toUInt32(0))"),

            FieldType.SFixed32 => new(
                field.Name,
                number,
                "Int32",
                $"if(mapContains(f, {number}) AND length({bytes}) = 4, reinterpretAsInt32({bytes}), toInt32(0))"),

            FieldType.Fixed64 => new(
                field.Name,
                number,
                "UInt64",
                $"if(mapContains(f, {number}) AND length({bytes}) = 8, reinterpretAsUInt64({bytes}), toUInt64(0))"),

            FieldType.SFixed64 => new(
                field.Name,
                number,
                "Int64",
                $"if(mapContains(f, {number}) AND length({bytes}) = 8, reinterpretAsInt64({bytes}), toInt64(0))"),

            FieldType.Float => new(
                field.Name,
                number,
                "Float32",
                $"if(mapContains(f, {number}) AND length({bytes}) = 4, reinterpretAsFloat32({bytes}), toFloat32(0))"),

            FieldType.Double => new(
                field.Name,
                number,
                "Float64",
                $"if(mapContains(f, {number}) AND length({bytes}) = 8, reinterpretAsFloat64({bytes}), toFloat64(0))"),

            _ => throw new InvalidOperationException(
                $"Kafka key field '{field.Name}' has unsupported type '{field.FieldType}'.")
        };
    }
}
