namespace ClickHouseSchemaGen.Generation;

public static class KafkaQueueMetadataColumns
{
    public static IReadOnlyList<ClickHouseColumn> Create(KafkaTableConfig config)
    {
        List<ClickHouseColumn> columns =
        [
            ClickHouseColumn.Create(
                "_headers.name",
                "Array(String)",
                MappingStrategy.Direct,
                comment: "kafka message headers"),
            ClickHouseColumn.Create(
                "_headers.value",
                "Array(String)",
                MappingStrategy.Direct,
                comment: "kafka message headers"),
            ClickHouseColumn.Create(
                "headers_name",
                "Array(LowCardinality(String))",
                MappingStrategy.Direct,
                comment: "kafka message headers",
                aliasExpression: "CAST(`_headers.name`, 'Array(LowCardinality(String))')"),
            ClickHouseColumn.Create(
                "headers_value",
                "Array(String)",
                MappingStrategy.Direct,
                comment: "kafka message headers",
                aliasExpression: "`_headers.value`")
        ];

        if (config.Key is not { } key)
            return columns;

        columns.Insert(
            0,
            ClickHouseColumn.Create(
                "_key",
                "String",
                MappingStrategy.Direct,
                comment: "kafka message key"));

        var fields = ProtobufKeyFieldMapper.MapFields(
            ResolveKeyDescriptor(key.MessageType));

        if (ProtobufKeyFieldMapper.IsSingleStringKey(fields))
        {
            columns.Add(
                ClickHouseColumn.Create(
                    $"key_{fields[0].Name}",
                    fields[0].ClickHouseType,
                    MappingStrategy.Direct,
                    comment: "decoded kafka key field",
                    aliasExpression: ProtobufKeyDecoder.SimpleStringFieldExpression(
                        key.SkipBytes,
                        fields[0].FieldNumber)));
            return columns;
        }

        columns.Add(
            ClickHouseColumn.Create(
                ProtobufKeyDecoder.KeyPayloadAlias,
                "String",
                MappingStrategy.Direct,
                comment: "kafka key payload after Confluent envelope",
                aliasExpression: ProtobufKeyDecoder.PayloadExpression("_key", key.SkipBytes)));
        columns.Add(
            ClickHouseColumn.Create(
                ProtobufKeyDecoder.KeyFieldsAlias,
                "Map(UInt32, String)",
                MappingStrategy.Direct,
                comment: "decoded kafka key wire fields",
                aliasExpression: ProtobufKeyDecoder.ProtoFieldsExpression(ProtobufKeyDecoder.KeyPayloadAlias)));

        foreach (var field in fields)
        {
            columns.Add(
                ClickHouseColumn.Create(
                    $"key_{field.Name}",
                    field.ClickHouseType,
                    MappingStrategy.Direct,
                    comment: "decoded kafka key field",
                    aliasExpression: field.TupleValueExpression));
        }

        return columns;
    }

    private static MessageDescriptor ResolveKeyDescriptor(string messageType)
    {
        var type = Type.GetType(messageType, throwOnError: true)
            ?? throw new InvalidOperationException($"Message type '{messageType}' was not found.");

        if (!typeof(IMessage).IsAssignableFrom(type))
            throw new InvalidOperationException($"Type '{messageType}' is not a protobuf message.");

        var descriptorProperty = type.GetProperty("Descriptor", BindingFlags.Public | BindingFlags.Static)
            ?? throw new InvalidOperationException($"Type '{messageType}' has no Descriptor property.");

        return (MessageDescriptor)descriptorProperty.GetValue(null)!;
    }
}
