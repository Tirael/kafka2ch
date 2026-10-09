namespace ClickHouseSchemaGen.Mapping;


public sealed class KafkaKeyColumnMapper(DenormalizationPlanner planner)
{
    public const string KeyColumn = "_key";

    public const string ColumnPrefix = KeyColumn + ".";

    private static readonly HashSet<MappingStrategy> DecodableStrategies =
    [
        MappingStrategy.Direct,
        MappingStrategy.Optional,
        MappingStrategy.Flatten,
        MappingStrategy.WellKnownType
    ];

    public static bool IsKeyColumn(string columnName) =>
        columnName.StartsWith(ColumnPrefix, StringComparison.Ordinal);

    public IReadOnlyList<ClickHouseColumn> MapKeyColumns(KafkaTableConfig table, CodegenDefaults defaults)
    {
        if (!table.Key.IsProtobuf)
            return [];

        var descriptor = ProtoDescriptorResolver.ResolveDescriptor(table.Key.MessageType!);
        var payload = $"substring({KeyColumn}, {(table.Key.SkipBytes ?? table.Kafka.SkipBytes) + 1})";

        return planner.MapMessage(descriptor, defaults, table.Key.FieldOverrides)
            .Select(column => column with
            {
                Name = ColumnPrefix + column.Name,
                SourceFieldPath = ColumnPrefix + column.SourceFieldPath,
                FieldNumberPath = $"{KafkaMetaColumnFactory.KeyFieldPath}:{column.FieldNumberPath}",
                Comment = $"key {descriptor.FullName}",
                SourceExpression = $"CAST({BuildDecodeExpression(table.TableName, descriptor, column, payload)} AS {column.Type})"
            })
            .ToList();
    }

    private static string BuildDecodeExpression(
        string tableName,
        MessageDescriptor keyDescriptor,
        ClickHouseColumn column,
        string payload)
    {
        if (!DecodableStrategies.Contains(column.Strategy))
            throw Unsupported(tableName, column, $"mapping strategy {column.Strategy}");

        var segments = column.Name.Split('.');
        var message = payload;
        var descriptor = keyDescriptor;

        for (var i = 0; i < segments.Length - 1; i++)
        {
            var parent = FindField(tableName, descriptor, segments[i], column);
            if (parent.FieldType != FieldType.Message || parent.IsRepeatedOrMap())
                throw Unsupported(tableName, column, $"field '{parent.Name}' is not a singular message");

            message = $"{ProtobufWireSqlFunctions.Bytes}({message}, {parent.FieldNumber})";
            descriptor = parent.MessageType;
        }

        var field = FindField(tableName, descriptor, segments[^1], column);
        if (field.IsRepeatedOrMap())
            throw Unsupported(tableName, column, "repeated and map fields");
        if (field.ContainingOneof is { IsSynthetic: false })
            throw Unsupported(tableName, column, "oneof fields");

        if (field.FieldType == FieldType.Message)
        {
            if (!WellKnownTypeRegistry.IsWrapper(field.MessageType))
                throw Unsupported(tableName, column, $"message type {field.MessageType.FullName}");

            var wrapped = $"{ProtobufWireSqlFunctions.Bytes}({message}, {field.FieldNumber})";
            var wrappedValue = DecodeScalar(field.MessageType.FindFieldByNumber(1), wrapped);
            return $"if({ProtobufWireSqlFunctions.Has}({message}, {field.FieldNumber}), {wrappedValue}, NULL)";
        }

        var value = DecodeScalar(field, message);
        return column.Type.StartsWith("Nullable(", StringComparison.Ordinal)
            ? $"if({ProtobufWireSqlFunctions.Has}({message}, {field.FieldNumber}), {value}, NULL)"
            : value;
    }

    private static string DecodeScalar(FieldDescriptor field, string message)
    {
        var bits = $"{ProtobufWireSqlFunctions.Bits}({message}, {field.FieldNumber})";

        return field.FieldType switch
        {
            FieldType.String or FieldType.Bytes => $"{ProtobufWireSqlFunctions.Bytes}({message}, {field.FieldNumber})",
            FieldType.UInt64 or FieldType.Fixed64 => bits,
            FieldType.UInt32 or FieldType.Fixed32 => $"toUInt32({bits})",
            FieldType.Int64 or FieldType.SFixed64 => $"reinterpretAsInt64({bits})",
            FieldType.Int32 or FieldType.SFixed32 or FieldType.Enum => $"reinterpretAsInt32(toUInt32({bits}))",
            FieldType.SInt64 => $"{ProtobufWireSqlFunctions.ZigZag}({bits})",
            FieldType.SInt32 => $"toInt32({ProtobufWireSqlFunctions.ZigZag}({bits}))",
            FieldType.Bool => $"{bits} != 0",
            FieldType.Float => $"reinterpretAsFloat32(toUInt32({bits}))",
            FieldType.Double => $"reinterpretAsFloat64({bits})",
            _ => throw new NotSupportedException($"Unsupported protobuf key field type {field.FieldType} for '{field.FullName}'.")
        };
    }

    private static FieldDescriptor FindField(
        string tableName,
        MessageDescriptor descriptor,
        string name,
        ClickHouseColumn column) =>
        descriptor.FindFieldByName(name)
        ?? throw Unsupported(tableName, column, $"no field '{name}' in {descriptor.FullName} (overridden column name?)");

    private static NotSupportedException Unsupported(string tableName, ClickHouseColumn column, string reason) =>
        new($"Kafka table '{tableName}': key column '{ColumnPrefix}{column.Name}' cannot be decoded from _key ({reason}). " +
            "Protobuf keys support singular scalar, enum, nested message and google.protobuf wrapper fields.");
}
