namespace ClickHouseSchemaGen.Generation;

public static class KafkaTableGenerator
{
    public static string Generate(
        KafkaTableConfig config,
        IReadOnlyList<ClickHouseColumn> columns,
        IReadOnlyList<ClickHouseColumn>? keyColumns = null)
    {
        var builder = new StringBuilder()
            .AppendLine(SqlScriptWriter.GeneratedHeader)
            .AppendLine();

        if (keyColumns is { Count: > 0 })
            AppendKeyDecoding(builder, config, keyColumns);

        builder
            .AppendLine($"CREATE TABLE {config.TableName}")
            .AppendLine("(");

        SqlScriptWriter.AppendColumnDefinitions(builder, columns);

        builder.AppendLine(")")
            .AppendLine("ENGINE = Kafka")
            .AppendLine("SETTINGS");

        List<string> settings =
        [
            $"kafka_broker_list = '{config.Kafka.BrokerList}'",
            $"kafka_topic_list = '{config.Kafka.Topic}'",
            $"kafka_group_name = '{config.Kafka.GroupName}'",
            "kafka_format = 'ProtobufSingle'",
            $"kafka_schema = '{config.ProtoFile}:{config.MessageName}'",
            $"kafka_schema_registry_skip_bytes = {config.Kafka.SkipBytes}",
            $"kafka_num_consumers = {config.Kafka.NumConsumers}"
        ];

        if (RequiresFlattenNested(columns, config))
            settings.Add("flatten_nested = 0");

        if (config.Kafka.ProtobufOneofPresence && columns.Any(IsOneofPresenceColumn))
            settings.Add("input_format_protobuf_oneof_presence = 1");

        if (ShouldFlattenGoogleWrappers(columns, config))
            settings.Add("input_format_protobuf_flatten_google_wrappers = 1");

        SqlScriptWriter.AppendCommaSeparatedLines(builder, settings);
        builder.AppendLine();

        return builder.ToString();
    }

    private static void AppendKeyDecoding(
        StringBuilder builder,
        KafkaTableConfig config,
        IReadOnlyList<ClickHouseColumn> keyColumns)
    {
        builder
            .AppendLine("-- Protobuf wire-format helpers for decoding the Kafka key (_key) in materialized views.")
            .AppendLine(ProtobufWireSqlFunctions.Definitions.TrimEnd())
            .AppendLine()
            .AppendLine($"-- {config.TableName} has a protobuf Kafka key ({config.Key.MessageType}); ClickHouse exposes it only as raw _key String.")
            .AppendLine("-- Materialized views over this table can map the decoded key columns like value columns:");

        foreach (var column in keyColumns)
            builder.AppendLine($"--   {SqlColumnFormatter.FormatColumnName(column.Name)} {column.Type}");

        builder.AppendLine();
    }

    private static bool IsOneofPresenceColumn(ClickHouseColumn column) =>
        column.Comment == "oneof presence";

    private static bool ShouldFlattenGoogleWrappers(IReadOnlyList<ClickHouseColumn> columns, KafkaTableConfig config) =>
        config.Kafka.ProtobufFlattenGoogleWrappers
        && columns.Any(column => column.FlattensGoogleWrapper || column.Strategy == MappingStrategy.WellKnownType);

    private static bool RequiresFlattenNested(IReadOnlyList<ClickHouseColumn> columns, KafkaTableConfig config) =>
        !config.Kafka.FlattenNested
        && columns.Any(column => column.Type.StartsWith("Nested(", StringComparison.Ordinal));
}
