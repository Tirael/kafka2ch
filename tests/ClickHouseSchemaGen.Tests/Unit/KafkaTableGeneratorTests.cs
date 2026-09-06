namespace ClickHouseSchemaGen.Tests.Unit;

public sealed class KafkaTableGeneratorTests
{
    [Fact]
    public void GivenOrdersQueueConfig_WhenGenerated_ThenSqlContainsKafkaSettingsAndColumns()
    {
        // Arrange
        var config = OrdersQueueTestConfig.Create();
        var columns = new DenormalizationPlanner().MapMessage(
            ProtoToClickHouseMapper.ResolveDescriptor(config.MessageType),
            OrdersQueueTestConfig.Defaults,
            config.FieldOverrides);

        // Act
        var sql = KafkaTableGenerator.Generate(config, columns);

        // Assert
        sql.Should().Contain("CREATE TABLE orders_queue");
        sql.Should().MatchRegex(@"category\s+LowCardinality\(String\)");
        sql.Should().Contain("Nested(sku String, qty UInt32, unit_price Float64)");
        sql.Should().Contain("Map(String, String)");
        sql.Should().Contain("flatten_nested = 0");
        sql.Should().Contain("kafka_format = 'ProtobufSingle'");
        sql.Should().Contain("kafka_schema = 'order_event:OrderEvent'");
        sql.Should().Contain("kafka_schema_registry_skip_bytes = 6");
    }
}
