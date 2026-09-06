using ClickHouseSchemaGen;
using ClickHouseSchemaGen.Models;
using ClickHouseSchemaGen.Tests.Support;

namespace ClickHouseSchemaGen.Tests.Unit;

public sealed class KafkaTableGeneratorTests
{
    private readonly KafkaTableGenerator _sut = new();

    [Fact]
    public void GivenOrdersQueueConfig_WhenGenerated_ThenSqlContainsKafkaSettingsAndColumns()
    {
        // Arrange
        var config = OrdersQueueTestConfig.Create();
        var columns = new ProtoToClickHouseMapper().MapMessage(
            ProtoToClickHouseMapper.ResolveDescriptor(config.MessageType),
            config.FieldOverrides);

        // Act
        var sql = _sut.Generate(config, columns);

        // Assert
        sql.Should().Contain("CREATE TABLE orders_queue");
        sql.Should().MatchRegex(@"category\s+LowCardinality\(String\)");
        sql.Should().Contain("`price.currency`");
        sql.Should().Contain("tags                 Array(LowCardinality(String))");
        sql.Should().Contain("Enum8('ORDER_STATUS_UNSPECIFIED' = 0, 'ORDER_STATUS_CREATED' = 1, 'ORDER_STATUS_PAID' = 2)");
        sql.Should().Contain("kafka_format = 'ProtobufSingle'");
        sql.Should().Contain("kafka_schema = 'order_event:OrderEvent'");
        sql.Should().Contain("kafka_schema_registry_skip_bytes = 6");
    }
}
