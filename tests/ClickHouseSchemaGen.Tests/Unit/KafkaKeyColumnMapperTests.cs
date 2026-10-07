namespace ClickHouseSchemaGen.Tests.Unit;

public sealed class KafkaKeyColumnMapperTests
{
    private readonly KafkaKeyColumnMapper _sut = new(new DenormalizationPlanner());

    [Fact]
    public void GivenDefaultStringKey_WhenMapKeyColumns_ThenReturnsNoColumns()
    {
        // Arrange
        var table = OrdersQueueTestConfig.Create();

        // Act
        var columns = _sut.MapKeyColumns(table, OrdersQueueTestConfig.Defaults);

        // Assert
        table.Key.Format.Should().Be(KafkaKeyFormats.String);
        columns.Should().BeEmpty();
    }

    [Fact]
    public void GivenOrderKeyProtobuf_WhenMapKeyColumns_ThenDecodesFieldAfterConfluentEnvelope()
    {
        // Arrange
        var table = OrdersQueueTestConfig.Create();
        table.Key = new KafkaKeyConfig
        {
            Format = KafkaKeyFormats.Protobuf,
            MessageType = "Sandbox.Contracts.OrderKey, Sandbox.Contracts"
        };

        // Act
        var columns = _sut.MapKeyColumns(table, OrdersQueueTestConfig.Defaults);

        // Assert
        columns.Should().ContainSingle();
        columns[0].Name.Should().Be("_key.order_id");
        columns[0].Type.Should().Be("String");
        columns[0].SourceExpression.Should().Be("CAST(protobufWireBytes(substring(_key, 7), 1) AS String)");
    }

    [Fact]
    public void GivenCompositeKey_WhenMapKeyColumns_ThenUsesValueMappingRulesForTypes()
    {
        // Arrange
        var table = CreateCompositeKeyTable();

        // Act
        var columns = _sut.MapKeyColumns(table, OrdersQueueTestConfig.Defaults);

        // Assert
        MappingTestSupport.NameAndTypes(columns).Should().Equal(
            ("_key.tenant", "LowCardinality(String)"),
            ("_key.int32_value", "Int32"),
            ("_key.int64_value", "Int64"),
            ("_key.uint32_value", "UInt32"),
            ("_key.uint64_value", "UInt64"),
            ("_key.sint32_value", "Int32"),
            ("_key.sint64_value", "Int64"),
            ("_key.fixed32_value", "UInt32"),
            ("_key.fixed64_value", "UInt64"),
            ("_key.sfixed32_value", "Int32"),
            ("_key.sfixed64_value", "Int64"),
            ("_key.flag", "UInt8"),
            ("_key.ratio", "Float32"),
            ("_key.score", "Float64"),
            ("_key.raw", "String"),
            ("_key.status", "Enum8('SAMPLE_STATUS_UNSPECIFIED' = 0, 'SAMPLE_STATUS_ACTIVE' = 1, 'SAMPLE_STATUS_ARCHIVED' = 2)"),
            ("_key.region", "Nullable(String)"),
            ("_key.scope.name", "String"),
            ("_key.scope.level", "Int32"),
            ("_key.issued_at.seconds", "Int64"),
            ("_key.issued_at.nanos", "Int32"),
            ("_key.label", "Nullable(String)"));
    }

    [Fact]
    public void GivenNestedAndOptionalKeyFields_WhenMapKeyColumns_ThenBuildsPresenceAwareExpressions()
    {
        // Arrange
        var table = CreateCompositeKeyTable();
        table.Key.SkipBytes = 0;

        // Act
        var columns = _sut.MapKeyColumns(table, OrdersQueueTestConfig.Defaults)
            .ToDictionary(column => column.Name);

        // Assert
        columns["_key.scope.level"].SourceExpression.Should().Be(
            "CAST(reinterpretAsInt32(toUInt32(protobufWireBits(protobufWireBytes(substring(_key, 1), 18), 2))) AS Int32)");
        columns["_key.region"].SourceExpression.Should().Be(
            "CAST(if(protobufWireHas(substring(_key, 1), 17), protobufWireBytes(substring(_key, 1), 17), NULL) AS Nullable(String))");
        columns["_key.sint64_value"].SourceExpression.Should().Contain("protobufWireZigZag(protobufWireBits(substring(_key, 1), 7))");
        columns["_key.label"].SourceExpression.Should().Be(
            "CAST(if(protobufWireHas(substring(_key, 1), 20), protobufWireBytes(protobufWireBytes(substring(_key, 1), 20), 1), NULL) AS Nullable(String))");
    }

    [Fact]
    public void GivenRepeatedKeyField_WhenMapKeyColumns_ThenThrowsNotSupported()
    {
        // Arrange
        var table = OrdersQueueTestConfig.Create();
        table.Key = new KafkaKeyConfig
        {
            Format = KafkaKeyFormats.Protobuf,
            MessageType = "Sandbox.Contracts.TestFixtures.RepeatedKey, Sandbox.Contracts"
        };

        // Act
        var act = () => _sut.MapKeyColumns(table, OrdersQueueTestConfig.Defaults);

        // Assert
        act.Should().Throw<NotSupportedException>().WithMessage("*orders_queue*_key.parts*");
    }

    internal static KafkaTableConfig CreateCompositeKeyTable()
    {
        var table = OrdersQueueTestConfig.Create();
        table.Key = new KafkaKeyConfig
        {
            Format = KafkaKeyFormats.Protobuf,
            MessageType = "Sandbox.Contracts.TestFixtures.CompositeKey, Sandbox.Contracts",
            FieldOverrides = new Dictionary<string, FieldOverrideConfig>(StringComparer.OrdinalIgnoreCase)
            {
                ["tenant"] = new() { Type = "LowCardinality(String)" }
            }
        };
        return table;
    }
}
