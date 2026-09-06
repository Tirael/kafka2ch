namespace ClickHouseSchemaGen.Tests.Unit;

public sealed class ProtoToClickHouseMapperTests
{
    private readonly ProtoToClickHouseMapper _sut = new();

    [Fact]
    public void GivenOrderEventDescriptor_WhenMapped_ThenColumnsMatchExpectedSchema()
    {
        // Arrange
        var config = OrdersQueueTestConfig.Create();

        // Act
        var columns = _sut.MapMessage(
            OrderEvent.Descriptor,
            OrdersQueueTestConfig.Defaults,
            config.FieldOverrides);

        // Assert
        columns.Select(column => (column.Name, column.Type)).Should().BeEquivalentTo([
            ("order_id", "String"),
            ("category", "LowCardinality(String)"),
            ("price.currency", "String"),
            ("price.amount", "Float64"),
            ("quantity", "UInt32"),
            ("event_time.seconds", "Int64"),
            ("event_time.nanos", "Int32"),
            ("status", "Enum8('ORDER_STATUS_UNSPECIFIED' = 0, 'ORDER_STATUS_CREATED' = 1, 'ORDER_STATUS_PAID' = 2)"),
            ("tags", "Array(LowCardinality(String))"),
            ("items", "Nested(sku String, qty UInt32, unit_price Float64, line_status Enum8('ORDER_STATUS_UNSPECIFIED' = 0, 'ORDER_STATUS_CREATED' = 1, 'ORDER_STATUS_PAID' = 2))"),
            ("metadata", "Map(String, String)"),
            ("note", "Nullable(String)"),
            ("card.last4", "String"),
            ("card.network", "String"),
            ("cash.received", "Float64"),
            ("wallet.provider", "String"),
            ("wallet.wallet_id", "String"),
            ("payment", "Enum8('absent' = 0, 'card' = 11, 'cash' = 12, 'wallet' = 13)"),
            ("promo_code", "Nullable(String)"),
            ("status_history", "Array(Enum8('ORDER_STATUS_UNSPECIFIED' = 0, 'ORDER_STATUS_CREATED' = 1, 'ORDER_STATUS_PAID' = 2))"),
            ("loyalty_points", "Nullable(Int32)")
        ], options => options.WithStrictOrdering());
    }

    [Fact]
    public void GivenRepeatedStringField_WhenMappedWithoutOverride_ThenUsesArrayOfString()
    {
        // Arrange
        var tagsField = OrderEvent.Descriptor.Fields.InDeclarationOrder().Single(field => field.Name == "tags");

        // Act
        var columns = _sut.MapMessage(
            OrderEvent.Descriptor,
            OrdersQueueTestConfig.Defaults,
            overrides: MappingTestSupport.EmptyOverrides);

        // Assert
        tagsField.IsRepeated.Should().BeTrue();
        columns.Single(column => column.Name == "tags").Type.Should().Be("Array(String)");
    }

    [Fact]
    public void GivenAssemblyQualifiedMessageType_WhenResolved_ThenDescriptorMatchesOrderEvent()
    {
        // Arrange
        const string messageType = "Sandbox.Contracts.OrderEvent, Sandbox.Contracts";

        // Act
        var descriptor = ProtoToClickHouseMapper.ResolveDescriptor(messageType);

        // Assert
        descriptor.Name.Should().Be("OrderEvent");
        descriptor.FullName.Should().Be("sandbox.orders.v1.OrderEvent");
    }
}
