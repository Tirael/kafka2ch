using ClickHouseSchemaGen;
using ClickHouseSchemaGen.Models;
using ClickHouseSchemaGen.Tests.Support;
using Sandbox.Contracts;

namespace ClickHouseSchemaGen.Tests.Unit;

public sealed class ProtoToClickHouseMapperTests
{
    private readonly ProtoToClickHouseMapper _sut = new();

    [Fact]
    public void GivenOrderEventDescriptor_WhenMapped_ThenColumnsMatchExpectedSchema()
    {
        // Arrange
        var overrides = OrdersQueueTestConfig.Create().FieldOverrides;

        // Act
        var columns = _sut.MapMessage(OrderEvent.Descriptor, overrides);

        // Assert
        columns.Select(column => (column.Name, column.Type)).Should().BeEquivalentTo([
            ("order_id", "String"),
            ("category", "LowCardinality(String)"),
            ("price.currency", "String"),
            ("price.amount", "Float64"),
            ("quantity", "UInt32"),
            ("event_time_unix_ms", "Int64"),
            ("status", "Enum8('ORDER_STATUS_UNSPECIFIED' = 0, 'ORDER_STATUS_CREATED' = 1, 'ORDER_STATUS_PAID' = 2)"),
            ("tags", "Array(LowCardinality(String))")
        ], options => options.WithStrictOrdering());
    }

    [Fact]
    public void GivenRepeatedStringField_WhenMappedWithoutOverride_ThenUsesArrayOfString()
    {
        // Arrange
        var tagsField = OrderEvent.Descriptor.Fields.InDeclarationOrder().Single(field => field.Name == "tags");

        // Act
        var columns = _sut.MapMessage(OrderEvent.Descriptor, overrides: new Dictionary<string, FieldOverrideConfig>());

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
