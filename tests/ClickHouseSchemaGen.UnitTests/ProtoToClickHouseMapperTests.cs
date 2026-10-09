namespace ClickHouseSchemaGen.UnitTests;

public sealed class ProtoToClickHouseMapperTests
{
    private readonly ProtoToClickHouseMapper _sut = new();

    [Fact]
    public void GivenOrderEventDescriptor_WhenMapped_ThenColumnsMatchExpectedSchema()
    {

        var config = OrdersQueueTestConfig.Create();


        var columns = _sut.MapMessage(
            OrderEvent.Descriptor,
            OrdersQueueTestConfig.Defaults,
            config.FieldOverrides);


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
            ("items", "Nested(sku String, qty UInt32, unit_price Float64, line_status Enum8('ORDER_STATUS_UNSPECIFIED' = 0, 'ORDER_STATUS_CREATED' = 1, 'ORDER_STATUS_PAID' = 2), parts Array(Tuple(sku String, qty UInt32, weight Nullable(Float64))))"),
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
            ("loyalty_points", "Nullable(Int32)"),
            ("attachments", "Tuple(invoices Array(Tuple(name String, note Nullable(String))), receipts Array(Tuple(name String, note Nullable(String))))")
        ], options => options.WithStrictOrdering());
    }

    [Fact]
    public void GivenShipmentEventDescriptor_WhenMapped_ThenNestedRepeatedAndIndependentListsStaySeparate()
    {

        var columns = _sut.MapMessage(
            ShipmentEvent.Descriptor,
            OrdersQueueTestConfig.Defaults,
            MappingTestSupport.EmptyOverrides);


        var checkpoints = columns.Should().ContainSingle(column => column.Name == "checkpoints").Subject;
        checkpoints.Type.Should().StartWith("Nested(");
        checkpoints.Type.Should().Contain("scans Array(Tuple(code String, operator_note Nullable(String)))");
        checkpoints.Type.IndexOf("Nested(", "Nested(".Length, StringComparison.Ordinal).Should().Be(-1);
        checkpoints.FlattensGoogleWrapper.Should().BeTrue();

        var documents = columns.Should().ContainSingle(column => column.Name == "documents").Subject;
        documents.Type.Should().StartWith("Tuple(");
        documents.Type.Should().Contain("labels Array(Tuple(id String, pages Nullable(Int32)))");
        documents.Type.Should().Contain("customs_forms Array(Tuple(id String, pages Nullable(Int32)))");
        documents.Type.Should().NotContain("Nested(");
        documents.FlattensGoogleWrapper.Should().BeTrue();
    }

    [Fact]
    public void GivenDottedFieldOverrides_WhenMapped_ThenAppliesThemOnNestedPaths()
    {
        var orderColumns = _sut.MapMessage(
            OrderEvent.Descriptor,
            OrdersQueueTestConfig.Defaults,
            new Dictionary<string, FieldOverrideConfig>(StringComparer.OrdinalIgnoreCase)
            {
                ["price.currency"] = new() { Type = "LowCardinality(String)" },
                ["card.last4"] = new() { Type = "FixedString(4)" },
                ["items.sku"] = new() { Type = "LowCardinality(String)" },
                ["attachments.invoices"] = new() { Type = "Array(String)" }
            });
        var shipmentColumns = _sut.MapMessage(
            ShipmentEvent.Descriptor,
            OrdersQueueTestConfig.Defaults,
            new Dictionary<string, FieldOverrideConfig>(StringComparer.OrdinalIgnoreCase)
            {
                ["destination.country"] = new() { Type = "LowCardinality(String)" },
                ["destination.city"] = new() { Type = "LowCardinality(String)" }
            });

        orderColumns.Single(column => column.Name == "price.currency").Type.Should().Be("LowCardinality(String)");
        orderColumns.Single(column => column.Name == "price.amount").Type.Should().Be("Float64");
        orderColumns.Single(column => column.Name == "card.last4").Type.Should().Be("FixedString(4)");
        orderColumns.Single(column => column.Name == "card.network").Type.Should().Be("String");
        orderColumns.Single(column => column.Name == "items").Type.Should().Contain("sku LowCardinality(String)");
        orderColumns.Single(column => column.Name == "attachments").Type.Should().Contain("invoices Array(String)");
        shipmentColumns.Single(column => column.Name == "destination.country").Type.Should().Be("LowCardinality(String)");
        shipmentColumns.Single(column => column.Name == "destination.city").Type.Should().Be("LowCardinality(String)");
        shipmentColumns.Single(column => column.Name == "destination.street").Type.Should().Be("String");
    }

    [Fact]
    public void GivenRepeatedStringField_WhenMappedWithoutOverride_ThenUsesArrayOfString()
    {

        var tagsField = OrderEvent.Descriptor.Fields.InDeclarationOrder().Single(field => field.Name == "tags");


        var columns = _sut.MapMessage(
            OrderEvent.Descriptor,
            OrdersQueueTestConfig.Defaults,
            overrides: MappingTestSupport.EmptyOverrides);


        tagsField.IsRepeated.Should().BeTrue();
        columns.Single(column => column.Name == "tags").Type.Should().Be("Array(String)");
    }

    [Fact]
    public void GivenAssemblyQualifiedMessageType_WhenResolved_ThenDescriptorMatchesOrderEvent()
    {

        const string messageType = "Sandbox.Contracts.OrderEvent, Sandbox.Contracts";


        var descriptor = ProtoToClickHouseMapper.ResolveDescriptor(messageType);


        descriptor.Name.Should().Be("OrderEvent");
        descriptor.FullName.Should().Be("sandbox.orders.v1.OrderEvent");
    }
}
