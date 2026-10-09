namespace ClickHouseSchemaGen.IntegrationTests;

public sealed class GeneratedSchemaClickHouseIntegrationTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _clickHouse = new ClickHouseBuilder("clickhouse/clickhouse-server:25.11")
        .WithBindMount(RepoPaths.FormatSchemasDirectory, "/var/lib/clickhouse/format_schemas")
        .Build();

    public async Task InitializeAsync() => await _clickHouse.StartAsync();

    public async Task DisposeAsync() => await _clickHouse.DisposeAsync();

    [Fact]
    public async Task GivenGeneratedKafkaTableDdl_WhenAppliedToClickHouse_ThenTableHasExpectedColumns()
    {

        var config = OrdersQueueTestConfig.Create();
        var ddl = SchemaGeneratorFactory.Create()
            .GenerateKafkaTableSql(config, OrdersQueueTestConfig.Defaults);


        var execResult = await _clickHouse.ExecScriptAsync(ddl);
        await using var connection = new ClickHouseConnection(_clickHouse.GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "DESCRIBE TABLE orders_queue";
        await using var reader = await command.ExecuteReaderAsync();

        var columns = new List<(string Name, string Type)>();
        while (await reader.ReadAsync())
            columns.Add((reader.GetString(0), reader.GetString(1)));


        execResult.ExitCode.Should().Be(0, execResult.Stderr);
        columns.Select(column => column.Name).Should().BeEquivalentTo([
            "order_id",
            "category",
            "price.currency",
            "price.amount",
            "quantity",
            "event_time.seconds",
            "event_time.nanos",
            "status",
            "tags",
            "items",
            "metadata",
            "note",
            "card.last4",
            "card.network",
            "cash.received",
            "wallet.provider",
            "wallet.wallet_id",
            "payment",
            "promo_code",
            "status_history",
            "loyalty_points",
            "attachments"
        ], options => options.WithStrictOrdering());
        columns.Single(column => column.Name == "items").Type.Should().Contain("parts Array(Tuple(");
        columns.Single(column => column.Name == "metadata").Type.Should().Be("Map(String, String)");
        columns.Single(column => column.Name == "note").Type.Should().Be("Nullable(String)");
    }

    [Fact]
    public async Task GivenOrderEventProtobuf_WhenInsertedViaProtobufSingle_ThenRowIsReadable()
    {

        var config = OrdersQueueTestConfig.Create();
        var mapper = new ProtoToClickHouseMapper();
        var columns = mapper.MapMessage(
            ProtoToClickHouseMapper.ResolveDescriptor(config.MessageType),
            OrdersQueueTestConfig.Defaults,
            config.FieldOverrides);
        var columnDefinitions = string.Join(
            ",\n    ",
            columns.Select(column => SqlColumnFormatter.FormatBareDefinition(column.Name, column.Type)));

        var createTableSql = $"""
            CREATE TABLE order_events_ingest
            (
                {columnDefinitions}
            )
            ENGINE = MergeTree
            ORDER BY order_id
            SETTINGS flatten_nested = 0, input_format_protobuf_oneof_presence = 1, input_format_protobuf_flatten_google_wrappers = 1
            """;

        var execResult = await _clickHouse.ExecScriptAsync(createTableSql);
        execResult.ExitCode.Should().Be(0, execResult.Stderr);

        var orderEvent = new OrderEvent
        {
            OrderId = "ord-integration-1",
            Category = "books",
            Price = new Money { Currency = "USD", Amount = 19.99 },
            Quantity = 2,
            EventTime = Google.Protobuf.WellKnownTypes.Timestamp.FromDateTimeOffset(
                DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_000)),
            Status = OrderStatus.Paid,
            Note = "integration-note",
            Card = new CardPayment { Last4 = "4242", Network = "visa" },
            PromoCode = "PROMO-42",
            LoyaltyPoints = 100
        };
        orderEvent.Tags.AddRange(["promo", "vip"]);
        var lineItem = new LineItem
        {
            Sku = "SKU-42",
            Qty = 3,
            UnitPrice = 9.99,
            LineStatus = OrderStatus.Paid
        };
        lineItem.Parts.Add(new ItemPart { Sku = "PART-1", Qty = 1, Weight = 0.4 });
        orderEvent.Items.Add(lineItem);
        orderEvent.Attachments = new OrderAttachments();
        orderEvent.Attachments.Invoices.Add(new Attachment { Name = "invoice.pdf", Note = "paid" });
        orderEvent.Attachments.Receipts.Add(new Attachment { Name = "receipt.pdf" });
        orderEvent.Metadata["source"] = "integration-test";
        orderEvent.StatusHistory.Add(OrderStatus.Created);
        orderEvent.StatusHistory.Add(OrderStatus.Paid);

        using var payload = new MemoryStream();
        orderEvent.WriteTo(payload);
        var insertQuery =
            "INSERT INTO order_events_ingest SETTINGS format_schema='order_event:OrderEvent', input_format_protobuf_oneof_presence=1, input_format_protobuf_flatten_google_wrappers=1 FORMAT ProtobufSingle";


        await InsertProtobufAsync(insertQuery, payload.ToArray());

        await using var connection = new ClickHouseConnection(_clickHouse.GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT
                order_id,
                category,
                `price.currency`,
                `price.amount`,
                quantity,
                `event_time.seconds`,
                toString(status) AS status,
                tags,
                items.sku,
                metadata['source'],
                note,
                `card.last4`,
                toString(payment) AS payment,
                promo_code,
                toJSONString(items.parts.sku) AS part_skus,
                toJSONString(items.parts.weight) AS part_weights,
                toJSONString(attachments.invoices.note) AS invoice_notes,
                toJSONString(attachments.receipts.name) AS receipt_names
            FROM order_events_ingest
            WHERE order_id = 'ord-integration-1'
            """;
        await using var reader = await command.ExecuteReaderAsync();
        var hasRow = await reader.ReadAsync();


        hasRow.Should().BeTrue();
        reader.GetString(0).Should().Be("ord-integration-1");
        reader.GetString(1).Should().Be("books");
        reader.GetString(2).Should().Be("USD");
        reader.GetDouble(3).Should().Be(19.99);
        reader.GetFieldValue<uint>(4).Should().Be(2u);
        reader.GetInt64(5).Should().Be(1_700_000_000);
        reader.GetString(6).Should().Be("ORDER_STATUS_PAID");
        reader.GetFieldValue<string[]>(7).Should().BeEquivalentTo(["promo", "vip"]);
        reader.GetFieldValue<string[]>(8).Should().BeEquivalentTo(["SKU-42"]);
        reader.GetString(9).Should().Be("integration-test");
        reader.GetString(10).Should().Be("integration-note");
        reader.GetString(11).Should().Be("4242");
        reader.GetString(12).Should().Be("card");
        reader.GetString(13).Should().Be("PROMO-42");
        reader.GetString(14).Should().Contain("PART-1");
        reader.GetString(15).Should().Contain("0.4");
        reader.GetString(16).Should().Contain("paid");
        reader.GetString(17).Should().Contain("receipt.pdf");
    }

    [Fact]
    public async Task GivenShipmentEventProtobuf_WhenInsertedViaProtobufSingle_ThenNestedScansAndDocumentsAreReadable()
    {

        var columns = new DenormalizationPlanner().MapMessage(
            ShipmentEvent.Descriptor,
            OrdersQueueTestConfig.Defaults,
            MappingTestSupport.EmptyOverrides);
        var columnDefinitions = string.Join(
            ",\n    ",
            columns.Select(column => SqlColumnFormatter.FormatBareDefinition(column.Name, column.Type)));
        var createTableSql = $"""
            CREATE TABLE shipment_events_ingest
            (
                {columnDefinitions}
            )
            ENGINE = MergeTree
            ORDER BY shipment_id
            SETTINGS flatten_nested = 0, input_format_protobuf_oneof_presence = 1, input_format_protobuf_flatten_google_wrappers = 1
            """;
        var execResult = await _clickHouse.ExecScriptAsync(createTableSql);
        execResult.ExitCode.Should().Be(0, execResult.Stderr);

        var shippedAt = Timestamp.FromDateTimeOffset(DateTimeOffset.Parse("2024-06-01T12:00:00Z"));
        var shipmentEvent = new ShipmentEvent
        {
            ShipmentId = "shp-integration-1",
            OrderId = "ord-integration-1",
            ShippedAt = shippedAt,
            Status = ShipmentStatus.InTransit,
            Destination = new Address
            {
                Country = "DE",
                City = "Berlin",
                Street = "Main",
                PostalCode = "10115"
            },
            Documents = new ShipmentDocuments()
        };
        var checkpoint = new TrackingCheckpoint
        {
            RecordedAt = shippedAt,
            Location = "hub",
            Status = ShipmentStatus.InTransit
        };
        checkpoint.Scans.Add(new Scan { Code = "SCAN-1", OperatorNote = "loaded" });
        shipmentEvent.Checkpoints.Add(checkpoint);
        shipmentEvent.Documents.Labels.Add(new Document { Id = "label-1", Pages = 2 });
        shipmentEvent.Documents.CustomsForms.Add(new Document { Id = "customs-1" });

        using var payload = new MemoryStream();
        shipmentEvent.WriteTo(payload);


        await InsertProtobufAsync(
            "INSERT INTO shipment_events_ingest SETTINGS format_schema='shipment_event:ShipmentEvent', input_format_protobuf_oneof_presence=1, input_format_protobuf_flatten_google_wrappers=1 FORMAT ProtobufSingle",
            payload.ToArray());

        await using var connection = new ClickHouseConnection(_clickHouse.GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT
                toJSONString(checkpoints.scans.code) AS scan_codes,
                toJSONString(checkpoints.scans.operator_note) AS scan_notes,
                toJSONString(documents.labels.pages) AS label_pages,
                toJSONString(documents.customs_forms.id) AS customs_ids
            FROM shipment_events_ingest
            WHERE shipment_id = 'shp-integration-1'
            """;
        await using var reader = await command.ExecuteReaderAsync();
        var hasRow = await reader.ReadAsync();


        hasRow.Should().BeTrue();
        reader.GetString(0).Should().Contain("SCAN-1");
        reader.GetString(1).Should().Contain("loaded");
        reader.GetString(2).Should().Contain("2");
        reader.GetString(3).Should().Contain("customs-1");
    }

    [Fact]
    public async Task GivenOneofMessage_WhenInsertedViaProtobufSingle_ThenActiveBranchIsReadable()
    {

        await CreateIngestTableAsync(
            OneofMessage.Descriptor,
            "oneof_messages_ingest",
            settings: "SETTINGS input_format_protobuf_oneof_presence = 1");

        var message = new OneofMessage { Number = 42 };
        using var payload = new MemoryStream();
        message.WriteTo(payload);


        await InsertProtobufAsync(
            "INSERT INTO oneof_messages_ingest SETTINGS format_schema='mapping_fixtures:OneofMessage', input_format_protobuf_oneof_presence=1 FORMAT ProtobufSingle",
            payload.ToArray());

        await using var connection = new ClickHouseConnection(_clickHouse.GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT text, number, toString(payload) AS payload
            FROM oneof_messages_ingest
            """;
        await using var reader = await command.ExecuteReaderAsync();
        await reader.ReadAsync();


        reader.GetString(0).Should().BeEmpty();
        reader.GetInt32(1).Should().Be(42);
        reader.GetString(2).Should().Be("number");
    }

    [Fact]
    public async Task GivenTimestampMessage_WhenInsertedViaProtobufSingle_ThenTimestampIsReadable()
    {

        await CreateIngestTableAsync(
            TimestampFieldsMessage.Descriptor,
            "timestamp_messages_ingest");

        var message = new TimestampFieldsMessage
        {
            CreatedAt = Timestamp.FromDateTimeOffset(DateTimeOffset.Parse("2024-01-15T10:30:00Z"))
        };
        using var payload = new MemoryStream();
        message.WriteTo(payload);


        await InsertProtobufAsync(
            "INSERT INTO timestamp_messages_ingest SETTINGS format_schema='mapping_fixtures:TimestampFieldsMessage' FORMAT ProtobufSingle",
            payload.ToArray());

        await using var connection = new ClickHouseConnection(_clickHouse.GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT
                toDateTime64(created_at.seconds + created_at.nanos / 1000000000.0, 3) AS created_at
            FROM timestamp_messages_ingest
            """;
        var createdAt = await command.ExecuteScalarAsync();


        createdAt.Should().NotBeNull();
        Convert.ToDateTime(createdAt).Should().Be(DateTime.Parse("2024-01-15T10:30:00Z").ToUniversalTime());
    }

    private async Task CreateIngestTableAsync(
        MessageDescriptor descriptor,
        string tableName,
        string settings = "")
    {
        var columns = MappingTestSupport.MapFixture(descriptor);
        var columnDefinitions = string.Join(
            ",\n    ",
            columns.Select(column => SqlColumnFormatter.FormatBareDefinition(column.Name, column.Type)));
        var createTableSql = $"""
            CREATE TABLE {tableName}
            (
                {columnDefinitions}
            )
            ENGINE = MergeTree
            ORDER BY tuple()
            {settings}
            """;

        var execResult = await _clickHouse.ExecScriptAsync(createTableSql);
        execResult.ExitCode.Should().Be(0, execResult.Stderr);
    }

    private async Task InsertProtobufAsync(string query, byte[] payload)
    {
        var connectionStringBuilder = new ClickHouseConnectionStringBuilder(_clickHouse.GetConnectionString());
        var port = _clickHouse.GetMappedPublicPort(8123);
        using var httpClient = new HttpClient { BaseAddress = new Uri($"http://{_clickHouse.Hostname}:{port}") };

        if (!string.IsNullOrEmpty(connectionStringBuilder.Password))
        {
            var credentials = Convert.ToBase64String(
                Encoding.UTF8.GetBytes($"{connectionStringBuilder.Username}:{connectionStringBuilder.Password}"));
            httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", credentials);
        }

        using var content = new ByteArrayContent(payload);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");

        using var response = await httpClient.PostAsync($"?query={Uri.EscapeDataString(query)}", content);
        var responseBody = await response.Content.ReadAsStringAsync();
        response.IsSuccessStatusCode.Should().BeTrue($"insert failed: {responseBody}");
    }
}
