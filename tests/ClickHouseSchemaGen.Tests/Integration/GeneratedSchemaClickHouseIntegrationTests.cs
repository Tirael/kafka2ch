namespace ClickHouseSchemaGen.Tests.Integration;

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
        // Arrange
        var config = OrdersQueueTestConfig.Create();
        var ddl = SchemaGeneratorFactory.Create()
            .GenerateKafkaTableSql(config, OrdersQueueTestConfig.Defaults);

        // Act
        var execResult = await _clickHouse.ExecScriptAsync(ddl);
        await using var connection = new ClickHouseConnection(_clickHouse.GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "DESCRIBE TABLE orders_queue";
        await using var reader = await command.ExecuteReaderAsync();

        var columns = new List<(string Name, string Type)>();
        while (await reader.ReadAsync())
            columns.Add((reader.GetString(0), reader.GetString(1)));

        // Assert
        execResult.ExitCode.Should().Be(0, execResult.Stderr);
        columns.Select(column => column.Name).Should().BeEquivalentTo([
            "order_id",
            "category",
            "price.currency",
            "price.amount",
            "quantity",
            "event_time_unix_ms",
            "status",
            "tags",
            "items",
            "metadata",
            "note"
        ], options => options.WithStrictOrdering());
        columns.Single(column => column.Name == "items").Type.Should().StartWith("Nested(");
        columns.Single(column => column.Name == "metadata").Type.Should().Be("Map(String, String)");
        columns.Single(column => column.Name == "note").Type.Should().Be("Nullable(String)");
    }

    [Fact]
    public async Task GivenOrderEventProtobuf_WhenInsertedViaProtobufSingle_ThenRowIsReadable()
    {
        // Arrange
        var config = OrdersQueueTestConfig.Create();
        var mapper = new ProtoToClickHouseMapper();
        var columns = mapper.MapMessage(
            ProtoToClickHouseMapper.ResolveDescriptor(config.MessageType),
            OrdersQueueTestConfig.Defaults,
            config.FieldOverrides);
        var columnDefinitions = string.Join(",\n    ", columns.Select(BuildColumnDefinition));

        var createTableSql = $"""
            CREATE TABLE order_events_ingest
            (
                {columnDefinitions}
            )
            ENGINE = MergeTree
            ORDER BY order_id
            SETTINGS flatten_nested = 0
            """;

        var execResult = await _clickHouse.ExecScriptAsync(createTableSql);
        execResult.ExitCode.Should().Be(0, execResult.Stderr);

        var orderEvent = new OrderEvent
        {
            OrderId = "ord-integration-1",
            Category = "books",
            Price = new Money { Currency = "USD", Amount = 19.99 },
            Quantity = 2,
            EventTimeUnixMs = 1_700_000_000_000,
            Status = OrderStatus.Paid,
            Note = "integration-note"
        };
        orderEvent.Tags.AddRange(["promo", "vip"]);
        orderEvent.Items.Add(new LineItem { Sku = "SKU-42", Qty = 3, UnitPrice = 9.99 });
        orderEvent.Metadata["source"] = "integration-test";

        using var payload = new MemoryStream();
        orderEvent.WriteTo(payload);
        var insertQuery =
            "INSERT INTO order_events_ingest SETTINGS format_schema='order_event:OrderEvent' FORMAT ProtobufSingle";

        // Act
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
                event_time_unix_ms,
                toString(status) AS status,
                tags,
                items.sku,
                metadata['source'],
                note
            FROM order_events_ingest
            WHERE order_id = 'ord-integration-1'
            """;
        await using var reader = await command.ExecuteReaderAsync();
        var hasRow = await reader.ReadAsync();

        // Assert
        hasRow.Should().BeTrue();
        reader.GetString(0).Should().Be("ord-integration-1");
        reader.GetString(1).Should().Be("books");
        reader.GetString(2).Should().Be("USD");
        reader.GetDouble(3).Should().Be(19.99);
        reader.GetFieldValue<uint>(4).Should().Be(2u);
        reader.GetInt64(5).Should().Be(1_700_000_000_000);
        reader.GetString(6).Should().Be("ORDER_STATUS_PAID");
        reader.GetFieldValue<string[]>(7).Should().BeEquivalentTo(["promo", "vip"]);
        reader.GetFieldValue<string[]>(8).Should().BeEquivalentTo(["SKU-42"]);
        reader.GetString(9).Should().Be("integration-test");
        reader.GetString(10).Should().Be("integration-note");
    }

    [Fact]
    public async Task GivenOneofMessage_WhenInsertedViaProtobufSingle_ThenActiveBranchIsReadable()
    {
        // Arrange
        await CreateIngestTableAsync(
            OneofMessage.Descriptor,
            "oneof_messages_ingest",
            settings: "SETTINGS input_format_protobuf_oneof_presence = 1");

        var message = new OneofMessage { Number = 42 };
        using var payload = new MemoryStream();
        message.WriteTo(payload);

        // Act
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

        // Assert
        reader.GetString(0).Should().BeEmpty();
        reader.GetInt32(1).Should().Be(42);
        reader.GetString(2).Should().Be("number");
    }

    [Fact]
    public async Task GivenTimestampMessage_WhenInsertedViaProtobufSingle_ThenTimestampIsReadable()
    {
        // Arrange
        await CreateIngestTableAsync(
            TimestampFieldsMessage.Descriptor,
            "timestamp_messages_ingest");

        var message = new TimestampFieldsMessage
        {
            CreatedAt = Timestamp.FromDateTimeOffset(DateTimeOffset.Parse("2024-01-15T10:30:00Z"))
        };
        using var payload = new MemoryStream();
        message.WriteTo(payload);

        // Act
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

        // Assert
        createdAt.Should().NotBeNull();
        Convert.ToDateTime(createdAt).Should().Be(DateTime.Parse("2024-01-15T10:30:00Z").ToUniversalTime());
    }

    private async Task CreateIngestTableAsync(
        MessageDescriptor descriptor,
        string tableName,
        string settings = "")
    {
        var columns = new DenormalizationPlanner().MapMessage(
            descriptor,
            OrdersQueueTestConfig.Defaults,
            new Dictionary<string, FieldOverrideConfig>());
        var columnDefinitions = string.Join(",\n    ", columns.Select(BuildColumnDefinition));
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

    private static string BuildColumnDefinition(ClickHouseColumn column) =>
        $"{SqlColumnFormatter.FormatColumnName(column.Name)} {column.Type}";

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
