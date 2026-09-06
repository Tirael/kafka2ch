using System.Net.Http.Headers;
using System.Text;
using ClickHouse.Client.ADO;
using ClickHouseSchemaGen;
using ClickHouseSchemaGen.Models;
using ClickHouseSchemaGen.Tests.Support;
using Google.Protobuf;
using Sandbox.Contracts;
using Sandbox.Contracts.Common;
using Testcontainers.ClickHouse;

namespace ClickHouseSchemaGen.Tests.Integration;

public sealed class GeneratedSchemaClickHouseIntegrationTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _clickHouse = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:25.11")
        .WithBindMount(RepoPaths.FormatSchemasDirectory, "/var/lib/clickhouse/format_schemas")
        .Build();

    public async Task InitializeAsync() => await _clickHouse.StartAsync();

    public async Task DisposeAsync() => await _clickHouse.DisposeAsync();

    [Fact]
    public async Task GivenGeneratedKafkaTableDdl_WhenAppliedToClickHouse_ThenTableHasExpectedColumns()
    {
        // Arrange
        var config = OrdersQueueTestConfig.Create();
        var ddl = new ClickHouseSchemaGenerator(new ProtoToClickHouseMapper(), new KafkaTableGenerator())
            .GenerateKafkaTableSql(config);

        // Act
        var execResult = await _clickHouse.ExecScriptAsync(ddl);
        await using var connection = new ClickHouseConnection(_clickHouse.GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "DESCRIBE TABLE orders_queue";
        await using var reader = await command.ExecuteReaderAsync();

        var columns = new List<(string Name, string Type)>();
        while (await reader.ReadAsync())
        {
            columns.Add((reader.GetString(0), reader.GetString(1)));
        }

        // Assert
        execResult.ExitCode.Should().Be(0, execResult.Stderr);
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
    public async Task GivenOrderEventProtobuf_WhenInsertedViaProtobufSingle_ThenRowIsReadable()
    {
        // Arrange
        var config = OrdersQueueTestConfig.Create();
        var mapper = new ProtoToClickHouseMapper();
        var columns = mapper.MapMessage(
            ProtoToClickHouseMapper.ResolveDescriptor(config.MessageType),
            config.FieldOverrides);
        var columnDefinitions = string.Join(",\n    ", columns.Select(BuildColumnDefinition));

        var createTableSql = $"""
            CREATE TABLE order_events_ingest
            (
                {columnDefinitions}
            )
            ENGINE = MergeTree
            ORDER BY order_id
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
            Status = OrderStatus.Paid
        };
        orderEvent.Tags.AddRange(["promo", "vip"]);

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
                tags
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
    }

    private static string BuildColumnDefinition(ClickHouseColumn column)
    {
        var name = column.Name.Contains('.') ? $"`{column.Name}`" : column.Name;
        return $"{name} {column.Type}";
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
