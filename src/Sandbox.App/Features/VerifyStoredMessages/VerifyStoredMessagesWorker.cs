using System.Globalization;
using System.Reflection;
using Google.Protobuf.Reflection;
using Sandbox.App.Features.ComplexPayload;

namespace Sandbox.App.Features.VerifyStoredMessages;

public sealed class VerifyStoredMessagesWorker(
    KafkaClientFactory kafkaClientFactory,
    StoredMessageReader reader,
    IOptions<VerifyStoredMessagesOptions> options,
    TimeProvider timeProvider,
    ILogger<VerifyStoredMessagesWorker> logger) : BackgroundService
{
    private readonly VerifyStoredMessagesOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var consumer = kafkaClientFactory.CreateByteConsumer(_options.GroupId);
        consumer.Subscribe([_options.OrdersTopic, _options.ShipmentsTopic]);
        logger.LogInformation(
            "Checking stored messages from {OrdersTopic} and {ShipmentsTopic}",
            _options.OrdersTopic,
            _options.ShipmentsTopic);

        while (!stoppingToken.IsCancellationRequested)
        {
            ConsumeResult<byte[], byte[]> result;
            try
            {
                result = consumer.Consume(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (ConsumeException ex)
            {
                logger.LogError(ex, "Failed to consume a message for the stored-content check");
                await Task.Delay(TimeSpan.FromSeconds(2), timeProvider, stoppingToken);
                continue;
            }

            if (result.Message?.Value is null)
                continue;

            try
            {
                await VerifyAsync(result, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(
                    ex,
                    "Failed to verify {Topic} offset {Offset}",
                    result.Topic,
                    result.Offset);
            }

            consumer.Commit(result);
        }

        consumer.Close();
    }

    private async Task VerifyAsync(ConsumeResult<byte[], byte[]> result, CancellationToken cancellationToken)
    {
        var payload = ConfluentProtobufEnvelope.Payload(result.Message.Value);
        if (result.Topic == _options.OrdersTopic)
        {
            var orderEvent = new OrderEvent();
            orderEvent.MergeFrom(payload);
            await VerifyOrderAsync(orderEvent, cancellationToken);
            return;
        }

        if (result.Topic == _options.ShipmentsTopic)
        {
            var shipmentEvent = new ShipmentEvent();
            shipmentEvent.MergeFrom(payload);
            await VerifyShipmentAsync(shipmentEvent, cancellationToken);
            return;
        }

        logger.LogWarning("Skipping message from unexpected topic {Topic}", result.Topic);
    }

    private async Task VerifyOrderAsync(OrderEvent orderEvent, CancellationToken cancellationToken)
    {
        var id = IdLiteral(orderEvent.OrderId);
        var fullTable = Table(_options.OrdersTable);
        var witness = await WaitForRowAsync(
            CaseFileWitness.SelectSql(fullTable, "order_id", id),
            cancellationToken);
        List<string> diffs = [];
        if (witness.Length == 0)
        {
            diffs.Add($"no row in {fullTable}");
        }
        else
        {
            diffs.AddRange(CaseFileWitness.Compare(orderEvent.CaseFile, witness));
            diffs.AddRange(await CompareProtobufAsync(
                orderEvent,
                $"SELECT * FROM {fullTable} WHERE order_id = '{id}' SETTINGS format_schema = 'order_event:OrderEvent', output_format_protobuf_nullables_with_google_wrappers = 1 FORMAT ProtobufSingle",
                cancellationToken));
        }

        diffs.AddRange(await CompareProjectionAsync(
            SelectProjectionSql(Table(_options.OrdersProjectionTable), "order_id", id, "category, toString(amount), toString(quantity), status, payment"),
            [
                orderEvent.Category,
                FormatDouble(orderEvent.Price.Amount),
                orderEvent.Quantity.ToString(CultureInfo.InvariantCulture),
                ProtoEnumName(orderEvent.Status),
                PaymentName(orderEvent)
            ],
            cancellationToken));

        Report("order", orderEvent.OrderId, PayloadSize.KafkaValueBytes(orderEvent), diffs);
    }

    private async Task VerifyShipmentAsync(ShipmentEvent shipmentEvent, CancellationToken cancellationToken)
    {
        var id = IdLiteral(shipmentEvent.ShipmentId);
        var fullTable = Table(_options.ShipmentsTable);
        var witness = await WaitForRowAsync(
            CaseFileWitness.SelectSql(fullTable, "shipment_id", id),
            cancellationToken);
        List<string> diffs = [];
        if (witness.Length == 0)
        {
            diffs.Add($"no row in {fullTable}");
        }
        else
        {
            diffs.AddRange(CaseFileWitness.Compare(shipmentEvent.CaseFile, witness));
            diffs.AddRange(await CompareProtobufAsync(
                shipmentEvent,
                $"SELECT * FROM {fullTable} WHERE shipment_id = '{id}' SETTINGS format_schema = 'shipment_event:ShipmentEvent', output_format_protobuf_nullables_with_google_wrappers = 1 FORMAT ProtobufSingle",
                cancellationToken));
        }

        diffs.AddRange(await CompareProjectionAsync(
            SelectProjectionSql(
                Table(_options.ShipmentsProjectionTable),
                "shipment_id",
                id,
                "status, country, city, delivery_outcome"),
            [
                ProtoEnumName(shipmentEvent.Status),
                shipmentEvent.Destination.Country,
                shipmentEvent.Destination.City,
                DeliveryOutcomeName(shipmentEvent)
            ],
            cancellationToken));

        Report("shipment", shipmentEvent.ShipmentId, PayloadSize.KafkaValueBytes(shipmentEvent), diffs);
    }

    private async Task<IReadOnlyList<string>> CompareProtobufAsync(
        IMessage expected,
        string sql,
        CancellationToken cancellationToken)
    {
        try
        {
            var stored = await reader.ReadProtobufAsync(sql, cancellationToken);
            if (stored is null)
                return ["protobuf export returned no row"];

            var actual = expected.Descriptor.Parser.ParseFrom(stored);
            return MessageContentDiff.Compare(expected, actual);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return [$"protobuf export failed: {ex.Message}"];
        }
    }

    private async Task<IReadOnlyList<string>> CompareProjectionAsync(
        string sql,
        IReadOnlyList<string> expected,
        CancellationToken cancellationToken)
    {
        var deadline = timeProvider.GetUtcNow().AddMilliseconds(_options.WaitTimeoutMs);
        string[] row = [];
        while (timeProvider.GetUtcNow() <= deadline)
        {
            try
            {
                row = await reader.ReadRowAsync(sql, cancellationToken);
                if (row.Length > 0)
                    break;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "Projection table is not readable yet");
            }

            if (timeProvider.GetUtcNow() >= deadline)
                break;

            await Task.Delay(TimeSpan.FromMilliseconds(_options.PollIntervalMs), timeProvider, cancellationToken);
        }

        if (row.Length == 0)
            return ["projection row is missing"];

        if (row.Length != expected.Count)
            return [$"projection column count kafka={expected.Count} clickhouse={row.Length}"];

        List<string> diffs = [];
        for (var i = 0; i < expected.Count; i++)
        {
            if (!ProjectionEquals(expected[i], row[i]))
                diffs.Add($"projection[{i}]: kafka='{expected[i]}' clickhouse='{row[i]}'");
        }

        return diffs;
    }

    private async Task<string[]> WaitForRowAsync(string sql, CancellationToken cancellationToken)
    {
        var deadline = timeProvider.GetUtcNow().AddMilliseconds(_options.WaitTimeoutMs);
        while (true)
        {
            try
            {
                var row = await reader.ReadRowAsync(sql, cancellationToken);
                if (row.Length > 0)
                    return row;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "Full table is not readable yet");
            }

            if (timeProvider.GetUtcNow() >= deadline)
                return [];

            await Task.Delay(TimeSpan.FromMilliseconds(_options.PollIntervalMs), timeProvider, cancellationToken);
        }
    }

    private void Report(string kind, string id, int payloadBytes, IReadOnlyList<string> diffs)
    {
        if (diffs.Count == 0)
        {
            logger.LogInformation(
                "Stored {Kind} {Id} matches ClickHouse ({PayloadBytes} bytes)",
                kind,
                id,
                payloadBytes);
            return;
        }

        logger.LogError(
            "Stored {Kind} {Id} differs from ClickHouse ({PayloadBytes} bytes): {Diffs}",
            kind,
            id,
            payloadBytes,
            string.Join("; ", diffs));
    }

    private static string SelectProjectionSql(string table, string idColumn, string id, string columns) =>
        $"SELECT {columns} FROM {table} WHERE {idColumn} = '{id}' LIMIT 1";

    private static bool ProjectionEquals(string expected, string actual)
    {
        if (string.Equals(expected, actual, StringComparison.Ordinal))
            return true;

        return double.TryParse(expected, NumberStyles.Float, CultureInfo.InvariantCulture, out var expectedNumber)
            && double.TryParse(actual, NumberStyles.Float, CultureInfo.InvariantCulture, out var actualNumber)
            && expectedNumber.Equals(actualNumber);
    }

    private static string FormatDouble(double value) =>
        value.ToString("G17", CultureInfo.InvariantCulture);

    private static string ProtoEnumName(Enum value)
    {
        var member = value.GetType().GetMember(value.ToString()).FirstOrDefault();
        var original = member?.GetCustomAttribute<OriginalNameAttribute>();
        return original?.Name ?? value.ToString();
    }

    private static string PaymentName(OrderEvent orderEvent) => orderEvent.PaymentCase switch
    {
        OrderEvent.PaymentOneofCase.Card => "card",
        OrderEvent.PaymentOneofCase.Cash => "cash",
        OrderEvent.PaymentOneofCase.Wallet => "wallet",
        _ => "absent"
    };

    private static string DeliveryOutcomeName(ShipmentEvent shipmentEvent) =>
        shipmentEvent.DeliveryOutcomeCase switch
        {
            ShipmentEvent.DeliveryOutcomeOneofCase.Delivered => "delivered",
            ShipmentEvent.DeliveryOutcomeOneofCase.Failed => "failed",
            _ => "absent"
        };

    private static string IdLiteral(string id)
    {
        if (!Guid.TryParse(id, out var parsed) || !string.Equals(parsed.ToString(), id, StringComparison.Ordinal))
            throw new InvalidOperationException($"Refusing to query an unexpected id '{id}'.");

        return id;
    }

    private static string Table(string name)
    {
        if (name.Length == 0 || name.Any(ch => !char.IsAsciiLetterOrDigit(ch) && ch != '_'))
            throw new InvalidOperationException($"Refusing to query an unexpected table '{name}'.");

        return name;
    }
}
