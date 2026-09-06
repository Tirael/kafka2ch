namespace Sandbox.App.Features.ReadAggregates;

public sealed class ReadAggregatesWorker(
    ClickHouseClientFactory connectionFactory,
    IOptions<ReadAggregatesOptions> options,
    TimeProvider timeProvider,
    ILogger<ReadAggregatesWorker> logger) : BackgroundService
{
    private readonly ReadAggregatesOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var (orders, shipments) = await StartupRetry.ExecuteAsync(
                    () => QueryAggregatesAsync(stoppingToken),
                    new RetryContext(logger, timeProvider, stoppingToken));

                LogAggregates(
                    orders,
                    "No order aggregates in the last {WindowMinutes} minutes",
                    row => logger.LogInformation(
                        "Order aggregate {Minute:u} {Category}: orders={OrdersCount} amount={TotalAmount:F2} qty={TotalQty}",
                        row.Minute,
                        row.Category,
                        row.OrdersCount,
                        row.TotalAmount,
                        row.TotalQty));
                LogAggregates(
                    shipments,
                    "No shipment aggregates in the last {WindowMinutes} minutes",
                    row => logger.LogInformation(
                        "Shipment aggregate {Minute:u} {Status}: shipments={ShipmentsCount}",
                        row.Minute,
                        row.Status,
                        row.ShipmentsCount));
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to read aggregates from ClickHouse");
            }

            await Task.Delay(TimeSpan.FromMilliseconds(_options.IntervalMs), timeProvider, stoppingToken);
        }
    }

    private void LogAggregates<T>(
        IReadOnlyList<T> rows,
        string emptyMessage,
        Action<T> logRow)
    {
        if (rows.Count == 0)
        {
            logger.LogInformation(emptyMessage, _options.WindowMinutes);
            return;
        }

        foreach (var row in rows)
            logRow(row);
    }

    private async Task<(IReadOnlyList<OrderAggregateRow> Orders, IReadOnlyList<ShipmentAggregateRow> Shipments)>
        QueryAggregatesAsync(CancellationToken cancellationToken)
    {
        await using var connection = connectionFactory.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        var orders = await QueryOrderAggregatesAsync(connection, cancellationToken);
        var shipments = await QueryShipmentAggregatesAsync(connection, cancellationToken);
        return (orders, shipments);
    }

    private Task<IReadOnlyList<OrderAggregateRow>> QueryOrderAggregatesAsync(
        ClickHouseConnection connection,
        CancellationToken cancellationToken) =>
        QueryAggregatesAsync(
            connection,
            $"""
            SELECT minute, category,
                   sum(orders_count) AS orders_count,
                   sum(total_amount) AS total_amount,
                   sum(total_qty) AS total_qty
            FROM orders_agg_1m
            WHERE minute >= now() - INTERVAL {_options.WindowMinutes} MINUTE
            GROUP BY minute, category
            ORDER BY minute DESC, category
            """,
            reader => new OrderAggregateRow(
                reader.GetDateTime(0),
                reader.GetString(1),
                Convert.ToUInt64(reader.GetValue(2)),
                reader.GetDouble(3),
                Convert.ToUInt64(reader.GetValue(4))),
            cancellationToken);

    private Task<IReadOnlyList<ShipmentAggregateRow>> QueryShipmentAggregatesAsync(
        ClickHouseConnection connection,
        CancellationToken cancellationToken) =>
        QueryAggregatesAsync(
            connection,
            $"""
            SELECT minute, status,
                   sum(shipments_count) AS shipments_count
            FROM shipments_agg_1m
            WHERE minute >= now() - INTERVAL {_options.WindowMinutes} MINUTE
            GROUP BY minute, status
            ORDER BY minute DESC, status
            """,
            reader => new ShipmentAggregateRow(
                reader.GetDateTime(0),
                reader.GetString(1),
                Convert.ToUInt64(reader.GetValue(2))),
            cancellationToken);

    private static async Task<IReadOnlyList<T>> QueryAggregatesAsync<T>(
        ClickHouseConnection connection,
        string commandText,
        Func<System.Data.Common.DbDataReader, T> mapRow,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = commandText;

        List<T> rows = [];
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
            rows.Add(mapRow(reader));

        return rows;
    }
}
