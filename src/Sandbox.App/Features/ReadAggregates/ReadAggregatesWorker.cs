using ClickHouse.Client.ADO;
using Microsoft.Extensions.Options;
using Sandbox.App.Common;

namespace Sandbox.App.Features.ReadAggregates;

public sealed class ReadAggregatesWorker : BackgroundService
{
    private readonly ClickHouseClientFactory _connectionFactory;
    private readonly ReadAggregatesOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<ReadAggregatesWorker> _logger;

    public ReadAggregatesWorker(
        ClickHouseClientFactory connectionFactory,
        IOptions<ReadAggregatesOptions> options,
        TimeProvider timeProvider,
        ILogger<ReadAggregatesWorker> logger)
    {
        _connectionFactory = connectionFactory;
        _options = options.Value;
        _timeProvider = timeProvider;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var rows = await StartupRetry.ExecuteAsync(
                    () => QueryAggregatesAsync(stoppingToken),
                    _logger,
                    _timeProvider,
                    stoppingToken);

                if (rows.Count == 0)
                {
                    _logger.LogInformation(
                        "No aggregates in the last {WindowMinutes} minutes",
                        _options.WindowMinutes);
                }
                else
                {
                    foreach (var row in rows)
                    {
                        _logger.LogInformation(
                            "Aggregate {Minute:u} {Category}: orders={OrdersCount} amount={TotalAmount:F2} qty={TotalQty}",
                            row.Minute,
                            row.Category,
                            row.OrdersCount,
                            row.TotalAmount,
                            row.TotalQty);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to read aggregates from ClickHouse");
            }

            await Task.Delay(TimeSpan.FromMilliseconds(_options.IntervalMs), _timeProvider, stoppingToken);
        }
    }

    private async Task<IReadOnlyList<OrderAggregateRow>> QueryAggregatesAsync(CancellationToken cancellationToken)
    {
        await using var connection = _connectionFactory.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT minute, category,
                   sum(orders_count) AS orders_count,
                   sum(total_amount) AS total_amount,
                   sum(total_qty) AS total_qty
            FROM orders_agg_1m
            WHERE minute >= now() - INTERVAL {_options.WindowMinutes} MINUTE
            GROUP BY minute, category
            ORDER BY minute DESC, category
            """;

        var rows = new List<OrderAggregateRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new OrderAggregateRow(
                reader.GetDateTime(0),
                reader.GetString(1),
                Convert.ToUInt64(reader.GetValue(2)),
                reader.GetDouble(3),
                Convert.ToUInt64(reader.GetValue(4))));
        }

        return rows;
    }
}
