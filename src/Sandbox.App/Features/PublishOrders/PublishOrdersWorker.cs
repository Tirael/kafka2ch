using Confluent.Kafka;
using Microsoft.Extensions.Options;
using Sandbox.Contracts;

namespace Sandbox.App.Features.PublishOrders;

public sealed class PublishOrdersWorker : BackgroundService
{
    private readonly IProducer<OrderKey, OrderEvent> _producer;
    private readonly PublishOrdersOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<PublishOrdersWorker> _logger;

    public PublishOrdersWorker(
        IProducer<OrderKey, OrderEvent> producer,
        IOptions<PublishOrdersOptions> options,
        TimeProvider timeProvider,
        ILogger<PublishOrdersWorker> logger)
    {
        _producer = producer;
        _options = options.Value;
        _timeProvider = timeProvider;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var now = _timeProvider.GetUtcNow();
            var (key, orderEvent) = OrderEventFactory.CreateRandom(now);

            try
            {
                var result = await _producer.ProduceAsync(
                    _options.Topic,
                    new Message<OrderKey, OrderEvent> { Key = key, Value = orderEvent },
                    stoppingToken);

                _logger.LogInformation(
                    "Published order {OrderId} to {Topic} partition {Partition} offset {Offset}",
                    orderEvent.OrderId,
                    _options.Topic,
                    result.Partition,
                    result.Offset);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to publish order {OrderId}", orderEvent.OrderId);
            }

            await Task.Delay(TimeSpan.FromMilliseconds(_options.IntervalMs), _timeProvider, stoppingToken);
        }
    }
}
