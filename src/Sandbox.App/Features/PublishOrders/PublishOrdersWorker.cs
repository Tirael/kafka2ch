using Sandbox.App.Features.ComplexPayload;

namespace Sandbox.App.Features.PublishOrders;

public sealed class PublishOrdersWorker(
    IProducer<OrderKey, OrderEvent> producer,
    IOptions<PublishOrdersOptions> options,
    TimeProvider timeProvider,
    ILogger<PublishOrdersWorker> logger) : BackgroundService
{
    private readonly PublishOrdersOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var now = timeProvider.GetUtcNow();
            var (key, orderEvent) = OrderEventFactory.CreateRandom(now);

            try
            {
                var result = await producer.ProduceAsync(
                    _options.Topic,
                    new Message<OrderKey, OrderEvent> { Key = key, Value = orderEvent },
                    stoppingToken);

                logger.LogInformation(
                    "Published order {OrderId} ({PayloadBytes} bytes) to {Topic} partition {Partition} offset {Offset}",
                    orderEvent.OrderId,
                    PayloadSize.KafkaValueBytes(orderEvent),
                    _options.Topic,
                    result.Partition,
                    result.Offset);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to publish order {OrderId}", orderEvent.OrderId);
            }

            await Task.Delay(TimeSpan.FromMilliseconds(_options.IntervalMs), timeProvider, stoppingToken);
        }
    }
}
