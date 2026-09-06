namespace Sandbox.App.Features.PublishShipments;

public sealed class PublishShipmentsWorker(
    IProducer<ShipmentKey, ShipmentEvent> producer,
    IOptions<PublishShipmentsOptions> options,
    TimeProvider timeProvider,
    ILogger<PublishShipmentsWorker> logger) : BackgroundService
{
    private readonly PublishShipmentsOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var now = timeProvider.GetUtcNow();
            var (key, shipmentEvent) = ShipmentEventFactory.CreateRandom(now);

            try
            {
                var result = await producer.ProduceAsync(
                    _options.Topic,
                    new Message<ShipmentKey, ShipmentEvent> { Key = key, Value = shipmentEvent },
                    stoppingToken);

                logger.LogInformation(
                    "Published shipment {ShipmentId} for order {OrderId} to {Topic} partition {Partition} offset {Offset}",
                    shipmentEvent.ShipmentId,
                    shipmentEvent.OrderId,
                    _options.Topic,
                    result.Partition,
                    result.Offset);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to publish shipment {ShipmentId}", shipmentEvent.ShipmentId);
            }

            await Task.Delay(TimeSpan.FromMilliseconds(_options.IntervalMs), timeProvider, stoppingToken);
        }
    }
}
