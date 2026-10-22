namespace Sandbox.App.Features.PublishOrders;

public sealed class PublishOrdersWorker(
    IProducer<OrderKey, OrderEvent> producer,
    IOptions<PublishOrdersOptions> options,
    TimeProvider timeProvider,
    IHostApplicationLifetime lifetime,
    ILogger<PublishOrdersWorker> logger) : BackgroundService
{
    private readonly PublishOrdersOptions _options = options.Value;
    private long _claimed;
    private long _published;
    private long _failed;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var started = timeProvider.GetTimestamp();
        var workers = Enumerable.Range(0, Math.Max(1, _options.Parallelism))
            .Select(_ => PublishLoopAsync(started, stoppingToken));

        await Task.WhenAll(workers);

        var elapsed = timeProvider.GetElapsedTime(started);
        logger.LogInformation(
            "Publishing finished: {Published} published, {Failed} failed to {Topic} in {ElapsedMs} ms ({Rate:F0} msg/s)",
            Interlocked.Read(ref _published),
            Interlocked.Read(ref _failed),
            _options.Topic,
            (long)elapsed.TotalMilliseconds,
            Interlocked.Read(ref _published) / Math.Max(elapsed.TotalSeconds, 0.001));

        if (_options.StopApplicationWhenDone)
            lifetime.StopApplication();
    }

    private async Task PublishLoopAsync(long started, CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var sequence = Interlocked.Increment(ref _claimed);
            if (_options.Count > 0 && sequence > _options.Count)
                return;

            if (_options.RatePerSecond > 0)
                await DelayUntilScheduledAsync(started, sequence, stoppingToken);

            await PublishAsync(sequence, stoppingToken);

            if (_options.RatePerSecond <= 0 && _options.IntervalMs > 0)
                await Task.Delay(TimeSpan.FromMilliseconds(_options.IntervalMs), timeProvider, stoppingToken);
        }
    }

    private async Task DelayUntilScheduledAsync(long started, long sequence, CancellationToken stoppingToken)
    {
        var scheduled = TimeSpan.FromSeconds((sequence - 1) / _options.RatePerSecond);
        var wait = scheduled - timeProvider.GetElapsedTime(started);
        if (wait > TimeSpan.Zero)
            await Task.Delay(wait, timeProvider, stoppingToken);
    }

    private async Task PublishAsync(long sequence, CancellationToken stoppingToken)
    {
        var (key, orderEvent) = OrderEventFactory.CreateRandom(timeProvider.GetUtcNow());

        try
        {
            var result = await producer.ProduceAsync(
                _options.Topic,
                new Message<OrderKey, OrderEvent> { Key = key, Value = orderEvent },
                stoppingToken);

            var published = Interlocked.Increment(ref _published);
            if (published % Math.Max(1, _options.LogEvery) == 0)
            {
                logger.LogInformation(
                    "Published order {OrderId} to {Topic} partition {Partition} offset {Offset} (#{Sequence})",
                    orderEvent.OrderId,
                    _options.Topic,
                    result.Partition,
                    result.Offset,
                    sequence);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Interlocked.Increment(ref _failed);
            logger.LogError(ex, "Failed to publish order {OrderId}", orderEvent.OrderId);
        }
    }
}
