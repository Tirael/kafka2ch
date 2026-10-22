namespace Sandbox.App.Features.ConsumerHealth;

public sealed class ConsumerHealthWorker(
    ClickHouseClientFactory connectionFactory,
    KafkaClientFactory kafkaClientFactory,
    IOptions<ConsumerHealthOptions> options,
    TimeProvider timeProvider,
    ILogger<ConsumerHealthWorker> logger) : BackgroundService
{
    private readonly ConsumerHealthOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var admin = kafkaClientFactory.CreateAdminClient();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var snapshot = await ReadSnapshotAsync(admin, stoppingToken);
                Log(ConsumerHealthEvaluator.Evaluate(snapshot, _options.Thresholds));
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogError(ex, "Failed to read Kafka consumer health from ClickHouse");
            }

            await Task.Delay(TimeSpan.FromMilliseconds(_options.IntervalMs), timeProvider, stoppingToken);
        }
    }

    private async Task<ConsumerHealthSnapshot> ReadSnapshotAsync(IAdminClient admin, CancellationToken cancellationToken)
    {
        await using var connection = connectionFactory.CreateConnection();
        await connection.OpenAsync(cancellationToken);
        var snapshot = await ConsumerHealthReader.ReadAsync(connection, _options.Query, cancellationToken);

        try
        {
            return await snapshot.WithEndOffsetsAsync(admin, TimeSpan.FromMilliseconds(_options.KafkaTimeoutMs));
        }
        catch (KafkaException ex)
        {
            logger.LogWarning(ex, "Failed to read Kafka end offsets, consumer lag is unknown");
            return snapshot;
        }
    }

    private void Log(ConsumerHealthReport report)
    {
        var level = report.Status switch
        {
            ConsumerHealthStatus.Healthy => LogLevel.Information,
            ConsumerHealthStatus.Degraded => LogLevel.Warning,
            _ => LogLevel.Error
        };

        logger.Log(
            level,
            "Kafka consumers of {QueueTable}: {Status} consumers={Consumers} partitions={AssignedPartitions} " +
            "lag={Lag} lastCommit={LastCommitTime:u} errors={RecentErrors}/{ErrorWindowMinutes}m total={TotalErrors}" +
            "{Issues}",
            _options.QueueTable,
            report.Status,
            report.Consumers,
            report.AssignedPartitions,
            report.Lag?.ToString() ?? "unknown",
            report.LastCommitTime,
            report.RecentErrors,
            _options.ErrorWindowMinutes,
            report.TotalErrors,
            report.Issues.Count == 0 ? "" : "; " + string.Join("; ", report.Issues));
    }
}
