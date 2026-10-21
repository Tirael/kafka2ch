namespace Sandbox.App.Features.ConsumerHealth;

public sealed class ConsumerHealthOptions
{
    public const string SectionName = "ConsumerHealth";

    public int IntervalMs { get; set; } = 30000;

    public string QueueTable { get; set; } = "orders_queue";

    public string ErrorsTable { get; set; } = "orders_queue_errors";

    public string? Cluster { get; set; }

    public int MaxPollAgeSeconds { get; set; } = 60;

    public int MaxCommitAgeMinutes { get; set; } = 5;

    public int ErrorWindowMinutes { get; set; } = 5;

    public int KafkaTimeoutMs { get; set; } = 10000;

    public ConsumerHealthQuery Query =>
        new(QueueTable, ErrorsTable, string.IsNullOrWhiteSpace(Cluster) ? null : Cluster, ErrorWindow);

    public ConsumerHealthThresholds Thresholds =>
        new(TimeSpan.FromSeconds(MaxPollAgeSeconds), TimeSpan.FromMinutes(MaxCommitAgeMinutes), ErrorWindow);

    private TimeSpan ErrorWindow => TimeSpan.FromMinutes(ErrorWindowMinutes);
}
