namespace Sandbox.Observability;

public enum ConsumerHealthStatus
{
    Healthy,
    Degraded,
    Unhealthy
}

public sealed record KafkaPartitionAssignment(string Topic, int Partition, long CurrentOffset, long? EndOffset = null)
{
    public long? Lag => EndOffset is { } end && CurrentOffset >= 0 ? Math.Max(0, end - CurrentOffset) : null;
}

public sealed record KafkaConsumerState(
    string Host,
    string ConsumerId,
    IReadOnlyList<KafkaPartitionAssignment> Assignments,
    DateTime? LastExceptionTime,
    string? LastException,
    DateTime? LastPollTime,
    DateTime? LastCommitTime,
    ulong NumCommits,
    bool IsCurrentlyUsed);

public sealed record ConsumerHealthSnapshot(
    DateTime CheckedAt,
    IReadOnlyList<KafkaConsumerState> Consumers,
    ulong RecentErrors,
    ulong TotalErrors);

public sealed record ConsumerHealthThresholds(TimeSpan MaxPollAge, TimeSpan MaxCommitAge, TimeSpan ErrorWindow);

public sealed record ConsumerHealthReport(
    ConsumerHealthStatus Status,
    IReadOnlyList<string> Issues,
    int Consumers,
    int AssignedPartitions,
    long? Lag,
    DateTime? LastCommitTime,
    ulong RecentErrors,
    ulong TotalErrors);
