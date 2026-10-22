using Sandbox.Observability;

namespace ClickHouseSchemaGen.UnitTests;

public sealed class ConsumerHealthEvaluatorTests
{
    private static readonly DateTime Now = new(2026, 10, 21, 12, 0, 0, DateTimeKind.Utc);

    private static readonly ConsumerHealthThresholds Thresholds = new(
        MaxPollAge: TimeSpan.FromMinutes(1),
        MaxCommitAge: TimeSpan.FromMinutes(5),
        ErrorWindow: TimeSpan.FromMinutes(5));

    [Fact]
    public void GivenAssignedConsumerWithFreshPollAndCommit_WhenEvaluate_ThenHealthy()
    {
        var report = Evaluate(Consumer(Assignment(0, 100, 100), Assignment(1, 50, 50)));

        report.Status.Should().Be(ConsumerHealthStatus.Healthy);
        report.Issues.Should().BeEmpty();
        report.Consumers.Should().Be(1);
        report.AssignedPartitions.Should().Be(2);
        report.Lag.Should().Be(0);
        report.LastCommitTime.Should().Be(Now.AddSeconds(-10));
    }

    [Fact]
    public void GivenNoConsumerRows_WhenEvaluate_ThenUnhealthy()
    {
        var report = Evaluate();

        report.Status.Should().Be(ConsumerHealthStatus.Unhealthy);
        report.Issues.Should().ContainSingle().Which.Should().Contain("no Kafka consumers");
    }

    [Fact]
    public void GivenConsumersWithoutAssignments_WhenEvaluate_ThenUnhealthy()
    {
        var report = Evaluate(Consumer(), Consumer() with { ConsumerId = "consumer-2" });

        report.Status.Should().Be(ConsumerHealthStatus.Unhealthy);
        report.AssignedPartitions.Should().Be(0);
        report.Issues.Should().ContainSingle().Which.Should().Contain("no partitions assigned");
    }

    [Fact]
    public void GivenIdleConsumerNextToAssignedOne_WhenEvaluate_ThenHealthy()
    {
        var idle = Consumer() with { ConsumerId = "consumer-2", LastPollTime = null, LastCommitTime = null };

        var report = Evaluate(Consumer(Assignment(0, 10, 10)), idle);

        report.Status.Should().Be(ConsumerHealthStatus.Healthy);
        report.AssignedPartitions.Should().Be(1);
    }

    [Fact]
    public void GivenAssignedConsumerWithStalePoll_WhenEvaluate_ThenUnhealthy()
    {
        var report = Evaluate(Consumer(Assignment(0, 10, 10)) with { LastPollTime = Now.AddMinutes(-3) });

        report.Status.Should().Be(ConsumerHealthStatus.Unhealthy);
        report.Issues.Should().ContainSingle().Which.Should().Contain("has not polled");
    }

    [Fact]
    public void GivenStaleCommitAndPositiveLag_WhenEvaluate_ThenUnhealthy()
    {
        var report = Evaluate(Consumer(Assignment(0, 10, 25), Assignment(1, 5, 10)) with
        {
            LastCommitTime = Now.AddMinutes(-10)
        });

        report.Status.Should().Be(ConsumerHealthStatus.Unhealthy);
        report.Lag.Should().Be(20);
        report.Issues.Should().ContainSingle().Which.Should().Contain("no commit").And.Contain("lag 20");
    }

    [Fact]
    public void GivenStaleCommitAndZeroLag_WhenEvaluate_ThenHealthyBecauseTopicIsIdle()
    {
        var report = Evaluate(Consumer(Assignment(0, 10, 10)) with { LastCommitTime = Now.AddMinutes(-10) });

        report.Status.Should().Be(ConsumerHealthStatus.Healthy);
    }

    [Fact]
    public void GivenStaleCommitAndUnknownLag_WhenEvaluate_ThenDegraded()
    {
        var report = Evaluate(Consumer(Assignment(0, 10, null)) with { LastCommitTime = null });

        report.Status.Should().Be(ConsumerHealthStatus.Degraded);
        report.Lag.Should().BeNull();
        report.Issues.Should().ContainSingle().Which.Should().Contain("no commit").And.Contain("lag unknown");
    }

    [Fact]
    public void GivenUnfetchedPartitionOffset_WhenEvaluate_ThenLagSkipsIt()
    {
        var report = Evaluate(Consumer(Assignment(0, -1001, 40), Assignment(1, 7, 9)));

        report.Lag.Should().Be(2);
    }

    [Fact]
    public void GivenRecentConsumerException_WhenEvaluate_ThenDegradedWithExceptionText()
    {
        var report = Evaluate(Consumer(Assignment(0, 1, 1)) with
        {
            LastExceptionTime = Now.AddMinutes(-1),
            LastException = "Local: Broker transport failure"
        });

        report.Status.Should().Be(ConsumerHealthStatus.Degraded);
        report.Issues.Should().ContainSingle().Which.Should().Contain("Local: Broker transport failure");
    }

    [Fact]
    public void GivenOldConsumerException_WhenEvaluate_ThenHealthy()
    {
        var report = Evaluate(Consumer(Assignment(0, 1, 1)) with
        {
            LastExceptionTime = Now.AddHours(-1),
            LastException = "Local: Broker transport failure"
        });

        report.Status.Should().Be(ConsumerHealthStatus.Healthy);
    }

    [Fact]
    public void GivenRecentParseErrors_WhenEvaluate_ThenDegradedWithErrorCount()
    {
        var report = Evaluate(recentErrors: 3, totalErrors: 7, Consumer(Assignment(0, 1, 1)));

        report.Status.Should().Be(ConsumerHealthStatus.Degraded);
        report.RecentErrors.Should().Be(3);
        report.TotalErrors.Should().Be(7);
        report.Issues.Should().ContainSingle().Which.Should().Contain("3 parse errors");
    }

    [Fact]
    public void GivenOnlyOldParseErrors_WhenEvaluate_ThenHealthy()
    {
        var report = Evaluate(recentErrors: 0, totalErrors: 7, Consumer(Assignment(0, 1, 1)));

        report.Status.Should().Be(ConsumerHealthStatus.Healthy);
        report.TotalErrors.Should().Be(7);
    }

    [Fact]
    public void GivenParseErrorsAndNoAssignments_WhenEvaluate_ThenWorstStatusWinsAndAllIssuesReported()
    {
        var report = Evaluate(recentErrors: 1, totalErrors: 1, Consumer());

        report.Status.Should().Be(ConsumerHealthStatus.Unhealthy);
        report.Issues.Should().HaveCount(2);
    }

    private static ConsumerHealthReport Evaluate(params KafkaConsumerState[] consumers) =>
        Evaluate(0, 0, consumers);

    private static ConsumerHealthReport Evaluate(
        ulong recentErrors,
        ulong totalErrors,
        params KafkaConsumerState[] consumers) =>
        ConsumerHealthEvaluator.Evaluate(new ConsumerHealthSnapshot(Now, consumers, recentErrors, totalErrors), Thresholds);

    private static KafkaConsumerState Consumer(params KafkaPartitionAssignment[] assignments) =>
        new(
            Host: "clickhouse",
            ConsumerId: "consumer-1",
            Assignments: assignments,
            LastExceptionTime: null,
            LastException: null,
            LastPollTime: Now.AddSeconds(-1),
            LastCommitTime: Now.AddSeconds(-10),
            NumCommits: 5,
            IsCurrentlyUsed: true);

    private static KafkaPartitionAssignment Assignment(int partition, long currentOffset, long? endOffset) =>
        new("orders", partition, currentOffset, endOffset);
}
