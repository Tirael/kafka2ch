using System.Collections.Concurrent;
using System.Globalization;
using Confluent.Kafka;
using Sandbox.Observability;

namespace ClickHouseSchemaGen.IntegrationTests;

public sealed class ConsumerHealthE2ETests : IAsyncLifetime
{
    private static readonly TimeSpan ConsumeTimeout = TimeSpan.FromMinutes(3);
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(500);

    private static readonly ConsumerHealthQuery Query =
        new("orders_queue", "orders_queue_errors", Cluster: null, ErrorWindow: TimeSpan.FromMinutes(5));

    private static readonly ConsumerHealthThresholds Thresholds = new(
        MaxPollAge: TimeSpan.FromMinutes(1),
        MaxCommitAge: TimeSpan.FromMinutes(5),
        ErrorWindow: TimeSpan.FromMinutes(5));

    private readonly KafkaPipelineFixture _pipeline = new();

    public Task InitializeAsync() => _pipeline.InitializeAsync();

    public Task DisposeAsync() => _pipeline.DisposeAsync();

    [Fact]
    public async Task GivenConsumedOrdersThenBrokenPayload_WhenReadConsumerHealth_ThenAssignmentsAndParseErrorsAreReported()
    {
        const int count = 100;
        byte[] brokenPayload = [0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x0A, 0x64, 0x62, 0x72, 0x6F, 0x6B, 0x65, 0x6E];

        using (var producer = _pipeline.CreateProducer())
        {
            await KafkaPipelineFixture.PublishAsync(
                producer,
                Enumerable.Range(0, count).Select(E2eOrders.Create).ToList(),
                new ConcurrentQueue<string>());
            producer.Flush(TimeSpan.FromSeconds(30));
        }

        (await _pipeline.WaitForCountAsync("SELECT count() FROM orders", count, ConsumeTimeout)).Should().Be(count);

        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _pipeline.KafkaBootstrapAddress })
            .Build();

        var consuming = await WaitForSnapshotAsync(admin, snapshot =>
            snapshot.Consumers.SelectMany(consumer => consumer.Assignments).Count() == KafkaPipelineFixture.TopicPartitions);
        var assignments = consuming.Consumers.SelectMany(consumer => consumer.Assignments).ToList();
        assignments.Select(assignment => (assignment.Topic, assignment.Partition))
            .Should().BeEquivalentTo(Enumerable.Range(0, KafkaPipelineFixture.TopicPartitions)
                .Select(partition => (KafkaPipelineFixture.Topic, partition)));
        assignments.Should().OnlyContain(assignment => assignment.EndOffset != null);
        consuming.Consumers.Should().OnlyContain(consumer => consumer.NumCommits > 0 && consumer.LastCommitTime != null);

        var consumingReport = ConsumerHealthEvaluator.Evaluate(consuming, Thresholds);
        consumingReport.Status.Should().Be(ConsumerHealthStatus.Healthy, string.Join("; ", consumingReport.Issues));
        consumingReport.AssignedPartitions.Should().Be(KafkaPipelineFixture.TopicPartitions);
        consumingReport.Lag.Should().NotBeNull();
        consumingReport.RecentErrors.Should().Be(0UL);

        await _pipeline.PublishRawAsync(brokenPayload, partition: 0);

        var broken = await WaitForSnapshotAsync(admin, snapshot => snapshot.RecentErrors > 0);
        var brokenReport = ConsumerHealthEvaluator.Evaluate(broken, Thresholds);
        brokenReport.Status.Should().NotBe(ConsumerHealthStatus.Healthy);
        brokenReport.RecentErrors.Should().Be(1UL);
        brokenReport.TotalErrors.Should().Be(1UL);
        brokenReport.Issues.Should().Contain(issue => issue.Contains("1 parse errors"));
        brokenReport.AssignedPartitions.Should().Be(KafkaPipelineFixture.TopicPartitions);

        var metrics = await ScrapeMetricsAsync();
        MetricValue(metrics, "ClickHouseMetrics_KafkaAssignedPartitions").Should().BeGreaterThan(0);
        MetricValue(metrics, "ClickHouseProfileEvents_KafkaCommits").Should().BeGreaterThan(0);
        MetricValue(metrics, "ClickHouseProfileEvents_KafkaMessagesPolled").Should().BeGreaterThan(0);
        MetricValue(metrics, "ClickHouseProfileEvents_KafkaMessagesFailed").Should().BeGreaterThan(0);
    }

    private async Task<ConsumerHealthSnapshot> WaitForSnapshotAsync(
        IAdminClient admin,
        Func<ConsumerHealthSnapshot, bool> condition)
    {
        var deadline = DateTimeOffset.UtcNow + ConsumeTimeout;
        while (true)
        {
            await using var connection = new ClickHouseConnection(_pipeline.ClickHouseConnectionString);
            await connection.OpenAsync();
            var snapshot = await (await ConsumerHealthReader.ReadAsync(connection, Query, CancellationToken.None))
                .WithEndOffsetsAsync(admin, TimeSpan.FromSeconds(10));
            if (condition(snapshot) || DateTimeOffset.UtcNow >= deadline)
                return snapshot;

            await Task.Delay(PollInterval);
        }
    }

    private async Task<string> ScrapeMetricsAsync()
    {
        using var http = new HttpClient();
        var deadline = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(30);
        while (true)
        {
            using var response = await http.GetAsync(_pipeline.PrometheusMetricsUri);
            var body = await response.Content.ReadAsStringAsync();
            if (response.IsSuccessStatusCode)
                return body;

            if (DateTimeOffset.UtcNow >= deadline)
            {
                response.IsSuccessStatusCode.Should().BeTrue(
                    $"GET {_pipeline.PrometheusMetricsUri} returned {(int)response.StatusCode}: {body}\n" +
                    await _pipeline.ReadServerErrorLogAsync());
            }

            await Task.Delay(PollInterval);
        }
    }

    private static double MetricValue(string metrics, string name)
    {
        var line = metrics.Split('\n').SingleOrDefault(line => line.StartsWith(name + " ", StringComparison.Ordinal));
        line.Should().NotBeNull($"{name} must be exported by the Prometheus endpoint");
        return double.Parse(line![(name.Length + 1)..].Trim(), CultureInfo.InvariantCulture);
    }
}
