using System.Collections.Concurrent;

namespace ClickHouseSchemaGen.IntegrationTests;

public sealed class KafkaClickHousePipelineE2ETests : IAsyncLifetime
{
    private static readonly TimeSpan ConsumeTimeout = TimeSpan.FromMinutes(3);

    private readonly KafkaPipelineFixture _pipeline = new();

    public Task InitializeAsync() => _pipeline.InitializeAsync();

    public Task DisposeAsync() => _pipeline.DisposeAsync();

    [Fact]
    public async Task GivenPublishedOrders_WhenConsumedThroughKafkaEngine_ThenOrdersKeysAndAggregatesMatch()
    {
        const int count = 1000;
        var orders = Enumerable.Range(0, count).Select(E2eOrders.Create).ToList();
        var published = new ConcurrentQueue<string>();

        using (var producer = _pipeline.CreateProducer())
        {
            await KafkaPipelineFixture.PublishAsync(producer, orders, published);
            producer.Flush(TimeSpan.FromSeconds(30));
        }

        var consumed = await _pipeline.WaitForCountAsync("SELECT count() FROM orders", count, ConsumeTimeout);

        consumed.Should().Be(count);
        (await _pipeline.QueryStringsAsync("SELECT order_id FROM orders"))
            .Should().BeEquivalentTo(published);
        Convert.ToUInt64(await _pipeline.ScalarAsync("SELECT countIf(`kafka_key.order_id` != order_id) FROM orders"))
            .Should().Be(0UL);
        Convert.ToUInt64(await _pipeline.ScalarAsync("SELECT sum(orders_count) FROM orders_agg_1m"))
            .Should().Be(count);
        Convert.ToUInt64(await _pipeline.ScalarAsync("SELECT sum(total_qty) FROM orders_agg_1m"))
            .Should().Be((ulong)Enumerable.Range(0, count).Sum(sequence => E2eOrders.QuantityOf(sequence)));
    }

    [Fact]
    public async Task GivenRedeliveredKafkaMessages_WhenStoredTwice_ThenFinalAndAggregatesCountEachMessageOnce()
    {
        const int count = 200;
        var orders = Enumerable.Range(0, count).Select(E2eOrders.Create).ToList();
        var published = new ConcurrentQueue<string>();

        using (var producer = _pipeline.CreateProducer())
        {
            await KafkaPipelineFixture.PublishAsync(producer, orders, published);
            producer.Flush(TimeSpan.FromSeconds(30));
        }

        (await _pipeline.WaitForCountAsync("SELECT count() FROM orders", count, ConsumeTimeout)).Should().Be(count);

        await _pipeline.ExecuteAsync("SYSTEM STOP MERGES orders");
        await _pipeline.ExecuteAsync("INSERT INTO orders SELECT * FROM orders");
        Convert.ToUInt64(await _pipeline.ScalarAsync("SELECT count() FROM orders"))
            .Should().Be(2 * count, "the redelivered copies must be physically stored before deduplication");

        await _pipeline.RefreshAggregatesAsync();

        Convert.ToUInt64(await _pipeline.ScalarAsync("SELECT count() FROM orders FINAL")).Should().Be(count);
        Convert.ToUInt64(await _pipeline.ScalarAsync("SELECT uniqExact(kafka_partition, kafka_offset) FROM orders"))
            .Should().Be(count);
        Convert.ToUInt64(await _pipeline.ScalarAsync("SELECT sum(orders_count) FROM orders_agg_1m")).Should().Be(count);
        Convert.ToUInt64(await _pipeline.ScalarAsync("SELECT sum(total_qty) FROM orders_agg_1m"))
            .Should().Be((ulong)Enumerable.Range(0, count).Sum(sequence => E2eOrders.QuantityOf(sequence)));
    }

    [Fact]
    public async Task GivenQueueRecreateMigration_WhenAppliedDuringPublishing_ThenNoOrdersAreLost()
    {
        var migrationSql = KafkaRecreateMigrationFixture.GenerateSql(_pipeline.ConfigPath);
        var published = new ConcurrentQueue<string>();
        using var stop = new CancellationTokenSource();

        using var producer = _pipeline.CreateProducer();
        var publishing = PublishContinuouslyAsync(producer, published, stop.Token);

        (await _pipeline.WaitForCountAsync("SELECT count() FROM orders", 1, ConsumeTimeout))
            .Should().BeGreaterThan(0UL, "consumption must be live before the migration starts");

        var publishedBeforeMigration = published.Count;
        await _pipeline.ApplyMigrationAsync(KafkaRecreateMigrationFixture.FileName, migrationSql);
        var publishedAfterMigration = published.Count;

        await Task.Delay(TimeSpan.FromSeconds(3));
        await stop.CancelAsync();
        await publishing;
        producer.Flush(TimeSpan.FromSeconds(30));

        publishedAfterMigration.Should().BeGreaterThan(publishedBeforeMigration, "publishing must overlap the migration");
        Convert.ToString(await _pipeline.ScalarAsync("SHOW CREATE TABLE orders_queue"))
            .Should().Contain($"kafka_num_consumers = {KafkaRecreateMigrationFixture.NewNumConsumers}");
        Convert.ToUInt64(await _pipeline.ScalarAsync(
                $"SELECT count() FROM schema_migrations WHERE version = '{KafkaRecreateMigrationFixture.Version}'"))
            .Should().Be(1UL);

        var expected = published.ToHashSet(StringComparer.Ordinal);
        await _pipeline.WaitForCountAsync("SELECT uniqExact(order_id) FROM orders", (ulong)expected.Count, ConsumeTimeout);

        var stored = (await _pipeline.QueryStringsAsync("SELECT DISTINCT order_id FROM orders"))
            .ToHashSet(StringComparer.Ordinal);
        expected.Except(stored).Should().BeEmpty($"all {expected.Count} published orders must reach orders");
        stored.Except(expected).Should().BeEmpty();
    }

    private static async Task PublishContinuouslyAsync(
        Confluent.Kafka.IProducer<OrderKey, OrderEvent> producer,
        ConcurrentQueue<string> published,
        CancellationToken stoppingToken)
    {
        const int batchSize = 20;
        var sequence = 0;
        while (!stoppingToken.IsCancellationRequested)
        {
            var batch = Enumerable.Range(sequence, batchSize).Select(E2eOrders.Create).ToList();
            sequence += batchSize;
            await KafkaPipelineFixture.PublishAsync(producer, batch, published);
            await Task.Delay(TimeSpan.FromMilliseconds(100), CancellationToken.None);
        }
    }
}
