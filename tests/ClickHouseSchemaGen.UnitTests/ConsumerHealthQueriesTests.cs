using Sandbox.Observability;

namespace ClickHouseSchemaGen.UnitTests;

public sealed class ConsumerHealthQueriesTests
{
    [Fact]
    public void GivenSingleNode_WhenKafkaConsumers_ThenReadsLocalSystemTableForQueue()
    {
        var sql = ConsumerHealthQueries.KafkaConsumers("orders_queue", cluster: null);

        sql.Should().Contain("FROM system.kafka_consumers");
        sql.Should().Contain("table = 'orders_queue'");
        sql.Should().Contain("database = currentDatabase()");
        sql.Should().Contain("assignments.partition_id");
        sql.Should().Contain("assignments.current_offset");
        sql.Should().Contain("last_poll_time");
        sql.Should().Contain("last_commit_time");
        sql.Should().Contain("num_commits");
        sql.Should().Contain("exceptions.text");
        sql.Should().NotContain("clusterAllReplicas");
    }

    [Fact]
    public void GivenCluster_WhenKafkaConsumers_ThenReadsAllReplicas()
    {
        var sql = ConsumerHealthQueries.KafkaConsumers("orders_queue", "kafka2ch");

        sql.Should().Contain("FROM clusterAllReplicas('kafka2ch', system.kafka_consumers)");
        sql.Should().Contain("hostName()");
    }

    [Fact]
    public void GivenErrorsTable_WhenErrors_ThenCountsRecentAndTotalRows()
    {
        var sql = ConsumerHealthQueries.Errors("orders_queue_errors", TimeSpan.FromMinutes(5));

        sql.Should().Contain("FROM orders_queue_errors");
        sql.Should().Contain("countIf(received_at >= now() - INTERVAL 300 SECOND)");
        sql.Should().Contain("count()");
    }

    [Theory]
    [InlineData("orders_queue'; DROP TABLE orders; --")]
    [InlineData("")]
    [InlineData("1orders")]
    public void GivenInvalidIdentifier_WhenBuildQueries_ThenThrows(string identifier)
    {
        var consumers = () => ConsumerHealthQueries.KafkaConsumers(identifier, null);
        var cluster = () => ConsumerHealthQueries.KafkaConsumers("orders_queue", identifier);
        var errors = () => ConsumerHealthQueries.Errors(identifier, TimeSpan.FromMinutes(5));

        consumers.Should().Throw<ArgumentException>();
        cluster.Should().Throw<ArgumentException>();
        errors.Should().Throw<ArgumentException>();
    }
}
