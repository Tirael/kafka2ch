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

    [Fact]
    public void GivenSingleNode_WhenPipelineTopology_ThenUsesDependenciesTableAndDetachedTables()
    {
        var sql = ConsumerHealthQueries.PipelineTopology("orders_queue", "orders_mv", cluster: null);

        sql.Should().Contain("system.tables");
        sql.Should().Contain("system.detached_tables");
        sql.Should().Contain("dependencies_table");
        sql.Should().Contain("has(dependencies_table, 'orders_mv')");
        sql.Should().Contain("engine = 'Kafka'");
        sql.Should().Contain("name = 'orders_queue'");
        sql.Should().NotContain("missing_dependencies");
        sql.Should().NotContain("clusterAllReplicas");
    }

    [Fact]
    public void GivenCluster_WhenPipelineTopology_ThenReadsAllReplicas()
    {
        var sql = ConsumerHealthQueries.PipelineTopology("orders_queue", "orders_mv", "kafka2ch");

        sql.Should().Contain("clusterAllReplicas('kafka2ch', system.tables)");
        sql.Should().Contain("clusterAllReplicas('kafka2ch', system.detached_tables)");
        sql.Should().Contain("has(dependencies_table, 'orders_mv')");
        sql.Should().NotContain("missing_dependencies");
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
        var pipeline = () => ConsumerHealthQueries.PipelineTopology(identifier, "orders_mv", null);
        var view = () => ConsumerHealthQueries.PipelineTopology("orders_queue", identifier, null);

        consumers.Should().Throw<ArgumentException>();
        cluster.Should().Throw<ArgumentException>();
        errors.Should().Throw<ArgumentException>();
        pipeline.Should().Throw<ArgumentException>();
        view.Should().Throw<ArgumentException>();
    }
}
