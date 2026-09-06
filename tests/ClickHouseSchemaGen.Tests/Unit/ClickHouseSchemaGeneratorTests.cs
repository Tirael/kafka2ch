namespace ClickHouseSchemaGen.Tests.Unit;

public sealed class ClickHouseSchemaGeneratorTests
{
    private readonly ClickHouseSchemaGenerator _sut = SchemaGeneratorFactory.Create();

    [Fact]
    public void GivenOrdersQueueConfig_WhenGenerateKafkaTableSql_ThenMatchesCommittedInitSqlShape()
    {
        // Arrange
        var config = OrdersQueueTestConfig.Create();

        // Act
        var sql = _sut.GenerateKafkaTableSql(config, OrdersQueueTestConfig.Defaults);

        // Assert
        sql.Should().Contain("CREATE TABLE orders_queue");
        sql.Should().Contain("ENGINE = Kafka");
        sql.Should().Contain("kafka_num_consumers = 1");
        sql.TrimEnd().Should().EndWith(";");
    }

    [Fact]
    public void GivenCodegenConfigFile_WhenGenerateFromConfigFile_ThenWritesOrdersQueueAndPipelineSql()
    {
        // Arrange
        var outputDirectory = Path.Combine(Path.GetTempPath(), $"clickhouse-schema-gen-{Guid.NewGuid():N}");
        var configPath = Path.Combine(outputDirectory, "clickhouse.codegen.json");
        Directory.CreateDirectory(outputDirectory);
        File.Copy(RepoPaths.CodegenConfigPath, configPath);

        var configJson = File.ReadAllText(configPath)
            .Replace("../../docker/clickhouse/init/01_orders_queue.sql", "generated_queue.sql")
            .Replace("../../docker/clickhouse/init/03_pipeline.sql", "generated_pipeline.sql");
        File.WriteAllText(configPath, configJson);

        var queuePath = Path.Combine(outputDirectory, "generated_queue.sql");
        var pipelinePath = Path.Combine(outputDirectory, "generated_pipeline.sql");

        try
        {
            // Act
            _sut.GenerateFromConfigFile(configPath);

            // Assert
            File.Exists(queuePath).Should().BeTrue();
            File.Exists(pipelinePath).Should().BeTrue();
            File.ReadAllText(queuePath).Should().Contain("CREATE TABLE orders_queue");
            File.ReadAllText(pipelinePath).Should().Contain("CREATE MATERIALIZED VIEW orders_mv");
            File.ReadAllText(pipelinePath).Should().Contain("CREATE TABLE orders_agg_1m");
        }
        finally
        {
            if (Directory.Exists(outputDirectory))
                Directory.Delete(outputDirectory, recursive: true);
        }
    }
}
