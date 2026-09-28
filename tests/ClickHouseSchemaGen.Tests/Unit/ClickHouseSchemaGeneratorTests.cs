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
            .Replace("../../docker/clickhouse/init/02_shipments_queue.sql", "generated_shipments_queue.sql")
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
            File.ReadAllText(pipelinePath).Should().Contain("sandbox_parse_proto_string(_key, 6, 1)");
            File.ReadAllText(pipelinePath).Should().Contain("key_order_id");
            File.ReadAllText(pipelinePath).Should().Contain("key_shipment_id");
            File.ReadAllText(pipelinePath).Should().Contain("CAST(_headers.name, 'Array(LowCardinality(String))')");
            File.ReadAllText(pipelinePath).Should().Contain("AS headers_name");
            File.ReadAllText(pipelinePath).Should().Contain("_headers.value");
            File.ReadAllText(pipelinePath).Should().Contain("AS headers_value");
            File.ReadAllText(pipelinePath).Should().Contain("Array(LowCardinality(String))");
        }
        finally
        {
            if (Directory.Exists(outputDirectory))
                Directory.Delete(outputDirectory, recursive: true);
        }
    }

    [Fact]
    public void GivenKeyMessageWithSeveralFields_WhenGenerateFromConfigFile_ThenThrows()
    {
        var outputDirectory = Path.Combine(Path.GetTempPath(), $"clickhouse-schema-gen-{Guid.NewGuid():N}");
        var configPath = Path.Combine(outputDirectory, "clickhouse.codegen.json");
        Directory.CreateDirectory(outputDirectory);
        var configJson = File.ReadAllText(RepoPaths.CodegenConfigPath)
            .Replace("../../docker/clickhouse/init/01_orders_queue.sql", "generated_queue.sql")
            .Replace("../../docker/clickhouse/init/02_shipments_queue.sql", "generated_shipments_queue.sql")
            .Replace("../../docker/clickhouse/init/03_pipeline.sql", "generated_pipeline.sql")
            .Replace(
                "Sandbox.Contracts.OrderKey, Sandbox.Contracts",
                "Sandbox.Contracts.OrderEvent, Sandbox.Contracts")
            .Replace("\"messageName\": \"OrderKey\"", "\"messageName\": \"OrderEvent\"");
        File.WriteAllText(configPath, configJson);

        try
        {
            var act = () => _sut.GenerateFromConfigFile(configPath);

            act.Should().Throw<InvalidOperationException>()
                .WithMessage("*exactly one singular string field*");
        }
        finally
        {
            if (Directory.Exists(outputDirectory))
                Directory.Delete(outputDirectory, recursive: true);
        }
    }
}
