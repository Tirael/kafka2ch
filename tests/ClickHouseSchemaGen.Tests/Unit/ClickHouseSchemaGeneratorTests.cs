using ClickHouseSchemaGen;
using ClickHouseSchemaGen.Tests.Support;

namespace ClickHouseSchemaGen.Tests.Unit;

public sealed class ClickHouseSchemaGeneratorTests
{
    private readonly ClickHouseSchemaGenerator _sut =
        new(new ProtoToClickHouseMapper(), new KafkaTableGenerator());

    [Fact]
    public void GivenOrdersQueueConfig_WhenGenerateKafkaTableSql_ThenMatchesCommittedInitSqlShape()
    {
        // Arrange
        var config = OrdersQueueTestConfig.Create();

        // Act
        var sql = _sut.GenerateKafkaTableSql(config);

        // Assert
        sql.Should().Contain("CREATE TABLE orders_queue");
        sql.Should().Contain("tags                 Array(LowCardinality(String))");
        sql.Should().Contain("ENGINE = Kafka");
        sql.Should().EndWith("kafka_num_consumers = 1;\n");
    }

    [Fact]
    public void GivenCodegenConfigFile_WhenGenerateFromConfigFile_ThenWritesOrdersQueueSql()
    {
        // Arrange
        var outputDirectory = Path.Combine(Path.GetTempPath(), $"clickhouse-schema-gen-{Guid.NewGuid():N}");
        var configPath = Path.Combine(outputDirectory, "clickhouse.codegen.json");
        Directory.CreateDirectory(outputDirectory);
        File.Copy(RepoPaths.CodegenConfigPath, configPath);

        var configJson = File.ReadAllText(configPath)
            .Replace("../../docker/clickhouse/init/01_orders_queue.sql", "generated.sql");
        File.WriteAllText(configPath, configJson);

        var outputPath = Path.Combine(outputDirectory, "generated.sql");

        try
        {
            // Act
            _sut.GenerateFromConfigFile(configPath);

            // Assert
            File.Exists(outputPath).Should().BeTrue();
            File.ReadAllText(outputPath).Should().Contain("CREATE TABLE orders_queue");
        }
        finally
        {
            if (Directory.Exists(outputDirectory))
                Directory.Delete(outputDirectory, recursive: true);
        }
    }
}
