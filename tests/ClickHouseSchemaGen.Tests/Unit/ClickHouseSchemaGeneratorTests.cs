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
            .Replace("../../docker/clickhouse/init/01_orders_queue.sql", "generated_orders_queue.sql")
            .Replace("../../docker/clickhouse/init/02_shipments_queue.sql", "generated_shipments_queue.sql")
            .Replace("../../docker/clickhouse/init/03_pipeline.sql", "generated_pipeline.sql");
        File.WriteAllText(configPath, configJson);

        var ordersQueuePath = Path.Combine(outputDirectory, "generated_orders_queue.sql");
        var pipelinePath = Path.Combine(outputDirectory, "generated_pipeline.sql");

        try
        {
            // Act
            _sut.GenerateFromConfigFile(configPath);

            // Assert
            File.Exists(ordersQueuePath).Should().BeTrue();
            File.Exists(pipelinePath).Should().BeTrue();
            File.ReadAllText(ordersQueuePath).Should().Contain("CREATE TABLE orders_queue");
            File.ReadAllText(pipelinePath).Should().Contain("CREATE MATERIALIZED VIEW orders_mv");
            File.ReadAllText(pipelinePath).Should().Contain("CREATE TABLE orders_agg_1m");
        }
        finally
        {
            if (Directory.Exists(outputDirectory))
                Directory.Delete(outputDirectory, recursive: true);
        }
    }

    [Fact]
    public void GivenEmptyPipelineColumns_WhenGenerateFromConfigFile_ThenExpandsAllQueueColumns()
    {
        var outputDirectory = Path.Combine(Path.GetTempPath(), $"clickhouse-schema-gen-{Guid.NewGuid():N}");
        var configPath = Path.Combine(outputDirectory, "clickhouse.codegen.json");
        Directory.CreateDirectory(outputDirectory);

        var config = new CodegenConfig
        {
            Defaults = OrdersQueueTestConfig.Defaults,
            KafkaTables =
            [
                new KafkaTableConfig
                {
                    MessageType = "Sandbox.Contracts.OrderEvent, Sandbox.Contracts",
                    TableName = "orders_queue",
                    ProtoFile = "order_event",
                    MessageName = "OrderEvent",
                    OutputPath = "generated_queue.sql",
                    Kafka = new KafkaSettingsConfig
                    {
                        Topic = "orders",
                        GroupName = "clickhouse-orders"
                    },
                    FieldOverrides = new Dictionary<string, FieldOverrideConfig>(StringComparer.OrdinalIgnoreCase)
                    {
                        ["status"] = new FieldOverrideConfig { Enum8 = true }
                    }
                }
            ],
            Pipeline = new PipelineConfig
            {
                OutputPath = "generated_pipeline.sql",
                MergeTreeTables =
                [
                    new MergeTreeTableConfig
                    {
                        TableName = "orders_raw",
                        SourceTable = "orders_queue",
                        OrderBy = "(order_id)",
                        Columns = []
                    }
                ],
                MaterializedViews =
                [
                    new MaterializedViewConfig
                    {
                        Name = "orders_raw_mv",
                        SourceTable = "orders_queue",
                        TargetTable = "orders_raw",
                        Columns = []
                    }
                ]
            }
        };

        File.WriteAllText(
            configPath,
            JsonSerializer.Serialize(config, new JsonSerializerOptions { WriteIndented = true }));

        try
        {
            _sut.GenerateFromConfigFile(configPath);

            var pipelineSql = File.ReadAllText(Path.Combine(outputDirectory, "generated_pipeline.sql"));
            pipelineSql.Should().Contain("CREATE TABLE orders_raw");
            pipelineSql.Should().Contain("order_id");
            pipelineSql.Should().Contain("`price.amount`");
            pipelineSql.Should().Contain("CREATE MATERIALIZED VIEW orders_raw_mv TO orders_raw AS");
            pipelineSql.Should().Contain("AS `price.amount`");
            pipelineSql.Should().Contain("FROM orders_queue;");
        }
        finally
        {
            if (Directory.Exists(outputDirectory))
                Directory.Delete(outputDirectory, recursive: true);
        }
    }
}
