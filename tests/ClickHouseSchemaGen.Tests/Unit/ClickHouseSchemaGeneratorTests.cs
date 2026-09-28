using System.Text.Json;

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
    public void GivenMultiFieldKey_WhenGenerateFromConfigFile_ThenEmitsWireScannerAndTupleProjection()
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
                        GroupName = "clickhouse-orders",
                        SkipBytes = 6
                    },
                    Key = new KeyMessageConfig
                    {
                        MessageType = "Sandbox.Contracts.TestFixtures.MultiFieldKey, Sandbox.Contracts",
                        ProtoFile = "mapping_fixtures",
                        MessageName = "MultiFieldKey",
                        SkipBytes = 6
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
                        TableName = "orders",
                        OrderBy = "(order_id)",
                        Columns =
                        [
                            new PipelineColumnConfig { Name = "order_id", Type = "String" }
                        ]
                    }
                ],
                MaterializedViews =
                [
                    new MaterializedViewConfig
                    {
                        Name = "orders_mv",
                        SourceTable = "orders_queue",
                        TargetTable = "orders",
                        Columns =
                        [
                            new PipelineColumnMapping { Source = "order_id", Target = "order_id" }
                        ]
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
            pipelineSql.Should().Contain("sandbox_proto_fields");
            pipelineSql.Should().Contain("sandbox_parse_key_orders_queue");
            pipelineSql.Should().Contain("key_id");
            pipelineSql.Should().Contain("key_shard");
            pipelineSql.Should().Contain("key_delta");
            pipelineSql.Should().Contain("key_score");
            pipelineSql.Should().Contain("tupleElement(_sandbox_key, 1)");
            pipelineSql.Should().Contain("sandbox_parse_key_orders_queue(_key) AS _sandbox_key");
            pipelineSql.Should().Contain("FROM\n(\n    SELECT\n        *,");
            pipelineSql.Should().NotContain("sandbox_parse_proto_string");
        }
        finally
        {
            if (Directory.Exists(outputDirectory))
                Directory.Delete(outputDirectory, recursive: true);
        }
    }

    [Fact]
    public void GivenKeyMessageWithNestedFields_WhenGenerateFromConfigFile_ThenThrows()
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
                .WithMessage("*nested message*");
        }
        finally
        {
            if (Directory.Exists(outputDirectory))
                Directory.Delete(outputDirectory, recursive: true);
        }
    }
}
