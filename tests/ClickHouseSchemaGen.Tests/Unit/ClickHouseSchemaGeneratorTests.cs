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
            var viewCount = pipelineSql.Split("CREATE MATERIALIZED VIEW").Length - 1;
            viewCount.Should().Be(1);
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

    [Fact]
    public void GivenScreenshotMirrorConfig_WhenGenerateFromConfigFile_ThenAppliesOverridesTtlAndFlattenNested()
    {
        var outputDirectory = Path.Combine(Path.GetTempPath(), $"clickhouse-schema-gen-{Guid.NewGuid():N}");
        var configPath = Path.Combine(outputDirectory, "clickhouse.codegen.json");
        Directory.CreateDirectory(outputDirectory);

        var config = new CodegenConfig
        {
            Defaults = OrdersQueueTestConfig.Defaults,
            KafkaTables =
            [
                CreateKafkaTable(
                    "Sandbox.Contracts.OrderEvent, Sandbox.Contracts",
                    "orders_queue",
                    "order_event",
                    "OrderEvent",
                    "orders",
                    new Dictionary<string, FieldOverrideConfig>(StringComparer.OrdinalIgnoreCase)
                    {
                        ["category"] = new() { Type = "LowCardinality(String)" },
                        ["status"] = new() { Enum8 = true },
                        ["tags"] = new() { Type = "Array(LowCardinality(String))" },
                        ["status_history"] = new() { Enum8 = true }
                    }),
                CreateKafkaTable(
                    "Sandbox.Contracts.ShipmentEvent, Sandbox.Contracts",
                    "shipments_queue",
                    "shipment_event",
                    "ShipmentEvent",
                    "shipments",
                    new Dictionary<string, FieldOverrideConfig>(StringComparer.OrdinalIgnoreCase)
                    {
                        ["status"] = new() { Enum8 = true },
                        ["status_history"] = new() { Enum8 = true },
                        ["destination.country"] = new() { Type = "LowCardinality(String)" },
                        ["destination.city"] = new() { Type = "LowCardinality(String)" }
                    })
            ],
            Pipeline = new PipelineConfig
            {
                OutputPath = "generated_pipeline.sql",
                MergeTreeTables =
                [
                    new MergeTreeTableConfig
                    {
                        TableName = "orders",
                        SourceTable = "orders_queue",
                        OrderBy = "order_id",
                        Ttl = "toDateTime(`event_time.seconds`) + INTERVAL 1 DAY"
                    },
                    new MergeTreeTableConfig
                    {
                        TableName = "shipments",
                        SourceTable = "shipments_queue",
                        OrderBy = "shipment_id",
                        Ttl = "toDateTime(`shipped_at.seconds`) + INTERVAL 1 DAY"
                    }
                ]
            }
        };

        File.WriteAllText(configPath, JsonSerializer.Serialize(config, new JsonSerializerOptions { WriteIndented = true }));

        try
        {
            _sut.GenerateFromConfigFile(configPath);

            var shipmentsQueueSql = File.ReadAllText(Path.Combine(outputDirectory, "generated_shipments_queue.sql"));
            var pipelineSql = File.ReadAllText(Path.Combine(outputDirectory, "generated_pipeline.sql"));

            shipmentsQueueSql.Should().Contain("`destination.country` LowCardinality(String)");
            shipmentsQueueSql.Should().Contain("`destination.city`   LowCardinality(String)");
            pipelineSql.Should().Contain("TTL toDateTime(`event_time.seconds`) + INTERVAL 1 DAY");
            pipelineSql.Should().Contain("TTL toDateTime(`shipped_at.seconds`) + INTERVAL 1 DAY");
            pipelineSql.Should().Contain("SETTINGS flatten_nested = 0;");
            pipelineSql.Should().Contain("CREATE MATERIALIZED VIEW orders_mv TO orders AS");
            pipelineSql.Should().Contain("CREATE MATERIALIZED VIEW shipments_mv TO shipments AS");
            pipelineSql.Should().Contain("`destination.country`");
        }
        finally
        {
            if (Directory.Exists(outputDirectory))
                Directory.Delete(outputDirectory, recursive: true);
        }
    }

    [Fact]
    public void GivenAutoMirrorWithUnknownTtlColumn_WhenGenerateFromConfigFile_ThenThrows()
    {
        var outputDirectory = Path.Combine(Path.GetTempPath(), $"clickhouse-schema-gen-{Guid.NewGuid():N}");
        var configPath = Path.Combine(outputDirectory, "clickhouse.codegen.json");
        Directory.CreateDirectory(outputDirectory);

        var config = new CodegenConfig
        {
            Defaults = OrdersQueueTestConfig.Defaults,
            KafkaTables =
            [
                CreateKafkaTable(
                    "Sandbox.Contracts.OrderEvent, Sandbox.Contracts",
                    "orders_queue",
                    "order_event",
                    "OrderEvent",
                    "orders",
                    new Dictionary<string, FieldOverrideConfig>())
            ],
            Pipeline = new PipelineConfig
            {
                OutputPath = "generated_pipeline.sql",
                MergeTreeTables =
                [
                    new MergeTreeTableConfig
                    {
                        TableName = "orders",
                        SourceTable = "orders_queue",
                        OrderBy = "order_id",
                        Ttl = "toDateTime(`shipped_at.seconds`) + INTERVAL 1 DAY"
                    }
                ]
            }
        };

        File.WriteAllText(configPath, JsonSerializer.Serialize(config, new JsonSerializerOptions { WriteIndented = true }));

        try
        {
            var act = () => _sut.GenerateFromConfigFile(configPath);

            act.Should().Throw<InvalidOperationException>()
                .WithMessage("*shipped_at.seconds*orders*");
        }
        finally
        {
            if (Directory.Exists(outputDirectory))
                Directory.Delete(outputDirectory, recursive: true);
        }
    }

    [Fact]
    public void GivenAutoColumnsWithoutMaterializedView_WhenGenerateFromConfigFile_ThenWritesMirrorView()
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
            var viewCount = pipelineSql.Split("CREATE MATERIALIZED VIEW").Length - 1;
            viewCount.Should().Be(1);
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

    private static KafkaTableConfig CreateKafkaTable(
        string messageType,
        string tableName,
        string protoFile,
        string messageName,
        string topic,
        Dictionary<string, FieldOverrideConfig> fieldOverrides) => new()
    {
        MessageType = messageType,
        TableName = tableName,
        ProtoFile = protoFile,
        MessageName = messageName,
        OutputPath = $"generated_{tableName}.sql",
        Kafka = new KafkaSettingsConfig
        {
            Topic = topic,
            GroupName = $"clickhouse-{topic}"
        },
        FieldOverrides = fieldOverrides
    };
}
