namespace ClickHouseSchemaGen.UnitTests;

public sealed class ClickHouseSchemaGeneratorTests
{
    private readonly ClickHouseSchemaGenerator _sut = SchemaGeneratorFactory.Create();

    [Fact]
    public void GivenOrdersQueueConfig_WhenGenerateKafkaTableSql_ThenMatchesCommittedInitSqlShape()
    {

        var config = OrdersQueueTestConfig.Create();


        var sql = _sut.GenerateKafkaTableSql(config, OrdersQueueTestConfig.Defaults);


        sql.Should().Contain("CREATE TABLE orders_queue");
        sql.Should().Contain("ENGINE = Kafka");
        sql.Should().Contain("kafka_num_consumers = 1");
        sql.TrimEnd().Should().EndWith(";");
    }

    [Fact]
    public void GivenCodegenConfigFile_WhenGenerateFromConfigFile_ThenWritesOrdersQueueAndPipelineSql()
    {

        var outputDirectory = Path.Combine(Path.GetTempPath(), $"clickhouse-schema-gen-{Guid.NewGuid():N}");
        var configPath = Path.Combine(outputDirectory, "clickhouse.codegen.json");
        Directory.CreateDirectory(outputDirectory);
        File.Copy(RepoPaths.CodegenConfigPath, configPath);

        var configJson = File.ReadAllText(configPath)
            .Replace("../../docker/clickhouse/init/01_orders_queue.sql", "generated_orders_queue.sql")
            .Replace("../../docker/clickhouse/init/02_pipeline.sql", "generated_pipeline.sql");
        File.WriteAllText(configPath, configJson);

        var ordersQueuePath = Path.Combine(outputDirectory, "generated_orders_queue.sql");
        var pipelinePath = Path.Combine(outputDirectory, "generated_pipeline.sql");

        try
        {

            _sut.GenerateFromConfigFile(configPath);


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
                        ["status_history"] = new() { Enum8 = true },
                        ["price.currency"] = new() { Type = "LowCardinality(String)" }
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
                    }
                ]
            }
        };

        File.WriteAllText(configPath, JsonSerializer.Serialize(config, new JsonSerializerOptions { WriteIndented = true }));

        try
        {
            _sut.GenerateFromConfigFile(configPath);

            var ordersQueueSql = File.ReadAllText(Path.Combine(outputDirectory, "generated_orders_queue.sql"));
            var pipelineSql = File.ReadAllText(Path.Combine(outputDirectory, "generated_pipeline.sql"));

            ordersQueueSql.Should().Contain("category LowCardinality(String)");
            ordersQueueSql.Should().Contain("`price.currency` LowCardinality(String)");
            pipelineSql.Should().Contain("TTL toDateTime(`event_time.seconds`) + INTERVAL 1 DAY");
            pipelineSql.Should().Contain("SETTINGS flatten_nested = 0;");
            pipelineSql.Should().Contain("CREATE MATERIALIZED VIEW orders_mv TO orders AS");
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

    [Fact]
    public void GivenProtobufKey_WhenGenerateKafkaTableSql_ThenQueueDdlHasNoKeyColumns()
    {

        var config = OrdersQueueTestConfig.Create();
        config.Key = new KafkaKeyConfig
        {
            Format = KafkaKeyFormats.Protobuf,
            MessageType = "Sandbox.Contracts.OrderKey, Sandbox.Contracts"
        };


        var sql = _sut.GenerateKafkaTableSql(config, OrdersQueueTestConfig.Defaults);


        sql.Should().NotContain("_key");
        sql.Should().NotContain("CREATE OR REPLACE FUNCTION");
    }

    [Fact]
    public void GivenMaterializedViewMappingKeyColumns_WhenGenerateFromConfigFile_ThenInlinesDecodeExpressions()
    {
        var outputDirectory = Path.Combine(Path.GetTempPath(), $"clickhouse-schema-gen-{Guid.NewGuid():N}");
        var configPath = Path.Combine(outputDirectory, "clickhouse.codegen.json");
        Directory.CreateDirectory(outputDirectory);

        var config = CreateProtobufKeyConfig(
            new MergeTreeTableConfig
            {
                TableName = "orders",
                OrderBy = "(tenant, order_id)",
                Columns =
                [
                    new PipelineColumnConfig { Name = "order_id", Type = "String" },
                    new PipelineColumnConfig { Name = "tenant", Type = "LowCardinality(String)" },
                    new PipelineColumnConfig { Name = "scope_level", Type = "Int32" }
                ]
            },
            new MaterializedViewConfig
            {
                Name = "orders_mv",
                SourceTable = "orders_queue",
                TargetTable = "orders",
                Columns =
                [
                    new PipelineColumnMapping { Source = "order_id", Target = "order_id" },
                    new PipelineColumnMapping { Source = "_key.tenant", Target = "tenant" },
                    new PipelineColumnMapping
                    {
                        Source = "_key.scope.level",
                        Target = "scope_level",
                        Expression = "`_key.scope.level` * 10"
                    }
                ]
            });

        File.WriteAllText(configPath, JsonSerializer.Serialize(config, new JsonSerializerOptions { WriteIndented = true }));

        try
        {
            _sut.GenerateFromConfigFile(configPath, checkSnapshot: false);

            var queueSql = File.ReadAllText(Path.Combine(outputDirectory, "generated_orders_queue.sql"));
            var pipelineSql = File.ReadAllText(Path.Combine(outputDirectory, "generated_pipeline.sql"));

            queueSql.Should().NotContain("CREATE OR REPLACE FUNCTION");
            pipelineSql.Split("CREATE OR REPLACE FUNCTION protobufWireField AS").Should().HaveCount(2);
            pipelineSql.IndexOf("CREATE OR REPLACE FUNCTION protobufWireBits AS", StringComparison.Ordinal)
                .Should().BeLessThan(pipelineSql.IndexOf("CREATE MATERIALIZED VIEW orders_mv", StringComparison.Ordinal));
            pipelineSql.Should().Contain(
                "CAST(protobufWireBytes(substring(_key, 7), 1) AS LowCardinality(String)) AS tenant,");
            pipelineSql.Should().Contain(
                "(CAST(reinterpretAsInt32(toUInt32(protobufWireBits(protobufWireBytes(substring(_key, 7), 18), 2))) AS Int32)) * 10 AS scope_level");
            pipelineSql.Should().NotContain("`_key.");
        }
        finally
        {
            if (Directory.Exists(outputDirectory))
                Directory.Delete(outputDirectory, recursive: true);
        }
    }

    [Fact]
    public void GivenPersistedKafkaKeyWithProtobufKey_WhenBuildPlan_ThenAddsTypedKafkaKeyColumns()
    {

        var config = CreateProtobufKeyConfig(new MergeTreeTableConfig
        {
            TableName = "orders_raw",
            SourceTable = "orders_queue",
            OrderBy = "(`kafka_key.tenant`, order_id)"
        });
        config.Defaults.PersistKafkaMeta = new PersistKafkaMetaConfig { Key = true };


        var plan = _sut.BuildPlan(config);


        var table = plan.MergeTreeTables.Single().Config;
        table.Columns.Should().ContainSingle(column => column.Name == "kafka_key" && column.Type == "String");
        table.Columns.Should().ContainSingle(column =>
            column.Name == "kafka_key.tenant"
            && column.Type == "LowCardinality(String)"
            && column.FieldNumberPath == "kafka:_key:1");
        table.Columns.Should().ContainSingle(column =>
            column.Name == "kafka_key.scope.level" && column.FieldNumberPath == "kafka:_key:18.2");
        table.Columns.Should().NotContain(column => column.Name.StartsWith("_key", StringComparison.Ordinal));

        var view = plan.MaterializedViews.Single().Config;
        view.Columns.Should().ContainSingle(mapping => mapping.Source == "_key" && mapping.Target == "kafka_key");
        view.Columns.Single(mapping => mapping.Target == "kafka_key.tenant").Expression
            .Should().Be("CAST(protobufWireBytes(substring(_key, 7), 1) AS LowCardinality(String))");
        view.Columns.Select(mapping => mapping.Target).Should().OnlyHaveUniqueItems();
    }

    [Fact]
    public void GivenProtobufKeyWithoutPersistedKafkaKey_WhenBuildPlan_ThenMirrorHasNoKeyColumns()
    {

        var config = CreateProtobufKeyConfig(new MergeTreeTableConfig
        {
            TableName = "orders_raw",
            SourceTable = "orders_queue",
            OrderBy = "order_id"
        });


        var plan = _sut.BuildPlan(config);


        plan.MergeTreeTables.Single().Config.Columns
            .Should().NotContain(column => column.Name.Contains("key", StringComparison.Ordinal));
        ProtobufWireSqlFunctions.IsUsedBy(plan.MaterializedViews.Single().Config).Should().BeFalse();
    }

    [Fact]
    public void GivenStringKeyAndKeyColumnMapping_WhenBuildPlan_ThenThrows()
    {

        var config = CreateProtobufKeyConfig(
            new MergeTreeTableConfig
            {
                TableName = "orders",
                OrderBy = "order_id",
                Columns = [new PipelineColumnConfig { Name = "order_id", Type = "String" }]
            },
            new MaterializedViewConfig
            {
                Name = "orders_mv",
                SourceTable = "orders_queue",
                TargetTable = "orders",
                Columns = [new PipelineColumnMapping { Source = "_key.order_id", Target = "order_id" }]
            });
        config.KafkaTables[0].Key = new KafkaKeyConfig();


        var act = () => _sut.BuildPlan(config);


        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*orders_mv*_key.order_id*orders_queue*protobuf*");
    }

    [Fact]
    public void GivenMaterializedViewUsingKeyFunctions_WhenGenerateStandalone_ThenPrependsFunctionDefinitions()
    {
        var view = new MaterializedViewConfig
        {
            Name = "orders_mv",
            SourceTable = "orders_queue",
            TargetTable = "orders",
            Columns =
            [
                new PipelineColumnMapping
                {
                    Source = "_key.order_id",
                    Target = "order_id",
                    Expression = "CAST(protobufWireBytes(substring(_key, 7), 1) AS String)"
                }
            ]
        };

        var standalone = MaterializedViewGenerator.Generate(view);
        var withoutDefinitions = MaterializedViewGenerator.Generate(view, includeFunctionDefinitions: false);

        standalone.Should().StartWith("CREATE OR REPLACE FUNCTION protobufWireVarint AS");
        withoutDefinitions.Should().StartWith("CREATE MATERIALIZED VIEW orders_mv TO orders AS");
    }

    [Fact]
    public void GivenStringKey_WhenMappingRawKeyColumn_ThenSelectsVirtualKeyColumn()
    {
        var sql = MaterializedViewGenerator.Generate(new MaterializedViewConfig
        {
            Name = "orders_mv",
            SourceTable = "orders_queue",
            TargetTable = "orders",
            Columns = [new PipelineColumnMapping { Source = "_key", Target = "message_key" }]
        });

        sql.Should().NotContain("FUNCTION");
        sql.Should().Contain("_key                         AS message_key");
    }

    private static CodegenConfig CreateProtobufKeyConfig(
        MergeTreeTableConfig mergeTreeTable,
        MaterializedViewConfig? materializedView = null)
    {
        var table = CreateKafkaTable(
            "Sandbox.Contracts.OrderEvent, Sandbox.Contracts",
            "orders_queue",
            "order_event",
            "OrderEvent",
            "orders",
            new Dictionary<string, FieldOverrideConfig>(StringComparer.OrdinalIgnoreCase));
        table.Key = OrdersQueueTestConfig.CreateCompositeKeyTable().Key;

        return new CodegenConfig
        {
            Defaults = OrdersQueueTestConfig.Defaults,
            KafkaTables = [table],
            Pipeline = new PipelineConfig
            {
                OutputPath = "generated_pipeline.sql",
                MergeTreeTables = [mergeTreeTable],
                MaterializedViews = materializedView is null ? [] : [materializedView]
            }
        };
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
