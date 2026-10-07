namespace ClickHouseSchemaGen.Tests.Unit;

public sealed class CodegenConfigValidatorTests
{
    private readonly CodegenConfigValidator _sut = new();

    [Fact]
    public void GivenValidConfig_WhenValidate_ThenSucceeds()
    {
        var config = CreateValidConfig();

        var result = _sut.Validate(config);

        result.IsValid.Should().BeTrue();
    }

    [Fact]
    public void GivenEmptyKafkaTables_WhenValidate_ThenFails()
    {
        var config = CreateValidConfig();
        config.KafkaTables = [];

        var result = _sut.Validate(config);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(error => error.PropertyName == "KafkaTables");
    }

    [Fact]
    public void GivenDuplicateKafkaTableNames_WhenValidate_ThenFails()
    {
        var config = CreateValidConfig();
        config.KafkaTables.Add(CreateKafkaTable("orders_queue", "orders", "generated/duplicate.sql"));

        var result = _sut.Validate(config);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(error =>
            error.ErrorMessage == "Kafka table names must be unique.");
    }

    [Fact]
    public void GivenInvalidRepeatedMessageStrategy_WhenValidate_ThenFails()
    {
        var config = CreateValidConfig();
        config.Defaults.RepeatedMessageStrategy = "invalid";

        var result = _sut.Validate(config);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(error =>
            error.PropertyName == "Defaults.RepeatedMessageStrategy");
    }

    [Fact]
    public void GivenMaterializedViewWithUnknownSourceTable_WhenValidate_ThenFails()
    {
        var config = CreateValidConfig();
        config.Pipeline!.MaterializedViews[0].SourceTable = "missing_queue";

        var result = _sut.Validate(config);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(error =>
            error.ErrorMessage.Contains("unknown Kafka source table 'missing_queue'", StringComparison.Ordinal));
    }

    [Fact]
    public void GivenInvalidFieldOverrideStrategy_WhenValidate_ThenFails()
    {
        var config = CreateValidConfig();
        config.KafkaTables[0].FieldOverrides["status"] = new FieldOverrideConfig
        {
            Strategy = "not-a-strategy"
        };

        var result = _sut.Validate(config);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(error =>
            error.ErrorMessage == "Must be a valid mapping strategy name.");
    }

    [Fact]
    public void GivenMergeTreeTableWithTtl_WhenValidate_ThenSucceeds()
    {
        var config = CreateValidConfig();
        config.Pipeline!.MergeTreeTables[0].Ttl = "event_time + INTERVAL 90 DAY";

        var result = _sut.Validate(config);

        result.IsValid.Should().BeTrue();
    }

    [Fact]
    public void GivenTtlWithBacktickedDottedColumn_WhenValidate_ThenSucceeds()
    {
        var config = CreateValidConfig();
        config.Pipeline!.MergeTreeTables[0].Columns =
        [
            new PipelineColumnConfig { Name = "order_id", Type = "String" },
            new PipelineColumnConfig { Name = "event_time.seconds", Type = "Int64" },
            new PipelineColumnConfig { Name = "event_time.nanos", Type = "Int32" }
        ];
        config.Pipeline.MergeTreeTables[0].Ttl = "toDateTime(`event_time.seconds`) + INTERVAL 1 DAY";

        var result = _sut.Validate(config);

        result.IsValid.Should().BeTrue();
    }

    [Fact]
    public void GivenTtlWithUnknownDottedColumn_WhenValidate_ThenFails()
    {
        var config = CreateValidConfig();
        config.Pipeline!.MergeTreeTables[0].Columns =
        [
            new PipelineColumnConfig { Name = "order_id", Type = "String" },
            new PipelineColumnConfig { Name = "event_time.seconds", Type = "Int64" }
        ];
        config.Pipeline.MergeTreeTables[0].Ttl = "toDateTime(`shipped_at.seconds`) + INTERVAL 1 DAY";

        var result = _sut.Validate(config);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(error =>
            error.ErrorMessage.Contains("TTL expression references unknown columns", StringComparison.Ordinal));
    }

    [Fact]
    public void GivenMergeTreeTtlWithUnknownColumn_WhenValidate_ThenFails()
    {
        var config = CreateValidConfig();
        config.Pipeline!.MergeTreeTables =
        [
            new MergeTreeTableConfig
            {
                TableName = "shipments",
                OrderBy = "(shipped_at, shipment_id)",
                Ttl = "event_time + INTERVAL 1 DAY",
                Columns =
                [
                    new PipelineColumnConfig { Name = "shipment_id", Type = "String" },
                    new PipelineColumnConfig { Name = "shipped_at", Type = "DateTime64(3)" }
                ]
            }
        ];
        config.Pipeline.MaterializedViews[0].TargetTable = "shipments";
        config.Pipeline.MaterializedViews[0].SourceTable = "orders_queue";

        var result = _sut.Validate(config);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(error =>
            error.ErrorMessage.Contains("TTL expression references unknown columns", StringComparison.Ordinal)
            && error.ErrorMessage.Contains("shipments", StringComparison.Ordinal));
    }

    [Fact]
    public void GivenMergeTreeTableWithBlankTtl_WhenValidate_ThenFails()
    {
        var config = CreateValidConfig();
        config.Pipeline!.MergeTreeTables[0].Ttl = "   ";

        var result = _sut.Validate(config);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(error =>
            error.ErrorMessage == "TTL expression must not be blank when provided.");
    }

    [Fact]
    public void GivenEmptyColumnsWithSourceTable_WhenValidate_ThenSucceeds()
    {
        var config = CreateValidConfig();
        config.Pipeline!.MergeTreeTables[0].Columns = [];
        config.Pipeline.MergeTreeTables[0].SourceTable = "orders_queue";
        config.Pipeline.MaterializedViews[0].Columns = [];

        var result = _sut.Validate(config);

        result.IsValid.Should().BeTrue();
    }

    [Fact]
    public void GivenEmptyMergeTreeColumnsWithoutSourceTable_WhenValidate_ThenFails()
    {
        var config = CreateValidConfig();
        config.Pipeline!.MergeTreeTables[0].Columns = [];
        config.Pipeline.MergeTreeTables[0].SourceTable = null;

        var result = _sut.Validate(config);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(error =>
            error.ErrorMessage.Contains("sourceTable is required when columns is empty", StringComparison.Ordinal));
    }

    [Fact]
    public void GivenAutoColumnsWithoutMaterializedView_WhenValidate_ThenSucceeds()
    {
        var config = CreateValidConfig();
        config.Pipeline!.MergeTreeTables[0].Columns = [];
        config.Pipeline.MergeTreeTables[0].SourceTable = "orders_queue";
        config.Pipeline.MaterializedViews = [];

        var result = _sut.Validate(config);

        result.IsValid.Should().BeTrue();
    }

    [Fact]
    public void GivenAutoViewNameConflict_WhenValidate_ThenFails()
    {
        var config = CreateValidConfig();
        config.Pipeline!.MergeTreeTables[0].Columns = [];
        config.Pipeline.MergeTreeTables[0].SourceTable = "orders_queue";
        config.Pipeline.MaterializedViews[0].Name = "orders_mv";
        config.Pipeline.MaterializedViews[0].TargetTable = "orders_other";
        config.Pipeline.MergeTreeTables.Add(new MergeTreeTableConfig
        {
            TableName = "orders_other",
            OrderBy = "(order_id)",
            Columns = [new PipelineColumnConfig { Name = "order_id", Type = "String" }]
        });

        var result = _sut.Validate(config);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(error =>
            error.ErrorMessage.Contains("auto-generates materialized view 'orders_mv'", StringComparison.Ordinal)
            && error.ErrorMessage.Contains("already used", StringComparison.Ordinal));
    }

    [Fact]
    public void GivenMergeTreeWithUnknownSourceTable_WhenValidate_ThenFails()
    {
        var config = CreateValidConfig();
        config.Pipeline!.MergeTreeTables[0].SourceTable = "missing_queue";

        var result = _sut.Validate(config);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(error =>
            error.ErrorMessage.Contains("unknown Kafka source table 'missing_queue'", StringComparison.Ordinal));
    }

    [Fact]
    public void GivenProtobufKeyWithMessageType_WhenValidate_ThenSucceeds()
    {
        var config = CreateValidConfig();
        config.KafkaTables[0].Key = new KafkaKeyConfig
        {
            Format = "Protobuf",
            MessageType = "Sandbox.Contracts.OrderKey, Sandbox.Contracts",
            SkipBytes = 6,
            FieldOverrides = new Dictionary<string, FieldOverrideConfig>
            {
                ["order_id"] = new() { Type = "LowCardinality(String)" }
            }
        };

        var result = _sut.Validate(config);

        result.IsValid.Should().BeTrue();
    }

    [Fact]
    public void GivenUnknownKeyFormat_WhenValidate_ThenFails()
    {
        var config = CreateValidConfig();
        config.KafkaTables[0].Key.Format = "avro";

        var result = _sut.Validate(config);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(error => error.PropertyName == "KafkaTables[0].Key.Format");
    }

    [Fact]
    public void GivenProtobufKeyWithoutMessageType_WhenValidate_ThenFails()
    {
        var config = CreateValidConfig();
        config.KafkaTables[0].Key.Format = KafkaKeyFormats.Protobuf;

        var result = _sut.Validate(config);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(error =>
            error.ErrorMessage == "messageType is required when key format is protobuf.");
    }

    [Fact]
    public void GivenStringKeyWithProtobufSettings_WhenValidate_ThenFails()
    {
        var config = CreateValidConfig();
        config.KafkaTables[0].Key.MessageType = "Sandbox.Contracts.OrderKey, Sandbox.Contracts";

        var result = _sut.Validate(config);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(error =>
            error.ErrorMessage.Contains("only valid when key format is protobuf", StringComparison.Ordinal));
    }

    [Fact]
    public void GivenInvalidKeyOverridePathOrNegativeSkipBytes_WhenValidate_ThenFails()
    {
        var config = CreateValidConfig();
        config.KafkaTables[0].Key = new KafkaKeyConfig
        {
            Format = KafkaKeyFormats.Protobuf,
            MessageType = "Sandbox.Contracts.OrderKey, Sandbox.Contracts",
            SkipBytes = -1,
            FieldOverrides = new Dictionary<string, FieldOverrideConfig> { ["bad path"] = new() }
        };

        var result = _sut.Validate(config);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(error => error.PropertyName == "KafkaTables[0].Key.SkipBytes");
        result.Errors.Should().Contain(error => error.ErrorMessage == "Field override key must be a valid field path.");
    }

    private static CodegenConfig CreateValidConfig() => new()
    {
        Defaults = OrdersQueueTestConfig.Defaults,
        KafkaTables = [CreateKafkaTable("orders_queue", "orders", "generated/orders_queue.sql")],
        Pipeline = new PipelineConfig
        {
            OutputPath = "generated/pipeline.sql",
            MergeTreeTables =
            [
                new MergeTreeTableConfig
                {
                    TableName = "orders",
                    OrderBy = "(event_time, order_id)",
                    Columns =
                    [
                        new PipelineColumnConfig { Name = "order_id", Type = "String" },
                        new PipelineColumnConfig { Name = "event_time", Type = "DateTime64(3)" }
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

    private static KafkaTableConfig CreateKafkaTable(string tableName, string topic, string outputPath) => new()
    {
        MessageType = "Sandbox.Contracts.OrderEvent, Sandbox.Contracts",
        TableName = tableName,
        ProtoFile = "order_event",
        MessageName = "OrderEvent",
        OutputPath = outputPath,
        Kafka = new KafkaSettingsConfig
        {
            BrokerList = "kafka:9092",
            Topic = topic,
            GroupName = $"clickhouse-{topic}",
            SkipBytes = 6,
            NumConsumers = 1
        }
    };
}
