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
    public void GivenMergeTreeTableWithBlankTtl_WhenValidate_ThenFails()
    {
        var config = CreateValidConfig();
        config.Pipeline!.MergeTreeTables[0].Ttl = "   ";

        var result = _sut.Validate(config);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(error =>
            error.ErrorMessage == "TTL expression must not be blank when provided.");
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
