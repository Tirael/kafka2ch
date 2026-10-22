namespace ClickHouseSchemaGen.UnitTests;

public sealed class KafkaNamedCollectionTests
{
    private const string CollectionName = "kafka_orders";

    [Fact]
    public void GivenNamedCollection_WhenGenerateKafkaTable_ThenEngineReferencesCollectionWithoutBrokerList()
    {
        var config = OrdersQueueTestConfig.Create();
        UseNamedCollection(config.Kafka);

        var sql = KafkaTableGenerator.Generate(config, MapColumns(config));

        sql.Should().Contain($"ENGINE = Kafka({CollectionName}){SqlText.NewLine}SETTINGS");
        sql.Should().NotContain("kafka_broker_list");
        sql.Should().NotContain("sasl", "secrets live only in the named collection");
        sql.Should().Contain("kafka_topic_list = 'orders'");
        sql.Should().Contain("kafka_group_name = 'clickhouse-orders'");
        sql.Should().Contain("kafka_format = 'ProtobufSingle'");
        sql.Should().Contain("kafka_schema = 'order_event:OrderEvent'");
    }

    [Fact]
    public void GivenBrokerList_WhenGenerateKafkaTable_ThenEngineHasNoCollectionAndRendersBrokerList()
    {
        var config = OrdersQueueTestConfig.Create();

        var sql = KafkaTableGenerator.Generate(config, MapColumns(config));

        sql.Should().Contain($"ENGINE = Kafka{SqlText.NewLine}SETTINGS");
        sql.Should().Contain("kafka_broker_list = 'kafka:9092'");
    }

    [Fact]
    public void GivenBrokerListAndNamedCollection_WhenValidate_ThenFailsAsMutuallyExclusive()
    {
        var config = LoadFixture();
        config.KafkaTables[0].Kafka.BrokerList = "kafka:9092";
        config.KafkaTables[0].Kafka.NamedCollection = CollectionName;

        var result = new CodegenConfigValidator().Validate(config);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(error =>
            error.ErrorMessage.Contains("brokerList", StringComparison.Ordinal)
            && error.ErrorMessage.Contains("namedCollection", StringComparison.Ordinal)
            && error.ErrorMessage.Contains("mutually exclusive", StringComparison.Ordinal));
    }

    [Fact]
    public void GivenNamedCollectionOnly_WhenValidate_ThenSucceeds()
    {
        var config = LoadFixture();
        UseNamedCollection(config.KafkaTables[0].Kafka);

        new CodegenConfigValidator().Validate(config).IsValid.Should().BeTrue();
    }

    [Theory]
    [InlineData("kafka-orders")]
    [InlineData("1kafka")]
    [InlineData("kafka orders")]
    [InlineData(" ")]
    public void GivenNamedCollectionThatIsNotIdentifier_WhenValidate_ThenFails(string name)
    {
        var config = LoadFixture();
        config.KafkaTables[0].Kafka.BrokerList = null;
        config.KafkaTables[0].Kafka.NamedCollection = name;

        var result = new CodegenConfigValidator().Validate(config);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(error => error.PropertyName.Contains("NamedCollection", StringComparison.Ordinal));
    }

    [Fact]
    public void GivenNamedCollection_WhenSnapshot_ThenStoresCollectionNameOnlyAndRoundTripsWithoutDiffOrDrift()
    {
        var config = LoadFixture();
        UseNamedCollection(config.KafkaTables[0].Kafka);
        var plan = SchemaGeneratorFactory.Create().BuildPlan(config);
        var path = Path.Combine(Path.GetTempPath(), $"snapshot-{Guid.NewGuid():N}.json");

        try
        {
            SchemaSnapshotSerializer.Save(path, SchemaSnapshotMapper.FromPlan(plan));
            var json = File.ReadAllText(path);
            json.Should().Contain($"\"namedCollection\": \"{CollectionName}\"");
            json.Should().NotContain("brokerList");

            var snapshot = SchemaSnapshotSerializer.Load(path, plan);
            snapshot.KafkaTables[0].Kafka.BrokerList.Should().BeNull();
            SchemaDiffer.Diff(SchemaSnapshotMapper.ToPlan(snapshot, config), plan).Changes.Should().BeEmpty();
            var act = () => SnapshotDriftChecker.Check(plan, snapshot, TestContractsPaths.CodegenConfigPath);
            act.Should().NotThrow();
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void GivenSameNamedCollection_WhenBrokersChangeInsideCollection_ThenNoDiffAndIdenticalQueueDdl()
    {
        var generator = SchemaGeneratorFactory.Create();
        var oldConfig = LoadFixture();
        UseNamedCollection(oldConfig.KafkaTables[0].Kafka);
        var oldPlan = generator.BuildPlan(oldConfig);
        var snapshot = SchemaSnapshotMapper.FromPlan(oldPlan);

        var newConfig = LoadFixture();
        UseNamedCollection(newConfig.KafkaTables[0].Kafka);
        var newPlan = generator.BuildPlan(newConfig);

        SchemaDiffer.Diff(SchemaSnapshotMapper.ToPlan(snapshot, newConfig), newPlan).Changes.Should().BeEmpty();
        SchemaSnapshotSerializer.SerializeToString(snapshot).Should().NotContain("kafka:9092");
        SchemaPlanRenderer.RenderInitScripts(newPlan).Values
            .Should().NotContain(script => script.Contains("kafka_broker_list", StringComparison.Ordinal));
    }

    [Fact]
    public void GivenBrokerListSwitchedToNamedCollection_WhenDiff_ThenRecreatesQueueAutomatically()
    {
        var generator = SchemaGeneratorFactory.Create();
        var oldConfig = LoadFixture();
        var snapshot = SchemaSnapshotMapper.FromPlan(generator.BuildPlan(oldConfig));
        var newConfig = LoadFixture();
        UseNamedCollection(newConfig.KafkaTables[0].Kafka);
        var newPlan = generator.BuildPlan(newConfig);
        var oldPlan = SchemaSnapshotMapper.ToPlan(snapshot, newConfig);

        var diff = SchemaDiffer.Diff(oldPlan, newPlan);
        var migrationPlan = MigrationPolicy.Apply(diff, oldPlan, newPlan);
        var sql = MigrationSqlGenerator.Generate(
            migrationPlan,
            oldPlan,
            newPlan,
            "kafka_named_collection",
            parentChecksum: null,
            targetChecksum: "checksum",
            DateTimeOffset.UnixEpoch);

        diff.Changes.OfType<KafkaTableChanged>().Should().ContainSingle(change =>
            change.TableName == "orders_queue"
            && change.Reason.Contains("namedCollection changed", StringComparison.Ordinal));
        migrationPlan.RequiresManualApproval.Should().BeFalse();
        sql.Should().Contain("DROP TABLE IF EXISTS orders_queue;");
        sql.Should().Contain($"ENGINE = Kafka({CollectionName})");
        sql.Should().NotContain("kafka_broker_list");
    }

    [Fact]
    public void GivenNamedCollectionRenamed_WhenDiff_ThenEmitsKafkaTableChanged()
    {
        var generator = SchemaGeneratorFactory.Create();
        var oldConfig = LoadFixture();
        UseNamedCollection(oldConfig.KafkaTables[0].Kafka);
        var newConfig = LoadFixture();
        UseNamedCollection(newConfig.KafkaTables[0].Kafka);
        newConfig.KafkaTables[0].Kafka.NamedCollection = "kafka_orders_v2";

        var diff = SchemaDiffer.Diff(generator.BuildPlan(oldConfig), generator.BuildPlan(newConfig));

        diff.Changes.Should().ContainSingle().Which.Should().Be(
            new KafkaTableChanged("orders_queue", "namedCollection changed"));
    }

    private static void UseNamedCollection(KafkaSettingsConfig kafka)
    {
        kafka.BrokerList = null;
        kafka.NamedCollection = CollectionName;
    }

    private static CodegenConfig LoadFixture() => CodegenConfigLoader.Load(TestContractsPaths.CodegenConfigPath);

    private static IReadOnlyList<ClickHouseColumn> MapColumns(KafkaTableConfig config) =>
        MappingTestSupport.MapFixture(ProtoToClickHouseMapper.ResolveDescriptor(config.MessageType), config.FieldOverrides);
}
