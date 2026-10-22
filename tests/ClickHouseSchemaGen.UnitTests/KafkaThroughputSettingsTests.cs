namespace ClickHouseSchemaGen.UnitTests;

public sealed class KafkaThroughputSettingsTests
{
    public static TheoryData<string, string> SettingChanges => new()
    {
        { nameof(KafkaSettingsConfig.MaxBlockSize), "maxBlockSize changed" },
        { nameof(KafkaSettingsConfig.FlushIntervalMs), "flushIntervalMs changed" },
        { nameof(KafkaSettingsConfig.PollMaxBatchSize), "pollMaxBatchSize changed" },
        { nameof(KafkaSettingsConfig.ThreadPerConsumer), "threadPerConsumer changed" }
    };

    [Fact]
    public void GivenDefaultThroughputSettings_WhenGenerateKafkaTable_ThenOmitsThem()
    {
        var config = OrdersQueueTestConfig.Create();

        var sql = KafkaTableGenerator.Generate(config, MapColumns(config));

        sql.Should().Contain("kafka_num_consumers = 1");
        sql.Should().NotContain("kafka_max_block_size");
        sql.Should().NotContain("kafka_flush_interval_ms");
        sql.Should().NotContain("kafka_poll_max_batch_size");
        sql.Should().NotContain("kafka_thread_per_consumer");
    }

    [Fact]
    public void GivenThroughputSettings_WhenGenerateKafkaTable_ThenRendersThemAfterNumConsumers()
    {
        var config = OrdersQueueTestConfig.Create();
        config.Kafka.NumConsumers = 4;
        config.Kafka.MaxBlockSize = 65536;
        config.Kafka.FlushIntervalMs = 1000;
        config.Kafka.PollMaxBatchSize = 16384;
        config.Kafka.ThreadPerConsumer = true;

        var sql = KafkaTableGenerator.Generate(config, MapColumns(config));

        AssertAscending(sql,
            "kafka_num_consumers = 4,",
            "kafka_max_block_size = 65536,",
            "kafka_flush_interval_ms = 1000,",
            "kafka_poll_max_batch_size = 16384,",
            "kafka_thread_per_consumer = 1,");
    }

    [Fact]
    public void GivenThreadPerConsumerDisabled_WhenGenerateKafkaTable_ThenRendersZero()
    {
        var config = OrdersQueueTestConfig.Create();
        config.Kafka.ThreadPerConsumer = false;

        KafkaTableGenerator.Generate(config, MapColumns(config)).Should().Contain("kafka_thread_per_consumer = 0");
    }

    [Theory]
    [MemberData(nameof(SettingChanges))]
    public void GivenThroughputSettingChanged_WhenDiff_ThenEmitsKafkaTableChangedAndRecreatesQueue(string setting, string reason)
    {
        var generator = SchemaGeneratorFactory.Create();
        var oldConfig = LoadFixture();
        var oldPlan = generator.BuildPlan(oldConfig);
        var newConfig = LoadFixture();
        Set(newConfig.KafkaTables[0].Kafka, setting);
        var newPlan = generator.BuildPlan(newConfig);

        var diff = SchemaDiffer.Diff(oldPlan, newPlan);

        diff.Changes.OfType<KafkaTableChanged>().Should().ContainSingle()
            .Which.Should().Be(new KafkaTableChanged("orders_queue", reason));
        var plan = MigrationPolicy.Apply(diff, oldPlan, newPlan);
        plan.RequiresManualApproval.Should().BeFalse();
        MigrationSqlGenerator.Generate(
                plan,
                oldPlan,
                newPlan,
                "tune_kafka",
                parentChecksum: null,
                targetChecksum: "target",
                new DateTimeOffset(2099, 1, 3, 0, 0, 0, TimeSpan.Zero))
            .Should().Contain("DROP TABLE IF EXISTS orders_queue;")
            .And.Contain("CREATE TABLE orders_queue");
    }

    [Fact]
    public void GivenDefaultThroughputSettings_WhenSerializeSnapshot_ThenOmitsFields()
    {
        var plan = SchemaGeneratorFactory.Create().BuildPlan(LoadFixture());

        var json = SchemaSnapshotSerializer.SerializeToString(SchemaSnapshotMapper.FromPlan(plan));

        json.Should().NotContain("maxBlockSize");
        json.Should().NotContain("flushIntervalMs");
        json.Should().NotContain("pollMaxBatchSize");
        json.Should().NotContain("threadPerConsumer");
    }

    [Fact]
    public void GivenThroughputSettings_WhenSnapshotRoundTrip_ThenPersistedWithoutDiffOrDrift()
    {
        var config = LoadFixture();
        foreach (var setting in SettingChanges.Select(row => (string)row[0]))
            Set(config.KafkaTables[0].Kafka, setting);
        var plan = SchemaGeneratorFactory.Create().BuildPlan(config);
        var path = Path.Combine(Path.GetTempPath(), $"snapshot-{Guid.NewGuid():N}.json");

        try
        {
            SchemaSnapshotSerializer.Save(path, SchemaSnapshotMapper.FromPlan(plan));
            var json = File.ReadAllText(path);
            json.Should().Contain("\"maxBlockSize\": 65536");
            json.Should().Contain("\"flushIntervalMs\": 1000");
            json.Should().Contain("\"pollMaxBatchSize\": 16384");
            json.Should().Contain("\"threadPerConsumer\": true");

            var snapshot = SchemaSnapshotSerializer.Load(path);
            SchemaDiffer.Diff(SchemaSnapshotMapper.ToPlan(snapshot, config), plan).Changes.Should().BeEmpty();
            var act = () => SnapshotDriftChecker.Check(plan, snapshot, TestContractsPaths.CodegenConfigPath);
            act.Should().NotThrow();
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Theory]
    [MemberData(nameof(SettingChanges))]
    public void GivenSnapshotWithoutSetting_WhenCheckPlanWithIt_ThenReportsDrift(string setting, string reason)
    {
        var generator = SchemaGeneratorFactory.Create();
        var snapshot = SchemaSnapshotMapper.FromPlan(generator.BuildPlan(LoadFixture()));
        var config = LoadFixture();
        Set(config.KafkaTables[0].Kafka, setting);

        var act = () => SnapshotDriftChecker.Check(generator.BuildPlan(config), snapshot, TestContractsPaths.CodegenConfigPath);

        act.Should().Throw<SchemaDriftException>().Which.Differences
            .Should().Contain($"Kafka table 'orders_queue' {reason}.");
    }

    [Theory]
    [InlineData(0, null, null, "MaxBlockSize")]
    [InlineData(10_000_001, null, null, "MaxBlockSize")]
    [InlineData(null, 99, null, "FlushIntervalMs")]
    [InlineData(null, 600_001, null, "FlushIntervalMs")]
    [InlineData(null, null, 0, "PollMaxBatchSize")]
    [InlineData(1000, null, 2000, "pollMaxBatchSize")]
    public void GivenOutOfRangeThroughputSettings_WhenValidate_ThenFails(
        int? maxBlockSize,
        int? flushIntervalMs,
        int? pollMaxBatchSize,
        string expected)
    {
        var config = LoadFixture();
        var kafka = config.KafkaTables[0].Kafka;
        kafka.MaxBlockSize = maxBlockSize;
        kafka.FlushIntervalMs = flushIntervalMs;
        kafka.PollMaxBatchSize = pollMaxBatchSize;

        var result = new CodegenConfigValidator().Validate(config);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(error =>
            error.ErrorMessage.Contains(expected, StringComparison.Ordinal)
            || error.PropertyName.Contains(expected, StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(1, 100, 1)]
    [InlineData(65536, 7500, 65536)]
    [InlineData(10_000_000, 600_000, null)]
    [InlineData(null, null, 1_000_000)]
    public void GivenThroughputSettingsInRange_WhenValidate_ThenSucceeds(int? maxBlockSize, int? flushIntervalMs, int? pollMaxBatchSize)
    {
        var config = LoadFixture();
        var kafka = config.KafkaTables[0].Kafka;
        kafka.MaxBlockSize = maxBlockSize;
        kafka.FlushIntervalMs = flushIntervalMs;
        kafka.PollMaxBatchSize = pollMaxBatchSize;
        kafka.ThreadPerConsumer = true;

        new CodegenConfigValidator().Validate(config).IsValid.Should().BeTrue();
    }

    [Fact]
    public void GivenCodegenJsonWithThroughputSettings_WhenLoad_ThenBindsCamelCaseNames()
    {
        var path = Path.Combine(Path.GetTempPath(), $"codegen-{Guid.NewGuid():N}.json");
        var json = File.ReadAllText(TestContractsPaths.CodegenConfigPath).Replace(
            "\"numConsumers\": 1,",
            "\"numConsumers\": 2, \"maxBlockSize\": 65536, \"flushIntervalMs\": 1000, \"pollMaxBatchSize\": 16384, \"threadPerConsumer\": true,",
            StringComparison.Ordinal);
        json.Should().Contain("maxBlockSize");

        try
        {
            File.WriteAllText(path, json);
            var kafka = CodegenConfigLoader.Load(path).KafkaTables[0].Kafka;

            kafka.MaxBlockSize.Should().Be(65536);
            kafka.FlushIntervalMs.Should().Be(1000);
            kafka.PollMaxBatchSize.Should().Be(16384);
            kafka.ThreadPerConsumer.Should().BeTrue();
        }
        finally
        {
            File.Delete(path);
        }
    }

    private static void Set(KafkaSettingsConfig kafka, string setting)
    {
        switch (setting)
        {
            case nameof(KafkaSettingsConfig.MaxBlockSize):
                kafka.MaxBlockSize = 65536;
                break;
            case nameof(KafkaSettingsConfig.FlushIntervalMs):
                kafka.FlushIntervalMs = 1000;
                break;
            case nameof(KafkaSettingsConfig.PollMaxBatchSize):
                kafka.PollMaxBatchSize = 16384;
                break;
            case nameof(KafkaSettingsConfig.ThreadPerConsumer):
                kafka.ThreadPerConsumer = true;
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(setting), setting, null);
        }
    }

    private static CodegenConfig LoadFixture() => CodegenConfigLoader.Load(TestContractsPaths.CodegenConfigPath);

    private static IReadOnlyList<ClickHouseColumn> MapColumns(KafkaTableConfig config) =>
        MappingTestSupport.MapFixture(ProtoToClickHouseMapper.ResolveDescriptor(config.MessageType), config.FieldOverrides);

    private static void AssertAscending(string sql, params string[] fragments)
    {
        var from = 0;
        foreach (var fragment in fragments)
        {
            var index = sql.IndexOf(fragment, from, StringComparison.Ordinal);
            index.Should().BeGreaterThanOrEqualTo(0, $"'{fragment}' must follow previous fragments in:{Environment.NewLine}{sql}");
            from = index + fragment.Length;
        }
    }
}
