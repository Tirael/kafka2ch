using System.Text.Json.Nodes;

namespace ClickHouseSchemaGen.UnitTests;

public sealed class FormatSchemaRequirementTests
{
    private const string Hash = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

    [Fact]
    public void GivenRequirement_WhenRenderMarker_ThenUsesRequireFormatSchemaSyntax() =>
        new FormatSchemaRequirement("order_event.proto", Hash).ToMarker()
            .Should().Be($"-- require:format_schema order_event.proto sha256={Hash}");

    [Theory]
    [InlineData("-- require:format_schema order_event.proto sha256=" + Hash, "order_event.proto")]
    [InlineData("  --   require:format_schema   common/money.proto   SHA256=" + Hash + "  ", "common/money.proto")]
    public void GivenMarker_WhenParse_ThenReturnsFileAndChecksum(string line, string expectedFile)
    {
        FormatSchemaRequirement.TryParse(line, out var requirement).Should().BeTrue();

        requirement.Should().Be(new FormatSchemaRequirement(expectedFile, Hash));
    }

    [Fact]
    public void GivenRenderedMarker_WhenParse_ThenRoundTrips()
    {
        var requirement = new FormatSchemaRequirement("nested/dir/order_event.proto", Hash);

        FormatSchemaRequirement.TryParse(requirement.ToMarker(), out var parsed).Should().BeTrue();

        parsed.Should().Be(requirement);
    }

    [Theory]
    [InlineData("-- await:kafka_consumers_empty orders_queue")]
    [InlineData("-- require:format_schema order_event.proto")]
    [InlineData("-- require:format_schema order_event.proto sha256=xyz")]
    [InlineData("-- require:format_schema ../etc/passwd.proto sha256=" + Hash)]
    [InlineData("-- require:format_schema /abs/order_event.proto sha256=" + Hash)]
    [InlineData("-- require:format_schema o'rder.proto sha256=" + Hash)]
    [InlineData("SELECT 1")]
    public void GivenInvalidOrForeignLine_WhenParse_ThenReturnsFalse(string line)
    {
        FormatSchemaRequirement.TryParse(line, out var requirement).Should().BeFalse();

        requirement.Should().BeNull();
    }

    [Theory]
    [InlineData("order_event", "order_event.proto")]
    [InlineData("order_event.proto", "order_event.proto")]
    [InlineData("common/money", "common/money.proto")]
    public void GivenKafkaProtoFile_WhenResolveFileName_ThenAppendsProtoExtension(string protoFile, string expected) =>
        FormatSchemaRequirement.FileNameFor(protoFile).Should().Be(expected);

    [Fact]
    public void GivenFormatSchemasDirectory_WhenComputeChecksums_ThenHashesCanonicalProtoText()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"kafka2ch-format-schema-{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        try
        {
            File.WriteAllText(Path.Combine(directory, "order_event.proto"), "syntax = \"proto3\";\r\n\r\nmessage A {}  \r\n");
            var table = OrdersQueueTestConfig.Create();

            var checksums = FormatSchemaRequirement.ComputeChecksums([table], directory);

            checksums.Should().ContainSingle()
                .Which.Should().Be(new KeyValuePair<string, string>(
                    "order_event.proto",
                    SqlChecksum.Compute("syntax = \"proto3\";\n\nmessage A {}\n")));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public void GivenMissingProtoFile_WhenComputeChecksums_ThenThrowsWithPath()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"kafka2ch-format-schema-{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        try
        {
            var act = () => FormatSchemaRequirement.ComputeChecksums([OrdersQueueTestConfig.Create()], directory);

            act.Should().Throw<FileNotFoundException>().WithMessage("*order_event.proto*");
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }
}

public sealed class MigrationSqlGeneratorFormatSchemaMarkerTests
{
    private const string Hash = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

    [Fact]
    public void GivenRecreatedQueueAndChecksums_WhenGenerate_ThenWritesMarkerBeforeDetach()
    {
        var sql = KafkaRecreateMigrationFixture.GenerateSql(
            TestContractsPaths.CodegenConfigPath,
            new Dictionary<string, string> { ["order_event.proto"] = Hash });

        var marker = $"-- require:format_schema order_event.proto sha256={Hash}";
        sql.Should().Contain(marker + SqlText.NewLine);
        sql.IndexOf(marker, StringComparison.Ordinal)
            .Should().BeLessThan(sql.IndexOf("DETACH TABLE IF EXISTS orders_queue", StringComparison.Ordinal));
        MigrationRunner.SplitStatements(sql)
            .Select(statement => FormatSchemaRequirement.TryParse(statement, out var requirement) ? requirement : null)
            .OfType<FormatSchemaRequirement>()
            .Should().Equal(new FormatSchemaRequirement("order_event.proto", Hash));
    }

    [Fact]
    public void GivenNoChecksums_WhenGenerate_ThenWritesNoMarker()
    {
        var sql = KafkaRecreateMigrationFixture.GenerateSql(TestContractsPaths.CodegenConfigPath);

        sql.Should().NotContain("require:format_schema");
    }

    [Fact]
    public void GivenChecksumsWithoutRecreatedFile_WhenGenerate_ThenThrows()
    {
        var act = () => KafkaRecreateMigrationFixture.GenerateSql(
            TestContractsPaths.CodegenConfigPath,
            new Dictionary<string, string> { ["other.proto"] = Hash });

        act.Should().Throw<InvalidOperationException>().WithMessage("*order_event.proto*");
    }

    [Fact]
    public void GivenMetaOnlyChangeAndChecksums_WhenGenerate_ThenWritesNoMarker()
    {
        var config = CodegenConfigLoader.Load(TestContractsPaths.CodegenConfigPath);
        var generator = SchemaGeneratorFactory.Create();
        var oldPlan = generator.BuildPlan(config);
        var newConfig = CodegenConfigLoader.Load(TestContractsPaths.CodegenConfigPath);
        newConfig.Defaults.PersistKafkaMeta.Partition = true;
        var newPlan = generator.BuildPlan(newConfig);
        var plan = MigrationPolicy.Apply(SchemaDiffer.Diff(oldPlan, newPlan), oldPlan, newPlan);

        var sql = MigrationSqlGenerator.Generate(
            plan,
            oldPlan,
            newPlan,
            "meta_only",
            parentChecksum: null,
            targetChecksum: "target",
            DateTimeOffset.UnixEpoch,
            new Dictionary<string, string> { ["order_event.proto"] = Hash });

        sql.Should().Contain("DETACH TABLE IF EXISTS orders_queue");
        sql.Should().NotContain("require:format_schema");
    }

    [Fact]
    public void GivenConfiguredFormatSchemasDirectory_WhenOrchestratorMigrates_ThenMigrationCarriesProtoChecksum()
    {
        var root = Path.Combine(Path.GetTempPath(), $"kafka2ch-orchestrator-{Guid.NewGuid():N}");
        var schemas = Path.Combine(root, "format_schemas");
        Directory.CreateDirectory(schemas);
        Directory.CreateDirectory(Path.Combine(root, "clickhouse", "init"));
        try
        {
            var protoText = "syntax = \"proto3\";\nmessage OrderEvent {}\n";
            File.WriteAllText(Path.Combine(schemas, "order_event.proto"), protoText);
            var configPath = Path.Combine(root, "clickhouse.codegen.json");
            var config = JsonNode.Parse(File.ReadAllText(TestContractsPaths.CodegenConfigPath))!.AsObject();
            config["migrations"]!["formatSchemasDirectory"] = "format_schemas";
            File.WriteAllText(configPath, config.ToJsonString());

            var orchestrator = new MigrationOrchestrator(SchemaGeneratorFactory.Create());
            orchestrator.Init(configPath);
            config["kafkaTables"]![0]!["kafka"]!["numConsumers"] = 3;
            File.WriteAllText(configPath, config.ToJsonString());

            var result = orchestrator.Migrate(configPath, "bump", allowManual: false);

            result.ExitCode.Should().Be(0, result.Message);
            File.ReadAllText(result.MigrationPath!).Should().Contain(
                $"-- require:format_schema order_event.proto sha256={SqlChecksum.Compute(protoText)}");
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }
}

public sealed class MigrationPreflightDryRunTests
{
    private const string Database = "kafka2ch_preflight_test";

    [Fact]
    public void GivenKafkaCreateOnCluster_WhenRewriteForDryRun_ThenTargetsPreflightTableWithoutClusterAndWithIsolatedGroup()
    {
        var statement = SqlText.Normalize("""
            CREATE TABLE IF NOT EXISTS orders_queue ON CLUSTER kafka2ch
            (
                order_id String
            )
            ENGINE = Kafka
            SETTINGS
                kafka_broker_list = 'kafka:9092',
                kafka_group_name = 'clickhouse-orders',
                kafka_format = 'ProtobufSingle';
            """);

        var rewritten = MigrationPreflight.RewriteKafkaCreateForDryRun(statement, Database);

        rewritten.Should().NotBeNull();
        rewritten!.Queue.Should().Be("orders_queue");
        rewritten.Sql.Should().StartWith($"CREATE TABLE {Database}.orders_queue__preflight{SqlText.NewLine}(");
        rewritten.Sql.Should().NotContain("ON CLUSTER");
        rewritten.Sql.Should().Contain("kafka_group_name = 'clickhouse-orders__preflight'");
        rewritten.Sql.Should().Contain("kafka_broker_list = 'kafka:9092'");
        rewritten.Sql.Should().NotEndWith(";");
    }

    [Theory]
    [InlineData("CREATE TABLE orders (id String) ENGINE = MergeTree ORDER BY id;")]
    [InlineData("CREATE MATERIALIZED VIEW orders_mv TO orders AS SELECT 1 FROM orders_queue;")]
    [InlineData("DROP TABLE IF EXISTS orders_queue;")]
    public void GivenNonKafkaStatement_WhenRewriteForDryRun_ThenReturnsNull(string statement) =>
        MigrationPreflight.RewriteKafkaCreateForDryRun(statement, Database).Should().BeNull();

    [Fact]
    public void GivenMaterializedViewOverPreflightQueue_WhenRewriteForDryRun_ThenExplainsSelectAgainstPreflightTable()
    {
        var statement = SqlText.Normalize("""
            CREATE MATERIALIZED VIEW orders_mv ON CLUSTER kafka2ch TO orders AS
            SELECT
                order_id AS order_id,
                _key     AS kafka_key
            FROM orders_queue;
            """);

        var rewritten = MigrationPreflight.RewriteViewForDryRun(
            statement,
            Database,
            new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "orders_queue" });

        rewritten.Should().Be(
            "EXPLAIN QUERY TREE SELECT" + SqlText.NewLine
                + "    order_id AS order_id," + SqlText.NewLine
                + "    _key     AS kafka_key" + SqlText.NewLine
                + $"FROM {Database}.orders_queue__preflight");
    }

    [Fact]
    public void GivenMaterializedViewOverOtherSource_WhenRewriteForDryRun_ThenReturnsNull()
    {
        var statement = "CREATE MATERIALIZED VIEW orders_agg_mv TO orders_agg_1m AS SELECT count() AS c FROM orders;";

        MigrationPreflight.RewriteViewForDryRun(
                statement,
                Database,
                new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "orders_queue" })
            .Should().BeNull();
    }

    [Fact]
    public void GivenFunctionDefinitions_WhenCollectDefinedFunctions_ThenReturnsNames()
    {
        var statements = MigrationRunner.SplitStatements(
            ProtobufWireSqlFunctions.Definitions + "CREATE FUNCTION plainFn AS (x) -> x;").ToList();

        MigrationPreflight.CollectDefinedFunctions(statements)
            .Should().Contain(["protobufWireVarint", "protobufWireBytes", "plainFn"]);
    }

    [Fact]
    public void GivenCluster_WhenBuildFormatSchemaQuery_ThenReadsFileOnAllReplicas()
    {
        var sql = MigrationPreflight.FormatSchemaQuery("order_event.proto", "kafka2ch");

        sql.Should().Contain("clusterAllReplicas('kafka2ch', view(");
        sql.Should().Contain("getServerSetting('format_schema_path') AS format_schema_path");
        sql.Should().Contain(
            "file(concat(format_schema_path, if(endsWith(format_schema_path, '/'), '', '/'), 'order_event.proto'), NULL)");
        sql.Should().Contain("getServerSetting('user_files_path')");
    }

    [Fact]
    public void GivenSingleNode_WhenBuildFormatSchemaQuery_ThenQueriesLocally()
    {
        var sql = MigrationPreflight.FormatSchemaQuery("order_event.proto", null);

        sql.Should().NotContain("clusterAllReplicas");
        sql.Should().StartWith("SELECT hostName()");
    }

    [Fact]
    public void GivenReplicaRows_WhenVerify_ThenReportsMismatchMissingAndInaccessiblePaths()
    {
        var expected = SqlChecksum.Compute("syntax = \"proto3\";\n");
        var requirement = new FormatSchemaRequirement("order_event.proto", expected);

        var errors = MigrationPreflight.VerifyFormatSchema(
            requirement,
            [
                new FormatSchemaReplicaState("ch1", "/var/lib/clickhouse/user_files/format_schemas/", "/var/lib/clickhouse/user_files/", "syntax = \"proto3\";\r\n"),
                new FormatSchemaReplicaState("ch2", "/var/lib/clickhouse/user_files/format_schemas/", "/var/lib/clickhouse/user_files/", "syntax = \"proto2\";\n"),
                new FormatSchemaReplicaState("ch3", "/var/lib/clickhouse/user_files/format_schemas/", "/var/lib/clickhouse/user_files/", null),
                new FormatSchemaReplicaState("ch4", "/var/lib/clickhouse/format_schemas/", "/var/lib/clickhouse/user_files/", null)
            ]);

        errors.Should().HaveCount(3);
        errors[0].Should().Contain("ch2").And.Contain("order_event.proto").And.Contain(expected);
        errors[1].Should().Contain("ch3").And.Contain("not found");
        errors[2].Should().Contain("ch4").And.Contain("user_files_path").And.Contain("--skip-preflight");
    }

    [Fact]
    public void GivenNoReplicaRows_WhenVerify_ThenReportsError() =>
        MigrationPreflight.VerifyFormatSchema(new FormatSchemaRequirement("order_event.proto", "00"), [])
            .Should().ContainSingle();
}
