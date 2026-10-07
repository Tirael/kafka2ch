namespace ClickHouseSchemaGen.Tests.Integration;

public sealed class ProtobufKeyDecodingClickHouseIntegrationTests : IAsyncLifetime
{
    private const string NullMarker = "<null>";

    private static readonly byte[] ConfluentEnvelope = [0x00, 0x00, 0x00, 0x00, 0x2A, 0x00];

    private readonly ClickHouseContainer _clickHouse = new ClickHouseBuilder("clickhouse/clickhouse-server:25.11")
        .WithBindMount(RepoPaths.FormatSchemasDirectory, "/var/lib/clickhouse/format_schemas")
        .Build();

    public async Task InitializeAsync() => await _clickHouse.StartAsync();

    public async Task DisposeAsync() => await _clickHouse.DisposeAsync();

    [Fact]
    public async Task GivenConfluentFramedCompositeKey_WhenDecodedWithGeneratedExpressions_ThenMatchesProtobufValues()
    {
        // Arrange
        var key = new CompositeKey
        {
            Tenant = "tenant-ü",
            Int32Value = -5,
            Int64Value = -9_000_000_000,
            Uint32Value = 4_000_000_000,
            Uint64Value = 18_446_744_073_709_551_000,
            Sint32Value = -7,
            Sint64Value = -123_456_789_012,
            Fixed32Value = uint.MaxValue,
            Fixed64Value = 1_234_567_890_123_456_789,
            Sfixed32Value = -2,
            Sfixed64Value = -3,
            Flag = true,
            Ratio = 1.5f,
            Score = -2.25,
            Raw = ByteString.CopyFrom(0x00, 0xFF, 0x41),
            Status = SampleStatus.Archived,
            Region = "eu",
            Scope = new KeyScope { Name = "team", Level = -1 },
            IssuedAt = new Timestamp { Seconds = 1_700_000_000, Nanos = 5 },
            Label = "lbl"
        };

        // Act
        var decoded = await DecodeAsync(key);

        // Assert
        decoded.Should().BeEquivalentTo(new Dictionary<string, string>
        {
            ["_key.tenant"] = "tenant-ü",
            ["_key.int32_value"] = "-5",
            ["_key.int64_value"] = "-9000000000",
            ["_key.uint32_value"] = "4000000000",
            ["_key.uint64_value"] = "18446744073709551000",
            ["_key.sint32_value"] = "-7",
            ["_key.sint64_value"] = "-123456789012",
            ["_key.fixed32_value"] = "4294967295",
            ["_key.fixed64_value"] = "1234567890123456789",
            ["_key.sfixed32_value"] = "-2",
            ["_key.sfixed64_value"] = "-3",
            ["_key.flag"] = "1",
            ["_key.ratio"] = "1.5",
            ["_key.score"] = "-2.25",
            ["_key.raw"] = "00FF41",
            ["_key.status"] = "SAMPLE_STATUS_ARCHIVED",
            ["_key.region"] = "eu",
            ["_key.scope.name"] = "team",
            ["_key.scope.level"] = "-1",
            ["_key.issued_at.seconds"] = "1700000000",
            ["_key.issued_at.nanos"] = "5",
            ["_key.label"] = "lbl"
        });
    }

    [Fact]
    public async Task GivenEmptyCompositeKey_WhenDecoded_ThenReturnsProtoDefaultsAndNullsForPresenceFields()
    {
        // Act
        var decoded = await DecodeAsync(new CompositeKey());

        // Assert
        decoded["_key.tenant"].Should().BeEmpty();
        decoded["_key.int64_value"].Should().Be("0");
        decoded["_key.flag"].Should().Be("0");
        decoded["_key.status"].Should().Be("SAMPLE_STATUS_UNSPECIFIED");
        decoded["_key.scope.level"].Should().Be("0");
        decoded["_key.region"].Should().Be(NullMarker);
        decoded["_key.label"].Should().Be(NullMarker);
    }

    [Fact]
    public async Task GivenRepositoryCodegenConfig_WhenGeneratedScriptsApplied_ThenKeyMappedViewsAreCreated()
    {
        // Arrange
        var outputDirectory = Path.Combine(Path.GetTempPath(), $"clickhouse-schema-gen-{Guid.NewGuid():N}");
        var configPath = Path.Combine(outputDirectory, "clickhouse.codegen.json");
        Directory.CreateDirectory(outputDirectory);
        File.WriteAllText(configPath, File.ReadAllText(RepoPaths.CodegenConfigPath).Replace("../../docker/clickhouse/init/", ""));

        try
        {
            SchemaGeneratorFactory.Create().GenerateFromConfigFile(configPath);

            // Act
            foreach (var script in new[] { "01_orders_queue.sql", "02_shipments_queue.sql", "03_pipeline.sql" })
            {
                var execResult = await _clickHouse.ExecScriptAsync(File.ReadAllText(Path.Combine(outputDirectory, script)));
                execResult.ExitCode.Should().Be(0, $"{script}: {execResult.Stderr}");
            }

            var createViewSql = await QueryScalarAsync("SELECT create_table_query FROM system.tables WHERE name = 'orders_mv'");

            // Assert: SQL UDFs are inlined into the stored view definition.
            createViewSql.Should().Contain("[substring(_key, 7)]");
            createViewSql.Should().Contain("AS `_key.order_id`");
        }
        finally
        {
            if (Directory.Exists(outputDirectory))
                Directory.Delete(outputDirectory, recursive: true);
        }
    }

    private async Task<Dictionary<string, string>> DecodeAsync(CompositeKey key)
    {
        var table = KafkaKeyColumnMapperTestsTable();
        var keyColumns = new KafkaKeyColumnMapper(new DenormalizationPlanner())
            .MapKeyColumns(table, OrdersQueueTestConfig.Defaults);

        var setup = await _clickHouse.ExecScriptAsync(ProtobufWireSqlFunctions.Definitions);
        setup.ExitCode.Should().Be(0, setup.Stderr);

        var keyHex = Convert.ToHexString([.. ConfluentEnvelope, .. key.ToByteArray()]);
        var withClause = string.Join(
            ",\n    ",
            keyColumns.Select(column => $"{column.SourceExpression} AS {SqlColumnFormatter.FormatColumnName(column.Name)}"));
        var selectList = string.Join(
            ", ",
            keyColumns.Select(column =>
            {
                var name = SqlColumnFormatter.FormatColumnName(column.Name);
                var value = column.Name == "_key.raw" ? $"hex({name})" : $"toString({name})";
                return $"ifNull({value}, '{NullMarker}')";
            }));

        var values = await QueryRowAsync($"""
            WITH
                {withClause}
            SELECT {selectList}
            FROM (SELECT unhex('{keyHex}') AS _key)
            """);

        return keyColumns
            .Select((column, index) => (column.Name, Value: values[index]))
            .ToDictionary(pair => pair.Name, pair => pair.Value);
    }

    private static KafkaTableConfig KafkaKeyColumnMapperTestsTable() =>
        Unit.KafkaKeyColumnMapperTests.CreateCompositeKeyTable();

    private async Task<string> QueryScalarAsync(string sql) => (await QueryRowAsync(sql))[0];

    private async Task<string[]> QueryRowAsync(string sql)
    {
        await using var connection = new ClickHouseConnection(_clickHouse.GetConnectionString());
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();

        return Enumerable.Range(0, reader.FieldCount)
            .Select(index => Convert.ToString(reader.GetValue(index))!)
            .ToArray();
    }
}
