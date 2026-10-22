namespace ClickHouseSchemaGen.IntegrationTests;

public sealed class ProtobufWireUdfPropertyClickHouseIntegrationTests : IAsyncLifetime
{
    private const string NullMarker = "<null>";
    private const int SampleCount = 40;

    private readonly ClickHouseContainer _clickHouse;
    private IReadOnlyList<ClickHouseColumn> _compositeKeyColumns = null!;
    private IReadOnlyList<ClickHouseColumn> _orderKeyColumns = null!;

    public ProtobufWireUdfPropertyClickHouseIntegrationTests()
    {
        TestContainersEnv.SetVariables();
        _clickHouse = new ClickHouseBuilder(TestContainersEnv.FullClickHouseImageName)
            .WithResourceMapping(FormatSchemas.Directory, "/var/lib/clickhouse/format_schemas")
            .Build();
    }

    public async Task InitializeAsync()
    {
        await _clickHouse.StartAsync();
        var setup = await _clickHouse.ExecScriptAsync(ProtobufWireSqlFunctions.Definitions);
        setup.ExitCode.Should().Be(0, setup.Stderr);

        var mapper = new KafkaKeyColumnMapper(new DenormalizationPlanner());
        _compositeKeyColumns = mapper.MapKeyColumns(
            OrdersQueueTestConfig.CreateCompositeKeyTable(),
            OrdersQueueTestConfig.Defaults);
        var orderTable = OrdersQueueTestConfig.Create();
        orderTable.Key = new KafkaKeyConfig
        {
            Format = KafkaKeyFormats.Protobuf,
            MessageType = "ClickHouseSchemaGen.TestContracts.OrderKey, ClickHouseSchemaGen.TestContracts"
        };
        _orderKeyColumns = mapper.MapKeyColumns(orderTable, OrdersQueueTestConfig.Defaults);
    }

    public async Task DisposeAsync() => await _clickHouse.DisposeAsync();

    [Fact]
    public async Task GivenRandomCompositeKeysWithUnknownFields_WhenDecodedByUdf_ThenMatchesGoogleProtobuf()
    {
        foreach (var (key, noise) in ProtobufWirePayloadGen.Sample(
                     ProtobufWirePayloadGen.CompositeKeyCases(),
                     SampleCount))
        {
            var framed = ProtobufWirePayloadFactory.FrameConfluent(
                ProtobufWirePayloadFactory.WithSurroundingUnknownFields(key.ToByteArray(), noise));
            var decoded = await DecodeAsync(framed, _compositeKeyColumns);
            decoded.Should().BeEquivalentTo(
                ProtobufWirePayloadFactory.ExpectedCompositeKeyValues(key, NullMarker),
                "composite key seed noise={0} bytes={1}",
                noise.Seed,
                framed.Length);
        }
    }

    [Fact]
    public async Task GivenRandomOrderKeysWithUnknownFields_WhenDecodedByUdf_ThenMatchesGoogleProtobuf()
    {
        foreach (var (key, noise) in ProtobufWirePayloadGen.Sample(
                     ProtobufWirePayloadGen.OrderKeyCases(),
                     SampleCount))
        {
            var framed = ProtobufWirePayloadFactory.FrameConfluent(
                ProtobufWirePayloadFactory.WithSurroundingUnknownFields(key.ToByteArray(), noise));
            var decoded = await DecodeAsync(framed, _orderKeyColumns);
            decoded.Should().BeEquivalentTo(
                ProtobufWirePayloadFactory.ExpectedOrderKeyValues(key),
                "order key seed noise={0} id={1}",
                noise.Seed,
                key.OrderId);
        }
    }

    [Fact]
    public async Task GivenEmptyCompositeKey_WhenDecodedByUdf_ThenMatchesGoogleProtobufDefaults()
    {
        var key = new CompositeKey();
        var framed = ProtobufWirePayloadFactory.FrameConfluent(key.ToByteArray());
        var decoded = await DecodeAsync(framed, _compositeKeyColumns);
        decoded.Should().BeEquivalentTo(
            ProtobufWirePayloadFactory.ExpectedCompositeKeyValues(key, NullMarker));
    }

    private async Task<Dictionary<string, string>> DecodeAsync(
        byte[] framedKey,
        IReadOnlyList<ClickHouseColumn> keyColumns)
    {
        var keyHex = Convert.ToHexString(framedKey);
        var withClause = string.Join(
            ",\n    ",
            keyColumns.Select(column =>
                $"{column.SourceExpression} AS {SqlColumnFormatter.FormatColumnName(column.Name)}"));
        var selectList = string.Join(
            ", ",
            keyColumns.Select(column => ProtobufWirePayloadFactory.SelectExpression(column, NullMarker)));

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
