namespace ClickHouseSchemaGen.Tests.Unit;

public sealed class DenormalizationPlannerTests
{
    private readonly DenormalizationPlanner _sut = new();
    private readonly CodegenDefaults _defaults = OrdersQueueTestConfig.Defaults;

    [Fact]
    public void GivenOptionalFieldsMessage_WhenMapped_ThenUsesNullableColumns()
    {
        // Arrange
        // Act
        var columns = _sut.MapMessage(OptionalFieldsMessage.Descriptor, _defaults, new Dictionary<string, FieldOverrideConfig>());

        // Assert
        columns.Should().BeEquivalentTo([
            new ClickHouseColumn("nickname", "Nullable(String)", "proto optional", "nickname", MappingStrategy.Optional),
            new ClickHouseColumn("bonus_points", "Nullable(Int32)", "proto optional", "bonus_points", MappingStrategy.Optional)
        ]);
    }

    [Fact]
    public void GivenMapFieldsMessage_WhenMapped_ThenUsesMapType()
    {
        // Act
        var columns = _sut.MapMessage(MapFieldsMessage.Descriptor, _defaults, new Dictionary<string, FieldOverrideConfig>());

        // Assert
        columns.Single().Type.Should().Be("Map(String, String)");
    }

    [Fact]
    public void GivenOneofMessage_WhenMapped_ThenCreatesBranchAndPresenceColumns()
    {
        // Act
        var columns = _sut.MapMessage(OneofMessage.Descriptor, _defaults, new Dictionary<string, FieldOverrideConfig>());

        // Assert
        columns.Select(column => (column.Name, column.Type)).Should().BeEquivalentTo([
            ("text", "Nullable(String)"),
            ("number", "Nullable(Int32)"),
            ("payload", "Enum8('absent' = 0, 'text' = 1, 'number' = 2)")
        ]);
    }

    [Fact]
    public void GivenTimestampFieldsMessage_WhenMapped_ThenFlattensTimestampFields()
    {
        // Act
        var columns = _sut.MapMessage(TimestampFieldsMessage.Descriptor, _defaults, new Dictionary<string, FieldOverrideConfig>());

        // Assert
        columns.Select(column => (column.Name, column.Type)).Should().BeEquivalentTo([
            ("created_at.seconds", "Int64"),
            ("created_at.nanos", "Int32")
        ]);
    }

    [Fact]
    public void GivenRepeatedEnumMessage_WhenMapped_ThenUsesArrayEnum8()
    {
        // Act
        var columns = _sut.MapMessage(RepeatedEnumMessage.Descriptor, _defaults, new Dictionary<string, FieldOverrideConfig>());

        // Assert
        columns.Single().Type.Should().Be(
            "Array(Enum8('SAMPLE_STATUS_UNSPECIFIED' = 0, 'SAMPLE_STATUS_ACTIVE' = 1, 'SAMPLE_STATUS_ARCHIVED' = 2))");
    }
}
