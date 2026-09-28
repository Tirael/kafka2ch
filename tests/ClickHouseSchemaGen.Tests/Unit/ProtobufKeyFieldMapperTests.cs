namespace ClickHouseSchemaGen.Tests.Unit;

public sealed class ProtobufKeyFieldMapperTests
{
    [Fact]
    public void GivenOrderKey_WhenMapFields_ThenSingleStringFastPath()
    {
        var fields = ProtobufKeyFieldMapper.MapFields(OrderKey.Descriptor);

        fields.Should().ContainSingle();
        fields[0].Name.Should().Be("order_id");
        fields[0].ClickHouseType.Should().Be("String");
        ProtobufKeyFieldMapper.IsSingleStringKey(fields).Should().BeTrue();
    }

    [Fact]
    public void GivenMultiFieldKey_WhenMapFields_ThenMapsSupportedScalarTypes()
    {
        var fields = ProtobufKeyFieldMapper.MapFields(MultiFieldKey.Descriptor);

        fields.Select(field => (field.Name, field.ClickHouseType)).Should().Equal(
            ("id", "String"),
            ("shard", "Int32"),
            ("active", "Bool"),
            ("revision", "UInt64"),
            ("region", "Int32"),
            ("delta", "Int64"),
            ("score", "Float64"),
            ("token", "String"),
            ("weight", "Float32"),
            ("crc", "UInt32"));
        ProtobufKeyFieldMapper.IsSingleStringKey(fields).Should().BeFalse();
    }

    [Fact]
    public void GivenNestedMessageKey_WhenMapFields_ThenThrows()
    {
        var act = () => ProtobufKeyFieldMapper.MapFields(OrderEvent.Descriptor);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*nested message*");
    }
}
