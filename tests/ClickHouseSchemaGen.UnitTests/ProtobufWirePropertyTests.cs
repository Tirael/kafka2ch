namespace ClickHouseSchemaGen.UnitTests;

public sealed class ProtobufWirePropertyTests
{
    [Fact]
    public void R14_ProtobufWireUdfPropertyCoverage_IsMissing() =>
        Assert.Fail(
            "R14: FsCheck property tests for protobufWire* vs Google.Protobuf are not wired yet.");
}
