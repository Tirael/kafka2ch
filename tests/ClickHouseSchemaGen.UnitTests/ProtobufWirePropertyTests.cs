namespace ClickHouseSchemaGen.UnitTests;

public sealed class ProtobufWirePropertyTests
{
    private const string NullMarker = "<null>";

    [Property(MaxTest = 100)]
    public bool GivenCompositeKeyPayload_WhenParsedByGoogleProtobuf_ThenRoundTrips()
    {
        var key = ProtobufWirePayloadGen.Sample(ProtobufWirePayloadGen.CompositeKeys(), 1).Single();
        var parsed = CompositeKey.Parser.ParseFrom(key.ToByteArray());
        return parsed.Equals(key);
    }

    [Property(MaxTest = 100)]
    public bool GivenOrderKeyPayload_WhenParsedByGoogleProtobuf_ThenRoundTrips()
    {
        var key = ProtobufWirePayloadGen.Sample(ProtobufWirePayloadGen.OrderKeys(), 1).Single();
        var parsed = OrderKey.Parser.ParseFrom(key.ToByteArray());
        return parsed.Equals(key);
    }

    [Property(MaxTest = 50)]
    public bool GivenCompositeKeyWithUnknownPackedAndFixedFields_WhenParsed_ThenKnownFieldsSurvive()
    {
        var sample = ProtobufWirePayloadGen.Sample(ProtobufWirePayloadGen.CompositeKeyCases(), 1).Single();
        var wire = ProtobufWirePayloadFactory.WithSurroundingUnknownFields(
            sample.Key.ToByteArray(),
            sample.Noise);
        var parsed = CompositeKey.Parser.ParseFrom(wire);
        return KnownFieldsMatch(sample.Key, parsed);
    }

    [Property(MaxTest = 50)]
    public bool GivenOrderKeyWithUnknownPackedAndFixedFields_WhenParsed_ThenKnownFieldsSurvive()
    {
        var sample = ProtobufWirePayloadGen.Sample(ProtobufWirePayloadGen.OrderKeyCases(), 1).Single();
        var wire = ProtobufWirePayloadFactory.WithSurroundingUnknownFields(
            sample.Key.ToByteArray(),
            sample.Noise);
        var parsed = OrderKey.Parser.ParseFrom(wire);
        return parsed.OrderId == sample.Key.OrderId;
    }

    [Fact]
    public void GivenMissingCompositeKeyFields_WhenExpectedValuesBuilt_ThenUsesProtoDefaultsAndNullMarkers()
    {
        var expected = ProtobufWirePayloadFactory.ExpectedCompositeKeyValues(new CompositeKey(), NullMarker);

        expected["_key.tenant"].Should().BeEmpty();
        expected["_key.int32_value"].Should().Be("0");
        expected["_key.sint32_value"].Should().Be("0");
        expected["_key.flag"].Should().Be("0");
        expected["_key.status"].Should().Be("SAMPLE_STATUS_UNSPECIFIED");
        expected["_key.region"].Should().Be(NullMarker);
        expected["_key.scope.level"].Should().Be("0");
        expected["_key.label"].Should().Be(NullMarker);
        expected["_key.ratio"].Should().Be(BitConverter.SingleToUInt32Bits(0f).ToString(CultureInfo.InvariantCulture));
    }

    [Fact]
    public void GivenNegativeAndZigZagValues_WhenExpectedValuesBuilt_ThenMatchesProtobufWireSemantics()
    {
        var key = new CompositeKey
        {
            Int32Value = -5,
            Sint32Value = -7,
            Sint64Value = -123_456_789_012,
            Fixed32Value = uint.MaxValue,
            Sfixed64Value = -3
        };

        var expected = ProtobufWirePayloadFactory.ExpectedCompositeKeyValues(key, NullMarker);

        expected["_key.int32_value"].Should().Be("-5");
        expected["_key.sint32_value"].Should().Be("-7");
        expected["_key.sint64_value"].Should().Be("-123456789012");
        expected["_key.fixed32_value"].Should().Be("4294967295");
        expected["_key.sfixed64_value"].Should().Be("-3");
    }

    private static bool KnownFieldsMatch(CompositeKey expected, CompositeKey actual)
    {
        var left = ProtobufWirePayloadFactory.ExpectedCompositeKeyValues(expected, NullMarker);
        var right = ProtobufWirePayloadFactory.ExpectedCompositeKeyValues(actual, NullMarker);
        return left.Count == right.Count
            && left.All(pair => right.TryGetValue(pair.Key, out var value) && value == pair.Value);
    }
}
