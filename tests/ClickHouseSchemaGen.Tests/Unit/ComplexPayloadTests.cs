using Sandbox.App.Features.ComplexPayload;
using Sandbox.App.Features.PublishOrders;
using Sandbox.App.Features.PublishShipments;
using Sandbox.App.Features.VerifyStoredMessages;

namespace ClickHouseSchemaGen.Tests.Unit;

public sealed class ComplexPayloadTests
{
    private static readonly DateTimeOffset Now = DateTimeOffset.Parse("2026-09-29T12:00:00Z");

    [Fact]
    public void GivenRandomOrdersAndShipments_WhenCreated_ThenKafkaValueStaysWithinBound()
    {
        for (var i = 0; i < 3; i++)
        {
            var (_, orderEvent) = OrderEventFactory.CreateRandom(Now.AddMinutes(i));
            var (_, shipmentEvent) = ShipmentEventFactory.CreateRandom(Now.AddMinutes(i), orderEvent.OrderId);

            PayloadSize.KafkaValueBytes(orderEvent).Should().BeInRange(PayloadSize.MinBytes, PayloadSize.MaxBytes);
            PayloadSize.KafkaValueBytes(shipmentEvent).Should().BeInRange(PayloadSize.MinBytes, PayloadSize.MaxBytes);
            orderEvent.CaseFile.Journal.Should().NotBeEmpty();
            orderEvent.CaseFile.Schedules[0].Windows[0].Slots.Should().NotBeEmpty();
            orderEvent.CaseFile.Evidence.Exhibits[0].Excerpts.Should().NotBeEmpty();
            shipmentEvent.CaseFile.Obligations.Should().NotBeEmpty();
            shipmentEvent.CaseFile.Counterparties.Should().NotBeEmpty();
        }
    }

    [Fact]
    public void GivenChangedNestedJournal_WhenCompared_ThenDifferenceNamesTheField()
    {
        var (_, orderEvent) = OrderEventFactory.CreateRandom(Now);
        var copy = orderEvent.Clone();
        copy.CaseFile.Journal[0].Body += "-changed";

        var diffs = MessageContentDiff.Compare(orderEvent, copy);

        diffs.Should().Contain(diff => diff.Contains("journal", StringComparison.Ordinal));
    }

    [Fact]
    public void GivenConfluentEnvelope_WhenStripped_ThenPayloadMatches()
    {
        var (_, orderEvent) = OrderEventFactory.CreateRandom(Now);
        var payload = orderEvent.ToByteArray();
        var framed = new byte[ConfluentProtobufEnvelope.HeaderLength + payload.Length];
        framed[ConfluentProtobufEnvelope.HeaderLength - 1] = 0;
        payload.CopyTo(framed.AsSpan(ConfluentProtobufEnvelope.HeaderLength));

        var parsed = OrderEvent.Parser.ParseFrom(ConfluentProtobufEnvelope.Payload(framed));

        MessageContentDiff.Compare(orderEvent, parsed).Should().BeEmpty();
    }
}
