namespace Sandbox.App.Features.PublishShipments;

public static class ShipmentEventFactory
{
    private static readonly string[] Countries = ["US", "DE", "RU", "GB"];
    private static readonly string[] Cities = ["New York", "Berlin", "Moscow", "London"];
    private static readonly string[] Streets = ["Main St", "Broadway", "Lenina", "Oxford St"];
    private static readonly string[] Locations = ["warehouse", "sorting-center", "local-hub", "doorstep"];
    private static readonly string[] CarrierKeys = ["carrier", "service_level", "tracking_prefix"];
    private static readonly string[] FailureReasons = ["address_not_found", "recipient_absent", "weather_delay"];
    private static readonly ShipmentStatus[] Statuses =
    [
        ShipmentStatus.Created,
        ShipmentStatus.InTransit,
        ShipmentStatus.Delivered,
        ShipmentStatus.Failed
    ];

    public static (ShipmentKey Key, ShipmentEvent Event) CreateRandom(DateTimeOffset now, string? orderId = null)
    {
        var shipmentId = Guid.NewGuid().ToString();
        var key = new ShipmentKey { ShipmentId = shipmentId };
        var status = Statuses[Random.Shared.Next(Statuses.Length)];
        var shipmentEvent = new ShipmentEvent
        {
            ShipmentId = shipmentId,
            OrderId = orderId ?? Guid.NewGuid().ToString(),
            ShippedAt = Google.Protobuf.WellKnownTypes.Timestamp.FromDateTimeOffset(now.AddMinutes(-Random.Shared.Next(5, 120))),
            Status = status,
            Destination = new Address
            {
                Country = Countries[Random.Shared.Next(Countries.Length)],
                City = Cities[Random.Shared.Next(Cities.Length)],
                Street = Streets[Random.Shared.Next(Streets.Length)],
                PostalCode = Random.Shared.Next(10000, 99999).ToString()
            }
        };

        var checkpointCount = Random.Shared.Next(1, 4);
        for (var i = 0; i < checkpointCount; i++)
        {
            shipmentEvent.Checkpoints.Add(new TrackingCheckpoint
            {
                RecordedAt = Google.Protobuf.WellKnownTypes.Timestamp.FromDateTimeOffset(now.AddMinutes(-Random.Shared.Next(1, 60))),
                Location = Locations[Random.Shared.Next(Locations.Length)],
                Status = Statuses[Random.Shared.Next(Statuses.Length)]
            });
        }

        shipmentEvent.CarrierMetadata["carrier"] = "sandbox-express";
        shipmentEvent.CarrierMetadata[CarrierKeys[Random.Shared.Next(CarrierKeys.Length)]] = "demo";

        if (Random.Shared.Next(0, 2) == 0)
            shipmentEvent.Instructions = "leave-at-door";

        ApplyDeliveryOutcome(shipmentEvent, now, status);
        ApplyPriority(shipmentEvent);
        ApplyStatusHistory(shipmentEvent, status);

        return (key, shipmentEvent);
    }

    private static void ApplyDeliveryOutcome(ShipmentEvent shipmentEvent, DateTimeOffset now, ShipmentStatus status)
    {
        if (status == ShipmentStatus.Delivered)
        {
            shipmentEvent.Delivered = new DeliveredInfo
            {
                DeliveredAt = Google.Protobuf.WellKnownTypes.Timestamp.FromDateTimeOffset(now),
                SignedBy = Random.Shared.Next(0, 2) == 0 ? "recipient" : "neighbor"
            };
            return;
        }

        if (status != ShipmentStatus.Failed)
            return;

        shipmentEvent.Failed = new FailedDelivery
        {
            Reason = FailureReasons[Random.Shared.Next(FailureReasons.Length)],
            RetryCount = Random.Shared.Next(1, 4)
        };
    }

    private static void ApplyPriority(ShipmentEvent shipmentEvent)
    {
        if (Random.Shared.Next(0, 2) == 0)
            shipmentEvent.Priority = Random.Shared.Next(1, 6);
    }

    private static void ApplyStatusHistory(ShipmentEvent shipmentEvent, ShipmentStatus status)
    {
        shipmentEvent.StatusHistory.Add(ShipmentStatus.Unspecified);
        shipmentEvent.StatusHistory.Add(ShipmentStatus.Created);
        shipmentEvent.StatusHistory.Add(status);
    }
}
