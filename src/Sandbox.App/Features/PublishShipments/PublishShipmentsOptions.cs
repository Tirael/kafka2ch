namespace Sandbox.App.Features.PublishShipments;

public sealed class PublishShipmentsOptions
{
    public const string SectionName = "PublishShipments";

    public string Topic { get; set; } = "shipments";

    public int IntervalMs { get; set; } = 750;
}
