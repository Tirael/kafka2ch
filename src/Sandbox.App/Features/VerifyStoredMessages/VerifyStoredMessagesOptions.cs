namespace Sandbox.App.Features.VerifyStoredMessages;

public sealed class VerifyStoredMessagesOptions
{
    public const string SectionName = "VerifyStoredMessages";

    public string OrdersTopic { get; set; } = "orders";

    public string ShipmentsTopic { get; set; } = "shipments";

    public string GroupId { get; set; } = "sandbox-stored-messages";

    public string OrdersTable { get; set; } = "orders_full";

    public string ShipmentsTable { get; set; } = "shipments_full";

    public string OrdersProjectionTable { get; set; } = "orders";

    public string ShipmentsProjectionTable { get; set; } = "shipments";

    public int PollIntervalMs { get; set; } = 1000;

    public int WaitTimeoutMs { get; set; } = 90_000;
}
