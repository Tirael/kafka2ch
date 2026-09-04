namespace Sandbox.App.Features.PublishOrders;

public sealed class PublishOrdersOptions
{
    public const string SectionName = "PublishOrders";

    public string Topic { get; set; } = "orders";

    public int IntervalMs { get; set; } = 500;
}
