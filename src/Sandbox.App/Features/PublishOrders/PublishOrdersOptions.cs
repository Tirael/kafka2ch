namespace Sandbox.App.Features.PublishOrders;

public sealed class PublishOrdersOptions
{
    public const string SectionName = "PublishOrders";

    public string Topic { get; set; } = "orders";

    public int IntervalMs { get; set; } = 500;

    public double RatePerSecond { get; set; }

    public long Count { get; set; }

    public int Parallelism { get; set; } = 1;

    public int LogEvery { get; set; } = 1;

    public bool StopApplicationWhenDone { get; set; }
}
