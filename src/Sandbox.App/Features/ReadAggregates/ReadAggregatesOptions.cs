namespace Sandbox.App.Features.ReadAggregates;

public sealed class ReadAggregatesOptions
{
    public const string SectionName = "ReadAggregates";

    public int IntervalMs { get; set; } = 5000;

    public int WindowMinutes { get; set; } = 10;
}
