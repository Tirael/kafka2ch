namespace Sandbox.App.Common;

public sealed class ClickHouseOptions
{
    public const string SectionName = "ClickHouse";

    public string Host { get; set; } = "clickhouse";

    public int Port { get; set; } = 8123;

    public string Database { get; set; } = "default";

    public string ConnectionString => $"Host={Host};Port={Port};Database={Database}";
}
