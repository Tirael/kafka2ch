namespace Sandbox.App.Common;

public sealed class ClickHouseOptions
{
    public const string SectionName = "ClickHouse";

    public string Host { get; set; } = "clickhouse";

    public int Port { get; set; } = 8123;

    public string Database { get; set; } = "default";

    public string Username { get; set; } = "default";

    public string Password { get; set; } = "";

    public string ConnectionString =>
        string.IsNullOrEmpty(Password)
            ? $"Host={Host};Port={Port};Username={Username};Database={Database}"
            : $"Host={Host};Port={Port};Username={Username};Password={Password};Database={Database}";
}
