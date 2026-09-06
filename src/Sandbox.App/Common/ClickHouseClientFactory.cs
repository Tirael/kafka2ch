namespace Sandbox.App.Common;

public sealed class ClickHouseClientFactory(IOptions<ClickHouseOptions> options)
{
    private readonly ClickHouseOptions _options = options.Value;

    public ClickHouseConnection CreateConnection() => new(_options.ConnectionString);
}
