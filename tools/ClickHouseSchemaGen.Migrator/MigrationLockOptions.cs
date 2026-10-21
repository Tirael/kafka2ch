namespace ClickHouseSchemaGen.Migrator;

public sealed record MigrationLockOptions
{
    public static readonly TimeSpan DefaultTimeout = TimeSpan.FromMinutes(5);

    public static readonly TimeSpan DefaultTtl = TimeSpan.FromMinutes(30);

    public TimeSpan Timeout { get; init; } = DefaultTimeout;

    public TimeSpan Ttl { get; init; } = DefaultTtl;

    public TimeSpan PollInterval { get; init; } = TimeSpan.FromSeconds(1);

    public TimeSpan RowLockSettleDelay { get; init; } = TimeSpan.FromSeconds(1);

    public string Owner { get; init; } = $"{Environment.MachineName}:{Environment.ProcessId}:{Guid.NewGuid():N}";
}
