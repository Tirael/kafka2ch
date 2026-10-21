using ClickHouse.Client;

namespace ClickHouseSchemaGen.Migrator;

public enum MigrationLockKind
{
    KeeperMap,
    HistoryRow
}

public sealed class MigrationLock : IAsyncDisposable
{
    public const string LockKey = "migrator";

    private static readonly TimeSpan ReleaseTimeout = TimeSpan.FromSeconds(30);

    private readonly ClickHouseConnection _connection;
    private readonly ILogger _logger;
    private readonly TimeProvider _timeProvider;
    private readonly string _releaseSql;
    private bool _released;

    private MigrationLock(
        ClickHouseConnection connection,
        ILogger logger,
        TimeProvider timeProvider,
        MigrationLockKind kind,
        string owner,
        string releaseSql)
    {
        _connection = connection;
        _logger = logger;
        _timeProvider = timeProvider;
        _releaseSql = releaseSql;
        Kind = kind;
        Owner = owner;
    }

    public MigrationLockKind Kind { get; }

    public string Owner { get; }

    public static async Task<MigrationLock> AcquireAsync(
        ClickHouseConnection connection,
        ClusterDdl cluster,
        MigrationLockOptions options,
        ILogger logger,
        TimeProvider timeProvider,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.Owner);
        if (options.Ttl < TimeSpan.FromSeconds(1))
            throw new ArgumentOutOfRangeException(nameof(options), "Migration lock TTL must be at least one second.");
        if (options.Timeout < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(options), "Migration lock timeout must not be negative.");

        var deadline = timeProvider.GetUtcNow() + options.Timeout;
        var owner = EscapeLiteral(options.Owner);
        var kind = await TryCreateKeeperMapTableAsync(connection, cluster, logger, cancellationToken)
            ? MigrationLockKind.KeeperMap
            : MigrationLockKind.HistoryRow;

        string releaseSql;
        if (kind == MigrationLockKind.KeeperMap)
        {
            await AcquireKeeperMapAsync(connection, cluster, options, deadline, logger, timeProvider, cancellationToken);
            releaseSql = $"DELETE FROM {KeeperMapTableName(cluster)} WHERE key = '{LockKey}' AND owner = '{owner}'";
        }
        else
        {
            await AcquireHistoryRowAsync(connection, cluster, options, deadline, logger, timeProvider, cancellationToken);
            releaseSql = HistoryRowReleaseSql(cluster, options.Owner);
        }

        logger.LogInformation("Acquired migration lock as {Owner} ({Kind})", options.Owner, kind);
        return new MigrationLock(connection, logger, timeProvider, kind, options.Owner, releaseSql);
    }

    public async ValueTask DisposeAsync()
    {
        if (_released)
            return;

        _released = true;
        using var timeout = new CancellationTokenSource(ReleaseTimeout, _timeProvider);
        try
        {
            await ExecuteAsync(_connection, _releaseSql, timeout.Token);
            _logger.LogInformation("Released migration lock {Owner}", Owner);
        }
        catch (Exception exception)
        {
            _logger.LogWarning(
                exception,
                "Failed to release migration lock {Owner}; it is released when its TTL expires",
                Owner);
        }
    }

    public static string KeeperMapTableName(ClusterDdl cluster) =>
        cluster.UsesReplicatedDatabase
            ? $"default.{SchemaMigrationsTable.LockTableName}"
            : SchemaMigrationsTable.LockTableName;

    public static string CreateKeeperMapTableSql(ClusterDdl cluster, string database) =>
        $"CREATE TABLE IF NOT EXISTS {KeeperMapTableName(cluster)} "
            + "(key String, owner String, expires_at DateTime) "
            + $"ENGINE = KeeperMap('/{EscapeLiteral(database)}/{SchemaMigrationsTable.LockTableName}') PRIMARY KEY key";

    public static bool RowLockWins(DateTime ownExpiresAt, IEnumerable<DateTime> otherActiveExpiresAt) =>
        otherActiveExpiresAt.All(other => ownExpiresAt < other);

    private static async Task<bool> TryCreateKeeperMapTableAsync(
        ClickHouseConnection connection,
        ClusterDdl cluster,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        var database = Convert.ToString(await ScalarAsync(connection, "SELECT currentDatabase()", cancellationToken))!;
        try
        {
            await ExecuteAsync(connection, CreateKeeperMapTableSql(cluster, database), cancellationToken);
            return true;
        }
        catch (ClickHouseServerException exception)
        {
            if (cluster.Enabled)
            {
                logger.LogWarning(
                    exception,
                    "KeeperMap is unavailable (set keeper_map_path_prefix); using best-effort migration lock rows in {Table}",
                    cluster.HistoryTableName);
            }
            else
            {
                logger.LogInformation(
                    "Keeper is unavailable ({Reason}); using best-effort migration lock rows in {Table}",
                    exception.Message,
                    cluster.HistoryTableName);
            }

            return false;
        }
    }

    private static async Task AcquireKeeperMapAsync(
        ClickHouseConnection connection,
        ClusterDdl cluster,
        MigrationLockOptions options,
        DateTimeOffset deadline,
        ILogger logger,
        TimeProvider timeProvider,
        CancellationToken cancellationToken)
    {
        var table = KeeperMapTableName(cluster);
        var insertSql =
            $"INSERT INTO {table} (key, owner, expires_at) "
                + $"SELECT '{LockKey}', '{EscapeLiteral(options.Owner)}', now() + toIntervalSecond({TtlSeconds(options.Ttl)})";
        string? retriedImmediatelyFor = null;
        Exception? lastError = null;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                await using var command = connection.CreateCommand();
                command.CommandText = insertSql;
                command.CustomSettings["keeper_map_strict_mode"] = 1;
                await command.ExecuteNonQueryAsync(cancellationToken);
                return;
            }
            catch (ClickHouseServerException exception)
            {
                lastError = exception;
            }

            var holder = await ReadKeeperMapHolderAsync(connection, table, cancellationToken);
            if (holder is { Expired: true })
            {
                logger.LogWarning(
                    "Migration lock held by {Owner} expired at {ExpiresAt:u}; taking over",
                    holder.Owner,
                    holder.ExpiresAt);
                await ExecuteAsync(
                    connection,
                    $"DELETE FROM {table} WHERE key = '{LockKey}' AND owner = '{EscapeLiteral(holder.Owner)}'",
                    cancellationToken);
            }

            var retryKey = holder?.Owner ?? string.Empty;
            if ((holder is null || holder.Expired) && retriedImmediatelyFor != retryKey)
            {
                retriedImmediatelyFor = retryKey;
                continue;
            }

            await WaitForRetryAsync(options, deadline, holder, lastError, logger, timeProvider, cancellationToken);
        }
    }

    private static async Task<LockHolder?> ReadKeeperMapHolderAsync(
        ClickHouseConnection connection,
        string table,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"SELECT owner, expires_at, expires_at <= now() FROM {table} WHERE key = '{LockKey}'";
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            return null;

        return new LockHolder(
            reader.GetString(0),
            reader.GetDateTime(1),
            Convert.ToInt32(reader.GetValue(2)) != 0);
    }

    private static async Task AcquireHistoryRowAsync(
        ClickHouseConnection connection,
        ClusterDdl cluster,
        MigrationLockOptions options,
        DateTimeOffset deadline,
        ILogger logger,
        TimeProvider timeProvider,
        CancellationToken cancellationToken)
    {
        var table = cluster.HistoryTableName;
        var deleteExpiredSql =
            $"DELETE FROM {table} WHERE kind = '{SchemaMigrationsTable.LockKind}' AND applied_at <= now()";
        var insertSql = SchemaMigrationsTable.InsertSql(
            LockKey,
            options.Owner,
            string.Empty,
            SchemaMigrationsTable.LockKind,
            $"now() + toIntervalSecond({TtlSeconds(options.Ttl)})",
            table);
        var releaseSql = HistoryRowReleaseSql(cluster, options.Owner);

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await ExecuteAsync(connection, deleteExpiredSql, cancellationToken);
            await ExecuteAsync(connection, insertSql, cancellationToken);
            await Task.Delay(options.RowLockSettleDelay, timeProvider, cancellationToken);

            var leases = await ReadActiveLeasesAsync(connection, table, cancellationToken);
            var own = leases.Where(lease => lease.Owner == options.Owner).Select(lease => (DateTime?)lease.ExpiresAt).Min();
            var others = leases.Where(lease => lease.Owner != options.Owner).ToList();
            if (own is not null && RowLockWins(own.Value, others.Select(lease => lease.ExpiresAt)))
                return;

            await ExecuteAsync(connection, releaseSql, cancellationToken);
            var holder = others.OrderBy(lease => lease.ExpiresAt).FirstOrDefault();
            await WaitForRetryAsync(options, deadline, holder, null, logger, timeProvider, cancellationToken);
        }
    }

    private static async Task<List<LockHolder>> ReadActiveLeasesAsync(
        ClickHouseConnection connection,
        string table,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"SELECT name, applied_at FROM {table} "
                + $"WHERE kind = '{SchemaMigrationsTable.LockKind}' AND applied_at > now()";
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var leases = new List<LockHolder>();
        while (await reader.ReadAsync(cancellationToken))
            leases.Add(new LockHolder(reader.GetString(0), reader.GetDateTime(1), false));
        return leases;
    }

    private static string HistoryRowReleaseSql(ClusterDdl cluster, string owner) =>
        $"DELETE FROM {cluster.HistoryTableName} "
            + $"WHERE kind = '{SchemaMigrationsTable.LockKind}' AND name = '{EscapeLiteral(owner)}'";

    private static async Task WaitForRetryAsync(
        MigrationLockOptions options,
        DateTimeOffset deadline,
        LockHolder? holder,
        Exception? lastError,
        ILogger logger,
        TimeProvider timeProvider,
        CancellationToken cancellationToken)
    {
        if (timeProvider.GetUtcNow() >= deadline)
        {
            var heldBy = holder is null
                ? "another migrator"
                : $"'{holder.Owner}' (expires at {holder.ExpiresAt:u})";
            throw new TimeoutException(
                $"Timed out after {options.Timeout} waiting for the migration lock held by {heldBy}.",
                lastError);
        }

        logger.LogInformation(
            "Migration lock is held by {Holder}; waiting up to {Deadline:u}",
            holder?.Owner ?? "another migrator",
            deadline);
        var jitter = options.PollInterval * Random.Shared.NextDouble();
        await Task.Delay(options.PollInterval + jitter, timeProvider, cancellationToken);
    }

    private static long TtlSeconds(TimeSpan ttl) => (long)Math.Ceiling(ttl.TotalSeconds);

    private static async Task ExecuteAsync(
        ClickHouseConnection connection,
        string sql,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<object?> ScalarAsync(
        ClickHouseConnection connection,
        string sql,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return await command.ExecuteScalarAsync(cancellationToken);
    }

    private static string EscapeLiteral(string value) => value.Replace("'", "''", StringComparison.Ordinal);

    private sealed record LockHolder(string Owner, DateTime ExpiresAt, bool Expired);
}
