namespace ClickHouseSchemaGen.Migrator;


public sealed class MigrationRunner(ILogger logger, TimeProvider timeProvider, ClusterConfig? cluster = null)
{
    private static readonly Regex AwaitConsumersMarker = new(
        @"^\s*--\s*await:kafka_consumers_empty\s+(?<table>[A-Za-z_][A-Za-z0-9_]*)(?:\s+ON\s+CLUSTER\s+(?<cluster>[A-Za-z_][A-Za-z0-9_]*))?\s*$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

    private readonly ClusterDdl _cluster = new(cluster);

    private static readonly TimeSpan ConsumerWaitTimeout = TimeSpan.FromSeconds(60);

    public async Task ApplyAsync(string connectionString, string migrationsDirectory, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(migrationsDirectory);
        Directory.CreateDirectory(migrationsDirectory);

        await using var connection = new ClickHouseConnection(connectionString);
        await OpenWithRetryAsync(connection, cancellationToken);
        await EnsureMigrationsTableAsync(connection, cancellationToken);

        var applied = await LoadAppliedAsync(connection, cancellationToken);
        var files = Directory.GetFiles(migrationsDirectory, "*.sql")
            .Select(path => new FileInfo(path))
            .OrderBy(file => file.Name, StringComparer.Ordinal)
            .ToList();

        foreach (var file in files)
        {
            var version = ParseVersion(file.Name);
            var checksum = ComputeChecksum(file.FullName);
            if (applied.TryGetValue(version, out var existingChecksum))
            {
                if (!string.Equals(existingChecksum, checksum, StringComparison.OrdinalIgnoreCase))
                {
                    throw new InvalidOperationException(
                        $"Checksum mismatch for applied migration '{file.Name}'. " +
                        "Do not edit migrations after they have been applied.");
                }

                continue;
            }

            if (applied.Count > 0)
            {
                var lastApplied = applied.Keys.OrderBy(key => key, StringComparer.Ordinal).Last();
                if (string.CompareOrdinal(version, lastApplied) < 0)
                {
                    throw new InvalidOperationException(
                        $"Out-of-order migration '{file.Name}' is pending after already applied '{lastApplied}'.");
                }
            }

            logger.LogInformation("Applying migration {Migration}", file.Name);
            await ApplyFileAsync(connection, file.FullName, cancellationToken);
            await RecordAppliedAsync(connection, version, file.Name, checksum, cancellationToken);
            applied[version] = checksum;
        }
    }

    private async Task OpenWithRetryAsync(ClickHouseConnection connection, CancellationToken cancellationToken)
    {
        var delay = TimeSpan.FromSeconds(1);
        var maxDelay = TimeSpan.FromSeconds(30);

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                await connection.OpenAsync(cancellationToken);
                return;
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                logger.LogWarning(exception, "ClickHouse unavailable, retrying in {Delay}", delay);
                await Task.Delay(delay, timeProvider, cancellationToken);
                delay = TimeSpan.FromMilliseconds(Math.Min(delay.TotalMilliseconds * 2, maxDelay.TotalMilliseconds));
            }
        }
    }

    private async Task EnsureMigrationsTableAsync(ClickHouseConnection connection, CancellationToken cancellationToken)
    {
        var statements = SchemaMigrationsTable.UpgradeStatementsFor(_cluster)
            .Prepend(SchemaMigrationsTable.CreateTableSqlFor(_cluster));
        foreach (var statement in statements)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = statement;
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private async Task<Dictionary<string, string>> LoadAppliedAsync(
        ClickHouseConnection connection,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"SELECT version, checksum FROM {_cluster.HistoryTableName} WHERE kind = '{SchemaMigrationsTable.MigrationKind}'";
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var applied = new Dictionary<string, string>(StringComparer.Ordinal);
        while (await reader.ReadAsync(cancellationToken))
            applied[reader.GetString(0)] = reader.GetString(1);
        return applied;
    }

    private async Task ApplyFileAsync(
        ClickHouseConnection connection,
        string path,
        CancellationToken cancellationToken)
    {
        var sql = await File.ReadAllTextAsync(path, cancellationToken);
        foreach (var statement in SplitStatements(sql))
        {
            var awaitMatch = AwaitConsumersMarker.Match(statement);
            if (awaitMatch.Success)
            {
                var markerCluster = awaitMatch.Groups["cluster"].Success ? awaitMatch.Groups["cluster"].Value : null;
                await WaitForConsumersEmptyAsync(
                    connection,
                    awaitMatch.Groups["table"].Value,
                    markerCluster ?? _cluster.Name,
                    cancellationToken);
                continue;
            }

            if (string.IsNullOrWhiteSpace(statement) || statement.TrimStart().StartsWith("--", StringComparison.Ordinal))
                continue;

            await using var command = connection.CreateCommand();
            command.CommandText = statement;
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
    }


    private async Task WaitForConsumersEmptyAsync(
        ClickHouseConnection connection,
        string tableName,
        string? clusterName,
        CancellationToken cancellationToken)
    {
        var database = await QueryCurrentDatabaseAsync(connection, cancellationToken);
        var source = clusterName is null
            ? "system.kafka_consumers"
            : $"clusterAllReplicas('{EscapeLiteral(clusterName)}', system.kafka_consumers)";
        var deadline = timeProvider.GetUtcNow() + ConsumerWaitTimeout;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await using var command = connection.CreateCommand();
            command.CommandText =
                $"SELECT count() FROM {source} " +
                $"WHERE database = '{EscapeLiteral(database)}' AND table = '{EscapeLiteral(tableName)}'";
            var count = Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken));
            if (count == 0)
                return;

            if (timeProvider.GetUtcNow() >= deadline)
            {
                throw new TimeoutException(
                    $"Timed out waiting for kafka consumers of '{tableName}' to become empty.");
            }

            await Task.Delay(TimeSpan.FromSeconds(1), timeProvider, cancellationToken);
        }
    }

    private static async Task<string> QueryCurrentDatabaseAsync(
        ClickHouseConnection connection,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT currentDatabase()";
        return Convert.ToString(await command.ExecuteScalarAsync(cancellationToken))!;
    }

    private async Task RecordAppliedAsync(
        ClickHouseConnection connection,
        string version,
        string name,
        string checksum,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        var appliedAt = timeProvider.GetUtcNow().UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss");
        command.CommandText = SchemaMigrationsTable.InsertSql(
            version,
            name,
            checksum,
            SchemaMigrationsTable.MigrationKind,
            $"toDateTime('{EscapeLiteral(appliedAt)}')",
            _cluster.HistoryTableName);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static string EscapeLiteral(string value) => value.Replace("'", "''", StringComparison.Ordinal);

    public static IEnumerable<string> SplitStatements(string sql) => SqlStatementSplitter.Split(sql);

    private static string ParseVersion(string fileName)
    {
        var underscore = fileName.IndexOf('_');
        if (underscore <= 0)
            throw new InvalidOperationException($"Migration file '{fileName}' must be named '{{version}}_{{name}}.sql'.");
        return fileName[..underscore];
    }

    private static string ComputeChecksum(string path)
    {
        var bytes = File.ReadAllBytes(path);
        var hash = SHA256.HashData(bytes);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }
}
