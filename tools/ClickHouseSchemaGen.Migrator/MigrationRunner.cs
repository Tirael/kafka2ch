namespace ClickHouseSchemaGen.Migrator;

public sealed class MigrationRunner(
    ILogger logger,
    TimeProvider timeProvider,
    ClusterConfig? cluster = null,
    MigrationLockOptions? lockOptions = null)
{
    private static readonly Regex AwaitConsumersMarker = new(
        @"^\s*--\s*await:kafka_consumers_empty\s+(?<table>[A-Za-z_][A-Za-z0-9_]*)(?:\s+ON\s+CLUSTER\s+(?<cluster>[A-Za-z_][A-Za-z0-9_]*))?\s*$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

    private static readonly Regex DetachTableStatement = new(
        @"^\s*DETACH\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?<table>[A-Za-z_][A-Za-z0-9_]*)(?:\s+ON\s+CLUSTER\s+(?<cluster>[A-Za-z_][A-Za-z0-9_]*))?(?:\s+PERMANENTLY)?\s*;?\s*$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

    private static readonly Regex AttachTableStatement = new(
        @"^\s*ATTACH\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?<table>[A-Za-z_][A-Za-z0-9_]*)(?:\s+ON\s+CLUSTER\s+(?<cluster>[A-Za-z_][A-Za-z0-9_]*))?\s*;?\s*$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

    private static readonly Regex DropTableStatement = new(
        @"^\s*DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?<table>[A-Za-z_][A-Za-z0-9_]*)(?:\s+ON\s+CLUSTER\s+(?<cluster>[A-Za-z_][A-Za-z0-9_]*))?(?:\s+SYNC)?\s*;?\s*$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

    private static readonly Regex CreateTableStatement = new(
        @"^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:[A-Za-z_][A-Za-z0-9_]*\.)?(?<table>[A-Za-z_][A-Za-z0-9_]*)\b",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

    private static readonly Regex DropViewStatement = new(
        @"^\s*DROP\s+VIEW\s+(?:IF\s+EXISTS\s+)?(?<view>[A-Za-z_][A-Za-z0-9_]*)(?:\s+ON\s+CLUSTER\s+(?<cluster>[A-Za-z_][A-Za-z0-9_]*))?(?:\s+SYNC)?\s*;?\s*$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

    private static readonly Regex CreateMaterializedViewStatement = new(
        @"^\s*CREATE\s+MATERIALIZED\s+VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:[A-Za-z_][A-Za-z0-9_]*\.)?(?<view>[A-Za-z_][A-Za-z0-9_]*)\b",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

    private static readonly Regex CreateObjectHeader = new(
        @"^(?<prefix>\s*CREATE\s+(?:MATERIALIZED\s+VIEW|TABLE)\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:`?[A-Za-z_][A-Za-z0-9_]*`?\.)?`?[A-Za-z_][A-Za-z0-9_]*`?)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

    private readonly ClusterDdl _cluster = new(cluster);

    private readonly MigrationLockOptions _lockOptions = lockOptions ?? new MigrationLockOptions();

    private static readonly TimeSpan ConsumerWaitTimeout = TimeSpan.FromSeconds(60);

    private static readonly TimeSpan ReattachTimeout = TimeSpan.FromSeconds(30);

    public async Task ApplyAsync(
        string connectionString,
        string migrationsDirectory,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(migrationsDirectory);
        Directory.CreateDirectory(migrationsDirectory);

        await using var connection = new ClickHouseConnection(connectionString);
        await OpenWithRetryAsync(connection, cancellationToken);
        await EnsureMigrationsTableAsync(connection, cancellationToken);
        var applied = await LoadAppliedAsync(connection, cancellationToken);
        foreach (var interrupted in FindInterruptedMigrations(await LoadJournalAsync(connection, cancellationToken)))
        {
            logger.LogWarning(
                "Migration {Migration} was started but never recorded as applied (interrupted run); re-applying it",
                interrupted);
        }

        var files = Directory.GetFiles(migrationsDirectory, "*.sql")
            .Select(path => new FileInfo(path))
            .OrderBy(file => file.Name, StringComparer.Ordinal)
            .ToList();

        foreach (var file in files)
        {
            var version = ParseVersion(file.Name);
            var checksum = SqlChecksum.ComputeFile(file.FullName);
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
            await RecordJournalAsync(connection, version, file.Name, checksum, SchemaMigrationsTable.StartedKind, cancellationToken);
            await ApplyFileAsync(connection, file.FullName, cancellationToken);
            await RecordJournalAsync(connection, version, file.Name, checksum, SchemaMigrationsTable.MigrationKind, cancellationToken);
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
            $"SELECT version, checksum FROM {_cluster.HistoryTableName} "
                + $"WHERE kind = '{SchemaMigrationsTable.MigrationKind}'";
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var applied = new Dictionary<string, string>(StringComparer.Ordinal);
        while (await reader.ReadAsync(cancellationToken))
            applied[reader.GetString(0)] = reader.GetString(1);
        return applied;
    }

    private async Task<List<(string Version, string Name, string Kind)>> LoadJournalAsync(
        ClickHouseConnection connection,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"SELECT version, name, kind FROM {_cluster.HistoryTableName} "
                + $"WHERE kind IN ('{SchemaMigrationsTable.StartedKind}', '{SchemaMigrationsTable.MigrationKind}')";
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var journal = new List<(string, string, string)>();
        while (await reader.ReadAsync(cancellationToken))
            journal.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2)));
        return journal;
    }

    public static IReadOnlyList<string> FindInterruptedMigrations(
        IEnumerable<(string Version, string Name, string Kind)> journal)
    {
        var rows = journal.ToList();
        var applied = rows
            .Where(row => row.Kind == SchemaMigrationsTable.MigrationKind)
            .Select(row => row.Version)
            .ToHashSet(StringComparer.Ordinal);
        return rows
            .Where(row => row.Kind == SchemaMigrationsTable.StartedKind && !applied.Contains(row.Version))
            .Select(row => row.Name)
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .ToList();
    }

    private async Task ApplyFileAsync(
        ClickHouseConnection connection,
        string path,
        CancellationToken cancellationToken)
    {
        var sql = await File.ReadAllTextAsync(path, cancellationToken);
        var detachedQueues = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        var droppedQueues = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        var droppedViews = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        var savedObjectDdl = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        var completed = false;
        try
        {
            foreach (var statement in SplitStatements(sql))
            {
                var awaitMatch = AwaitConsumersMarker.Match(statement);
                if (awaitMatch.Success)
                {
                    var markerCluster = awaitMatch.Groups["cluster"] is { Success: true, Value: var value }
                        ? value
                        : null;
                    await WaitForConsumersEmptyAsync(
                        connection,
                        awaitMatch.Groups["table"].Value,
                        markerCluster ?? _cluster.Name,
                        cancellationToken);
                    continue;
                }

                if (string.IsNullOrWhiteSpace(statement)
                    || statement.TrimStart().StartsWith("--", StringComparison.Ordinal))
                    continue;

                await SnapshotObjectDdlBeforeChangeAsync(connection, statement, savedObjectDdl, cancellationToken);

                await using var command = connection.CreateCommand();
                command.CommandText = statement;
                await command.ExecuteNonQueryAsync(cancellationToken);
                TrackQueueLifecycle(statement, detachedQueues, droppedQueues);
                TrackViewLifecycle(statement, droppedViews);
            }

            completed = true;
        }
        finally
        {
            if (!completed)
            {
                if (droppedQueues.Count > 0)
                    await RestoreDroppedObjectsAsync(connection, droppedQueues, savedObjectDdl);

                if (detachedQueues.Count > 0)
                    await ReattachDetachedQueuesAsync(connection, detachedQueues);

                if (droppedViews.Count > 0)
                    await RestoreDroppedObjectsAsync(connection, droppedViews, savedObjectDdl);
            }
        }
    }

    public static void TrackQueueLifecycle(
        string statement,
        IDictionary<string, string?> detachedQueues,
        IDictionary<string, string?>? droppedQueues = null)
    {
        var detach = DetachTableStatement.Match(statement);
        if (detach.Success)
        {
            detachedQueues[detach.Groups["table"].Value] =
                detach.Groups["cluster"] is { Success: true, Value: var value } ? value : null;
            return;
        }

        var attach = AttachTableStatement.Match(statement);
        if (attach.Success)
        {
            detachedQueues.Remove(attach.Groups["table"].Value);
            return;
        }

        var drop = DropTableStatement.Match(statement);
        if (drop.Success)
        {
            var tableName = drop.Groups["table"].Value;
            detachedQueues.Remove(tableName);
            if (droppedQueues is not null)
            {
                droppedQueues[tableName] =
                    drop.Groups["cluster"] is { Success: true, Value: var value } ? value : null;
            }

            return;
        }

        if (droppedQueues is null)
            return;

        var create = CreateTableStatement.Match(statement);
        if (create.Success)
            droppedQueues.Remove(create.Groups["table"].Value);
    }

    public static void TrackViewLifecycle(string statement, IDictionary<string, string?> droppedViews)
    {
        var drop = DropViewStatement.Match(statement);
        if (drop.Success)
        {
            droppedViews[drop.Groups["view"].Value] =
                drop.Groups["cluster"] is { Success: true, Value: var value } ? value : null;
            return;
        }

        var create = CreateMaterializedViewStatement.Match(statement);
        if (create.Success)
            droppedViews.Remove(create.Groups["view"].Value);
    }

    private static async Task SnapshotObjectDdlBeforeChangeAsync(
        ClickHouseConnection connection,
        string statement,
        IDictionary<string, string> savedObjectDdl,
        CancellationToken cancellationToken)
    {
        var objectName = ExtractTargetObjectForBackup(statement);
        if (objectName is null || savedObjectDdl.ContainsKey(objectName))
            return;

        try
        {
            await using var command = connection.CreateCommand();
            command.CommandText = $"SHOW CREATE TABLE {objectName}";
            var createSql = Convert.ToString(await command.ExecuteScalarAsync(cancellationToken));
            if (!string.IsNullOrWhiteSpace(createSql))
                savedObjectDdl[objectName] = createSql;
        }
        catch (Exception)
        {
        }
    }

    public static string? ExtractTargetObjectForBackup(string statement)
    {
        var detach = DetachTableStatement.Match(statement);
        if (detach.Success)
            return detach.Groups["table"].Value;

        var dropTable = DropTableStatement.Match(statement);
        if (dropTable.Success)
            return dropTable.Groups["table"].Value;

        var dropView = DropViewStatement.Match(statement);
        return dropView.Success ? dropView.Groups["view"].Value : null;
    }

    private async Task RestoreDroppedObjectsAsync(
        ClickHouseConnection connection,
        IReadOnlyDictionary<string, string?> droppedObjects,
        IReadOnlyDictionary<string, string> savedObjectDdl)
    {
        using var timeout = new CancellationTokenSource(ReattachTimeout, timeProvider);
        foreach (var (objectName, clusterName) in droppedObjects)
        {
            if (!savedObjectDdl.TryGetValue(objectName, out var createSql))
                continue;

            var onCluster = clusterName is null ? string.Empty : $" ON CLUSTER {clusterName}";
            var restoreSql = WithClusterClause(createSql, clusterName);
            try
            {
                logger.LogWarning(
                    "Migration failed after DROP of {Object}; restoring previous definition",
                    objectName);
                try
                {
                    await using var attachCommand = connection.CreateCommand();
                    attachCommand.CommandText = $"ATTACH TABLE IF NOT EXISTS {objectName}{onCluster}";
                    await attachCommand.ExecuteNonQueryAsync(timeout.Token);

                    await using var dropCommand = connection.CreateCommand();
                    dropCommand.CommandText = $"DROP TABLE IF EXISTS {objectName}{onCluster}";
                    await dropCommand.ExecuteNonQueryAsync(timeout.Token);
                }
                catch (Exception)
                {
                }

                await using var command = connection.CreateCommand();
                command.CommandText = restoreSql;
                await command.ExecuteNonQueryAsync(timeout.Token);
            }
            catch (Exception restoreException)
            {
                logger.LogError(
                    restoreException,
                    "Failed to restore {Object} after migration failure",
                    objectName);
            }
        }
    }

    public static string WithClusterClause(string createSql, string? clusterName) =>
        string.IsNullOrWhiteSpace(clusterName)
        || createSql.Contains(" ON CLUSTER ", StringComparison.OrdinalIgnoreCase)
            ? createSql
            : CreateObjectHeader.Replace(createSql, $"${{prefix}} ON CLUSTER {clusterName}", 1);

    private async Task ReattachDetachedQueuesAsync(
        ClickHouseConnection connection,
        IReadOnlyDictionary<string, string?> detachedQueues)
    {
        using var timeout = new CancellationTokenSource(ReattachTimeout, timeProvider);
        foreach (var (tableName, clusterName) in detachedQueues)
        {
            var onCluster = clusterName is null ? string.Empty : $" ON CLUSTER {clusterName}";
            var attachSql = $"ATTACH TABLE IF NOT EXISTS {tableName}{onCluster}";
            try
            {
                logger.LogWarning(
                    "Migration failed after DETACH of {Table}; re-attaching to resume Kafka consumption",
                    tableName);
                await using var command = connection.CreateCommand();
                command.CommandText = attachSql;
                await command.ExecuteNonQueryAsync(timeout.Token);
            }
            catch (Exception attachException)
            {
                logger.LogError(
                    attachException,
                    "Failed to re-attach {Table} after migration failure; "
                        + "Kafka consumption may stay stopped until ATTACH or server restart",
                    tableName);
            }
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

    private async Task RecordJournalAsync(
        ClickHouseConnection connection,
        string version,
        string name,
        string checksum,
        string kind,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        var appliedAt = timeProvider.GetUtcNow().UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss");
        command.CommandText = SchemaMigrationsTable.InsertSql(
            version,
            name,
            checksum,
            kind,
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
}
