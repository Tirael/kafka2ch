using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using ClickHouse.Client.ADO;
using ClickHouseSchemaGen.Migration;
using Microsoft.Extensions.Logging;

namespace ClickHouseSchemaGen.Migrator;

public sealed class MigrationRunner(ILogger logger, TimeProvider timeProvider)
{
    private static readonly Regex AwaitConsumersMarker = new(
        @"^\s*--\s*await:kafka_consumers_empty\s+(?<table>[A-Za-z_][A-Za-z0-9_]*)\s*$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

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

    private static async Task EnsureMigrationsTableAsync(ClickHouseConnection connection, CancellationToken cancellationToken)
    {
        foreach (var statement in SchemaMigrationsTable.UpgradeStatements.Prepend(SchemaMigrationsTable.CreateTableSql))
        {
            await using var command = connection.CreateCommand();
            command.CommandText = statement;
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private static async Task<Dictionary<string, string>> LoadAppliedAsync(
        ClickHouseConnection connection,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"SELECT version, checksum FROM schema_migrations WHERE kind = '{SchemaMigrationsTable.MigrationKind}'";
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
                await WaitForConsumersEmptyAsync(connection, awaitMatch.Groups["table"].Value, cancellationToken);
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
        CancellationToken cancellationToken)
    {
        var deadline = timeProvider.GetUtcNow() + ConsumerWaitTimeout;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await using var command = connection.CreateCommand();
            command.CommandText =
                "SELECT count() FROM system.kafka_consumers " +
                $"WHERE database = currentDatabase() AND table = '{EscapeLiteral(tableName)}'";
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
            $"toDateTime('{EscapeLiteral(appliedAt)}')");
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static string EscapeLiteral(string value) => value.Replace("'", "''", StringComparison.Ordinal);

    public static IEnumerable<string> SplitStatements(string sql)
    {
        var statements = new List<string>();
        var builder = new StringBuilder();
        var inSingleQuote = false;
        var inLineComment = false;

        for (var i = 0; i < sql.Length; i++)
        {
            var current = sql[i];
            var next = i + 1 < sql.Length ? sql[i + 1] : '\0';

            if (inLineComment)
            {
                builder.Append(current);
                if (current == '\n')
                    inLineComment = false;
                continue;
            }

            if (!inSingleQuote && current == '-' && next == '-')
            {
                // A comment before a statement is emitted on its own: otherwise the statement would be
                // skipped as a comment and await markers would not match.
                if (string.IsNullOrWhiteSpace(builder.ToString()))
                {
                    var lineEnd = sql.IndexOf('\n', i);
                    var end = lineEnd < 0 ? sql.Length : lineEnd;
                    statements.Add(sql[i..end].TrimEnd());
                    builder.Clear();
                    i = end;
                    continue;
                }

                inLineComment = true;
                builder.Append(current);
                continue;
            }

            if (current == '\'' && !inSingleQuote)
            {
                inSingleQuote = true;
                builder.Append(current);
                continue;
            }

            if (current == '\'' && inSingleQuote)
            {
                builder.Append(current);
                if (next == '\'')
                {
                    builder.Append(next);
                    i++;
                    continue;
                }

                inSingleQuote = false;
                continue;
            }

            if (current == ';' && !inSingleQuote)
            {
                var statement = builder.ToString().Trim();
                if (statement.Length > 0)
                    statements.Add(statement);
                builder.Clear();
                continue;
            }

            builder.Append(current);
        }

        var tail = builder.ToString().Trim();
        if (tail.Length > 0)
            statements.Add(tail);

        return statements;
    }

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
