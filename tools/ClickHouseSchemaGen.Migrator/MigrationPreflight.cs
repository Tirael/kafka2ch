namespace ClickHouseSchemaGen.Migrator;

public sealed class MigrationPreflightException(string message, Exception? innerException = null)
    : InvalidOperationException(message, innerException);

public sealed record FormatSchemaReplicaState(string Host, string FormatSchemaPath, string UserFilesPath, string? Content);

public sealed record KafkaDryRunTable(string Queue, string Sql);

public sealed partial class MigrationPreflight(ILogger logger, ClusterDdl cluster)
{
    public const string TableSuffix = "__preflight";

    public const string DatabasePrefix = "kafka2ch_preflight_";

    public async Task RunAsync(
        ClickHouseConnection connection,
        string migrationName,
        IReadOnlyList<string> statements,
        CancellationToken cancellationToken)
    {
        var requirements = statements
            .Select(statement => FormatSchemaRequirement.TryParse(statement, out var requirement) ? requirement : null)
            .OfType<FormatSchemaRequirement>()
            .Distinct()
            .ToList();

        await VerifyFormatSchemasAsync(connection, migrationName, requirements, cancellationToken);
        await DryRunAsync(connection, migrationName, statements, cancellationToken);
    }

    public static string FormatSchemaQuery(string fileName, string? clusterName)
    {
        var select =
            "SELECT hostName() AS host, "
                + "getServerSetting('format_schema_path') AS format_schema_path, "
                + "getServerSetting('user_files_path') AS user_files_path, "
                + "file(concat(format_schema_path, if(endsWith(format_schema_path, '/'), '', '/'), "
                + $"'{EscapeLiteral(fileName)}'), NULL) AS content";
        return string.IsNullOrWhiteSpace(clusterName)
            ? select
            : "SELECT host, format_schema_path, user_files_path, content "
                + $"FROM clusterAllReplicas('{EscapeLiteral(clusterName)}', view({select}))";
    }

    public static IReadOnlyList<string> VerifyFormatSchema(
        FormatSchemaRequirement requirement,
        IReadOnlyList<FormatSchemaReplicaState> replicas)
    {
        if (replicas.Count == 0)
            return [$"no replica reported the state of format schema '{requirement.FileName}'"];

        List<string> errors = [];
        foreach (var replica in replicas)
        {
            if (replica.Content is null)
            {
                errors.Add(IsInside(replica.FormatSchemaPath, replica.UserFilesPath)
                    ? $"host {replica.Host}: format schema '{requirement.FileName}' not found in '{replica.FormatSchemaPath}'"
                    : $"host {replica.Host}: format_schema_path '{replica.FormatSchemaPath}' is outside user_files_path "
                        + $"'{replica.UserFilesPath}', so file() cannot read '{requirement.FileName}'; "
                        + "place format_schema_path under user_files_path or run with --skip-preflight");
                continue;
            }

            var actual = SqlChecksum.Compute(replica.Content);
            if (!string.Equals(actual, requirement.Checksum, StringComparison.OrdinalIgnoreCase))
            {
                errors.Add(
                    $"host {replica.Host}: format schema '{requirement.FileName}' has sha256={actual}, "
                        + $"migration requires sha256={requirement.Checksum}");
            }
        }

        return errors;
    }

    public static KafkaDryRunTable? RewriteKafkaCreateForDryRun(string statement, string database)
    {
        var header = CreateTableHeader().Match(statement);
        if (!header.Success || !KafkaEngine().IsMatch(statement))
            return null;

        var queue = header.Groups["table"].Value;
        var body = KafkaGroupName().Replace(
            TrimTerminator(statement[(header.Index + header.Length)..]),
            match => match.Groups["prefix"].Value + match.Groups["group"].Value + TableSuffix + "'");
        return new KafkaDryRunTable(queue, $"CREATE TABLE {database}.{queue}{TableSuffix}{body}");
    }

    public static string? RewriteViewForDryRun(string statement, string database, IReadOnlySet<string> queues)
    {
        var view = CreateMaterializedViewAs().Match(statement);
        if (!view.Success)
            return null;

        var replaced = false;
        var select = FromTable().Replace(
            TrimTerminator(view.Groups["select"].Value),
            match =>
            {
                var table = match.Groups["table"].Value;
                if (!queues.Contains(table))
                    return match.Value;

                replaced = true;
                return $"{match.Groups["from"].Value}{database}.{table}{TableSuffix}";
            });
        return replaced ? $"EXPLAIN QUERY TREE {select}" : null;
    }

    public static IReadOnlyList<string> CollectDefinedFunctions(IEnumerable<string> statements) =>
        statements
            .Select(statement => CreateFunction().Match(statement))
            .Where(match => match.Success)
            .Select(match => match.Groups["name"].Value)
            .Distinct(StringComparer.Ordinal)
            .ToList();

    private async Task VerifyFormatSchemasAsync(
        ClickHouseConnection connection,
        string migrationName,
        IReadOnlyList<FormatSchemaRequirement> requirements,
        CancellationToken cancellationToken)
    {
        if (requirements.Count == 0)
            return;

        var clusterName = cluster.Enabled ? cluster.Name : null;
        List<string> errors = [];
        foreach (var requirement in requirements)
        {
            var replicas = await QueryFormatSchemaAsync(connection, requirement.FileName, clusterName, cancellationToken);
            errors.AddRange(VerifyFormatSchema(requirement, replicas));
        }

        if (errors.Count > 0)
        {
            throw new MigrationPreflightException(
                $"Preflight failed for migration '{migrationName}': format schemas do not match:"
                    + string.Concat(errors.Select(error => $"{Environment.NewLine}  - {error}")));
        }

        logger.LogInformation(
            "Preflight: format schemas {Files} verified for {Migration}",
            string.Join(", ", requirements.Select(r => r.FileName)),
            migrationName);
    }

    private static async Task<IReadOnlyList<FormatSchemaReplicaState>> QueryFormatSchemaAsync(
        ClickHouseConnection connection,
        string fileName,
        string? clusterName,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = FormatSchemaQuery(fileName, clusterName);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        List<FormatSchemaReplicaState> replicas = [];
        while (await reader.ReadAsync(cancellationToken))
        {
            replicas.Add(new FormatSchemaReplicaState(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3)));
        }

        return replicas;
    }

    private async Task DryRunAsync(
        ClickHouseConnection connection,
        string migrationName,
        IReadOnlyList<string> statements,
        CancellationToken cancellationToken)
    {
        var database = DatabasePrefix + Guid.NewGuid().ToString("N");
        var queues = statements
            .Select(statement => RewriteKafkaCreateForDryRun(statement, database))
            .OfType<KafkaDryRunTable>()
            .ToList();
        if (queues.Count == 0)
            return;

        var queueNames = queues.Select(q => q.Queue).ToHashSet(StringComparer.OrdinalIgnoreCase);
        var views = statements
            .Select(statement => (
                Name: CreateMaterializedViewAs().Match(statement).Groups["view"].Value,
                Sql: RewriteViewForDryRun(statement, database, queueNames)))
            .Where(view => view.Sql is not null)
            .ToList();

        await ExecuteAsync(connection, $"CREATE DATABASE {database} ENGINE = Atomic", cancellationToken);
        try
        {
            foreach (var queue in queues)
                await DryRunStatementAsync(connection, queue.Sql, $"queue '{queue.Queue}'", migrationName, cancellationToken);

            var pendingFunctions = views.Count == 0
                ? []
                : await MissingFunctionsAsync(connection, CollectDefinedFunctions(statements), cancellationToken);
            if (pendingFunctions.Count > 0)
            {
                logger.LogWarning(
                    "Preflight: skipping materialized view dry-run for {Migration}; it defines functions {Functions} that do not exist yet",
                    migrationName,
                    string.Join(", ", pendingFunctions));
            }
            else
            {
                foreach (var (name, sql) in views)
                {
                    await DryRunStatementAsync(
                        connection,
                        sql!,
                        $"materialized view '{name}'",
                        migrationName,
                        cancellationToken);
                }
            }
        }
        finally
        {
            try
            {
                await ExecuteAsync(connection, $"DROP DATABASE IF EXISTS {database} SYNC", CancellationToken.None);
            }
            catch (Exception exception)
            {
                logger.LogWarning(exception, "Preflight: failed to drop temporary database {Database}", database);
            }
        }

        logger.LogInformation(
            "Preflight: dry-run of {Queues} succeeded for {Migration}",
            string.Join(", ", queueNames.Order(StringComparer.Ordinal)),
            migrationName);
    }

    private static async Task DryRunStatementAsync(
        ClickHouseConnection connection,
        string sql,
        string subject,
        string migrationName,
        CancellationToken cancellationToken)
    {
        try
        {
            await ExecuteAsync(connection, sql, cancellationToken);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            throw new MigrationPreflightException(
                $"Preflight dry-run failed for {subject} in migration '{migrationName}': {exception.Message}",
                exception);
        }
    }

    private static async Task<IReadOnlyList<string>> MissingFunctionsAsync(
        ClickHouseConnection connection,
        IReadOnlyList<string> functions,
        CancellationToken cancellationToken)
    {
        if (functions.Count == 0)
            return [];

        await using var command = connection.CreateCommand();
        command.CommandText =
            "SELECT name FROM system.functions WHERE name IN ("
                + string.Join(", ", functions.Select(f => $"'{EscapeLiteral(f)}'"))
                + ")";
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var existing = new HashSet<string>(StringComparer.Ordinal);
        while (await reader.ReadAsync(cancellationToken))
            existing.Add(reader.GetString(0));
        return functions.Where(f => !existing.Contains(f)).ToList();
    }

    private static async Task ExecuteAsync(ClickHouseConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static bool IsInside(string path, string root)
    {
        static string WithSlash(string value) => value.EndsWith('/') ? value : value + "/";
        return WithSlash(path).StartsWith(WithSlash(root), StringComparison.Ordinal);
    }

    private static string TrimTerminator(string sql) => sql.TrimEnd().TrimEnd(';').TrimEnd();

    private static string EscapeLiteral(string value) =>
        value.Replace("\\", "\\\\", StringComparison.Ordinal).Replace("'", "''", StringComparison.Ordinal);

    [GeneratedRegex(
        @"^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:`?[A-Za-z_][A-Za-z0-9_]*`?\.)?`?(?<table>[A-Za-z_][A-Za-z0-9_]*)`?(?:\s+ON\s+CLUSTER\s+`?[A-Za-z_][A-Za-z0-9_]*`?)?",
        RegexOptions.CultureInvariant | RegexOptions.IgnoreCase)]
    private static partial Regex CreateTableHeader();

    [GeneratedRegex(@"\bENGINE\s*=\s*Kafka\b", RegexOptions.CultureInvariant | RegexOptions.IgnoreCase)]
    private static partial Regex KafkaEngine();

    [GeneratedRegex(
        @"(?<prefix>\bkafka_group_name\s*=\s*')(?<group>(?:[^']|'')*)'",
        RegexOptions.CultureInvariant | RegexOptions.IgnoreCase)]
    private static partial Regex KafkaGroupName();

    [GeneratedRegex(
        @"^\s*CREATE\s+MATERIALIZED\s+VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:`?[A-Za-z_][A-Za-z0-9_]*`?\.)?`?(?<view>[A-Za-z_][A-Za-z0-9_]*)`?(?:\s+ON\s+CLUSTER\s+`?[A-Za-z_][A-Za-z0-9_]*`?)?\s+TO\s+\S+\s+AS\s+(?<select>.+)$",
        RegexOptions.CultureInvariant | RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex CreateMaterializedViewAs();

    [GeneratedRegex(
        @"(?<from>\bFROM\s+)(?:`?[A-Za-z_][A-Za-z0-9_]*`?\.)?`?(?<table>[A-Za-z_][A-Za-z0-9_]*)`?(?![A-Za-z0-9_.])",
        RegexOptions.CultureInvariant | RegexOptions.IgnoreCase)]
    private static partial Regex FromTable();

    [GeneratedRegex(
        @"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(?:IF\s+NOT\s+EXISTS\s+)?(?<name>[A-Za-z_][A-Za-z0-9_]*)",
        RegexOptions.CultureInvariant | RegexOptions.IgnoreCase)]
    private static partial Regex CreateFunction();
}
