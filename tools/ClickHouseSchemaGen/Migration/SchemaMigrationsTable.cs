namespace ClickHouseSchemaGen.Migration;

public static class SchemaMigrationsTable
{
    public const string TableName = "schema_migrations";

    public const string MigrationKind = "migration";

    public const string InitKind = "init";

    public const string DefaultScriptFileName = "00_schema_migrations.sql";

    public const string CreateTableSql = """
        CREATE TABLE IF NOT EXISTS schema_migrations
        (
            version             String,
            name                String,
            checksum            String,
            applied_at          DateTime,
            kind                LowCardinality(String) DEFAULT 'migration'
        )
        ENGINE = MergeTree
        ORDER BY version
        """;

    // Upgrades for tables created by older releases. They are detected by schema, not recorded as rows,
    // so they must stay idempotent.
    public static IReadOnlyList<string> UpgradeStatements { get; } = UpgradeStatementsFor(ClusterDdl.SingleNode);

    /// <summary>
    /// In cluster mode the history is one replicated table spanning every node (no <c>{shard}</c> in its path),
    /// so the migrator sees the same rows whichever node it connects to.
    /// </summary>
    public static string CreateTableSqlFor(ClusterDdl cluster) =>
        cluster.Enabled
            ? CreateTableSql
                .Replace($"EXISTS {TableName}", $"EXISTS {TableName}{cluster.OnCluster}", StringComparison.Ordinal)
                .Replace("ENGINE = MergeTree", $"ENGINE = {cluster.HistoryEngine("MergeTree")}", StringComparison.Ordinal)
            : CreateTableSql;

    public static IReadOnlyList<string> UpgradeStatementsFor(ClusterDdl cluster) =>
    [
        $"ALTER TABLE {TableName}{cluster.OnCluster} ADD COLUMN IF NOT EXISTS kind LowCardinality(String) DEFAULT 'migration'"
    ];

    public static string InsertSql(
        string version,
        string name,
        string checksum,
        string kind,
        string appliedAtExpression = "now()") =>
        "INSERT INTO schema_migrations (version, name, checksum, applied_at, kind) VALUES " +
        $"('{EscapeSql(version)}', '{EscapeSql(name)}', '{EscapeSql(checksum)}', {appliedAtExpression}, '{EscapeSql(kind)}')";

    public static string AppendInitRecord(string outputPath, string sql)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(outputPath);
        ArgumentNullException.ThrowIfNull(sql);

        var fileName = Path.GetFileName(outputPath);
        var checksum = SchemaSnapshotSerializer.ComputeChecksum(sql);
        var record = InsertSql(ParseInitVersion(fileName), fileName, checksum, InitKind);
        return $"{sql.TrimEnd()}{Environment.NewLine}{Environment.NewLine}{record};{Environment.NewLine}";
    }

    public static string ParseInitVersion(string fileName)
    {
        var name = Path.GetFileNameWithoutExtension(fileName);
        var underscore = name.IndexOf('_');
        return underscore > 0 ? name[..underscore] : name;
    }

    private static string EscapeSql(string value) => value.Replace("'", "''", StringComparison.Ordinal);
}
