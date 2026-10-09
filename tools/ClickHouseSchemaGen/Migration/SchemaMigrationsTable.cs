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

    public static IReadOnlyList<string> UpgradeStatements { get; } = UpgradeStatementsFor(ClusterDdl.SingleNode);

    public static string CreateTableSqlFor(ClusterDdl cluster)
    {
        if (!cluster.Enabled)
            return CreateTableSql;

        return CreateTableSql
            .Replace(
                $"EXISTS {TableName}",
                $"EXISTS {cluster.HistoryTableName}{cluster.HistoryOnCluster}",
                StringComparison.Ordinal)
            .Replace(
                "ENGINE = MergeTree",
                $"ENGINE = {cluster.HistoryEngine("MergeTree")}",
                StringComparison.Ordinal);
    }

    public static IReadOnlyList<string> UpgradeStatementsFor(ClusterDdl cluster) =>
    [
        $"ALTER TABLE {cluster.HistoryTableName}{cluster.HistoryOnCluster} ADD COLUMN IF NOT EXISTS kind LowCardinality(String) DEFAULT 'migration'"
    ];

    public static string InsertSql(
        string version,
        string name,
        string checksum,
        string kind,
        string appliedAtExpression = "now()",
        string? tableName = null) =>
        $"INSERT INTO {tableName ?? TableName} (version, name, checksum, applied_at, kind) VALUES " +
        $"('{EscapeSql(version)}', '{EscapeSql(name)}', '{EscapeSql(checksum)}', {appliedAtExpression}, '{EscapeSql(kind)}')";

    public static string AppendInitRecord(string outputPath, string sql, ClusterDdl? cluster = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(outputPath);
        ArgumentNullException.ThrowIfNull(sql);

        var fileName = Path.GetFileName(outputPath);
        var checksum = SchemaSnapshotSerializer.ComputeChecksum(sql);
        var record = InsertSql(
            ParseInitVersion(fileName),
            fileName,
            checksum,
            InitKind,
            tableName: cluster?.HistoryTableName);
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
