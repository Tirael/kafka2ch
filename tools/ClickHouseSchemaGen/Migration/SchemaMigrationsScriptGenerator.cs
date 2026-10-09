namespace ClickHouseSchemaGen.Migration;

public static class SchemaMigrationsScriptGenerator
{
    public static string Generate(IReadOnlyList<MigrationFileInfo> migrations, ClusterDdl? cluster = null)
    {
        ArgumentNullException.ThrowIfNull(migrations);

        var ddl = cluster ?? ClusterDdl.SingleNode;
        var builder = new StringBuilder();

        if (ddl.UsesReplicatedDatabase)
            builder.Append(ddl.CreateReplicatedDatabaseSql()).AppendLine();

        builder
            .Append(SchemaMigrationsTable.CreateTableSqlFor(ddl)).AppendLine(";")
            .AppendLine();

        if (ddl.UsesReplicatedDatabase)
            builder.AppendLine(ddl.UseReplicatedDatabaseSql()).AppendLine();

        foreach (var migration in migrations.OrderBy(m => m.Version, StringComparer.Ordinal))
        {
            builder.Append(SchemaMigrationsTable.InsertSql(
                    migration.Version,
                    migration.Name,
                    migration.Checksum,
                    SchemaMigrationsTable.MigrationKind,
                    "toDateTime(0)",
                    ddl.HistoryTableName))
                .AppendLine(";");
        }

        return builder.ToString();
    }

    public static IReadOnlyList<MigrationFileInfo> ReadFromDirectory(string migrationsDirectory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(migrationsDirectory);

        if (!Directory.Exists(migrationsDirectory))
            return [];

        return Directory.EnumerateFiles(migrationsDirectory, "*.sql", SearchOption.TopDirectoryOnly)
            .Select(path =>
            {
                var fileName = Path.GetFileNameWithoutExtension(path);
                var underscore = fileName.IndexOf('_');
                if (underscore <= 0)
                    throw new InvalidOperationException($"Migration file '{path}' must be named '<version>_<name>.sql'.");

                var version = fileName[..underscore];
                var name = fileName[(underscore + 1)..];
                var checksum = SchemaSnapshotSerializer.ComputeChecksum(File.ReadAllText(path));
                return new MigrationFileInfo(version, name, checksum, path);
            })
            .OrderBy(m => m.Version, StringComparer.Ordinal)
            .ToList();
    }
}

public sealed record MigrationFileInfo(
    string Version,
    string Name,
    string Checksum,
    string Path);
