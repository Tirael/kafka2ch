namespace ClickHouseSchemaGen.Migration;

public static class SchemaMigrationsScriptGenerator
{
    public static string Generate(IReadOnlyList<MigrationFileInfo> migrations)
    {
        ArgumentNullException.ThrowIfNull(migrations);

        var builder = new StringBuilder()
            .AppendLine(SqlScriptWriter.GeneratedHeader)
            .AppendLine()
            .AppendLine("CREATE TABLE IF NOT EXISTS schema_migrations")
            .AppendLine("(")
            .AppendLine("    version             String,")
            .AppendLine("    name                String,")
            .AppendLine("    checksum            String,")
            .AppendLine("    applied_at          DateTime")
            .AppendLine(")")
            .AppendLine("ENGINE = MergeTree")
            .AppendLine("ORDER BY version;")
            .AppendLine();

        foreach (var migration in migrations.OrderBy(m => m.Version, StringComparer.Ordinal))
        {
            builder.AppendLine(
                "INSERT INTO schema_migrations (version, name, checksum, applied_at) VALUES " +
                $"('{EscapeSql(migration.Version)}', '{EscapeSql(migration.Name)}', '{EscapeSql(migration.Checksum)}', toDateTime(0));");
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

    private static string EscapeSql(string value) => value.Replace("'", "''", StringComparison.Ordinal);
}

public sealed record MigrationFileInfo(
    string Version,
    string Name,
    string Checksum,
    string Path);
