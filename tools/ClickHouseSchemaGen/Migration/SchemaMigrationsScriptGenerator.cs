namespace ClickHouseSchemaGen.Migration;

public static class SchemaMigrationsScriptGenerator
{
    public static string Generate(IReadOnlyList<MigrationFileInfo> migrations)
    {
        ArgumentNullException.ThrowIfNull(migrations);

        var builder = new StringBuilder()
            .AppendLine(SqlScriptWriter.GeneratedHeader)
            .AppendLine("-- Must sort before every init script: they record themselves into this table.")
            .AppendLine()
            .Append(SchemaMigrationsTable.CreateTableSql).AppendLine(";")
            .AppendLine();

        foreach (var migration in migrations.OrderBy(m => m.Version, StringComparer.Ordinal))
        {
            builder.Append(SchemaMigrationsTable.InsertSql(
                    migration.Version,
                    migration.Name,
                    migration.Checksum,
                    SchemaMigrationsTable.MigrationKind,
                    "toDateTime(0)"))
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
