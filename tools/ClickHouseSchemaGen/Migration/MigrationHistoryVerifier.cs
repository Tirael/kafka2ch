namespace ClickHouseSchemaGen.Migration;

public static class MigrationHistoryVerifier
{
    public static MigrationHistoryVerification Verify(
        IReadOnlyList<string> migrationsDirectories,
        string? baselineVersion)
    {
        ArgumentNullException.ThrowIfNull(migrationsDirectories);

        var versionsByDirectory = migrationsDirectories
            .Select(directory => (
                Directory: directory,
                Versions: SchemaMigrationsScriptGenerator.ReadFromDirectory(directory)
                    .Select(migration => migration.Version)
                    .Where(version => IsAfterBaseline(version, baselineVersion))
                    .ToHashSet(StringComparer.Ordinal)))
            .ToList();

        var allVersions = versionsByDirectory
            .SelectMany(entry => entry.Versions)
            .ToHashSet(StringComparer.Ordinal);

        var gaps = versionsByDirectory
            .Select(entry => new MigrationVersionGap(
                entry.Directory,
                allVersions.Where(version => !entry.Versions.Contains(version))
                    .Order(StringComparer.Ordinal)
                    .ToList()))
            .Where(gap => gap.MissingVersions.Count > 0)
            .ToList();

        return new MigrationHistoryVerification(baselineVersion, gaps);
    }

    private static bool IsAfterBaseline(string version, string? baselineVersion) =>
        string.IsNullOrWhiteSpace(baselineVersion)
        || string.CompareOrdinal(version, baselineVersion) > 0;
}

public sealed record MigrationHistoryVerification(
    string? BaselineVersion,
    IReadOnlyList<MigrationVersionGap> Gaps)
{
    public bool IsConsistent => Gaps.Count == 0;
}

public sealed record MigrationVersionGap(
    string MigrationsDirectory,
    IReadOnlyList<string> MissingVersions);
