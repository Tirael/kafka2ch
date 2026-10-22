using System.Globalization;

namespace ClickHouseSchemaGen.Migration;

public static partial class MigrationVersion
{
    public const string Format = "yyyyMMddHHmmss";

    public static string FromTime(DateTimeOffset time) =>
        time.UtcDateTime.ToString(Format, CultureInfo.InvariantCulture);

    public static bool TryParse(string version, out DateTimeOffset time)
    {
        var parsed = DateTime.TryParseExact(
            version,
            Format,
            CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
            out var dateTime);
        time = parsed ? new DateTimeOffset(dateTime, TimeSpan.Zero) : default;
        return parsed;
    }

    public static string Next(string? latest, DateTimeOffset now)
    {
        var candidate = FromTime(now);
        if (latest is null || string.CompareOrdinal(candidate, latest) > 0)
            return candidate;

        if (!TryParse(latest, out var latestTime))
        {
            throw new InvalidOperationException(
                $"Cannot derive a version after '{latest}'; pass --version <{Format}>.");
        }

        return FromTime(latestTime.AddSeconds(1));
    }

    public static string? ReadHeader(string sql, MigrationHeaderField field)
    {
        var match = field switch
        {
            MigrationHeaderField.Migration => MigrationHeaderRegex().Match(sql),
            MigrationHeaderField.ParentChecksum => ParentChecksumHeaderRegex().Match(sql),
            MigrationHeaderField.TargetChecksum => TargetChecksumHeaderRegex().Match(sql),
            _ => throw new ArgumentOutOfRangeException(nameof(field), field, null)
        };
        return match.Success ? match.Groups["value"].Value : null;
    }

    public static string RewriteMigrationHeader(string sql, string migrationName) =>
        MigrationHeaderRegex().Replace(sql, $"-- Migration: {migrationName}", 1);

    [GeneratedRegex(@"^-- Migration: (?<value>[^\r\n]*)", RegexOptions.Multiline | RegexOptions.CultureInvariant)]
    private static partial Regex MigrationHeaderRegex();

    [GeneratedRegex(@"^-- Parent checksum: (?<value>[0-9a-fA-F]+)", RegexOptions.Multiline | RegexOptions.CultureInvariant)]
    private static partial Regex ParentChecksumHeaderRegex();

    [GeneratedRegex(@"^-- Target checksum: (?<value>[0-9a-fA-F]+)", RegexOptions.Multiline | RegexOptions.CultureInvariant)]
    private static partial Regex TargetChecksumHeaderRegex();
}

public enum MigrationHeaderField
{
    Migration,
    ParentChecksum,
    TargetChecksum
}
