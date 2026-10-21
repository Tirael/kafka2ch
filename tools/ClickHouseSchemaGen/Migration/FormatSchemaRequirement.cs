using System.Diagnostics.CodeAnalysis;

namespace ClickHouseSchemaGen.Migration;

public sealed partial record FormatSchemaRequirement(string FileName, string Checksum)
{
    public const string ProtoExtension = ".proto";

    public string ToMarker() => $"-- require:format_schema {FileName} sha256={Checksum}";

    public static bool TryParse(string line, [NotNullWhen(true)] out FormatSchemaRequirement? requirement)
    {
        requirement = null;
        if (line is null)
            return false;

        var match = MarkerRegex().Match(line);
        if (!match.Success)
            return false;

        var fileName = match.Groups["file"].Value;
        if (fileName.Split('/').Any(segment => segment is "" or "." or ".."))
            return false;

        requirement = new FormatSchemaRequirement(fileName, match.Groups["hash"].Value.ToLowerInvariant());
        return true;
    }

    public static string FileNameFor(string protoFile)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(protoFile);
        var trimmed = protoFile.Trim();
        return trimmed.EndsWith(ProtoExtension, StringComparison.OrdinalIgnoreCase)
            ? trimmed
            : trimmed + ProtoExtension;
    }

    public static IReadOnlyDictionary<string, string> ComputeChecksums(
        IEnumerable<KafkaTableConfig> kafkaTables,
        string formatSchemasDirectory)
    {
        ArgumentNullException.ThrowIfNull(kafkaTables);
        ArgumentException.ThrowIfNullOrWhiteSpace(formatSchemasDirectory);

        var checksums = new SortedDictionary<string, string>(StringComparer.Ordinal);
        foreach (var fileName in kafkaTables.Select(table => FileNameFor(table.ProtoFile)).Distinct(StringComparer.Ordinal))
        {
            var path = Path.Combine(formatSchemasDirectory, fileName);
            if (!File.Exists(path))
            {
                throw new FileNotFoundException(
                    $"Format schema '{fileName}' not found in '{formatSchemasDirectory}'.",
                    path);
            }

            checksums[fileName] = SqlChecksum.ComputeFile(path);
        }

        return checksums;
    }

    [GeneratedRegex(
        @"^\s*--\s*require:format_schema\s+(?<file>[A-Za-z0-9_][A-Za-z0-9_./-]*\.proto)\s+sha256=(?<hash>[0-9A-Fa-f]{64})\s*$",
        RegexOptions.CultureInvariant | RegexOptions.IgnoreCase)]
    private static partial Regex MarkerRegex();
}
