using System.Security.Cryptography;

namespace ClickHouseSchemaGen.Generation;

/// <summary>
/// The single checksum used for SQL scripts (init self-records, baseline rows, applied migrations).
/// Hashes <see cref="Canonicalize"/> so the value does not depend on the OS or git checkout
/// (CRLF vs LF, BOM, trailing whitespace, trailing blank lines).
/// </summary>
public static class SqlChecksum
{
    private static readonly UTF8Encoding Utf8NoBom = new(encoderShouldEmitUTF8Identifier: false);

    public static string Compute(string sql)
    {
        ArgumentNullException.ThrowIfNull(sql);
        return Hash(Utf8NoBom.GetBytes(Canonicalize(sql)));
    }

    public static string ComputeFile(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        return Compute(File.ReadAllText(path));
    }

    public static string Canonicalize(string sql)
    {
        ArgumentNullException.ThrowIfNull(sql);

        var lines = SqlText.Normalize(sql.TrimStart('\uFEFF')).Split('\n');
        var builder = new StringBuilder(sql.Length);
        var pendingBlankLines = 0;
        foreach (var line in lines)
        {
            var trimmed = line.TrimEnd();
            if (trimmed.Length == 0)
            {
                pendingBlankLines++;
                continue;
            }

            if (builder.Length > 0)
                builder.Append('\n', pendingBlankLines);
            pendingBlankLines = 0;
            builder.Append(trimmed).Append(SqlText.NewLine);
        }

        return builder.ToString();
    }

    private static string Hash(byte[] bytes) => Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();
}
