using System.Text.RegularExpressions;

namespace Sandbox.App.Features.ReadAggregates;

public sealed partial class ReadAggregatesOptions
{
    public const string SectionName = "ReadAggregates";

    public int IntervalMs { get; set; } = 5000;

    public int WindowMinutes { get; set; } = 10;

    public string? Database { get; set; }

    public bool HasValidDatabase => Database is null || IdentifierRegex().IsMatch(Database);

    [GeneratedRegex("^[A-Za-z_][A-Za-z0-9_]*$")]
    private static partial Regex IdentifierRegex();
}
