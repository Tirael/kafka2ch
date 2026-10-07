namespace ClickHouseSchemaGen.Migration;

public sealed class ProtoCompatibilityException : Exception
{
    public ProtoCompatibilityException(IReadOnlyList<string> violations)
        : base(BuildMessage(violations))
    {
        Violations = violations;
    }

    public IReadOnlyList<string> Violations { get; }

    private static string BuildMessage(IReadOnlyList<string> violations) =>
        violations.Count == 0
            ? "Protobuf schema is not backward-compatible."
            : "Protobuf schema is not backward-compatible:"
              + Environment.NewLine
              + string.Join(Environment.NewLine, violations.Select(v => $"  - {v}"));
}
