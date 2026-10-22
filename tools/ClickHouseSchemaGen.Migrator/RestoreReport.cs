namespace ClickHouseSchemaGen.Migrator;

public sealed record RestoreFailure(string ObjectName, string Reason);

public sealed class RestoreReport
{
    private readonly List<string> _restored = [];
    private readonly List<RestoreFailure> _notRestored = [];

    public IReadOnlyList<string> Restored => _restored;

    public IReadOnlyList<RestoreFailure> NotRestored => _notRestored;

    public bool IsEmpty => _restored.Count == 0 && _notRestored.Count == 0;

    public void AddRestored(string objectName) => _restored.Add(objectName);

    public void AddNotRestored(string objectName, string reason) => _notRestored.Add(new RestoreFailure(objectName, reason));

    public string Summary =>
        $"restored: {(_restored.Count == 0 ? "none" : string.Join(", ", _restored))}; "
            + $"not restored: {(_notRestored.Count == 0 ? "none" : string.Join("; ", _notRestored.Select(f => $"{f.ObjectName} ({f.Reason})")))}";
}

public sealed class MigrationFailedException(string migrationName, RestoreReport report, Exception innerException)
    : InvalidOperationException(
        $"Migration '{migrationName}' failed: {innerException.Message.Trim()} Recovery report: {report.Summary}.",
        innerException)
{
    public string MigrationName { get; } = migrationName;

    public RestoreReport Report { get; } = report;
}
