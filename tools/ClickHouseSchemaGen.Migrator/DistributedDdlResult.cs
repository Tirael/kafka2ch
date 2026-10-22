namespace ClickHouseSchemaGen.Migrator;

public sealed record DistributedDdlHostStatus(string Host, long? Status, string? Error, long HostsRemaining);

public sealed class DistributedDdlException(string message, IReadOnlyList<string> failedHosts)
    : InvalidOperationException(message)
{
    public IReadOnlyList<string> FailedHosts { get; } = failedHosts;
}

public static partial class DistributedDdlResult
{
    public const string OutputModeSetting = "distributed_ddl_output_mode";

    public const string OutputMode = "never_throw";

    public static bool IsDistributedDdl(string statement) => OnClusterRegex().IsMatch(statement);

    public static DistributedDdlHostStatus ReadHost(IDataRecord record)
    {
        var ordinals = Enumerable.Range(0, record.FieldCount)
            .ToDictionary(record.GetName, i => i, StringComparer.OrdinalIgnoreCase);

        string? Text(string name) =>
            ordinals.TryGetValue(name, out var i) && !record.IsDBNull(i)
                ? Convert.ToString(record.GetValue(i), CultureInfo.InvariantCulture)
                : null;

        long? Number(string name) =>
            ordinals.TryGetValue(name, out var i) && !record.IsDBNull(i)
                ? Convert.ToInt64(record.GetValue(i), CultureInfo.InvariantCulture)
                : null;

        var host = Text("host") is { } name
            ? Text("port") is { } port ? $"{name}:{port}" : name
            : string.Join('|', new[] { Text("shard"), Text("replica") }.OfType<string>());

        return new DistributedDdlHostStatus(
            string.IsNullOrEmpty(host) ? "<unknown host>" : host,
            Number("status"),
            Text("error"),
            Number("num_hosts_remaining") ?? 0);
    }

    public static void EnsureSucceeded(string statement, IReadOnlyList<DistributedDdlHostStatus> hosts)
    {
        var failed = hosts.Where(h => h.Status is not 0).ToList();
        var remaining = hosts.Count == 0 ? 0 : hosts[^1].HostsRemaining;
        if (failed.Count == 0 && remaining == 0)
            return;

        var succeeded = hosts.Where(h => h.Status is 0).Select(h => h.Host).ToList();
        var failures = failed.Select(Describe).ToList();
        if (failed.Count == 0)
            failures.Add($"{remaining} host(s) did not report");

        throw new DistributedDdlException(
            $"Distributed DDL did not succeed on every host. Failed: {string.Join("; ", failures)}. "
                + $"Succeeded: {(succeeded.Count == 0 ? "none" : string.Join(", ", succeeded))}. "
                + $"Statement: {Shorten(statement)}",
            failed.Select(h => h.Host).ToList());
    }

    private static string Describe(DistributedDdlHostStatus host) =>
        host.Status is null
            ? $"{host.Host} (not finished before distributed_ddl_task_timeout)"
            : $"{host.Host} (code {host.Status}: {Shorten(host.Error ?? string.Empty)})";

    private static string Shorten(string text)
    {
        var singleLine = WhitespaceRegex().Replace(text, " ").Trim();
        return singleLine.Length <= 300 ? singleLine : singleLine[..300] + "...";
    }

    [GeneratedRegex(@"\bON\s+CLUSTER\s+[A-Za-z_`'""]", RegexOptions.CultureInvariant | RegexOptions.IgnoreCase)]
    private static partial Regex OnClusterRegex();

    [GeneratedRegex(@"\s+", RegexOptions.CultureInvariant)]
    private static partial Regex WhitespaceRegex();
}
