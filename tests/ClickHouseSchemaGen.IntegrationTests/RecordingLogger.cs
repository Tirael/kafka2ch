using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace ClickHouseSchemaGen.IntegrationTests;

public sealed class RecordingLogger : ILogger
{
    private readonly ConcurrentQueue<(LogLevel Level, string Message)> _entries = new();

    public IReadOnlyList<string> Warnings =>
        _entries.Where(entry => entry.Level == LogLevel.Warning).Select(entry => entry.Message).ToList();

    public IDisposable? BeginScope<TState>(TState state)
        where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter) =>
        _entries.Enqueue((logLevel, formatter(state, exception)));
}
