namespace Sandbox.App.Common;

public sealed record RetryContext(
    ILogger Logger,
    TimeProvider TimeProvider,
    CancellationToken CancellationToken = default);
