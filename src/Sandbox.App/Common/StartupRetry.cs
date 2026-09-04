namespace Sandbox.App.Common;

public static class StartupRetry
{
    public static T Execute<T>(
        Func<T> action,
        ILogger logger,
        TimeProvider timeProvider,
        CancellationToken cancellationToken = default)
    {
        var delay = TimeSpan.FromSeconds(1);
        var maxDelay = TimeSpan.FromSeconds(30);

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                return action();
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Startup failed, retrying in {Delay}", delay);
                Task.Delay(delay, timeProvider, cancellationToken).GetAwaiter().GetResult();
                delay = TimeSpan.FromMilliseconds(Math.Min(delay.TotalMilliseconds * 2, maxDelay.TotalMilliseconds));
            }
        }
    }

    public static async Task<T> ExecuteAsync<T>(
        Func<Task<T>> action,
        ILogger logger,
        TimeProvider timeProvider,
        CancellationToken cancellationToken = default)
    {
        var delay = TimeSpan.FromSeconds(1);
        var maxDelay = TimeSpan.FromSeconds(30);

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                return await action();
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Startup failed, retrying in {Delay}", delay);
                await Task.Delay(delay, timeProvider, cancellationToken);
                delay = TimeSpan.FromMilliseconds(Math.Min(delay.TotalMilliseconds * 2, maxDelay.TotalMilliseconds));
            }
        }
    }
}
