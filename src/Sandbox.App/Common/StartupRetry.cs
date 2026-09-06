namespace Sandbox.App.Common;

public static class StartupRetry
{
    public static T Execute<T>(Func<T> action, RetryContext context) =>
        ExecuteAsync(() => Task.FromResult(action()), context).GetAwaiter().GetResult();

    public static Task<T> ExecuteAsync<T>(Func<Task<T>> action, RetryContext context) =>
        RetryLoop(action, context);

    private static async Task<T> RetryLoop<T>(Func<Task<T>> action, RetryContext context)
    {
        var delay = TimeSpan.FromSeconds(1);
        var maxDelay = TimeSpan.FromSeconds(30);

        while (true)
        {
            context.CancellationToken.ThrowIfCancellationRequested();

            try
            {
                return await action();
            }
            catch (Exception ex)
            {
                context.Logger.LogWarning(ex, "Startup failed, retrying in {Delay}", delay);
                await Task.Delay(delay, context.TimeProvider, context.CancellationToken);
                delay = NextDelay(delay, maxDelay);
            }
        }
    }

    private static TimeSpan NextDelay(TimeSpan current, TimeSpan maxDelay) =>
        TimeSpan.FromMilliseconds(Math.Min(current.TotalMilliseconds * 2, maxDelay.TotalMilliseconds));
}
