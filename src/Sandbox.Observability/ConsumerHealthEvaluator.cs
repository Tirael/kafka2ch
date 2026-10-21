namespace Sandbox.Observability;

public static class ConsumerHealthEvaluator
{
    public static ConsumerHealthReport Evaluate(ConsumerHealthSnapshot snapshot, ConsumerHealthThresholds thresholds) =>
        new(ConsumerHealthStatus.Healthy, [], snapshot.Consumers.Count, 0, null, null, snapshot.RecentErrors, snapshot.TotalErrors);
}
