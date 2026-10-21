namespace Sandbox.Observability;

public static class ConsumerHealthEvaluator
{
    public static ConsumerHealthReport Evaluate(ConsumerHealthSnapshot snapshot, ConsumerHealthThresholds thresholds)
    {
        List<(ConsumerHealthStatus Status, string Message)> issues = [];
        var now = snapshot.CheckedAt;
        var assigned = snapshot.Consumers.Where(consumer => consumer.Assignments.Count > 0).ToList();
        var assignments = assigned.SelectMany(consumer => consumer.Assignments).ToList();
        var knownLags = assignments.Select(assignment => assignment.Lag).OfType<long>().ToList();
        long? lag = knownLags.Count > 0 ? knownLags.Sum() : null;
        var lastCommit = assigned.Max(consumer => consumer.LastCommitTime);

        if (snapshot.Consumers.Count == 0)
        {
            issues.Add((ConsumerHealthStatus.Unhealthy, "no Kafka consumers registered for the queue table"));
        }
        else if (assignments.Count == 0)
        {
            issues.Add((ConsumerHealthStatus.Unhealthy,
                $"no partitions assigned to {snapshot.Consumers.Count} Kafka consumer(s)"));
        }

        foreach (var consumer in assigned.Where(consumer => IsOlderThan(consumer.LastPollTime, now, thresholds.MaxPollAge)))
        {
            issues.Add((ConsumerHealthStatus.Unhealthy,
                $"consumer {consumer.Host}/{consumer.ConsumerId} has not polled since {Format(consumer.LastPollTime)}"));
        }

        if (assignments.Count > 0 && IsOlderThan(lastCommit, now, thresholds.MaxCommitAge))
        {
            var message = $"no commit since {Format(lastCommit)} (limit {Minutes(thresholds.MaxCommitAge)})";
            if (lag is null)
                issues.Add((ConsumerHealthStatus.Degraded, $"{message}, lag unknown"));
            else if (lag > 0)
                issues.Add((ConsumerHealthStatus.Unhealthy, $"{message}, lag {lag}"));
        }

        foreach (var consumer in snapshot.Consumers.Where(consumer =>
                     consumer.LastExceptionTime is { } time && now - time <= thresholds.ErrorWindow))
        {
            issues.Add((ConsumerHealthStatus.Degraded,
                $"consumer {consumer.Host}/{consumer.ConsumerId} exception at {Format(consumer.LastExceptionTime)}: " +
                consumer.LastException));
        }

        if (snapshot.RecentErrors > 0)
        {
            issues.Add((ConsumerHealthStatus.Degraded,
                $"{snapshot.RecentErrors} parse errors in the last {Minutes(thresholds.ErrorWindow)}"));
        }

        return new ConsumerHealthReport(
            issues.Count == 0 ? ConsumerHealthStatus.Healthy : issues.Max(issue => issue.Status),
            issues.Select(issue => issue.Message).ToList(),
            snapshot.Consumers.Count,
            assignments.Count,
            lag,
            lastCommit,
            snapshot.RecentErrors,
            snapshot.TotalErrors);
    }

    private static bool IsOlderThan(DateTime? time, DateTime now, TimeSpan limit) => time is null || now - time > limit;

    private static string Format(DateTime? time) => time is { } value ? value.ToString("u") : "never";

    private static string Minutes(TimeSpan span) => $"{span.TotalMinutes:0.#} min";
}
