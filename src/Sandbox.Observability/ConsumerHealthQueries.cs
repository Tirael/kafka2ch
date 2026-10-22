using System.Text.RegularExpressions;

namespace Sandbox.Observability;

public static partial class ConsumerHealthQueries
{
    public static string KafkaConsumers(string queueTable, string? cluster)
    {
        var source = cluster is null
            ? "system.kafka_consumers"
            : $"clusterAllReplicas('{Identifier(cluster, nameof(cluster))}', system.kafka_consumers)";

        return
            $"""
            SELECT
                hostName() AS host,
                consumer_id,
                assignments.topic AS topics,
                assignments.partition_id AS partitions,
                assignments.current_offset AS offsets,
                if(empty(exceptions.time), NULL, arrayMax(exceptions.time)) AS last_exception_time,
                if(empty(exceptions.text), NULL, exceptions.text[indexOf(exceptions.time, arrayMax(exceptions.time))]) AS last_exception,
                nullIf(last_poll_time, toDateTime(0)) AS last_poll_time,
                nullIf(last_commit_time, toDateTime(0)) AS last_commit_time,
                num_commits,
                is_currently_used
            FROM {source}
            WHERE database = currentDatabase() AND table = '{Identifier(queueTable, nameof(queueTable))}'
            ORDER BY host, consumer_id
            """;
    }

    public static string Errors(string errorsTable, TimeSpan window) =>
        $"""
        SELECT
            now() AS checked_at,
            countIf(received_at >= now() - INTERVAL {(long)window.TotalSeconds} SECOND) AS recent_errors,
            count() AS total_errors
        FROM {Identifier(errorsTable, nameof(errorsTable))}
        """;

    private static string Identifier(string value, string parameterName) =>
        IdentifierPattern().IsMatch(value)
            ? value
            : throw new ArgumentException($"'{value}' is not a valid ClickHouse identifier.", parameterName);

    [GeneratedRegex("^[A-Za-z_][A-Za-z0-9_]*$")]
    private static partial Regex IdentifierPattern();
}
