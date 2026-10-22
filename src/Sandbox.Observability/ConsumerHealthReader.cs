using System.Data.Common;
using ClickHouse.Client.ADO;

namespace Sandbox.Observability;

public sealed record ConsumerHealthQuery(string QueueTable, string ErrorsTable, string? Cluster, TimeSpan ErrorWindow);

public static class ConsumerHealthReader
{
    public static async Task<ConsumerHealthSnapshot> ReadAsync(
        ClickHouseConnection connection,
        ConsumerHealthQuery query,
        CancellationToken cancellationToken)
    {
        var (checkedAt, recentErrors, totalErrors) = await ReadErrorsAsync(connection, query, cancellationToken);
        var consumers = await ReadConsumersAsync(connection, query, cancellationToken);
        return new ConsumerHealthSnapshot(checkedAt, consumers, recentErrors, totalErrors);
    }

    private static async Task<(DateTime CheckedAt, ulong Recent, ulong Total)> ReadErrorsAsync(
        ClickHouseConnection connection,
        ConsumerHealthQuery query,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = ConsumerHealthQueries.Errors(query.ErrorsTable, query.ErrorWindow);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        return (reader.GetDateTime(0), Convert.ToUInt64(reader.GetValue(1)), Convert.ToUInt64(reader.GetValue(2)));
    }

    private static async Task<IReadOnlyList<KafkaConsumerState>> ReadConsumersAsync(
        ClickHouseConnection connection,
        ConsumerHealthQuery query,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = ConsumerHealthQueries.KafkaConsumers(query.QueueTable, query.Cluster);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        List<KafkaConsumerState> consumers = [];
        while (await reader.ReadAsync(cancellationToken))
        {
            var topics = (Array)reader.GetValue(2);
            var partitions = (Array)reader.GetValue(3);
            var offsets = (Array)reader.GetValue(4);
            var assignments = Enumerable.Range(0, topics.Length)
                .Select(index => new KafkaPartitionAssignment(
                    Convert.ToString(topics.GetValue(index))!,
                    Convert.ToInt32(partitions.GetValue(index)),
                    Convert.ToInt64(offsets.GetValue(index))))
                .ToList();

            consumers.Add(new KafkaConsumerState(
                reader.GetString(0),
                reader.GetString(1),
                assignments,
                NullableDateTime(reader, 5),
                reader.IsDBNull(6) ? null : reader.GetString(6),
                NullableDateTime(reader, 7),
                NullableDateTime(reader, 8),
                Convert.ToUInt64(reader.GetValue(9)),
                Convert.ToBoolean(reader.GetValue(10))));
        }

        return consumers;
    }

    private static DateTime? NullableDateTime(DbDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetDateTime(ordinal);
}
