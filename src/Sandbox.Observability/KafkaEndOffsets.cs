using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Sandbox.Observability;

public static class KafkaEndOffsets
{
    public static async Task<ConsumerHealthSnapshot> WithEndOffsetsAsync(
        this ConsumerHealthSnapshot snapshot,
        IAdminClient admin,
        TimeSpan timeout)
    {
        var partitions = snapshot.Consumers
            .SelectMany(consumer => consumer.Assignments)
            .Select(assignment => new TopicPartition(assignment.Topic, new Partition(assignment.Partition)))
            .Distinct()
            .ToList();
        if (partitions.Count == 0)
            return snapshot;

        var result = await admin.ListOffsetsAsync(
            partitions.Select(partition => new TopicPartitionOffsetSpec
            {
                TopicPartition = partition,
                OffsetSpec = OffsetSpec.Latest()
            }),
            new ListOffsetsOptions { RequestTimeout = timeout });

        var endOffsets = result.ResultInfos
            .Select(info => info.TopicPartitionOffsetError)
            .Where(offset => !offset.Error.IsError)
            .ToDictionary(offset => offset.TopicPartition, offset => offset.Offset.Value);

        return snapshot with
        {
            Consumers = snapshot.Consumers
                .Select(consumer => consumer with
                {
                    Assignments = consumer.Assignments
                        .Select(assignment => assignment with
                        {
                            EndOffset = endOffsets.TryGetValue(
                                new TopicPartition(assignment.Topic, new Partition(assignment.Partition)),
                                out var end)
                                ? end
                                : null
                        })
                        .ToList()
                })
                .ToList()
        };
    }
}
