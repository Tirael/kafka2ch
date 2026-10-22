namespace ClickHouseSchemaGen.Snapshot;

public static class KafkaKeyComparison
{
    public static IReadOnlyList<string> Compare(KafkaKeySnapshot? oldKey, KafkaKeySnapshot? newKey)
    {
        oldKey ??= new KafkaKeySnapshot();
        newKey ??= new KafkaKeySnapshot();

        List<string> reasons = [];

        if (!string.Equals(oldKey.Format, newKey.Format, StringComparison.OrdinalIgnoreCase))
            reasons.Add($"key format changed ('{oldKey.Format}' -> '{newKey.Format}')");

        if (!string.Equals(oldKey.MessageType, newKey.MessageType, StringComparison.Ordinal))
            reasons.Add("key messageType changed");

        if (oldKey.SkipBytes != newKey.SkipBytes)
            reasons.Add("key skipBytes changed");

        return reasons;
    }
}
