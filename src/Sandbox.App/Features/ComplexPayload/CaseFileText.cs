using ProtobufTimestamp = Google.Protobuf.WellKnownTypes.Timestamp;

namespace Sandbox.App.Features.ComplexPayload;

internal static class CaseFileText
{
    private const string Alphabet = "abcdefghijklmnopqrstuvwxyz0123456789 ";

    public static string Create(int seed, int length)
    {
        if (length <= 0)
            return string.Empty;

        var buffer = new char[length];
        var state = unchecked(seed * 397 ^ 0x51_ED_17);
        for (var i = 0; i < buffer.Length; i++)
        {
            state = unchecked(state * 1103515245 + 12345);
            buffer[i] = Alphabet[((state >>> 16) & 0x7FFF) % Alphabet.Length];
        }

        return new string(buffer);
    }

    public static JournalEntry JournalEntry(int index, int bodyLength)
    {
        var entryId = $"journal-{index:D4}";
        var body = $"{entryId}|{Create(index + 3, Math.Max(0, bodyLength))}";
        var entry = new JournalEntry
        {
            EntryId = entryId,
            RecordedAt = ProtobufTimestamp.FromDateTimeOffset(DateTimeOffset.UnixEpoch.AddMinutes(index + 1)),
            Actor = index % 2 == 0 ? "clerk" : "reviewer",
            Body = body
        };
        entry.Tags.Add(index % 3 == 0 ? "audit" : "note");
        entry.Tags.Add($"seq-{index}");
        entry.Context["channel"] = index % 2 == 0 ? "desk" : "batch";
        entry.Context["seq"] = index.ToString();
        return entry;
    }
}
