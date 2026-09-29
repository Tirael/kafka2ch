namespace Sandbox.App.Features.ComplexPayload;

public static class PayloadSize
{
    public const int EnvelopeBytes = 6;

    public const int MinBytes = 150 * 1024;

    public const int MaxBytes = 200 * 1024;

    public static int KafkaValueBytes(IMessage message) => message.CalculateSize() + EnvelopeBytes;

    public static void Fit(IMessage root, CaseFile file)
    {
        for (var attempt = 0; attempt < 10_000; attempt++)
        {
            var size = KafkaValueBytes(root);
            if (size >= MinBytes && size <= MaxBytes)
                return;

            if (size < MinBytes)
            {
                Grow(file, MinBytes - size);
                continue;
            }

            Shrink(file, size - MaxBytes);
        }

        throw new InvalidOperationException(
            $"Payload is {KafkaValueBytes(root)} bytes, outside {MinBytes}..{MaxBytes}.");
    }

    private static void Grow(CaseFile file, int deficit)
    {
        if (file.Journal.Count > 0 && deficit < 4096)
        {
            file.Journal[^1].Body += CaseFileText.Create(file.Journal.Count + 17, deficit + 8);
            return;
        }

        file.Journal.Add(CaseFileText.JournalEntry(file.Journal.Count, Math.Clamp(deficit, 256, 4096)));
    }

    private static void Shrink(CaseFile file, int overflow)
    {
        if (file.Journal.Count == 0)
        {
            throw new InvalidOperationException(
                "Payload exceeds the upper bound before the journal can be trimmed.");
        }

        var last = file.Journal[^1];
        if (last.Body.Length > overflow + 32)
        {
            last.Body = last.Body[..(last.Body.Length - overflow)];
            return;
        }

        file.Journal.RemoveAt(file.Journal.Count - 1);
    }
}
