namespace Sandbox.App.Features.VerifyStoredMessages;

public static class ConfluentProtobufEnvelope
{
    public const int HeaderLength = 6;

    public static byte[] Payload(ReadOnlySpan<byte> framed)
    {
        if (framed.Length < HeaderLength || framed[0] != 0 || framed[HeaderLength - 1] != 0)
        {
            throw new InvalidDataException(
                "Kafka value is not a Confluent protobuf payload with a single message index.");
        }

        return framed[HeaderLength..].ToArray();
    }
}
