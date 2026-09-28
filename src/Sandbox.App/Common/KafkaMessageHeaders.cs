using System.Text;

namespace Sandbox.App.Common;

internal static class KafkaMessageHeaders
{
    public static Headers ForEvent(string eventType) =>
        new()
        {
            { "source", "sandbox-app"u8.ToArray() },
            { "event-type", Encoding.UTF8.GetBytes(eventType) }
        };
}
