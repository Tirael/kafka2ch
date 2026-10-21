namespace ClickHouseSchemaGen.Descriptors;

internal static class ProtoDescriptorResolver
{
    public static MessageDescriptor ResolveDescriptor(string messageType)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageType);

        var type = Type.GetType(messageType, throwOnError: true)
            ?? throw new InvalidOperationException($"Message type '{messageType}' was not found.");

        if (!typeof(IMessage).IsAssignableFrom(type))
            throw new InvalidOperationException($"Type '{messageType}' is not a protobuf message.");

        var descriptorProperty = type.GetProperty(
            "Descriptor",
            BindingFlags.Public | BindingFlags.Static)
            ?? throw new InvalidOperationException($"Type '{messageType}' has no Descriptor property.");

        return (MessageDescriptor)descriptorProperty.GetValue(null)!;
    }
}
