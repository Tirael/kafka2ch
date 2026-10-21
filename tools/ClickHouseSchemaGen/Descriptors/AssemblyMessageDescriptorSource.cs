namespace ClickHouseSchemaGen.Descriptors;

public sealed class AssemblyMessageDescriptorSource : IMessageDescriptorSource
{
    public static AssemblyMessageDescriptorSource Instance { get; } = new();

    private AssemblyMessageDescriptorSource()
    {
    }

    public MessageDescriptor Resolve(string messageType) =>
        ProtoDescriptorResolver.ResolveDescriptor(messageType);
}
