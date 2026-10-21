namespace ClickHouseSchemaGen.Descriptors;

public sealed class DescriptorSetMessageDescriptorSource : IMessageDescriptorSource
{
    public static DescriptorSetMessageDescriptorSource Load(string path) =>
        throw new NotImplementedException();

    public MessageDescriptor Resolve(string messageType) =>
        throw new NotImplementedException();
}
