namespace ClickHouseSchemaGen.Descriptors;

public interface IMessageDescriptorSource
{
    MessageDescriptor Resolve(string messageType);
}
