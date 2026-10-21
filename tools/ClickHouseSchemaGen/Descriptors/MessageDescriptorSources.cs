namespace ClickHouseSchemaGen.Descriptors;

public static class MessageDescriptorSources
{
    public static IMessageDescriptorSource For(CodegenConfig config) =>
        string.IsNullOrWhiteSpace(config.DescriptorSetPath)
            ? AssemblyMessageDescriptorSource.Instance
            : DescriptorSetMessageDescriptorSource.Load(config.DescriptorSetPath);
}
