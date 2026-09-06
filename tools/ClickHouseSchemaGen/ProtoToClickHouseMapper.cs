using ClickHouseSchemaGen.Mapping;

namespace ClickHouseSchemaGen;

public sealed class ProtoToClickHouseMapper(DenormalizationPlanner planner)
{
    public ProtoToClickHouseMapper()
        : this(new DenormalizationPlanner())
    {
    }

    public IReadOnlyList<ClickHouseColumn> MapMessage(
        MessageDescriptor descriptor,
        IReadOnlyDictionary<string, FieldOverrideConfig> overrides) =>
        MapMessage(descriptor, new CodegenDefaults(), overrides);

    public IReadOnlyList<ClickHouseColumn> MapMessage(
        MessageDescriptor descriptor,
        CodegenDefaults defaults,
        IReadOnlyDictionary<string, FieldOverrideConfig> overrides) =>
        planner.MapMessage(descriptor, defaults, overrides);

    public static MessageDescriptor ResolveDescriptor(string messageType) =>
        ProtoDescriptorResolver.ResolveDescriptor(messageType);
}
