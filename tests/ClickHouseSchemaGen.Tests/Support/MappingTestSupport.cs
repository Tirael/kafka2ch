namespace ClickHouseSchemaGen.Tests.Support;

internal static class MappingTestSupport
{
    public static readonly IReadOnlyDictionary<string, FieldOverrideConfig> EmptyOverrides =
        new Dictionary<string, FieldOverrideConfig>();

    public static IReadOnlyList<ClickHouseColumn> MapFixture(
        MessageDescriptor descriptor,
        IReadOnlyDictionary<string, FieldOverrideConfig>? overrides = null) =>
        new DenormalizationPlanner().MapMessage(
            descriptor,
            OrdersQueueTestConfig.Defaults,
            overrides ?? EmptyOverrides);

    public static IEnumerable<(string Name, string Type)> NameAndTypes(IReadOnlyList<ClickHouseColumn> columns) =>
        columns.Select(column => (column.Name, column.Type));
}
