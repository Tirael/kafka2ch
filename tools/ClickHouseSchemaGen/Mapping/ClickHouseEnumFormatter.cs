namespace ClickHouseSchemaGen.Mapping;

internal static class ClickHouseEnumFormatter
{
    public static string FormatValue(string name, int number) =>
        $"'{name}' = {number}";

    public static string JoinValues(IEnumerable<(string Name, int Number)> values) =>
        string.Join(", ", values.Select(value => FormatValue(value.Name, value.Number)));
}
