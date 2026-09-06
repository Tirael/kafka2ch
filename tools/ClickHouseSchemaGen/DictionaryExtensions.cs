namespace ClickHouseSchemaGen;

internal static class DictionaryExtensions
{
    public static Dictionary<TKey, TValue> MergeInto<TKey, TValue>(
        this Dictionary<TKey, TValue> target,
        IEnumerable<KeyValuePair<TKey, TValue>>? source)
        where TKey : notnull
    {
        if (source is null)
            return target;

        foreach (var (key, value) in source)
            target[key] = value;

        return target;
    }
}
