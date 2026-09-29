using System.Collections;
using System.Globalization;
using Google.Protobuf.Reflection;

namespace Sandbox.App.Features.VerifyStoredMessages;

public static class MessageContentDiff
{
    private const int MaxDiffs = 25;

    public static IReadOnlyList<string> Compare(IMessage expected, IMessage actual)
    {
        List<string> diffs = [];
        CompareMessage(expected, actual, "$", diffs);
        if (diffs.Count > MaxDiffs)
        {
            var hidden = diffs.Count - MaxDiffs;
            diffs.RemoveRange(MaxDiffs, hidden);
            diffs.Add($"... {hidden} more differences");
        }

        return diffs;
    }

    private static void CompareMessage(IMessage expected, IMessage actual, string path, List<string> diffs)
    {
        if (expected.Descriptor.FullName != actual.Descriptor.FullName)
        {
            diffs.Add($"{path} type kafka={expected.Descriptor.FullName} clickhouse={actual.Descriptor.FullName}");
            return;
        }

        foreach (var field in expected.Descriptor.Fields.InFieldNumberOrder())
            CompareField(field, expected, actual, $"{path}.{field.Name}", diffs);
    }

    private static void CompareField(
        FieldDescriptor field,
        IMessage expected,
        IMessage actual,
        string path,
        List<string> diffs)
    {
        if (field.IsMap)
        {
            CompareMap(field, expected, actual, path, diffs);
            return;
        }

        if (field.IsRepeated)
        {
            CompareRepeated(field, expected, actual, path, diffs);
            return;
        }

        if (field.HasPresence && !field.Accessor.HasValue(expected))
            return;

        if (field.HasPresence && !field.Accessor.HasValue(actual))
        {
            diffs.Add($"{path} missing in ClickHouse");
            return;
        }

        CompareValue(field.Accessor.GetValue(expected), field.Accessor.GetValue(actual), path, diffs);
    }

    private static void CompareRepeated(
        FieldDescriptor field,
        IMessage expected,
        IMessage actual,
        string path,
        List<string> diffs)
    {
        var expectedList = (IList)field.Accessor.GetValue(expected);
        var actualList = (IList)field.Accessor.GetValue(actual);
        if (expectedList.Count != actualList.Count)
        {
            diffs.Add($"{path} count kafka={expectedList.Count} clickhouse={actualList.Count}");
        }

        var count = Math.Min(expectedList.Count, actualList.Count);
        for (var i = 0; i < count; i++)
            CompareValue(expectedList[i], actualList[i], $"{path}[{i}]", diffs);
    }

    private static void CompareMap(
        FieldDescriptor field,
        IMessage expected,
        IMessage actual,
        string path,
        List<string> diffs)
    {
        var expectedMap = (IDictionary)field.Accessor.GetValue(expected);
        var actualMap = (IDictionary)field.Accessor.GetValue(actual);
        if (expectedMap.Count != actualMap.Count)
            diffs.Add($"{path} count kafka={expectedMap.Count} clickhouse={actualMap.Count}");

        foreach (DictionaryEntry entry in expectedMap)
        {
            if (!actualMap.Contains(entry.Key))
            {
                diffs.Add($"{path}[{entry.Key}] missing in ClickHouse");
                continue;
            }

            CompareValue(entry.Value, actualMap[entry.Key], $"{path}[{entry.Key}]", diffs);
        }
    }

    private static void CompareValue(object? expected, object? actual, string path, List<string> diffs)
    {
        if (expected is IMessage expectedMessage && actual is IMessage actualMessage)
        {
            CompareMessage(expectedMessage, actualMessage, path, diffs);
            return;
        }

        if (!ValuesEqual(expected, actual))
        {
            diffs.Add($"{path}: kafka='{Format(expected)}' clickhouse='{Format(actual)}'");
        }
    }

    private static bool ValuesEqual(object? expected, object? actual)
    {
        if (expected is null || actual is null)
            return expected is null && actual is null;

        if (expected is double expectedDouble && actual is double actualDouble)
            return expectedDouble.Equals(actualDouble);

        if (expected is float expectedFloat && actual is float actualFloat)
            return expectedFloat.Equals(actualFloat);

        if (expected is ByteString expectedBytes && actual is ByteString actualBytes)
            return expectedBytes.Equals(actualBytes);

        if (expected.GetType() == actual.GetType())
            return expected.Equals(actual);

        return string.Equals(
            Convert.ToString(expected, CultureInfo.InvariantCulture),
            Convert.ToString(actual, CultureInfo.InvariantCulture),
            StringComparison.Ordinal);
    }

    private static string Format(object? value)
    {
        var text = value switch
        {
            null => "null",
            ByteString bytes => Convert.ToHexString(bytes.Span),
            _ => Convert.ToString(value, CultureInfo.InvariantCulture) ?? ""
        };

        return text.Length <= 120 ? text : string.Concat(text.AsSpan(0, 117), "...");
    }
}
