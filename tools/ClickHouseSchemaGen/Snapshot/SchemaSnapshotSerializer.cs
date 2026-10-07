using System.Security.Cryptography;
using System.Text.Json.Serialization;

namespace ClickHouseSchemaGen.Snapshot;

public static class SchemaSnapshotSerializer
{
    private static readonly JsonSerializerOptions ReadOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
        AllowTrailingCommas = true,
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) }
    };

    private static readonly JsonSerializerOptions WriteOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) }
    };

    public static SchemaSnapshot Load(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        return JsonSerializer.Deserialize<SchemaSnapshot>(File.ReadAllText(path), ReadOptions)
            ?? throw new InvalidOperationException($"Snapshot file '{path}' is empty or invalid.");
    }

    public static void Save(string path, SchemaSnapshot snapshot)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        ArgumentNullException.ThrowIfNull(snapshot);

        var normalized = NormalizeForDeterministicSerialization(snapshot);
        var json = JsonSerializer.Serialize(normalized, WriteOptions);
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(path))!);
        File.WriteAllText(path, json);
    }

    public static string SerializeToString(SchemaSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        return JsonSerializer.Serialize(NormalizeForDeterministicSerialization(snapshot), WriteOptions);
    }

    public static string ComputeChecksum(string content) =>
        ComputeChecksum(Encoding.UTF8.GetBytes(content));

    public static string ComputeChecksum(byte[] bytes)
    {
        ArgumentNullException.ThrowIfNull(bytes);
        return Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();
    }

    internal static SchemaSnapshot NormalizeForDeterministicSerialization(SchemaSnapshot snapshot) =>
        snapshot with
        {
            KafkaTables = snapshot.KafkaTables
                .OrderBy(t => t.TableName, StringComparer.Ordinal)
                .Select(NormalizeKafkaTable)
                .ToList(),
            MergeTreeTables = snapshot.MergeTreeTables
                .OrderBy(t => t.TableName, StringComparer.Ordinal)
                .Select(NormalizeMergeTreeTable)
                .ToList(),
            MaterializedViews = snapshot.MaterializedViews
                .OrderBy(v => v.Name, StringComparer.Ordinal)
                .Select(NormalizeMaterializedView)
                .ToList()
        };

    private static KafkaTableSnapshot NormalizeKafkaTable(KafkaTableSnapshot table) =>
        table with
        {
            Columns = table.Columns
                .OrderBy(c => ColumnSortKey(c), StringComparer.Ordinal)
                .ToList()
        };

    private static MergeTreeTableSnapshot NormalizeMergeTreeTable(MergeTreeTableSnapshot table) =>
        table with
        {
            Columns = table.Columns
                .OrderBy(c => ColumnSortKey(c), StringComparer.Ordinal)
                .ToList()
        };

    private static MaterializedViewSnapshot NormalizeMaterializedView(MaterializedViewSnapshot view) =>
        view with
        {
            Columns = view.Columns
                .OrderBy(c => c.Target, StringComparer.Ordinal)
                .ToList()
        };

    private static string ColumnSortKey(SnapshotColumn column) =>
        string.IsNullOrWhiteSpace(column.FieldNumberPath) ? column.Name : column.FieldNumberPath;
}
