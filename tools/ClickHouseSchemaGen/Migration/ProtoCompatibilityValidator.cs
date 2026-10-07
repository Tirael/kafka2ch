namespace ClickHouseSchemaGen.Migration;

public static class ProtoCompatibilityValidator
{
    public static void Validate(ResolvedSchemaPlan oldPlan, ResolvedSchemaPlan newPlan)
    {
        ArgumentNullException.ThrowIfNull(oldPlan);
        ArgumentNullException.ThrowIfNull(newPlan);

        var violations = CollectViolations(oldPlan, newPlan);
        if (violations.Count > 0)
            throw new ProtoCompatibilityException(violations);
    }

    internal static IReadOnlyList<string> CollectViolations(ResolvedSchemaPlan oldPlan, ResolvedSchemaPlan newPlan)
    {
        List<string> violations = [];

        var oldProtoColumns = CollectProtoColumns(oldPlan);
        var newProtoColumns = CollectProtoColumns(newPlan);

        foreach (var path in oldProtoColumns.Keys.Intersect(newProtoColumns.Keys, StringComparer.Ordinal))
        {
            var oldColumn = oldProtoColumns[path];
            var newColumn = newProtoColumns[path];

            if (!string.Equals(oldColumn.Type, newColumn.Type, StringComparison.Ordinal))
            {
                violations.Add(
                    $"Field '{path}' type changed from '{oldColumn.Type}' to '{newColumn.Type}' on table '{oldColumn.TableName}'.");
            }
        }

        foreach (var path in newProtoColumns.Keys.Except(oldProtoColumns.Keys, StringComparer.Ordinal))
        {
            if (oldProtoColumns.Values.Any(c => ReusesFieldNumber(path, c.FieldNumberPath) && c.FieldNumberPath != path))
            {
                var newColumn = newProtoColumns[path];
                violations.Add(
                    $"Field number path '{path}' on table '{newColumn.TableName}' reuses an existing field number with a different path.");
            }
        }

        foreach (var path in oldProtoColumns.Keys.Except(newProtoColumns.Keys, StringComparer.Ordinal))
        {
            var oldColumn = oldProtoColumns[path];
            if (IsEnumType(oldColumn.Type) && newProtoColumns.Values.Any(c => SharesEnumBase(path, c.FieldNumberPath)))
                continue;

            if (newProtoColumns.Values.Any(c => ReusesFieldNumber(path, c.FieldNumberPath)))
            {
                violations.Add(
                    $"Removed field '{path}' ({oldColumn.Name}) on table '{oldColumn.TableName}' was replaced by a different field with the same number.");
            }
        }

        foreach (var path in newProtoColumns.Keys)
        {
            if (oldProtoColumns.ContainsKey(path))
                continue;

            var newColumn = newProtoColumns[path];
            var conflicting = oldProtoColumns.Values.FirstOrDefault(c =>
                ReusesFieldNumber(path, c.FieldNumberPath) && !string.Equals(c.Type, newColumn.Type, StringComparison.Ordinal));

            if (conflicting is not null)
            {
                violations.Add(
                    $"Field number '{path}' reuses number from '{conflicting.FieldNumberPath}' with incompatible type '{conflicting.Type}' -> '{newColumn.Type}'.");
            }
        }

        CheckScalarKindChanges(oldProtoColumns, newProtoColumns, violations);

        return violations;
    }

    private static void CheckScalarKindChanges(
        IReadOnlyDictionary<string, ProtoColumnRef> oldColumns,
        IReadOnlyDictionary<string, ProtoColumnRef> newColumns,
        List<string> violations)
    {
        foreach (var path in oldColumns.Keys.Intersect(newColumns.Keys, StringComparer.Ordinal))
        {
            var oldKind = ScalarKind(oldColumns[path].Type);
            var newKind = ScalarKind(newColumns[path].Type);
            if (oldKind is not null && newKind is not null && oldKind != newKind)
            {
                violations.Add(
                    $"Field '{path}' scalar kind changed from '{oldKind}' to '{newKind}' on table '{newColumns[path].TableName}'.");
            }
        }
    }

    private static Dictionary<string, ProtoColumnRef> CollectProtoColumns(ResolvedSchemaPlan plan)
    {
        Dictionary<string, ProtoColumnRef> columns = new(StringComparer.Ordinal);

        foreach (var kafkaTable in plan.KafkaTables)
        {
            foreach (var column in kafkaTable.Columns.Where(c => !KafkaMetaColumnFactory.IsKafkaMetaPath(c.FieldNumberPath)))
            {
                var path = ColumnKey(column);
                columns[path] = new ProtoColumnRef(kafkaTable.Config.TableName, column.Name, path, column.Type);
            }
        }

        foreach (var mergeTree in plan.MergeTreeTables)
        {
            foreach (var column in mergeTree.Config.Columns.Where(c => !KafkaMetaColumnFactory.IsKafkaMetaPath(c.FieldNumberPath)))
            {
                var path = ColumnKey(column);
                columns.TryAdd(path, new ProtoColumnRef(mergeTree.Config.TableName, column.Name, path, column.Type));
            }
        }

        return columns;
    }

    private static bool ReusesFieldNumber(string leftPath, string rightPath)
    {
        if (string.Equals(leftPath, rightPath, StringComparison.Ordinal))
            return false;

        var leftRoot = RootFieldNumber(leftPath);
        var rightRoot = RootFieldNumber(rightPath);
        return leftRoot == rightRoot && !string.IsNullOrEmpty(leftRoot);
    }

    private static string RootFieldNumber(string path)
    {
        if (path.StartsWith("oneof:", StringComparison.Ordinal))
            return path;

        var dot = path.IndexOf('.');
        return dot < 0 ? path : path[..dot];
    }

    private static bool SharesEnumBase(string leftPath, string rightPath) =>
        string.Equals(RootFieldNumber(leftPath), RootFieldNumber(rightPath), StringComparison.Ordinal);

    private static bool IsEnumType(string type) => type.StartsWith("Enum8(", StringComparison.Ordinal) || type.StartsWith("Enum16(", StringComparison.Ordinal);

    private static string? ScalarKind(string type)
    {
        if (IsEnumType(type))
            return "enum";

        if (type.StartsWith("Nullable(", StringComparison.Ordinal))
            return ScalarKind(type["Nullable(".Length..^1]);

        return type switch
        {
            "String" or "LowCardinality(String)" => "string",
            "Float32" or "Float64" => "float",
            "Int8" or "Int16" or "Int32" or "Int64" or "UInt8" or "UInt16" or "UInt32" or "UInt64" => "integer",
            "Bool" => "bool",
            "DateTime" or "DateTime64(3)" or "DateTime64(6)" or "DateTime64(9)" => "datetime",
            _ => null
        };
    }

    private static string ColumnKey(ClickHouseColumn column) =>
        string.IsNullOrWhiteSpace(column.FieldNumberPath) ? column.Name : column.FieldNumberPath;

    private static string ColumnKey(PipelineColumnConfig column) =>
        string.IsNullOrWhiteSpace(column.FieldNumberPath) ? column.Name : column.FieldNumberPath!;

    private sealed record ProtoColumnRef(string TableName, string Name, string FieldNumberPath, string Type);
}
