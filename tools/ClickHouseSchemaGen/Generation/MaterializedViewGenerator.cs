using ClickHouseSchemaGen.Validation;

namespace ClickHouseSchemaGen.Generation;

public static class MaterializedViewGenerator
{
    public static string Generate(
        MaterializedViewConfig config,
        IReadOnlyList<ClickHouseColumn>? sourceColumns = null)
    {
        var computedColumns = ResolveComputedColumns(config, sourceColumns);
        var builder = new StringBuilder()
            .AppendLine($"CREATE MATERIALIZED VIEW {config.Name} TO {config.TargetTable} AS");

        if (computedColumns.Count > 0)
        {
            builder.AppendLine("WITH");
            SqlScriptWriter.AppendCommaSeparatedLines(
                builder,
                computedColumns
                    .Select(column => $"{column.SourceExpression} AS {SqlColumnFormatter.FormatColumnName(column.Name)}")
                    .ToList(),
                lastLineSuffix: string.Empty);
        }

        builder.AppendLine("SELECT");

        for (var i = 0; i < config.Columns.Count; i++)
        {
            var mapping = config.Columns[i];
            var comma = i < config.Columns.Count - 1 ? "," : string.Empty;
            builder.AppendLine($"    {FormatSelectItem(mapping, computedColumns)}{comma}");
        }

        return builder
            .AppendLine($"FROM {config.SourceTable};")
            .AppendLine()
            .ToString();
    }

    private static string FormatSelectItem(PipelineColumnMapping mapping, IReadOnlyList<ClickHouseColumn> computedColumns)
    {
        var target = SqlColumnFormatter.FormatColumnName(mapping.Target);

        // Re-aliasing a WITH alias to its own name is rejected as MULTIPLE_EXPRESSIONS_FOR_ALIAS.
        if (string.IsNullOrWhiteSpace(mapping.Expression)
            && mapping.Source == mapping.Target
            && computedColumns.Any(column => column.Name == mapping.Source))
        {
            return target;
        }

        return $"{ResolveExpression(mapping),-28} AS {target}";
    }

    private static List<ClickHouseColumn> ResolveComputedColumns(
        MaterializedViewConfig config,
        IReadOnlyList<ClickHouseColumn>? sourceColumns)
    {
        var computed = (sourceColumns ?? [])
            .Where(column => column.SourceExpression is not null)
            .ToList();

        var referenced = config.Columns
            .SelectMany(mapping => string.IsNullOrWhiteSpace(mapping.Expression)
                ? [mapping.Source]
                : ValidationRules.ExtractColumnCandidates(mapping.Expression))
            .ToHashSet(StringComparer.Ordinal);

        var unknownKeyColumn = referenced.FirstOrDefault(name =>
            KafkaKeyColumnMapper.IsKeyColumn(name) && computed.All(column => column.Name != name));
        if (unknownKeyColumn is not null)
        {
            var available = computed.Count == 0
                ? "none (set kafkaTables[].key.format to \"protobuf\")"
                : string.Join(", ", computed.Select(column => column.Name));
            throw new InvalidOperationException(
                $"Materialized view '{config.Name}' references unknown key column '{unknownKeyColumn}' " +
                $"of Kafka table '{config.SourceTable}'. Available key columns: {available}.");
        }

        return computed.Where(column => referenced.Contains(column.Name)).ToList();
    }

    private static string ResolveExpression(PipelineColumnMapping mapping) =>
        !string.IsNullOrWhiteSpace(mapping.Expression)
            ? mapping.Expression
            : SqlColumnFormatter.FormatColumnName(mapping.Source);
}
