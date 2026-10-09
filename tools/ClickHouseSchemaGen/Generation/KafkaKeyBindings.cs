using System.Text.RegularExpressions;

namespace ClickHouseSchemaGen.Generation;


public static partial class KafkaKeyBindings
{
    [GeneratedRegex(@"`(?<name>_key\.[^`]+)`")]
    private static partial Regex QuotedKeyColumnRegex();

    public static MaterializedViewConfig Apply(
        MaterializedViewConfig view,
        IReadOnlyList<ClickHouseColumn> keyColumns)
    {
        var expressions = keyColumns.ToDictionary(
            column => column.Name,
            column => column.SourceExpression!,
            StringComparer.Ordinal);

        return new MaterializedViewConfig
        {
            Name = view.Name,
            TargetTable = view.TargetTable,
            SourceTable = view.SourceTable,
            IncludeKafkaMeta = view.IncludeKafkaMeta,
            Columns = view.Columns.Select(mapping => Bind(view, mapping, expressions)).ToList()
        };
    }

    private static PipelineColumnMapping Bind(
        MaterializedViewConfig view,
        PipelineColumnMapping mapping,
        IReadOnlyDictionary<string, string> expressions)
    {
        if (string.IsNullOrWhiteSpace(mapping.Expression))
        {
            return KafkaKeyColumnMapper.IsKeyColumn(mapping.Source)
                ? new PipelineColumnMapping
                {
                    Source = mapping.Source,
                    Target = mapping.Target,
                    Expression = Resolve(view, mapping.Source, expressions)
                }
                : mapping;
        }

        var expression = QuotedKeyColumnRegex().Replace(
            mapping.Expression,
            match => $"({Resolve(view, match.Groups["name"].Value, expressions)})");

        return expression == mapping.Expression
            ? mapping
            : new PipelineColumnMapping { Source = mapping.Source, Target = mapping.Target, Expression = expression };
    }

    private static string Resolve(
        MaterializedViewConfig view,
        string keyColumn,
        IReadOnlyDictionary<string, string> expressions)
    {
        if (expressions.TryGetValue(keyColumn, out var expression))
            return expression;

        var available = expressions.Count == 0
            ? "none (set kafkaTables[].key.format to \"protobuf\")"
            : string.Join(", ", expressions.Keys);
        throw new InvalidOperationException(
            $"Materialized view '{view.Name}' references unknown key column '{keyColumn}' " +
            $"of Kafka table '{view.SourceTable}'. Available key columns: {available}.");
    }
}
