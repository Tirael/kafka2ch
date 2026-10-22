namespace ClickHouseSchemaGen.Generation;

public static class KafkaErrorStream
{
    public const string ValidRowsFilter = "length(_error) = 0";

    public const string ErrorRowsFilter = "length(_error) > 0";

    public const string OrderBy = "(_topic, _partition, _offset)";

    public static string TableName(string queueName) => $"{queueName}_errors";

    public static string ViewName(string queueName) => $"{queueName}_errors_mv";

    public static IEnumerable<KafkaTableConfig> StreamingQueues(CodegenConfig config) =>
        config.KafkaTables.Where(table => table.Kafka.StreamsErrors);

    public static MergeTreeTableConfig CreateTable(string queueName) =>
        new()
        {
            TableName = TableName(queueName),
            OrderBy = OrderBy,
            SourceTable = queueName,
            MaterializedViewsWriteThroughDistributed = false,
            Columns =
            [
                new PipelineColumnConfig { Name = "_topic", Type = "LowCardinality(String)" },
                new PipelineColumnConfig { Name = "_partition", Type = "UInt64" },
                new PipelineColumnConfig { Name = "_offset", Type = "UInt64" },
                new PipelineColumnConfig { Name = "_raw_message", Type = "String" },
                new PipelineColumnConfig { Name = "_error", Type = "String" },
                new PipelineColumnConfig { Name = "received_at", Type = "DateTime" }
            ]
        };

    public static MaterializedViewConfig CreateView(string queueName) =>
        new()
        {
            Name = ViewName(queueName),
            SourceTable = queueName,
            TargetTable = TableName(queueName),
            Where = ErrorRowsFilter,
            Columns =
            [
                new PipelineColumnMapping { Source = "_topic", Target = "_topic" },
                new PipelineColumnMapping { Source = "_partition", Target = "_partition" },
                new PipelineColumnMapping { Source = "_offset", Target = "_offset" },
                new PipelineColumnMapping { Source = "_raw_message", Target = "_raw_message" },
                new PipelineColumnMapping { Source = "_error", Target = "_error" },
                new PipelineColumnMapping { Source = "received_at", Target = "received_at", Expression = "now()" }
            ]
        };

    public static IEnumerable<string> GeneratedObjectNames(KafkaTableConfig queue) =>
        [TableName(queue.TableName), ViewName(queue.TableName)];

    public static ResolvedSchemaPlan Apply(ResolvedSchemaPlan plan)
    {
        var streamingQueues = StreamingQueues(plan.Config)
            .Select(queue => queue.TableName)
            .ToList();
        if (streamingQueues.Count == 0)
            return plan;

        var streaming = streamingQueues.ToHashSet(StringComparer.OrdinalIgnoreCase);
        var views = plan.MaterializedViews
            .Select(view => streaming.Contains(view.Config.SourceTable)
                ? view with { Config = WithWhere(view.Config, ValidRowsFilter) }
                : view)
            .Concat(streamingQueues.Select(queue => new MaterializedViewPlan
            {
                Config = CreateView(queue),
                Origin = PlanOrigin.Auto
            }))
            .ToList();
        var tables = plan.MergeTreeTables
            .Concat(streamingQueues.Select(queue => new MergeTreeTablePlan
            {
                Config = CreateTable(queue),
                Origin = PlanOrigin.Auto
            }))
            .ToList();

        return plan with { MergeTreeTables = tables, MaterializedViews = views };
    }

    private static MaterializedViewConfig WithWhere(MaterializedViewConfig view, string where) =>
        new()
        {
            Name = view.Name,
            TargetTable = view.TargetTable,
            SourceTable = view.SourceTable,
            IncludeKafkaMeta = view.IncludeKafkaMeta,
            Columns = view.Columns,
            Where = where
        };
}
