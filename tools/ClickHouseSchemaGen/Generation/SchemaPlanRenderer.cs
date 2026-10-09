namespace ClickHouseSchemaGen.Generation;

public static class SchemaPlanRenderer
{
    public static IReadOnlyDictionary<string, string> RenderInitScripts(ResolvedSchemaPlan plan)
    {
        var scripts = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var cluster = ClusterDdl.For(plan.Config);

        foreach (var kafkaTable in plan.KafkaTables)
        {
            scripts[kafkaTable.Config.OutputPath] = SchemaMigrationsTable.AppendInitRecord(
                kafkaTable.Config.OutputPath,
                cluster.WithDatabaseContext(
                    KafkaTableGenerator.Generate(kafkaTable.Config, kafkaTable.Columns, cluster: cluster)),
                cluster);
        }

        if (plan.Config.Pipeline is not null)
        {
            scripts[plan.Config.Pipeline.OutputPath] = SchemaMigrationsTable.AppendInitRecord(
                plan.Config.Pipeline.OutputPath,
                cluster.WithDatabaseContext(RenderPipelineSql(plan)),
                cluster);
        }

        return scripts;
    }

    public static string RenderPipelineSql(ResolvedSchemaPlan plan)
    {
        var cluster = ClusterDdl.For(plan.Config);
        var writeThroughByTable = plan.MergeTreeTables.ToDictionary(
            table => table.Config.TableName,
            table => table.Config.MaterializedViewsWriteThroughDistributed,
            StringComparer.OrdinalIgnoreCase);

        var pipelineBuilder = new StringBuilder();

        foreach (var mergeTreeTable in plan.MergeTreeTables)
            pipelineBuilder.Append(MergeTreeTableGenerator.Generate(mergeTreeTable.Config, cluster: cluster));

        if (plan.MaterializedViews.Any(view => ProtobufWireSqlFunctions.IsUsedBy(view.Config)))
        {
            pipelineBuilder
                .AppendLine(cluster.ProtobufWireFunctionDefinitions().TrimEnd())
                .AppendLine();
        }

        foreach (var materializedView in plan.MaterializedViews)
        {
            writeThroughByTable.TryGetValue(materializedView.Config.TargetTable, out var writeThrough);
            pipelineBuilder.Append(MaterializedViewGenerator.Generate(
                materializedView.Config,
                includeFunctionDefinitions: false,
                cluster: cluster,
                writeThroughDistributed: writeThrough));
        }

        if (!string.IsNullOrWhiteSpace(plan.TrailingSql))
        {
            var trailingSql = ClusterTrailingSqlRewriter.Rewrite(
                plan.TrailingSql,
                cluster,
                plan.MergeTreeTables.Select(table => table.Config.TableName));
            pipelineBuilder.AppendLine(trailingSql.Trim());
        }

        return pipelineBuilder.ToString();
    }
}
