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
                KafkaTableGenerator.Generate(kafkaTable.Config, kafkaTable.Columns, cluster: cluster));
        }

        if (plan.Config.Pipeline is not null)
        {
            scripts[plan.Config.Pipeline.OutputPath] = SchemaMigrationsTable.AppendInitRecord(
                plan.Config.Pipeline.OutputPath,
                RenderPipelineSql(plan));
        }

        return scripts;
    }

    public static string RenderPipelineSql(ResolvedSchemaPlan plan)
    {
        var cluster = ClusterDdl.For(plan.Config);
        var pipelineBuilder = new StringBuilder()
            .AppendLine(SqlScriptWriter.GeneratedHeader)
            .AppendLine();

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
            pipelineBuilder.Append(MaterializedViewGenerator.Generate(
                materializedView.Config,
                includeFunctionDefinitions: false,
                cluster: cluster));
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
