namespace ClickHouseSchemaGen.Generation;

public static class SchemaPlanRenderer
{
    public static IReadOnlyDictionary<string, string> RenderInitScripts(ResolvedSchemaPlan plan)
    {
        var scripts = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var kafkaTable in plan.KafkaTables)
        {
            scripts[kafkaTable.Config.OutputPath] = SchemaMigrationsTable.AppendInitRecord(
                kafkaTable.Config.OutputPath,
                KafkaTableGenerator.Generate(kafkaTable.Config, kafkaTable.Columns));
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
        var pipelineBuilder = new StringBuilder()
            .AppendLine(SqlScriptWriter.GeneratedHeader)
            .AppendLine();

        foreach (var mergeTreeTable in plan.MergeTreeTables)
            pipelineBuilder.Append(MergeTreeTableGenerator.Generate(mergeTreeTable.Config));

        foreach (var materializedView in plan.MaterializedViews)
            pipelineBuilder.Append(MaterializedViewGenerator.Generate(materializedView.Config));

        if (!string.IsNullOrWhiteSpace(plan.TrailingSql))
            pipelineBuilder.AppendLine(plan.TrailingSql.Trim());

        return pipelineBuilder.ToString();
    }
}
