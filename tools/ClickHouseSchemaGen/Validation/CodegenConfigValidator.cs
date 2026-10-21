namespace ClickHouseSchemaGen.Validation;

public sealed class CodegenConfigValidator : AbstractValidator<CodegenConfig>
{
    public CodegenConfigValidator()
    {
        RuleFor(config => config.Defaults).SetValidator(new CodegenDefaultsValidator());
        RuleFor(config => config.KafkaTables).NotEmpty();
        RuleForEach(config => config.KafkaTables).SetValidator(new KafkaTableConfigValidator());

        RuleForEach(config => config.FieldOverrides)
            .ChildRules(overrides =>
            {
                overrides.RuleFor(entry => entry.Key)
                    .Must(ValidationRules.IsFieldPath)
                    .WithMessage("Field override key must be a valid field path.");
                overrides.RuleFor(entry => entry.Value)
                    .SetValidator(new FieldOverrideConfigValidator());
            });

        When(config => config.Pipeline is not null, () =>
            RuleFor(config => config.Pipeline!)
                .SetValidator(new PipelineConfigValidator()));

        RuleFor(config => config.KafkaTables)
            .Must(tables => tables.Select(table => table.TableName).Distinct(StringComparer.OrdinalIgnoreCase).Count()
                == tables.Count)
            .WithMessage("Kafka table names must be unique.");

        RuleFor(config => config.KafkaTables)
            .Must(tables => tables.Select(table => table.OutputPath).Distinct(StringComparer.OrdinalIgnoreCase).Count()
                == tables.Count)
            .WithMessage("Kafka table output paths must be unique.");

        RuleFor(config => config)
            .Must(config => FindErrorStreamWithoutPipeline(config) is null)
            .WithMessage(config => FindErrorStreamWithoutPipeline(config) ?? "handleErrorMode 'stream' requires a pipeline.")
            .When(config => config.Pipeline is null);

        RuleFor(config => config)
            .Must(config => FindObjectNameConflict(config) is null)
            .WithMessage(config => FindObjectNameConflict(config) ?? "ClickHouse object names must be unique.")
            .When(config => config.Pipeline is not null);

        RuleFor(config => config)
            .Must(HasValidMaterializedViewReferences)
            .WithMessage(config => BuildMaterializedViewReferenceError(config))
            .When(config => config.Pipeline?.MaterializedViews.Count > 0);

        RuleFor(config => config)
            .Must(HasValidMergeTreeSourceTableReferences)
            .WithMessage(config => BuildMergeTreeSourceTableReferenceError(config))
            .When(config => config.Pipeline?.MergeTreeTables.Any(table =>
                !string.IsNullOrWhiteSpace(table.SourceTable)) == true);

        RuleFor(config => config)
            .Must(HasNoAutoMaterializedViewNameConflicts)
            .WithMessage(config => BuildAutoMaterializedViewNameError(config))
            .When(config => config.Pipeline?.MergeTreeTables.Any(table =>
                MaterializedViewAutoGenerator.ShouldCreate(table, config.Pipeline.MaterializedViews)) == true);

        When(config => config.Cluster?.Enabled == true, () =>
        {
            RuleFor(config => config.Cluster).SetValidator(new ClusterConfigValidator());
            RuleFor(config => config)
                .Must(config => FindLocalTableNameConflict(config) is null)
                .WithMessage(config => FindLocalTableNameConflict(config) ?? "Local table name conflict.");
        });

        RuleFor(config => config)
            .Must(VersionsScriptSortsBeforeInitScripts)
            .WithMessage(config =>
                $"Migrations versions script '{Path.GetFileName(config.Migrations.VersionsOutputPath)}' must sort " +
                "before every init script (e.g. '00_schema_migrations.sql'): init scripts record themselves " +
                "into schema_migrations.");
    }

    private static string? FindObjectNameConflict(CodegenConfig config)
    {
        if (config.Pipeline is null)
            return null;

        var kafkaNames = config.KafkaTables
            .Select(table => table.TableName)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        foreach (var table in config.Pipeline.MergeTreeTables)
        {
            if (kafkaNames.Contains(table.TableName))
                return $"MergeTree table '{table.TableName}' conflicts with a Kafka table of the same name.";
        }

        var tableNames = kafkaNames
            .Concat(config.Pipeline.MergeTreeTables.Select(table => table.TableName))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        foreach (var view in config.Pipeline.MaterializedViews)
        {
            if (tableNames.Contains(view.Name))
                return $"Materialized view '{view.Name}' conflicts with an existing table of the same name.";
        }

        var usedNames = tableNames
            .Concat(config.Pipeline.MaterializedViews.Select(view => view.Name))
            .Concat(AutoViewNames(config.Pipeline))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        foreach (var queue in KafkaErrorStream.StreamingQueues(config))
        {
            foreach (var name in KafkaErrorStream.GeneratedObjectNames(queue))
            {
                if (!usedNames.Add(name))
                {
                    return $"Kafka table '{queue.TableName}' with handleErrorMode '{KafkaHandleErrorModes.Stream}' " +
                        $"generates '{name}', but that name is already used. Rename the conflicting object.";
                }
            }
        }

        return null;
    }

    private static string? FindErrorStreamWithoutPipeline(CodegenConfig config)
    {
        var queue = KafkaErrorStream.StreamingQueues(config).FirstOrDefault();
        return queue is null
            ? null
            : $"Kafka table '{queue.TableName}' with handleErrorMode '{KafkaHandleErrorModes.Stream}' generates " +
                $"'{KafkaErrorStream.TableName(queue.TableName)}' in the pipeline script; configure pipeline.";
    }

    private static IEnumerable<string> AutoViewNames(PipelineConfig pipeline) =>
        pipeline.MergeTreeTables
            .Where(table => MaterializedViewAutoGenerator.ShouldCreate(table, pipeline.MaterializedViews))
            .Select(table => MaterializedViewAutoGenerator.DefaultName(table.TableName));

    private static string? FindLocalTableNameConflict(CodegenConfig config)
    {
        if (config.Pipeline is null)
            return null;

        var errorStreamNames = KafkaErrorStream.StreamingQueues(config)
            .SelectMany(KafkaErrorStream.GeneratedObjectNames);

        var names = config.KafkaTables.Select(table => table.TableName)
            .Concat(config.Pipeline.MergeTreeTables.Select(table => table.TableName))
            .Concat(config.Pipeline.MaterializedViews.Select(view => view.Name))
            .Concat(AutoViewNames(config.Pipeline))
            .Concat(errorStreamNames)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var storedTables = config.Pipeline.MergeTreeTables
            .Select(table => table.TableName)
            .Concat(KafkaErrorStream.StreamingQueues(config).Select(queue => KafkaErrorStream.TableName(queue.TableName)));

        foreach (var tableName in storedTables)
        {
            var localName = tableName + config.Cluster.LocalTableSuffix;
            if (!names.Add(localName))
            {
                return $"MergeTree table '{tableName}' stores rows in '{localName}' in cluster mode, " +
                    "but that name is already used. Rename the table or change cluster.localTableSuffix.";
            }
        }

        return null;
    }

    private static bool VersionsScriptSortsBeforeInitScripts(CodegenConfig config)
    {
        var versionsFileName = Path.GetFileName(config.Migrations.VersionsOutputPath);
        return InitScriptOutputPaths(config)
            .Select(Path.GetFileName)
            .All(fileName => string.CompareOrdinal(versionsFileName, fileName) < 0);
    }

    private static IEnumerable<string> InitScriptOutputPaths(CodegenConfig config)
    {
        foreach (var table in config.KafkaTables)
            yield return table.OutputPath;

        if (config.Pipeline is not null)
            yield return config.Pipeline.OutputPath;
    }

    private static bool HasValidMaterializedViewReferences(CodegenConfig config)
    {
        if (config.Pipeline is null)
            return true;

        var kafkaTables = config.KafkaTables
            .Select(table => table.TableName)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        var mergeTreeTables = config.Pipeline.MergeTreeTables
            .Select(table => table.TableName)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        return config.Pipeline.MaterializedViews.All(view =>
            kafkaTables.Contains(view.SourceTable)
            && mergeTreeTables.Contains(view.TargetTable));
    }

    private static string BuildMaterializedViewReferenceError(CodegenConfig config)
    {
        if (config.Pipeline is null)
            return "Pipeline materialized views reference unknown tables.";

        var kafkaTables = config.KafkaTables
            .Select(table => table.TableName)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        var mergeTreeTables = config.Pipeline.MergeTreeTables
            .Select(table => table.TableName)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        foreach (var view in config.Pipeline.MaterializedViews)
        {
            if (!kafkaTables.Contains(view.SourceTable))
                return $"Materialized view '{view.Name}' references unknown Kafka source table '{view.SourceTable}'.";

            if (!mergeTreeTables.Contains(view.TargetTable))
                return $"Materialized view '{view.Name}' references unknown MergeTree target table '{view.TargetTable}'.";
        }

        return "Pipeline materialized views reference unknown tables.";
    }

    private static bool HasValidMergeTreeSourceTableReferences(CodegenConfig config)
    {
        if (config.Pipeline is null)
            return true;

        var kafkaTables = config.KafkaTables
            .Select(table => table.TableName)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        return config.Pipeline.MergeTreeTables
            .Where(table => !string.IsNullOrWhiteSpace(table.SourceTable))
            .All(table => kafkaTables.Contains(table.SourceTable!));
    }

    private static string BuildMergeTreeSourceTableReferenceError(CodegenConfig config)
    {
        if (config.Pipeline is null)
            return "Pipeline MergeTree tables reference unknown Kafka source tables.";

        var kafkaTables = config.KafkaTables
            .Select(table => table.TableName)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        foreach (var table in config.Pipeline.MergeTreeTables)
        {
            if (!string.IsNullOrWhiteSpace(table.SourceTable) && !kafkaTables.Contains(table.SourceTable))
            {
                return $"MergeTree table '{table.TableName}' references unknown Kafka source table '{table.SourceTable}'.";
            }
        }

        return "Pipeline MergeTree tables reference unknown Kafka source tables.";
    }

    private static bool HasNoAutoMaterializedViewNameConflicts(CodegenConfig config) =>
        FindAutoMaterializedViewNameConflict(config) is null;

    private static string BuildAutoMaterializedViewNameError(CodegenConfig config) =>
        FindAutoMaterializedViewNameConflict(config)
        ?? "Auto-generated materialized view name conflicts with an existing view.";

    private static string? FindAutoMaterializedViewNameConflict(CodegenConfig config)
    {
        if (config.Pipeline is null)
            return null;

        var explicitViews = config.Pipeline.MaterializedViews;
        var usedNames = config.KafkaTables
            .Select(table => table.TableName)
            .Concat(config.Pipeline.MergeTreeTables.Select(table => table.TableName))
            .Concat(explicitViews.Select(view => view.Name))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        foreach (var table in config.Pipeline.MergeTreeTables)
        {
            if (!MaterializedViewAutoGenerator.ShouldCreate(table, explicitViews))
                continue;

            var name = MaterializedViewAutoGenerator.DefaultName(table.TableName);
            if (!usedNames.Add(name))
            {
                return $"MergeTree table '{table.TableName}' auto-generates materialized view '{name}', " +
                    "but that name is already used.";
            }
        }

        return null;
    }
}
