namespace ClickHouseSchemaGen.Validation;

internal sealed class CodegenDefaultsValidator : AbstractValidator<CodegenDefaults>
{
    public CodegenDefaultsValidator()
    {
        RuleFor(defaults => defaults.MaxFlattenDepth)
            .GreaterThan(0);

        RuleFor(defaults => defaults.RepeatedMessageStrategy)
            .NotEmpty()
            .Must(ValidationRules.RepeatedMessageStrategies.Contains)
            .WithMessage(
                $"Must be one of: {string.Join(", ", ValidationRules.RepeatedMessageStrategies)}.");

        RuleFor(defaults => defaults.EnumMaxValuesForEnum8)
            .GreaterThan(0);
    }
}

internal sealed class KafkaSettingsConfigValidator : AbstractValidator<KafkaSettingsConfig>
{
    public KafkaSettingsConfigValidator()
    {
        RuleFor(settings => settings.BrokerList)
            .NotEmpty()
            .When(settings => settings.BrokerList is not null);
        RuleFor(settings => settings.NamedCollection)
            .Must(name => name is null || ValidationRules.IsSqlIdentifier(name))
            .WithMessage("namedCollection must be a ClickHouse identifier (letters, digits, '_').");
        RuleFor(settings => settings.BrokerList)
            .Null()
            .WithMessage(settings =>
                $"brokerList and namedCollection are mutually exclusive: brokers of '{settings.NamedCollection}' " +
                "are defined in the ClickHouse named collection (kafka_broker_list), remove brokerList.")
            .When(settings => settings.NamedCollection is not null);
        RuleFor(settings => settings.Topic).NotEmpty();
        RuleFor(settings => settings.GroupName).NotEmpty();
        RuleFor(settings => settings.SkipBytes).GreaterThanOrEqualTo(0);
        RuleFor(settings => settings.NumConsumers).GreaterThan(0);
        RuleFor(settings => settings.MaxBlockSize)
            .InclusiveBetween(1, KafkaThroughputLimits.MaxBlockSize)
            .When(settings => settings.MaxBlockSize.HasValue);
        RuleFor(settings => settings.FlushIntervalMs)
            .InclusiveBetween(KafkaThroughputLimits.MinFlushIntervalMs, KafkaThroughputLimits.MaxFlushIntervalMs)
            .When(settings => settings.FlushIntervalMs.HasValue);
        RuleFor(settings => settings.PollMaxBatchSize)
            .InclusiveBetween(1, KafkaThroughputLimits.MaxBlockSize)
            .When(settings => settings.PollMaxBatchSize.HasValue);
        RuleFor(settings => settings.PollMaxBatchSize)
            .LessThanOrEqualTo(settings => settings.MaxBlockSize)
            .WithMessage(settings =>
                $"pollMaxBatchSize ({settings.PollMaxBatchSize}) must not exceed maxBlockSize ({settings.MaxBlockSize}): " +
                "a poll batch larger than the block is split anyway.")
            .When(settings => settings.PollMaxBatchSize.HasValue && settings.MaxBlockSize.HasValue);
        RuleFor(settings => settings.HandleErrorMode)
            .Must(mode => KafkaHandleErrorModes.All.Contains(mode, StringComparer.OrdinalIgnoreCase))
            .WithMessage($"handleErrorMode must be one of: {string.Join(", ", KafkaHandleErrorModes.All)}.");
        RuleFor(settings => settings.SkipBrokenMessages)
            .GreaterThanOrEqualTo(0)
            .When(settings => settings.SkipBrokenMessages.HasValue);
        RuleFor(settings => settings.SkipBrokenMessages)
            .Null()
            .WithMessage(settings =>
                $"skipBrokenMessages applies only to handleErrorMode '{KafkaHandleErrorModes.Default}'; " +
                $"'{settings.HandleErrorMode}' already keeps broken messages out of the consumer path.")
            .When(settings => !settings.UsesDefaultHandleErrorMode);
    }
}

internal sealed class FieldOverrideConfigValidator : AbstractValidator<FieldOverrideConfig>
{
    public FieldOverrideConfigValidator()
    {
        RuleFor(overrideConfig => overrideConfig.Strategy)
            .Must(strategy => strategy is null || ValidationRules.IsMappingStrategy(strategy))
            .WithMessage("Must be a valid mapping strategy name.");

        RuleFor(overrideConfig => overrideConfig.MaxDepth)
            .GreaterThan(0)
            .When(overrideConfig => overrideConfig.MaxDepth.HasValue);
    }
}

internal sealed class KafkaTableConfigValidator : AbstractValidator<KafkaTableConfig>
{
    public KafkaTableConfigValidator()
    {
        RuleFor(table => table.MessageType).NotEmpty();
        RuleFor(table => table.TableName)
            .NotEmpty()
            .Must(ValidationRules.IsSqlIdentifier)
            .WithMessage("Must be a valid ClickHouse identifier.");
        RuleFor(table => table.ProtoFile)
            .NotEmpty()
            .Must(ValidationRules.IsSqlIdentifier)
            .WithMessage("Must be a proto file name without path or extension.");
        RuleFor(table => table.MessageName)
            .NotEmpty()
            .Must(ValidationRules.IsSqlIdentifier)
            .WithMessage("Must be a valid protobuf message name.");
        RuleFor(table => table.OutputPath).NotEmpty();
        RuleFor(table => table.Kafka).SetValidator(new KafkaSettingsConfigValidator());
        RuleFor(table => table.Key).NotNull().SetValidator(new KafkaKeyConfigValidator());

        RuleForEach(table => table.FieldOverrides)
            .ChildRules(overrides =>
            {
                overrides.RuleFor(entry => entry.Key)
                    .Must(ValidationRules.IsFieldPath)
                    .WithMessage("Field override key must be a valid field path.");
                overrides.RuleFor(entry => entry.Value)
                    .SetValidator(new FieldOverrideConfigValidator());
            });
    }
}

internal sealed class KafkaKeyConfigValidator : AbstractValidator<KafkaKeyConfig>
{
    public KafkaKeyConfigValidator()
    {
        RuleFor(key => key.Format)
            .NotEmpty()
            .Must(format => KafkaKeyFormats.All.Contains(format, StringComparer.OrdinalIgnoreCase))
            .WithMessage($"Must be one of: {string.Join(", ", KafkaKeyFormats.All)}.");

        RuleFor(key => key.MessageType)
            .NotEmpty()
            .WithMessage("messageType is required when key format is protobuf.")
            .When(key => key.IsProtobuf);

        RuleFor(key => key)
            .Must(key => key.MessageType is null && key.SkipBytes is null && key.FieldOverrides.Count == 0)
            .WithMessage("messageType, skipBytes and fieldOverrides are only valid when key format is protobuf.")
            .When(key => !key.IsProtobuf);

        RuleFor(key => key.SkipBytes)
            .GreaterThanOrEqualTo(0)
            .When(key => key.SkipBytes.HasValue);

        RuleForEach(key => key.FieldOverrides)
            .ChildRules(overrides =>
            {
                overrides.RuleFor(entry => entry.Key)
                    .Must(ValidationRules.IsFieldPath)
                    .WithMessage("Field override key must be a valid field path.");
                overrides.RuleFor(entry => entry.Value)
                    .SetValidator(new FieldOverrideConfigValidator());
            });
    }
}

internal sealed class PipelineColumnConfigValidator : AbstractValidator<PipelineColumnConfig>
{
    public PipelineColumnConfigValidator()
    {
        RuleFor(column => column.Name)
            .NotEmpty()
            .Must(ValidationRules.IsFieldPath)
            .WithMessage("Must be a valid ClickHouse column name or nested field path.");
        RuleFor(column => column.Type).NotEmpty();
    }
}

internal sealed class PipelineColumnMappingValidator : AbstractValidator<PipelineColumnMapping>
{
    public PipelineColumnMappingValidator()
    {
        RuleFor(mapping => mapping.Source)
            .NotEmpty()
            .Must(ValidationRules.IsFieldPath)
            .WithMessage("Must be a valid source column name or nested field path.");
        RuleFor(mapping => mapping.Target)
            .NotEmpty()
            .Must(ValidationRules.IsFieldPath)
            .WithMessage("Must be a valid ClickHouse column name or nested field path.");
    }
}

internal sealed class MergeTreeTableConfigValidator : AbstractValidator<MergeTreeTableConfig>
{
    public MergeTreeTableConfigValidator()
    {
        RuleFor(table => table.TableName)
            .NotEmpty()
            .Must(ValidationRules.IsSqlIdentifier)
            .WithMessage("Must be a valid ClickHouse identifier.");
        RuleFor(table => table.OrderBy).NotEmpty();
        RuleFor(table => table.Engine)
            .Must(engine => MergeTreeEngines.Validate(engine) is null)
            .WithMessage(table => MergeTreeEngines.Validate(table.Engine)!);
        RuleFor(table => table.Ttl)
            .Must(ttl => ttl is null || !string.IsNullOrWhiteSpace(ttl))
            .WithMessage("TTL expression must not be blank when provided.");
        RuleFor(table => table)
            .Must(ValidationRules.TtlReferencesKnownColumns)
            .WithMessage(table =>
                $"TTL expression references unknown columns for table '{table.TableName}'. " +
                $"Available columns: {string.Join(", ", table.Columns.Select(column => column.Name))}.")
            .When(table => !string.IsNullOrWhiteSpace(table.Ttl) && table.Columns.Count > 0);
        RuleFor(table => table.SourceTable)
            .Must(sourceTable => sourceTable is null || ValidationRules.IsSqlIdentifier(sourceTable))
            .WithMessage("Must be a valid ClickHouse identifier when provided.");
        RuleFor(table => table.SourceTable)
            .NotEmpty()
            .WithMessage("sourceTable is required when columns is empty (auto-fill from Kafka queue).")
            .When(table => table.Columns.Count == 0);
        RuleForEach(table => table.Columns).SetValidator(new PipelineColumnConfigValidator());
    }
}

internal sealed class MaterializedViewConfigValidator : AbstractValidator<MaterializedViewConfig>
{
    public MaterializedViewConfigValidator()
    {
        RuleFor(view => view.Name)
            .NotEmpty()
            .Must(ValidationRules.IsSqlIdentifier)
            .WithMessage("Must be a valid ClickHouse identifier.");
        RuleFor(view => view.TargetTable)
            .NotEmpty()
            .Must(ValidationRules.IsSqlIdentifier)
            .WithMessage("Must be a valid ClickHouse identifier.");
        RuleFor(view => view.SourceTable)
            .NotEmpty()
            .Must(ValidationRules.IsSqlIdentifier)
            .WithMessage("Must be a valid ClickHouse identifier.");
        RuleForEach(view => view.Columns).SetValidator(new PipelineColumnMappingValidator());
    }
}

internal sealed class ClusterConfigValidator : AbstractValidator<ClusterConfig>
{
    public ClusterConfigValidator()
    {
        RuleFor(cluster => cluster.Name!.Trim())
            .Must(ValidationRules.IsSqlIdentifier)
            .OverridePropertyName(nameof(ClusterConfig.Name))
            .WithMessage("Cluster name must be a valid ClickHouse identifier.");
        RuleFor(cluster => cluster.DdlMode)
            .Must(mode => ClusterDdlModes.All.Contains(mode, StringComparer.OrdinalIgnoreCase))
            .WithMessage($"ddlMode must be one of: {string.Join(", ", ClusterDdlModes.All)}.");
        RuleFor(cluster => cluster.ReplicatedPath)
            .NotEmpty()
            .Must(path => path.Contains("{table}", StringComparison.Ordinal) || path.Contains("{uuid}", StringComparison.Ordinal))
            .WithMessage("replicatedPath must contain {table} or {uuid}: every replicated table needs its own Keeper path.");
        RuleFor(cluster => cluster.ReplicaName).NotEmpty();
        RuleFor(cluster => cluster.HistoryReplicatedPath)
            .NotEmpty()
            .Must(path => !path.Contains("{shard}", StringComparison.Ordinal))
            .WithMessage("historyReplicatedPath must not contain {shard}: every node keeps the full migration history.");
        RuleFor(cluster => cluster.HistoryReplicaName).NotEmpty();
        RuleFor(cluster => cluster.LocalTableSuffix)
            .NotEmpty()
            .Must(suffix => ValidationRules.IsSqlIdentifier("t" + suffix))
            .WithMessage("localTableSuffix must keep table names valid ClickHouse identifiers.");
        RuleFor(cluster => cluster.ShardingKey).NotEmpty();

        When(cluster => cluster.UsesReplicatedDatabase, () =>
        {
            RuleFor(cluster => cluster.ReplicatedDatabaseName)
                .NotEmpty()
                .Must(ValidationRules.IsSqlIdentifier)
                .WithMessage("replicatedDatabaseName must be a valid ClickHouse identifier.");
            RuleFor(cluster => cluster.ReplicatedDatabasePath).NotEmpty();
            RuleFor(cluster => cluster.ReplicatedDatabaseShardName).NotEmpty();
            RuleFor(cluster => cluster.ReplicatedDatabaseReplicaName).NotEmpty();
        });
    }
}

internal sealed class PipelineConfigValidator : AbstractValidator<PipelineConfig>
{
    public PipelineConfigValidator()
    {
        RuleFor(pipeline => pipeline.OutputPath).NotEmpty();
        RuleForEach(pipeline => pipeline.MergeTreeTables).SetValidator(new MergeTreeTableConfigValidator());
        RuleForEach(pipeline => pipeline.MaterializedViews).SetValidator(new MaterializedViewConfigValidator());
        RuleForEach(pipeline => pipeline.Aggregates).SetValidator(new AggregateConfigValidator());

        RuleFor(pipeline => pipeline.MergeTreeTables)
            .Must(tables => tables.Select(table => table.TableName).Distinct(StringComparer.OrdinalIgnoreCase).Count()
                == tables.Count)
            .WithMessage("MergeTree table names must be unique.")
            .When(pipeline => pipeline.MergeTreeTables.Count > 0);

        RuleFor(pipeline => pipeline.MaterializedViews)
            .Must(views => views.Select(view => view.Name).Distinct(StringComparer.OrdinalIgnoreCase).Count()
                == views.Count)
            .WithMessage("Materialized view names must be unique.")
            .When(pipeline => pipeline.MaterializedViews.Count > 0);

        RuleFor(pipeline => pipeline.Aggregates)
            .Must(aggregates => aggregates
                .SelectMany(aggregate => new[] { aggregate.Name, aggregate.ResolvedViewName })
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .Count() == aggregates.Count * 2)
            .WithMessage("Aggregate names and view names must be unique.")
            .When(pipeline => pipeline.Aggregates.Count > 0);
    }
}

internal sealed partial class AggregateConfigValidator : AbstractValidator<AggregateConfig>
{
    public AggregateConfigValidator() =>
        RuleFor(aggregate => aggregate).Custom((aggregate, context) =>
        {
            foreach (var error in FindErrors(aggregate))
                context.AddFailure(error);
        });

    private static IEnumerable<string> FindErrors(AggregateConfig aggregate)
    {
        var scope = $"Aggregate '{aggregate.Name}'";
        if (string.IsNullOrWhiteSpace(aggregate.Name) || !ValidationRules.IsSqlIdentifier(aggregate.Name.Trim()))
            yield return $"{scope}: name must be a valid ClickHouse identifier.";

        if (aggregate.ViewName is not null && !ValidationRules.IsSqlIdentifier(aggregate.ViewName.Trim()))
            yield return $"{scope}: viewName must be a valid ClickHouse identifier.";

        if (string.IsNullOrWhiteSpace(aggregate.SourceTable) || !ValidationRules.IsSqlIdentifier(aggregate.SourceTable.Trim()))
            yield return $"{scope}: sourceTable must be a valid ClickHouse identifier.";

        if (MergeTreeEngines.Validate(aggregate.Engine) is { } engineError)
            yield return $"{scope}: {engineError}";

        if (aggregate.OrderBy is not null && string.IsNullOrWhiteSpace(aggregate.OrderBy))
            yield return $"{scope}: orderBy must not be blank when provided.";

        foreach (var error in FindRefreshErrors(scope, aggregate.Refresh))
            yield return error;

        if (aggregate.TimeBucket is { } bucket)
        {
            if (!IsColumn(bucket.Column))
                yield return $"{scope}: timeBucket.column must be a column of the source table.";

            if (!AggregateFunctions.TimeBuckets.ContainsKey(bucket.Function?.Trim() ?? string.Empty))
            {
                yield return $"{scope}: timeBucket.function '{bucket.Function}' is not supported. " +
                    $"Must be one of: {string.Join(", ", AggregateFunctions.TimeBuckets.Keys)}.";
            }

            if (!IsColumn(bucket.Alias))
                yield return $"{scope}: timeBucket.alias must be a valid ClickHouse identifier.";
        }

        foreach (var column in aggregate.GroupBy.Where(column => !IsColumn(column)))
            yield return $"{scope}: groupBy entry '{column}' must be a column of the source table.";

        if (aggregate.Measures.Count == 0)
            yield return $"{scope}: must declare at least one measure.";

        foreach (var measure in aggregate.Measures)
        {
            foreach (var error in FindMeasureErrors(scope, measure))
                yield return error;
        }

        var duplicates = OutputColumns(aggregate)
            .GroupBy(name => name, StringComparer.OrdinalIgnoreCase)
            .Where(group => group.Count() > 1)
            .Select(group => group.Key);
        foreach (var duplicate in duplicates)
            yield return $"{scope}: duplicate column '{duplicate}' in the target table.";
    }

    private static IEnumerable<string> FindRefreshErrors(string scope, AggregateRefreshConfig? refresh)
    {
        if (refresh is null)
        {
            yield return $"{scope}: refresh is required.";
            yield break;
        }

        if (!AggregateRefreshModes.All.Contains(refresh.Mode?.Trim(), StringComparer.OrdinalIgnoreCase))
        {
            yield return $"{scope}: refresh.mode '{refresh.Mode}' is not supported. " +
                $"Must be one of: {string.Join(", ", AggregateRefreshModes.All)} " +
                "(an insert-triggered view would count redelivered Kafka rows before the source deduplicates them).";
        }

        if (refresh.Every is null || !RefreshIntervalRegex().IsMatch(refresh.Every.Trim()))
        {
            yield return $"{scope}: refresh.every '{refresh.Every}' must look like " +
                "'<n> SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|YEAR', e.g. '1 MINUTE'.";
        }
    }

    private static IEnumerable<string> FindMeasureErrors(string scope, AggregateMeasureConfig measure)
    {
        var function = AggregateFunctions.Canonical(measure.Function);
        if (function is null)
        {
            yield return $"{scope}: measure function '{measure.Function}' is not supported. " +
                $"Must be one of: {string.Join(", ", AggregateFunctions.Measures)}.";
        }
        else if (function == AggregateFunctions.Count && measure.Column is not null)
        {
            yield return $"{scope}: measure '{measure.Alias}': count() takes no column.";
        }
        else if (function != AggregateFunctions.Count && !IsColumn(measure.Column))
        {
            yield return $"{scope}: measure '{measure.Alias}': '{function}' requires a column of the source table.";
        }

        if (!IsColumn(measure.Alias))
            yield return $"{scope}: measure alias '{measure.Alias}' must be a valid ClickHouse identifier.";

        if (measure.Type is not null && string.IsNullOrWhiteSpace(measure.Type))
            yield return $"{scope}: measure '{measure.Alias}' type must not be blank when provided.";
    }

    private static IEnumerable<string> OutputColumns(AggregateConfig aggregate) =>
        (aggregate.TimeBucket is null ? [] : new[] { aggregate.TimeBucket.Alias })
            .Concat(aggregate.GroupBy)
            .Concat(aggregate.Measures.Select(measure => measure.Alias))
            .Where(name => !string.IsNullOrWhiteSpace(name))
            .Select(name => name.Trim());

    private static bool IsColumn(string? name) =>
        !string.IsNullOrWhiteSpace(name) && ValidationRules.IsSqlIdentifier(name.Trim());

    [GeneratedRegex(@"^[1-9][0-9]*\s+(SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|YEAR)S?$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex RefreshIntervalRegex();
}
