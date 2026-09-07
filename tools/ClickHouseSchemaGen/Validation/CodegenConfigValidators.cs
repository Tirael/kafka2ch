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
        RuleFor(settings => settings.BrokerList).NotEmpty();
        RuleFor(settings => settings.Topic).NotEmpty();
        RuleFor(settings => settings.GroupName).NotEmpty();
        RuleFor(settings => settings.SkipBytes).GreaterThanOrEqualTo(0);
        RuleFor(settings => settings.NumConsumers).GreaterThan(0);
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

internal sealed class PipelineColumnConfigValidator : AbstractValidator<PipelineColumnConfig>
{
    public PipelineColumnConfigValidator()
    {
        RuleFor(column => column.Name)
            .NotEmpty()
            .Must(ValidationRules.IsSqlIdentifier)
            .WithMessage("Must be a valid ClickHouse identifier.");
        RuleFor(column => column.Type).NotEmpty();
    }
}

internal sealed class PipelineColumnMappingValidator : AbstractValidator<PipelineColumnMapping>
{
    public PipelineColumnMappingValidator()
    {
        RuleFor(mapping => mapping.Source).NotEmpty();
        RuleFor(mapping => mapping.Target)
            .NotEmpty()
            .Must(ValidationRules.IsSqlIdentifier)
            .WithMessage("Must be a valid ClickHouse identifier.");
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
        RuleFor(table => table.Columns).NotEmpty();
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
        RuleFor(view => view.Columns).NotEmpty();
        RuleForEach(view => view.Columns).SetValidator(new PipelineColumnMappingValidator());
    }
}

internal sealed class PipelineConfigValidator : AbstractValidator<PipelineConfig>
{
    public PipelineConfigValidator()
    {
        RuleFor(pipeline => pipeline.OutputPath).NotEmpty();
        RuleForEach(pipeline => pipeline.MergeTreeTables).SetValidator(new MergeTreeTableConfigValidator());
        RuleForEach(pipeline => pipeline.MaterializedViews).SetValidator(new MaterializedViewConfigValidator());

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
    }
}
