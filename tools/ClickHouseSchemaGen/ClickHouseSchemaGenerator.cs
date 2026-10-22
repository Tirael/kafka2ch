namespace ClickHouseSchemaGen;

public sealed class ClickHouseSchemaGenerator(
    DenormalizationPlanner planner,
    CodegenConfigValidator? configValidator = null)
{
    private readonly CodegenConfigValidator _configValidator = configValidator ?? new();
    private readonly KafkaKeyColumnMapper _keyMapper = new(planner);

    public string GenerateKafkaTableSql(
        KafkaTableConfig config,
        CodegenDefaults defaults,
        CodegenConfig? rootConfig = null)
    {
        var descriptor = ProtoDescriptorResolver.ResolveDescriptor(config.MessageType);
        var overrides = MergeFieldOverrides(rootConfig?.FieldOverrides, config.FieldOverrides);
        var columns = planner.MapMessage(descriptor, defaults, overrides);
        return KafkaTableGenerator.Generate(
            config,
            columns,
            cluster: rootConfig is null ? null : ClusterDdl.For(rootConfig));
    }

    public string GenerateKafkaTableSql(KafkaTableConfig config, CodegenDefaults defaults) =>
        GenerateKafkaTableSql(config, defaults, rootConfig: null);

    public ResolvedSchemaPlan BuildPlan(CodegenConfig config)
    {
        _configValidator.ValidateAndThrow(config);

        var kafkaTables = new List<KafkaTablePlan>();
        var queueColumnsByTable = new Dictionary<string, IReadOnlyList<ClickHouseColumn>>(StringComparer.OrdinalIgnoreCase);
        var metaByQueue = new Dictionary<string, PersistKafkaMetaConfig>(StringComparer.OrdinalIgnoreCase);
        var keyColumnsByQueue = new Dictionary<string, IReadOnlyList<ClickHouseColumn>>(StringComparer.OrdinalIgnoreCase);

        foreach (var table in config.KafkaTables)
        {
            var columns = MapKafkaTableColumns(table, config);
            queueColumnsByTable[table.TableName] = columns;
            kafkaTables.Add(new KafkaTablePlan { Config = table, Columns = columns });
            keyColumnsByQueue[table.TableName] = _keyMapper.MapKeyColumns(table, config.Defaults);
            metaByQueue[table.TableName] = KafkaMetaColumnFactory.Resolve(
                config.Defaults.PersistKafkaMeta,
                table.PersistKafkaMeta,
                includeKafkaMeta: null);
        }

        if (config.Pipeline is null)
        {
            return new ResolvedSchemaPlan
            {
                Config = config,
                KafkaTables = kafkaTables,
                MergeTreeTables = [],
                MaterializedViews = [],
                TrailingSql = null
            };
        }

        var originalMergeTree = config.Pipeline.MergeTreeTables;
        var expandedTables = originalMergeTree
            .Select(table =>
            {
                var sourceTable = table.SourceTable ?? FindViewSourceTable(config.Pipeline, table.TableName);
                var meta = ResolveMetaForMergeTree(table, sourceTable, metaByQueue, config.Defaults.PersistKafkaMeta);
                var keyColumns = KeyColumnsFor(sourceTable, keyColumnsByQueue);
                return (
                    Original: table,
                    Expanded: PipelineColumnExpander.ExpandMergeTreeTable(table, queueColumnsByTable, meta, keyColumns),
                    Origin: table.Columns.Count == 0 ? PlanOrigin.Auto : PlanOrigin.Explicit,
                    Meta: meta);
            })
            .ToList();

        foreach (var item in expandedTables)
            EnsureTtlReferencesKnownColumns(item.Expanded);

        var mergeTreePlans = expandedTables
            .Select(item => new MergeTreeTablePlan
            {
                Config = item.Expanded,
                Origin = item.Origin
            })
            .ToList();

        var expandedByName = expandedTables.ToDictionary(
            item => item.Expanded.TableName,
            item => item,
            StringComparer.OrdinalIgnoreCase);

        var explicitViews = config.Pipeline.MaterializedViews
            .Select(view =>
            {
                expandedByName.TryGetValue(view.TargetTable, out var target);
                var meta = ResolveMetaForMaterializedView(
                    view,
                    target.Original,
                    metaByQueue,
                    config.Defaults.PersistKafkaMeta);
                var keyColumns = KeyColumnsFor(view.SourceTable, keyColumnsByQueue);
                var expanded = PipelineColumnExpander.ExpandMaterializedView(
                    view,
                    queueColumnsByTable,
                    meta,
                    target.Expanded,
                    keyColumns);
                return new MaterializedViewPlan
                {
                    Config = KafkaKeyBindings.Apply(expanded, keyColumns),
                    Origin = PlanOrigin.Explicit
                };
            })
            .ToList();

        var autoViews = MaterializedViewAutoGenerator.CreateForAutoColumns(
                originalMergeTree,
                expandedTables.Select(item => item.Expanded).ToList(),
                config.Pipeline.MaterializedViews,
                sourceTable => metaByQueue.GetValueOrDefault(sourceTable) ?? config.Defaults.PersistKafkaMeta,
                sourceTable => KeyColumnsFor(sourceTable, keyColumnsByQueue))
            .Select(view => new MaterializedViewPlan
            {
                Config = KafkaKeyBindings.Apply(view, KeyColumnsFor(view.SourceTable, keyColumnsByQueue)),
                Origin = PlanOrigin.Auto
            });

        var aggregates = config.Pipeline.Aggregates
            .Select(aggregate => AggregatePlanner.Resolve(aggregate, expandedByName[aggregate.SourceTable.Trim()].Expanded))
            .ToList();

        return KafkaErrorStream.Apply(new ResolvedSchemaPlan
        {
            Config = config,
            KafkaTables = kafkaTables,
            MergeTreeTables = mergeTreePlans,
            MaterializedViews = explicitViews.Concat(autoViews).ToList(),
            Aggregates = aggregates,
            TrailingSql = config.Pipeline.TrailingSql
        });
    }

    public void GenerateFromConfigFile(string configPath, bool checkSnapshot = true)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(configPath);

        var configDirectory = Path.GetDirectoryName(Path.GetFullPath(configPath))
            ?? throw new InvalidOperationException($"Could not resolve directory for config '{configPath}'.");

        var config = CodegenConfigLoader.Load(configPath);
        var plan = BuildPlan(config);
        var scripts = SchemaPlanRenderer.RenderInitScripts(plan);

        foreach (var (outputPath, sql) in scripts)
            WriteGeneratedSql(configDirectory, CodegenConfigLoader.ResolveInitScriptPath(config, outputPath), sql);

        var snapshotPath = CodegenConfigLoader.ResolvePath(configPath, config.Migrations.SnapshotPath);
        if (File.Exists(snapshotPath))
            new MigrationOrchestrator(this).WriteVersionsScript(configPath);

        if (!checkSnapshot)
            return;

        if (!File.Exists(snapshotPath))
        {
            Console.Error.WriteLine(
                $"ClickHouse schema snapshot not found at '{snapshotPath}'. " +
                "Run: ClickHouseSchemaGen.Cli migrate init --config <path>");
            return;
        }

        SnapshotDriftChecker.Check(plan, SchemaSnapshotSerializer.Load(snapshotPath, plan), configPath);
    }

    private static IReadOnlyList<ClickHouseColumn> KeyColumnsFor(
        string? sourceTable,
        IReadOnlyDictionary<string, IReadOnlyList<ClickHouseColumn>> keyColumnsByQueue) =>
        sourceTable is not null && keyColumnsByQueue.TryGetValue(sourceTable, out var keyColumns)
            ? keyColumns
            : [];

    private static string? FindViewSourceTable(PipelineConfig pipeline, string targetTable) =>
        pipeline.MaterializedViews
            .FirstOrDefault(view => string.Equals(view.TargetTable, targetTable, StringComparison.OrdinalIgnoreCase))
            ?.SourceTable;

    private static PersistKafkaMetaConfig ResolveMetaForMergeTree(
        MergeTreeTableConfig table,
        string? sourceTable,
        IReadOnlyDictionary<string, PersistKafkaMetaConfig> metaByQueue,
        PersistKafkaMetaConfig defaults)
    {
        PersistKafkaMetaConfig? fromQueue = null;
        if (!string.IsNullOrWhiteSpace(sourceTable))
            metaByQueue.TryGetValue(sourceTable, out fromQueue);

        return KafkaMetaColumnFactory.Resolve(defaults, fromQueue, table.IncludeKafkaMeta);
    }

    private static PersistKafkaMetaConfig ResolveMetaForMaterializedView(
        MaterializedViewConfig view,
        MergeTreeTableConfig? targetTable,
        IReadOnlyDictionary<string, PersistKafkaMetaConfig> metaByQueue,
        PersistKafkaMetaConfig defaults)
    {
        PersistKafkaMetaConfig? fromQueue = null;
        if (!string.IsNullOrWhiteSpace(view.SourceTable))
            metaByQueue.TryGetValue(view.SourceTable, out fromQueue);

        return KafkaMetaColumnFactory.Resolve(
            defaults,
            fromQueue,
            view.IncludeKafkaMeta ?? targetTable?.IncludeKafkaMeta);
    }

    private static void EnsureTtlReferencesKnownColumns(MergeTreeTableConfig table)
    {
        if (ValidationRules.TtlReferencesKnownColumns(table))
            return;

        throw new InvalidOperationException(
            $"TTL expression '{table.Ttl}' references unknown columns for MergeTree table '{table.TableName}'. " +
            $"Available columns: {string.Join(", ", table.Columns.Select(column => column.Name))}.");
    }

    private IReadOnlyList<ClickHouseColumn> MapKafkaTableColumns(KafkaTableConfig table, CodegenConfig rootConfig)
    {
        var descriptor = ProtoDescriptorResolver.ResolveDescriptor(table.MessageType);
        var overrides = MergeFieldOverrides(rootConfig.FieldOverrides, table.FieldOverrides);
        return planner.MapMessage(descriptor, rootConfig.Defaults, overrides);
    }

    private static void WriteGeneratedSql(string configDirectory, string outputPath, string sql)
    {
        var fullPath = Path.GetFullPath(Path.Combine(configDirectory, outputPath));
        Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);
        File.WriteAllText(fullPath, SqlText.Normalize(sql));
    }

    private static Dictionary<string, FieldOverrideConfig> MergeFieldOverrides(
        IReadOnlyDictionary<string, FieldOverrideConfig>? rootOverrides,
        IReadOnlyDictionary<string, FieldOverrideConfig> tableOverrides) =>
        new Dictionary<string, FieldOverrideConfig>(StringComparer.OrdinalIgnoreCase)
            .MergeInto(rootOverrides)
            .MergeInto(tableOverrides);
}

internal static class ProtoDescriptorResolver
{
    public static MessageDescriptor ResolveDescriptor(string messageType)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageType);

        var type = Type.GetType(messageType, throwOnError: true)
            ?? throw new InvalidOperationException($"Message type '{messageType}' was not found.");

        if (!typeof(IMessage).IsAssignableFrom(type))
            throw new InvalidOperationException($"Type '{messageType}' is not a protobuf message.");

        var descriptorProperty = type.GetProperty(
            "Descriptor",
            BindingFlags.Public | BindingFlags.Static)
            ?? throw new InvalidOperationException($"Type '{messageType}' has no Descriptor property.");

        return (MessageDescriptor)descriptorProperty.GetValue(null)!;
    }
}
