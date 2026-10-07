namespace ClickHouseSchemaGen.Snapshot;

public static class SchemaSnapshotMapper
{
    public static SchemaSnapshot FromPlan(ResolvedSchemaPlan plan, string? parentChecksum = null)
    {
        ArgumentNullException.ThrowIfNull(plan);

        var metaByQueue = plan.KafkaTables.ToDictionary(
            t => t.Config.TableName,
            t => KafkaMetaColumnFactory.Resolve(
                plan.Config.Defaults.PersistKafkaMeta,
                t.Config.PersistKafkaMeta,
                includeKafkaMeta: null),
            StringComparer.OrdinalIgnoreCase);

        return new SchemaSnapshot
        {
            ParentChecksum = parentChecksum,
            TrailingSqlHash = ComputeTrailingSqlHash(plan.TrailingSql),
            KafkaTables = plan.KafkaTables
                .Select(kafka => ToKafkaSnapshot(kafka, metaByQueue[kafka.Config.TableName]))
                .ToList(),
            MergeTreeTables = plan.MergeTreeTables.Select(ToMergeTreeSnapshot).ToList(),
            MaterializedViews = plan.MaterializedViews.Select(ToMaterializedViewSnapshot).ToList()
        };
    }

    public static ResolvedSchemaPlan ToPlan(SchemaSnapshot snapshot, CodegenConfig configForMessageTypes)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentNullException.ThrowIfNull(configForMessageTypes);

        var kafkaByTable = configForMessageTypes.KafkaTables.ToDictionary(
            t => t.TableName,
            StringComparer.OrdinalIgnoreCase);

        var kafkaPlans = snapshot.KafkaTables.Select(snapshotTable =>
        {
            if (!kafkaByTable.TryGetValue(snapshotTable.TableName, out var configEntry))
            {
                throw new InvalidOperationException(
                    $"Snapshot kafka table '{snapshotTable.TableName}' has no matching entry in codegen config.");
            }

            var config = CloneKafkaConfig(configEntry, snapshotTable);
            var columns = snapshotTable.Columns
                .Select(c => ClickHouseColumn.Create(c.Name, c.Type, MappingStrategy.Direct, fieldNumberPath: c.FieldNumberPath))
                .ToList();

            return new KafkaTablePlan { Config = config, Columns = columns };
        }).ToList();

        var mergeTreePlans = snapshot.MergeTreeTables
            .Select(ToMergeTreePlan)
            .ToList();

        var viewPlans = snapshot.MaterializedViews
            .Select(ToMaterializedViewPlan)
            .ToList();

        var pipeline = configForMessageTypes.Pipeline is null
            ? null
            : new PipelineConfig
            {
                OutputPath = configForMessageTypes.Pipeline.OutputPath,
                TrailingSql = configForMessageTypes.Pipeline.TrailingSql,
                MergeTreeTables = mergeTreePlans.Select(p => p.Config).ToList(),
                MaterializedViews = viewPlans
                    .Where(v => v.Origin == PlanOrigin.Explicit)
                    .Select(v => v.Config)
                    .ToList()
            };

        var config = new CodegenConfig
        {
            Defaults = configForMessageTypes.Defaults,
            FieldOverrides = configForMessageTypes.FieldOverrides,
            KafkaTables = kafkaPlans.Select(p => p.Config).ToList(),
            Pipeline = pipeline,
            Migrations = configForMessageTypes.Migrations
        };

        return new ResolvedSchemaPlan
        {
            Config = config,
            KafkaTables = kafkaPlans,
            MergeTreeTables = mergeTreePlans,
            MaterializedViews = viewPlans,
            TrailingSql = pipeline?.TrailingSql
        };
    }

    internal static string? ComputeTrailingSqlHash(string? trailingSql) =>
        string.IsNullOrWhiteSpace(trailingSql)
            ? null
            : SchemaSnapshotSerializer.ComputeChecksum(trailingSql.Trim());

    private static KafkaTableSnapshot ToKafkaSnapshot(KafkaTablePlan plan, PersistKafkaMetaConfig meta) =>
        new()
        {
            TableName = plan.Config.TableName,
            ProtoFile = plan.Config.ProtoFile,
            MessageName = plan.Config.MessageName,
            PersistKafkaMeta = meta,
            Kafka = new KafkaSettingsSnapshot
            {
                BrokerList = plan.Config.Kafka.BrokerList,
                Topic = plan.Config.Kafka.Topic,
                GroupName = plan.Config.Kafka.GroupName,
                SkipBytes = plan.Config.Kafka.SkipBytes,
                NumConsumers = plan.Config.Kafka.NumConsumers,
                FlattenNested = plan.Config.Kafka.FlattenNested,
                ProtobufOneofPresence = plan.Config.Kafka.ProtobufOneofPresence,
                ProtobufFlattenGoogleWrappers = plan.Config.Kafka.ProtobufFlattenGoogleWrappers
            },
            Columns = plan.Columns
                .Select(c => new SnapshotColumn
                {
                    Name = c.Name,
                    Type = c.Type,
                    FieldNumberPath = c.FieldNumberPath
                })
                .ToList()
        };

    private static MergeTreeTableSnapshot ToMergeTreeSnapshot(MergeTreeTablePlan plan) =>
        new()
        {
            TableName = plan.Config.TableName,
            Origin = plan.Origin,
            SourceTable = plan.Config.SourceTable,
            OrderBy = plan.Config.OrderBy,
            Ttl = plan.Config.Ttl,
            Columns = plan.Config.Columns
                .Select(c => new SnapshotColumn
                {
                    Name = c.Name,
                    Type = c.Type,
                    FieldNumberPath = c.FieldNumberPath ?? ""
                })
                .ToList()
        };

    private static MaterializedViewSnapshot ToMaterializedViewSnapshot(MaterializedViewPlan plan) =>
        new()
        {
            Name = plan.Config.Name,
            Origin = plan.Origin,
            SourceTable = plan.Config.SourceTable,
            TargetTable = plan.Config.TargetTable,
            Columns = plan.Config.Columns
                .Select(c => new SnapshotColumnMapping
                {
                    Source = c.Source,
                    Target = c.Target,
                    Expression = c.Expression
                })
                .ToList()
        };

    private static MergeTreeTablePlan ToMergeTreePlan(MergeTreeTableSnapshot snapshot) =>
        new()
        {
            Origin = snapshot.Origin,
            Config = new MergeTreeTableConfig
            {
                TableName = snapshot.TableName,
                OrderBy = snapshot.OrderBy,
                Ttl = snapshot.Ttl,
                SourceTable = snapshot.SourceTable,
                Columns = snapshot.Columns
                    .Select(c => new PipelineColumnConfig
                    {
                        Name = c.Name,
                        Type = c.Type,
                        FieldNumberPath = c.FieldNumberPath
                    })
                    .ToList()
            }
        };

    private static MaterializedViewPlan ToMaterializedViewPlan(MaterializedViewSnapshot snapshot) =>
        new()
        {
            Origin = snapshot.Origin,
            Config = new MaterializedViewConfig
            {
                Name = snapshot.Name,
                SourceTable = snapshot.SourceTable,
                TargetTable = snapshot.TargetTable,
                Columns = snapshot.Columns
                    .Select(c => new PipelineColumnMapping
                    {
                        Source = c.Source,
                        Target = c.Target,
                        Expression = c.Expression
                    })
                    .ToList()
            }
        };

    private static KafkaTableConfig CloneKafkaConfig(KafkaTableConfig template, KafkaTableSnapshot snapshot) =>
        new()
        {
            MessageType = template.MessageType,
            TableName = snapshot.TableName,
            ProtoFile = snapshot.ProtoFile,
            MessageName = snapshot.MessageName,
            OutputPath = template.OutputPath,
            Kafka = new KafkaSettingsConfig
            {
                BrokerList = snapshot.Kafka.BrokerList,
                Topic = snapshot.Kafka.Topic,
                GroupName = snapshot.Kafka.GroupName,
                SkipBytes = snapshot.Kafka.SkipBytes,
                NumConsumers = snapshot.Kafka.NumConsumers,
                FlattenNested = snapshot.Kafka.FlattenNested,
                ProtobufOneofPresence = snapshot.Kafka.ProtobufOneofPresence,
                ProtobufFlattenGoogleWrappers = snapshot.Kafka.ProtobufFlattenGoogleWrappers
            },
            PersistKafkaMeta = snapshot.PersistKafkaMeta,
            FieldOverrides = template.FieldOverrides
        };
}
