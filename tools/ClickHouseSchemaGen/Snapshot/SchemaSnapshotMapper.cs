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
            TrailingSqlHash = plan.TrailingSqlHash ?? ComputeTrailingSqlHash(plan.TrailingSql),
            Cluster = ToClusterSnapshot(plan.Config.Cluster),
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
            kafkaByTable.TryGetValue(snapshotTable.TableName, out var configEntry);

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
            && mergeTreePlans.Count == 0
            && viewPlans.Count == 0
            ? null
            : new PipelineConfig
            {
                OutputPath = configForMessageTypes.Pipeline?.OutputPath ?? "init/02_pipeline.sql",
                TrailingSql = configForMessageTypes.Pipeline?.TrailingSql,
                MergeTreeTables = mergeTreePlans.Select(p => p.Config).ToList(),
                MaterializedViews = viewPlans
                    .Select(v => v.Config)
                    .ToList()
            };

        var config = new CodegenConfig
        {
            Cluster = RestoreCluster(snapshot.Cluster, configForMessageTypes.Cluster),
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
            TrailingSql = pipeline?.TrailingSql,
            TrailingSqlHash = snapshot.TrailingSqlHash
        };
    }

    internal static ClusterSnapshot? ToClusterSnapshot(ClusterConfig? cluster) =>
        cluster?.Enabled != true
            ? null
            : new ClusterSnapshot
            {
                Name = cluster.Name!.Trim(),
                DdlMode = ClusterDdlModes.All.FirstOrDefault(mode =>
                        string.Equals(mode, cluster.DdlMode, StringComparison.OrdinalIgnoreCase))
                    ?? cluster.DdlMode,
                LocalTableSuffix = cluster.LocalTableSuffix,
                ShardingKey = cluster.ShardingKey.Trim(),
                MaterializedViewsWriteThroughDistributed = cluster.MaterializedViewsWriteThroughDistributed
            };

    internal static KafkaKeySnapshot ToKeySnapshot(KafkaKeyConfig key) =>
        new()
        {
            Format = key.Format.ToLowerInvariant(),
            MessageType = key.MessageType,
            SkipBytes = key.SkipBytes
        };

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
            Key = ToKeySnapshot(plan.Config.Key),
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
                ProtobufFlattenGoogleWrappers = plan.Config.Kafka.ProtobufFlattenGoogleWrappers,
                HandleErrorMode = plan.Config.Kafka.UsesDefaultHandleErrorMode
                    ? null
                    : plan.Config.Kafka.HandleErrorMode.ToLowerInvariant(),
                SkipBrokenMessages = plan.Config.Kafka.SkipBrokenMessages
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
            Engine = MergeTreeEngines.ToSnapshot(plan.Config.Engine),
            Ttl = plan.Config.Ttl,
            ShardingKey = plan.Config.ShardingKey,
            MaterializedViewsWriteThroughDistributed = plan.Config.MaterializedViewsWriteThroughDistributed,
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
            Where = plan.Config.Where,
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
                Engine = MergeTreeEngines.Normalize(snapshot.Engine),
                Ttl = snapshot.Ttl,
                ShardingKey = snapshot.ShardingKey,
                MaterializedViewsWriteThroughDistributed = snapshot.MaterializedViewsWriteThroughDistributed,
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
                Where = snapshot.Where,
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

    private static KafkaTableConfig CloneKafkaConfig(KafkaTableConfig? template, KafkaTableSnapshot snapshot) =>
        new()
        {
            MessageType = template?.MessageType ?? snapshot.MessageName,
            TableName = snapshot.TableName,
            ProtoFile = snapshot.ProtoFile,
            MessageName = snapshot.MessageName,
            OutputPath = template?.OutputPath ?? $"init/01_{snapshot.TableName}.sql",
            Kafka = new KafkaSettingsConfig
            {
                BrokerList = snapshot.Kafka.BrokerList,
                Topic = snapshot.Kafka.Topic,
                GroupName = snapshot.Kafka.GroupName,
                SkipBytes = snapshot.Kafka.SkipBytes,
                NumConsumers = snapshot.Kafka.NumConsumers,
                FlattenNested = snapshot.Kafka.FlattenNested,
                ProtobufOneofPresence = snapshot.Kafka.ProtobufOneofPresence,
                ProtobufFlattenGoogleWrappers = snapshot.Kafka.ProtobufFlattenGoogleWrappers,
                HandleErrorMode = snapshot.Kafka.HandleErrorMode ?? KafkaHandleErrorModes.Default,
                SkipBrokenMessages = snapshot.Kafka.SkipBrokenMessages
            },
            Key = new KafkaKeyConfig
            {
                Format = snapshot.Key?.Format ?? KafkaKeyFormats.String,
                MessageType = snapshot.Key?.MessageType,
                SkipBytes = snapshot.Key?.SkipBytes,
                FieldOverrides = template?.Key.FieldOverrides
                    ?? new Dictionary<string, FieldOverrideConfig>(StringComparer.OrdinalIgnoreCase)
            },
            PersistKafkaMeta = snapshot.PersistKafkaMeta,
            FieldOverrides = template?.FieldOverrides
                ?? new Dictionary<string, FieldOverrideConfig>(StringComparer.OrdinalIgnoreCase)
        };

    private static ClusterConfig RestoreCluster(ClusterSnapshot? snapshot, ClusterConfig template) =>
        snapshot is null
            ? new ClusterConfig()
            : new ClusterConfig
            {
                Name = snapshot.Name,
                DdlMode = snapshot.DdlMode,
                ReplicatedPath = template.ReplicatedPath,
                ReplicaName = template.ReplicaName,
                HistoryReplicatedPath = template.HistoryReplicatedPath,
                HistoryReplicaName = template.HistoryReplicaName,
                LocalTableSuffix = snapshot.LocalTableSuffix,
                ShardingKey = snapshot.ShardingKey,
                MaterializedViewsWriteThroughDistributed = snapshot.MaterializedViewsWriteThroughDistributed,
                ReplicatedDatabaseName = template.ReplicatedDatabaseName,
                ReplicatedDatabasePath = template.ReplicatedDatabasePath,
                ReplicatedDatabaseReplicaName = template.ReplicatedDatabaseReplicaName
            };
}
