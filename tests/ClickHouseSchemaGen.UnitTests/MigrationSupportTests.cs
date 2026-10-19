namespace ClickHouseSchemaGen.UnitTests;

public sealed class FieldNumberPathTests
{
    [Fact]
    public void GivenOrderEvent_WhenMapped_ThenColumnsHaveStableFieldNumberPaths()
    {
        var columns = new DenormalizationPlanner().MapMessage(
            OrderEvent.Descriptor,
            OrdersQueueTestConfig.Defaults,
            MappingTestSupport.EmptyOverrides);

        columns.Should().OnlyContain(column => !string.IsNullOrWhiteSpace(column.FieldNumberPath));
        columns.Single(column => column.Name == "price.amount").FieldNumberPath.Should().Be("3.2");
        columns.Single(column => column.Name == "payment").FieldNumberPath.Should().Be("oneof:payment");
    }
}

public sealed class KafkaMetaColumnFactoryTests
{
    [Fact]
    public void GivenKeyAndHeadersEnabled_WhenCreateMappings_ThenUsesVirtualColumns()
    {
        var meta = new PersistKafkaMetaConfig { Key = true, Headers = true };

        var columns = KafkaMetaColumnFactory.CreateMergeTreeColumns(meta);
        var mappings = KafkaMetaColumnFactory.CreateMappings(meta);

        columns.Select(column => (column.Name, column.Type, column.FieldNumberPath)).Should().Equal(
            ("kafka_key", "String", "kafka:_key"),
            ("kafka_headers", "Map(String, String)", "kafka:_headers"));
        mappings.Select(mapping => (mapping.Source, mapping.Target, mapping.Expression)).Should().Equal(
            ("_key", "kafka_key", (string?)null),
            ("_headers.name", "kafka_headers", "mapFromArrays(`_headers.name`, `_headers.value`)"));
    }
}

public sealed class SchemaDifferTests
{
    [Fact]
    public void GivenRenameWithSameFieldNumber_WhenDiff_ThenEmitsRenamed()
    {
        var oldPlan = PlanWithMergeTreeColumn("note", "String", "10");
        var newPlan = PlanWithMergeTreeColumn("comment", "String", "10");

        var diff = SchemaDiffer.Diff(oldPlan, newPlan);

        diff.Changes.OfType<ColumnRenamed>().Should().ContainSingle(change =>
            change.TableName == "orders"
            && change.OldName == "note"
            && change.NewName == "comment");
    }

    [Fact]
    public void GivenKafkaMetaEnabled_WhenDiff_ThenAddsMetaWithoutProtoFailure()
    {
        var oldPlan = PlanWithMergeTreeColumn("order_id", "String", "1");
        var newColumns = oldPlan.MergeTreeTables[0].Config.Columns
            .Append(new PipelineColumnConfig
            {
                Name = "kafka_key",
                Type = "String",
                FieldNumberPath = "kafka:_key"
            })
            .ToList();
        var newPlan = oldPlan with
        {
            MergeTreeTables =
            [
                oldPlan.MergeTreeTables[0] with
                {
                    Config = new MergeTreeTableConfig
                    {
                        TableName = "orders",
                        OrderBy = "(order_id)",
                        Columns = newColumns
                    }
                }
            ]
        };

        var act = () => ProtoCompatibilityValidator.Validate(oldPlan, newPlan);
        act.Should().NotThrow();

        var diff = SchemaDiffer.Diff(oldPlan, newPlan);
        diff.Changes.OfType<ColumnAdded>().Should().ContainSingle(change =>
            change.ColumnName == "kafka_key");
    }

    private static ResolvedSchemaPlan PlanWithMergeTreeColumn(string name, string type, string fieldNumberPath) =>
        new()
        {
            Config = new CodegenConfig { KafkaTables = [] },
            KafkaTables = [],
            MergeTreeTables =
            [
                new MergeTreeTablePlan
                {
                    Origin = PlanOrigin.Explicit,
                    Config = new MergeTreeTableConfig
                    {
                        TableName = "orders",
                        OrderBy = "(order_id)",
                        Columns =
                        [
                            new PipelineColumnConfig
                            {
                                Name = name,
                                Type = type,
                                FieldNumberPath = fieldNumberPath
                            }
                        ]
                    }
                }
            ],
            MaterializedViews = []
        };
}

public sealed class TypeCompatibilityTests
{
    [Theory]
    [InlineData("Int32", "Nullable(Int32)", TypeChangeKind.Safe)]
    [InlineData("String", "LowCardinality(String)", TypeChangeKind.Safe)]
    [InlineData("Nullable(String)", "String", TypeChangeKind.Destructive)]
    [InlineData("Float32", "Float64", TypeChangeKind.Safe)]
    [InlineData("DateTime", "DateTime64(3)", TypeChangeKind.Rewrite)]
    [InlineData("Int8", "UInt16", TypeChangeKind.Manual)]
    [InlineData("Int16", "UInt32", TypeChangeKind.Manual)]
    [InlineData("Int32", "UInt64", TypeChangeKind.Manual)]
    [InlineData("UInt8", "Int16", TypeChangeKind.Safe)]
    [InlineData("UInt16", "Int32", TypeChangeKind.Safe)]
    [InlineData("UInt32", "Int64", TypeChangeKind.Safe)]
    [InlineData("UInt64", "Int64", TypeChangeKind.Manual)]
    public void GivenTypePair_WhenClassify_ThenReturnsExpectedKind(string oldType, string newType, TypeChangeKind expected)
        => TypeCompatibility.Classify(oldType, newType).Should().Be(expected);
}

public sealed class ProtoCompatibilityValidatorTests
{
    [Fact]
    public void GivenTwoTablesWithSameFieldNumberPathAndIncompatibleChangeInFirst_WhenValidate_ThenThrows()
    {
        var oldPlan = CreateTwoTablePlan(firstTableType: "Int64", secondTableType: "String");
        var newPlan = CreateTwoTablePlan(firstTableType: "DateTime", secondTableType: "String");

        var act = () => ProtoCompatibilityValidator.Validate(oldPlan, newPlan);

        act.Should().Throw<ProtoCompatibilityException>()
            .Which.Violations.Should().Contain(violation =>
                violation.Contains("'1'", StringComparison.Ordinal)
                && violation.Contains("Int64", StringComparison.Ordinal)
                && violation.Contains("DateTime", StringComparison.Ordinal));
    }

    private static ResolvedSchemaPlan CreateTwoTablePlan(string firstTableType, string secondTableType) =>
        new()
        {
            Config = new CodegenConfig { KafkaTables = [] },
            KafkaTables = [],
            MergeTreeTables =
            [
                new MergeTreeTablePlan
                {
                    Origin = PlanOrigin.Explicit,
                    Config = new MergeTreeTableConfig
                    {
                        TableName = "orders",
                        OrderBy = "(id)",
                        Columns =
                        [
                            new PipelineColumnConfig
                            {
                                Name = "id",
                                Type = firstTableType,
                                FieldNumberPath = "1"
                            }
                        ]
                    }
                },
                new MergeTreeTablePlan
                {
                    Origin = PlanOrigin.Explicit,
                    Config = new MergeTreeTableConfig
                    {
                        TableName = "payments",
                        OrderBy = "(id)",
                        Columns =
                        [
                            new PipelineColumnConfig
                            {
                                Name = "id",
                                Type = secondTableType,
                                FieldNumberPath = "1"
                            }
                        ]
                    }
                }
            ],
            MaterializedViews = []
        };
}

public sealed class MigrationDefectTests
{
    [Fact]
    public void GivenSnapshotWithTrailingSqlRemoved_WhenDiffFromMappedSnapshot_ThenEmitsTrailingSqlChanged()
    {
        var config = CodegenConfigLoader.Load(RepoPaths.CodegenConfigPath);
        var generator = SchemaGeneratorFactory.Create();
        var oldPlan = generator.BuildPlan(config);
        var snapshot = SchemaSnapshotMapper.FromPlan(oldPlan);

        var newConfig = CodegenConfigLoader.Load(RepoPaths.CodegenConfigPath);
        newConfig.Pipeline!.TrailingSql = string.Empty;
        var newPlan = generator.BuildPlan(newConfig);
        var mappedOldPlan = SchemaSnapshotMapper.ToPlan(snapshot, newConfig);

        var diff = SchemaDiffer.Diff(mappedOldPlan, newPlan);

        diff.Changes.OfType<TrailingSqlChanged>().Should().ContainSingle();
    }

    [Fact]
    public void GivenSnapshotWithAutoMaterializedViewReplacedByIdenticalExplicitView_WhenDiffFromMappedSnapshot_ThenDetectsOriginChange()
    {
        var generator = SchemaGeneratorFactory.Create();
        var autoConfig = new CodegenConfig
        {
            Defaults = OrdersQueueTestConfig.Defaults,
            KafkaTables = [OrdersQueueTestConfig.Create()],
            Pipeline = new PipelineConfig
            {
                OutputPath = "generated/pipeline.sql",
                MergeTreeTables =
                [
                    new MergeTreeTableConfig
                    {
                        TableName = "orders_raw",
                        SourceTable = "orders_queue",
                        OrderBy = "(order_id)",
                        Columns = []
                    }
                ],
                MaterializedViews = []
            }
        };

        var oldPlan = generator.BuildPlan(autoConfig);
        var snapshot = SchemaSnapshotMapper.FromPlan(oldPlan);

        var explicitConfig = new CodegenConfig
        {
            Defaults = OrdersQueueTestConfig.Defaults,
            KafkaTables = [OrdersQueueTestConfig.Create()],
            Pipeline = new PipelineConfig
            {
                OutputPath = "generated/pipeline.sql",
                MergeTreeTables =
                [
                    new MergeTreeTableConfig
                    {
                        TableName = "orders_raw",
                        SourceTable = "orders_queue",
                        OrderBy = "(order_id)",
                        Columns = []
                    }
                ],
                MaterializedViews =
                [
                    new MaterializedViewConfig
                    {
                        Name = "orders_raw_mv",
                        SourceTable = "orders_queue",
                        TargetTable = "orders_raw",
                        Columns = oldPlan.MaterializedViews.Single().Config.Columns.ToList()
                    }
                ]
            }
        };

        var newPlan = generator.BuildPlan(explicitConfig);
        var mappedOldPlan = SchemaSnapshotMapper.ToPlan(snapshot, explicitConfig);

        var driftAct = () => SnapshotDriftChecker.Check(newPlan, snapshot, RepoPaths.CodegenConfigPath);
        driftAct.Should().Throw<SchemaDriftException>();

        var diff = SchemaDiffer.Diff(mappedOldPlan, newPlan);
        diff.Changes.Should().NotBeEmpty();
        diff.Changes.OfType<ViewChanged>().Should().ContainSingle(change => change.ViewName == "orders_raw_mv");
    }

    [Fact]
    public void GivenMergeTreeSourceTableChanged_WhenDiffAndGenerateSql_ThenEmitsSourceTableChanged()
    {
        var oldPlan = CreateMergeTreePlan("orders_queue", "(order_id)", ("order_id", "String", "1"));
        var newPlan = CreateMergeTreePlan("payments_queue", "(order_id)", ("order_id", "String", "1"));

        var diff = SchemaDiffer.Diff(oldPlan, newPlan);
        var migrationPlan = MigrationPolicy.Apply(diff, oldPlan, newPlan);

        diff.Changes.OfType<SourceTableChanged>().Should().ContainSingle(change =>
            change.TableName == "orders"
            && change.OldSourceTable == "orders_queue"
            && change.NewSourceTable == "payments_queue");

        var sql = MigrationSqlGenerator.Generate(
            migrationPlan,
            oldPlan,
            newPlan,
            "change_source",
            parentChecksum: null,
            targetChecksum: "checksum",
            DateTimeOffset.UnixEpoch);
        sql.Should().Contain("sourceTable changed");
    }

    [Fact]
    public void GivenStandaloneOrderByChange_WhenApplyPolicy_ThenMarksManualOrderByChange()
    {
        var oldPlan = CreateMergeTreePlan("orders_queue", "(order_id)", ("order_id", "String", "1"));
        var newPlan = CreateMergeTreePlan("orders_queue", "(order_id, status)", ("order_id", "String", "1"), ("status", "String", "2"));

        var diff = SchemaDiffer.Diff(oldPlan, newPlan);
        var migrationPlan = MigrationPolicy.Apply(diff, oldPlan, newPlan);

        migrationPlan.ManualChanges.OfType<OrderByOrTtlChanged>().Should().ContainSingle(change => change.TableName == "orders");
        migrationPlan.ManualChanges.OfType<ColumnAdded>().Should().ContainSingle(change => change.ColumnName == "status");
        migrationPlan.RequiresManualApproval.Should().BeTrue();
    }

    [Fact]
    public void GivenOrderByColumnRenamedAndWidenedWhenOrderByRemainsOldName_WhenDiffAndApplyPolicy_ThenMarksManualChanges()
    {
        var oldPlan = CreateMergeTreePlan("orders_queue", "(order_id)", ("order_id", "Int32", "1"));
        var newPlan = CreateMergeTreePlan("orders_queue", "(order_id)", ("id", "Int64", "1"));

        var diff = SchemaDiffer.Diff(oldPlan, newPlan);
        var migrationPlan = MigrationPolicy.Apply(diff, oldPlan, newPlan);

        diff.Changes.OfType<ColumnTypeChanged>().Should().ContainSingle(change =>
            change.TableName == "orders"
            && change.ColumnName == "id"
            && change.Kind == TypeChangeKind.Manual);
        migrationPlan.ManualChanges.OfType<ColumnRenamed>().Should().ContainSingle(change =>
            change.TableName == "orders"
            && change.OldName == "order_id"
            && change.NewName == "id");
        migrationPlan.RequiresManualApproval.Should().BeTrue();
    }

    [Fact]
    public void GivenKafkaProtoFileOrMessageNameChanged_WhenCheckDrift_ThenThrowsSchemaDriftException()
    {
        var config = CodegenConfigLoader.Load(RepoPaths.CodegenConfigPath);
        var generator = SchemaGeneratorFactory.Create();
        var plan = generator.BuildPlan(config);
        var snapshot = SchemaSnapshotMapper.FromPlan(plan);

        var modifiedConfig = CodegenConfigLoader.Load(RepoPaths.CodegenConfigPath);
        modifiedConfig.KafkaTables[0].ProtoFile = "renamed_order_event";
        var modifiedPlan = generator.BuildPlan(modifiedConfig);

        var act = () => SnapshotDriftChecker.Check(modifiedPlan, snapshot, RepoPaths.CodegenConfigPath);

        act.Should().Throw<SchemaDriftException>()
            .Which.Differences.Should().Contain(difference => difference.Contains("protoFile", StringComparison.Ordinal));
    }

    [Fact]
    public void GivenRemovedKafkaTableFromSnapshot_WhenMapToPlanAndDiff_ThenEmitsTableRemovedWithoutThrowing()
    {
        var generator = SchemaGeneratorFactory.Create();
        var secondQueue = OrdersQueueTestConfig.Create();
        secondQueue.TableName = "payments_queue";
        secondQueue.OutputPath = "generated/payments_queue.sql";

        var oldConfig = new CodegenConfig
        {
            Defaults = OrdersQueueTestConfig.Defaults,
            KafkaTables = [OrdersQueueTestConfig.Create(), secondQueue],
            Pipeline = new PipelineConfig
            {
                OutputPath = "generated/pipeline.sql",
                MergeTreeTables = [],
                MaterializedViews = []
            }
        };

        var oldPlan = generator.BuildPlan(oldConfig);
        var snapshot = SchemaSnapshotMapper.FromPlan(oldPlan);

        var newConfig = new CodegenConfig
        {
            Defaults = OrdersQueueTestConfig.Defaults,
            KafkaTables = [OrdersQueueTestConfig.Create()],
            Pipeline = new PipelineConfig
            {
                OutputPath = "generated/pipeline.sql",
                MergeTreeTables = [],
                MaterializedViews = []
            }
        };

        var mappedOldPlan = SchemaSnapshotMapper.ToPlan(snapshot, newConfig);
        var newPlan = generator.BuildPlan(newConfig);
        var diff = SchemaDiffer.Diff(mappedOldPlan, newPlan);

        diff.Changes.OfType<TableRemoved>().Should().ContainSingle(change =>
            change.TableName == "payments_queue" && change.Engine == "Kafka");
    }

    [Fact]
    public void GivenAddedKafkaTableAndPipeline_WhenGenerateMigrationSql_ThenCreatesNewKafkaTableWithoutDetachingIt()
    {
        var generator = SchemaGeneratorFactory.Create();
        var oldConfig = new CodegenConfig
        {
            Defaults = OrdersQueueTestConfig.Defaults,
            KafkaTables = [OrdersQueueTestConfig.Create()],
            Pipeline = new PipelineConfig
            {
                OutputPath = "generated/pipeline.sql",
                MergeTreeTables =
                [
                    new MergeTreeTableConfig
                    {
                        TableName = "orders",
                        SourceTable = "orders_queue",
                        OrderBy = "(order_id)",
                        Columns = [new PipelineColumnConfig { Name = "order_id", Type = "String", FieldNumberPath = "1" }]
                    }
                ],
                MaterializedViews =
                [
                    new MaterializedViewConfig
                    {
                        Name = "orders_mv",
                        SourceTable = "orders_queue",
                        TargetTable = "orders",
                        Columns = [new PipelineColumnMapping { Source = "order_id", Target = "order_id" }]
                    }
                ]
            }
        };

        var secondQueue = OrdersQueueTestConfig.Create();
        secondQueue.TableName = "events_queue";
        secondQueue.OutputPath = "generated/events_queue.sql";

        var newConfig = new CodegenConfig
        {
            Defaults = OrdersQueueTestConfig.Defaults,
            KafkaTables = [OrdersQueueTestConfig.Create(), secondQueue],
            Pipeline = new PipelineConfig
            {
                OutputPath = "generated/pipeline.sql",
                MergeTreeTables =
                [
                    .. oldConfig.Pipeline.MergeTreeTables,
                    new MergeTreeTableConfig
                    {
                        TableName = "events",
                        SourceTable = "events_queue",
                        OrderBy = "(order_id)",
                        Columns = [new PipelineColumnConfig { Name = "order_id", Type = "String", FieldNumberPath = "1" }]
                    }
                ],
                MaterializedViews =
                [
                    .. oldConfig.Pipeline.MaterializedViews,
                    new MaterializedViewConfig
                    {
                        Name = "events_mv",
                        SourceTable = "events_queue",
                        TargetTable = "events",
                        Columns = [new PipelineColumnMapping { Source = "order_id", Target = "order_id" }]
                    }
                ]
            }
        };

        var oldPlan = generator.BuildPlan(oldConfig);
        var newPlan = generator.BuildPlan(newConfig);
        var diff = SchemaDiffer.Diff(oldPlan, newPlan);
        var migrationPlan = MigrationPolicy.Apply(diff, oldPlan, newPlan);
        var sql = MigrationSqlGenerator.Generate(
            migrationPlan,
            oldPlan,
            newPlan,
            "add_events_pipeline",
            parentChecksum: null,
            targetChecksum: "checksum",
            DateTimeOffset.UnixEpoch);

        sql.Should().Contain("CREATE TABLE IF NOT EXISTS events_queue");
        sql.Should().Contain("CREATE TABLE IF NOT EXISTS events");
        sql.Should().Contain("CREATE MATERIALIZED VIEW events_mv");
        sql.Should().NotContain("DETACH TABLE IF EXISTS events_queue");
        sql.IndexOf("CREATE TABLE IF NOT EXISTS events_queue", StringComparison.Ordinal)
            .Should().BeLessThan(sql.IndexOf("CREATE MATERIALIZED VIEW events_mv", StringComparison.Ordinal));
    }

    [Fact]
    public void GivenColumnAddedAtStartOfTable_WhenGenerateMigrationSql_ThenEmitsFirstKeyword()
    {
        var oldPlan = CreateMergeTreePlan("orders_queue", "(order_id)", ("order_id", "String", "1"));
        var newPlan = CreateMergeTreePlan(
            "orders_queue",
            "(order_id)",
            ("tenant_id", "String", "2"),
            ("order_id", "String", "1"));

        var diff = SchemaDiffer.Diff(oldPlan, newPlan);
        var migrationPlan = MigrationPolicy.Apply(diff, oldPlan, newPlan);
        var sql = MigrationSqlGenerator.Generate(
            migrationPlan,
            oldPlan,
            newPlan,
            "add_tenant_first",
            parentChecksum: null,
            targetChecksum: "checksum",
            DateTimeOffset.UnixEpoch);

        sql.Should().Contain("ADD COLUMN IF NOT EXISTS tenant_id String DEFAULT '' FIRST");
    }

    private static ResolvedSchemaPlan CreateMergeTreePlan(
        string? sourceTable,
        string orderBy,
        params (string Name, string Type, string FieldNumberPath)[] columns) =>
        new()
        {
            Config = new CodegenConfig { KafkaTables = [] },
            KafkaTables = [],
            MergeTreeTables =
            [
                new MergeTreeTablePlan
                {
                    Origin = PlanOrigin.Explicit,
                    Config = new MergeTreeTableConfig
                    {
                        TableName = "orders",
                        SourceTable = sourceTable,
                        OrderBy = orderBy,
                        Columns = columns
                            .Select(column => new PipelineColumnConfig
                            {
                                Name = column.Name,
                                Type = column.Type,
                                FieldNumberPath = column.FieldNumberPath
                            })
                            .ToList()
                    }
                }
            ],
            MaterializedViews = []
        };
}

public sealed class SnapshotDriftCheckerTests
{
    [Fact]
    public void GivenMatchingSnapshot_WhenCheck_ThenDoesNotThrow()
    {
        var config = CodegenConfigLoader.Load(RepoPaths.CodegenConfigPath);
        var plan = SchemaGeneratorFactory.Create().BuildPlan(config);
        var snapshot = SchemaSnapshotMapper.FromPlan(plan);

        var act = () => SnapshotDriftChecker.Check(plan, snapshot, RepoPaths.CodegenConfigPath);

        act.Should().NotThrow();
    }

    [Fact]
    public void GivenDriftedSnapshot_WhenCheck_ThenThrowsWithDifferences()
    {
        var config = CodegenConfigLoader.Load(RepoPaths.CodegenConfigPath);
        var plan = SchemaGeneratorFactory.Create().BuildPlan(config);
        var snapshot = SchemaSnapshotMapper.FromPlan(plan) with { TrailingSqlHash = "deadbeef" };

        var act = () => SnapshotDriftChecker.Check(plan, snapshot, RepoPaths.CodegenConfigPath);

        act.Should().Throw<SchemaDriftException>()
            .Which.Differences.Should().Contain(difference => difference.Contains("trailingSql"));
    }
}
