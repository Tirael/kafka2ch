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
    public void GivenTypePair_WhenClassify_ThenReturnsExpectedKind(string oldType, string newType, TypeChangeKind expected)
    {
        TypeCompatibility.Classify(oldType, newType).Should().Be(expected);
    }
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
