using System.Collections;
using System.Reflection;
using Enum = System.Enum;
using Type = System.Type;

namespace ClickHouseSchemaGen.UnitTests;

public sealed class SnapshotV2Tests
{
    public static TheoryData<string> TopologyConfigs => new()
    {
        TestContractsPaths.CodegenConfigPath,
        TestContractsPaths.ClusterCodegenConfigPath,
        TestContractsPaths.ClusterReplicatedDbCodegenConfigPath
    };

    [Theory]
    [MemberData(nameof(TopologyConfigs))]
    public void GivenV1Snapshot_WhenLoadedWithCurrentPlan_ThenUpgradesToV2WithoutDriftOrDiff(string configPath)
    {
        var config = CodegenConfigLoader.Load(configPath);
        var plan = SchemaGeneratorFactory.Create().BuildPlan(config);
        var expected = SchemaSnapshotMapper.FromPlan(plan);
        var path = Path.Combine(Path.GetTempPath(), $"v1-snapshot-{Guid.NewGuid():N}.json");
        try
        {
            File.WriteAllText(path, ToV1Json(expected));

            SchemaSnapshotSerializer.Load(path).Version.Should().Be(1);
            var upgraded = SchemaSnapshotSerializer.Load(path, plan);

            upgraded.Version.Should().Be(SchemaSnapshot.CurrentVersion);
            upgraded.Cluster.Should().Be(expected.Cluster);
            upgraded.KafkaTables.Should().OnlyContain(table => table.Key != null);
            SchemaSnapshotSerializer.SerializeToString(upgraded).Should()
                .Be(SchemaSnapshotSerializer.SerializeToString(expected));
            CollectDrift(plan, upgraded).Should().BeEmpty();
            SchemaDiffer.Diff(SchemaSnapshotMapper.ToPlan(upgraded, config), plan).Changes.Should().BeEmpty();
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Theory]
    [MemberData(nameof(TopologyConfigs))]
    public void GivenV1SnapshotOnDisk_WhenSnapshotUpgrade_ThenRewritesV2AndMigrateFindsNoChanges(string configPath)
    {
        var directory = Path.Combine(Path.GetTempPath(), $"snapshot-upgrade-{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        try
        {
            foreach (var file in Directory.GetFiles(TestContractsPaths.RootDirectory, "*.json"))
                File.Copy(file, Path.Combine(directory, Path.GetFileName(file)));
            var localConfigPath = Path.Combine(directory, Path.GetFileName(configPath));
            var config = CodegenConfigLoader.Load(localConfigPath);
            var snapshotPath = CodegenConfigLoader.ResolvePath(localConfigPath, config.Migrations.SnapshotPath);
            var migrationsDirectory = CodegenConfigLoader.ResolvePath(localConfigPath, config.Migrations.MigrationsDirectory);
            var orchestrator = new MigrationOrchestrator();

            orchestrator.Init(localConfigPath);
            File.WriteAllText(snapshotPath, ToV1Json(SchemaSnapshotSerializer.Load(snapshotPath)));

            orchestrator.Status(localConfigPath).IsDrifted.Should().BeFalse();
            var upgrade = orchestrator.UpgradeSnapshot(localConfigPath);
            var result = orchestrator.Migrate(localConfigPath, "noop", allowManual: false);

            upgrade.FromVersion.Should().Be(1);
            upgrade.ToVersion.Should().Be(SchemaSnapshot.CurrentVersion);
            SchemaSnapshotSerializer.Load(snapshotPath).Version.Should().Be(SchemaSnapshot.CurrentVersion);
            orchestrator.Status(localConfigPath).IsDrifted.Should().BeFalse();
            result.Message.Should().Be("No schema changes detected.");
            result.MigrationPath.Should().BeNull();
            Directory.GetFiles(migrationsDirectory, "*.sql").Should().BeEmpty();
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public void GivenSnapshotFromNewerVersion_WhenLoadedWithPlan_ThenThrows()
    {
        var plan = SchemaGeneratorFactory.Create().BuildPlan(CodegenConfigLoader.Load(TestContractsPaths.CodegenConfigPath));
        var snapshot = SchemaSnapshotMapper.FromPlan(plan) with { Version = SchemaSnapshot.CurrentVersion + 1 };

        var act = () => SchemaSnapshotUpgrader.Upgrade(snapshot, plan);

        act.Should().Throw<InvalidOperationException>().WithMessage("*not supported*");
    }

    [Fact]
    public void GivenDdlModeChanged_WhenDiffFromSnapshot_ThenTopologyChangeIsManual()
    {
        var generator = SchemaGeneratorFactory.Create();
        var oldConfig = CodegenConfigLoader.Load(TestContractsPaths.ClusterReplicatedDbCodegenConfigPath);
        var snapshot = SchemaSnapshotMapper.FromPlan(generator.BuildPlan(oldConfig));

        var newConfig = CodegenConfigLoader.Load(TestContractsPaths.ClusterReplicatedDbCodegenConfigPath);
        newConfig.Cluster.DdlMode = ClusterDdlModes.OnCluster;
        var newPlan = generator.BuildPlan(newConfig);
        var oldPlan = SchemaSnapshotMapper.ToPlan(snapshot, newConfig);

        var diff = SchemaDiffer.Diff(oldPlan, newPlan);
        var migrationPlan = MigrationPolicy.Apply(diff, oldPlan, newPlan);
        var sql = MigrationSqlGenerator.Generate(
            migrationPlan,
            oldPlan,
            newPlan,
            "change_ddl_mode",
            parentChecksum: null,
            targetChecksum: "checksum",
            DateTimeOffset.UnixEpoch);

        diff.Changes.Should().ContainSingle().Which.Should().Be(
            new ClusterTopologyChanged("cluster", "ddlMode changed ('replicatedDatabase' -> 'onCluster')"));
        migrationPlan.RequiresManualApproval.Should().BeTrue();
        migrationPlan.AutomaticChanges.Should().BeEmpty();
        sql.Should().Contain("ddlMode changed ('replicatedDatabase' -> 'onCluster')");
        sql.Should().Contain("data/topology migration");
    }

    [Fact]
    public void GivenKeyFormatChanged_WhenDiffFromSnapshot_ThenRecreatesQueueAndViews()
    {
        var generator = SchemaGeneratorFactory.Create();
        var oldConfig = CreateKeyConfig(KafkaKeyFormats.String);
        var snapshot = SchemaSnapshotMapper.FromPlan(generator.BuildPlan(oldConfig));

        var newConfig = CreateKeyConfig(KafkaKeyFormats.Protobuf);
        var newPlan = generator.BuildPlan(newConfig);
        var oldPlan = SchemaSnapshotMapper.ToPlan(snapshot, newConfig);

        var diff = SchemaDiffer.Diff(oldPlan, newPlan);
        var migrationPlan = MigrationPolicy.Apply(diff, oldPlan, newPlan);
        var sql = MigrationSqlGenerator.Generate(
            migrationPlan,
            oldPlan,
            newPlan,
            "change_key_format",
            parentChecksum: null,
            targetChecksum: "checksum",
            DateTimeOffset.UnixEpoch);

        diff.Changes.OfType<KafkaTableChanged>().Should().ContainSingle(change =>
            change.TableName == "orders_queue"
            && change.Reason.Contains("key format changed ('string' -> 'protobuf')", StringComparison.Ordinal));
        diff.Changes.OfType<ViewChanged>().Should().ContainSingle(change => change.ViewName == "orders_mv");
        migrationPlan.RequiresManualApproval.Should().BeFalse();
        sql.Should().Contain("DROP TABLE IF EXISTS orders_queue;");
        sql.Should().Contain("CREATE TABLE orders_queue");
        sql.Should().Contain("CREATE MATERIALIZED VIEW orders_mv TO orders AS");
    }

    [Theory]
    [MemberData(nameof(TopologyConfigs))]
    public void GivenAnySnapshotField_WhenOnlyItDrifts_ThenDifferReportsChange(string configPath) =>
        AssertDriftImpliesDiff(CodegenConfigLoader.Load(configPath));

    [Theory]
    [MemberData(nameof(TopologyConfigs))]
    public void GivenNamedCollectionConfig_WhenAnySnapshotFieldDrifts_ThenDifferReportsChange(string configPath)
    {
        var config = CodegenConfigLoader.Load(configPath);
        foreach (var table in config.KafkaTables)
        {
            table.Kafka.BrokerList = null;
            table.Kafka.NamedCollection = "kafka_orders";
        }

        AssertDriftImpliesDiff(config);
    }

    private static void AssertDriftImpliesDiff(CodegenConfig config)
    {
        var plan = SchemaGeneratorFactory.Create().BuildPlan(config);
        var baseline = SchemaSnapshotMapper.FromPlan(plan);

        List<string> missingInDrift = [];
        List<string> missingInDiff = [];
        var mutations = SnapshotMutations.Enumerate(baseline).ToList();
        mutations.Should().NotBeEmpty();

        foreach (var (path, mutated) in mutations)
        {
            if (CollectDrift(plan, mutated).Count == 0)
            {
                missingInDrift.Add(path);
                continue;
            }

            var diff = SchemaDiffer.Diff(SchemaSnapshotMapper.ToPlan(mutated, config), plan);
            if (diff.Changes.Count == 0)
                missingInDiff.Add(path);
        }

        missingInDrift.Should().BeEmpty("every snapshot field must be compared by SnapshotDriftChecker");
        missingInDiff.Should().BeEmpty("DriftImpliesDiff: every drifted field must produce a SchemaDiffer change");
    }

    private static IReadOnlyList<string> CollectDrift(ResolvedSchemaPlan plan, SchemaSnapshot snapshot)
    {
        try
        {
            SnapshotDriftChecker.Check(plan, snapshot, "config.json");
            return [];
        }
        catch (SchemaDriftException exception)
        {
            return exception.Differences;
        }
    }

    private static string ToV1Json(SchemaSnapshot snapshot)
    {
        var root = System.Text.Json.Nodes.JsonNode.Parse(SchemaSnapshotSerializer.SerializeToString(snapshot))!.AsObject();
        root["version"] = 1;
        root.Remove("cluster");
        foreach (var table in root["kafkaTables"]!.AsArray())
            table!.AsObject().Remove("key");
        foreach (var table in root["mergeTreeTables"]!.AsArray())
        {
            table!.AsObject().Remove("shardingKey");
            table.AsObject().Remove("materializedViewsWriteThroughDistributed");
        }

        return root.ToJsonString(new JsonSerializerOptions { WriteIndented = true });
    }

    private static CodegenConfig CreateKeyConfig(string keyFormat)
    {
        var queue = OrdersQueueTestConfig.Create();
        queue.Key = new KafkaKeyConfig
        {
            Format = keyFormat,
            MessageType = keyFormat == KafkaKeyFormats.Protobuf
                ? "ClickHouseSchemaGen.TestContracts.OrderKey, ClickHouseSchemaGen.TestContracts"
                : null
        };

        return new CodegenConfig
        {
            Defaults = OrdersQueueTestConfig.Defaults,
            KafkaTables = [queue],
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
    }

    private static class SnapshotMutations
    {
        private static readonly HashSet<string> IgnoredRootProperties = new(StringComparer.Ordinal)
        {
            nameof(SchemaSnapshot.Version),
            nameof(SchemaSnapshot.ParentChecksum)
        };

        public static IEnumerable<(string Path, SchemaSnapshot Snapshot)> Enumerate(SchemaSnapshot baseline)
        {
            foreach (var path in LeafPaths(typeof(SchemaSnapshot), baseline, []))
            {
                var clone = Clone(baseline);
                Apply(clone, path);
                yield return (string.Join('.', path.Select(step => step.Name)), clone);
            }
        }

        private static IEnumerable<IReadOnlyList<Step>> LeafPaths(Type type, object instance, IReadOnlyList<Step> prefix)
        {
            foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                if (!property.CanWrite || property.GetIndexParameters().Length > 0)
                    continue;
                if (prefix.Count == 0 && IgnoredRootProperties.Contains(property.Name))
                    continue;

                var value = property.GetValue(instance);
                var propertyType = property.PropertyType;

                if (IsLeaf(propertyType))
                {
                    yield return [.. prefix, new Step(property, null)];
                    continue;
                }

                if (value is IList list)
                {
                    if (list.Count == 0)
                        continue;
                    var elementType = list[0]!.GetType();
                    foreach (var path in LeafPaths(elementType, list[0]!, [.. prefix, new Step(property, 0)]))
                        yield return path;
                    continue;
                }

                if (value is null)
                    continue;

                foreach (var path in LeafPaths(propertyType, value, [.. prefix, new Step(property, null)]))
                    yield return path;
            }
        }

        private static bool IsLeaf(Type type)
        {
            var underlying = Nullable.GetUnderlyingType(type) ?? type;
            return underlying.IsPrimitive || underlying.IsEnum || underlying == typeof(string);
        }

        private static void Apply(object root, IReadOnlyList<Step> path)
        {
            var target = root;
            for (var i = 0; i < path.Count - 1; i++)
            {
                var step = path[i];
                var next = step.Property.GetValue(target)!;
                target = step.Index is { } index ? ((IList)next)[index]! : next;
            }

            var leaf = path[^1].Property;
            leaf.SetValue(target, Mutate(leaf.PropertyType, leaf.GetValue(target)));
        }

        private static object? Mutate(Type type, object? value)
        {
            var underlying = Nullable.GetUnderlyingType(type) ?? type;
            if (underlying == typeof(string))
                return value is string text ? text + "_mutated" : "mutated";
            if (underlying == typeof(bool))
                return value is not true;
            if (underlying == typeof(int))
                return value is int number ? number + 1 : 1;
            if (underlying.IsEnum)
            {
                var values = Enum.GetValues(underlying);
                var current = value is null ? -1 : Array.IndexOf(values, value);
                return values.GetValue((current + 1) % values.Length);
            }

            throw new NotSupportedException($"Cannot mutate snapshot field of type {type}.");
        }

        private static SchemaSnapshot Clone(SchemaSnapshot snapshot)
        {
            var path = Path.Combine(Path.GetTempPath(), $"snapshot-{Guid.NewGuid():N}.json");
            try
            {
                SchemaSnapshotSerializer.Save(path, snapshot);
                var clone = SchemaSnapshotSerializer.Load(path);
                return clone with
                {
                    KafkaTables = clone.KafkaTables.ToList(),
                    MergeTreeTables = clone.MergeTreeTables.ToList(),
                    MaterializedViews = clone.MaterializedViews.ToList()
                };
            }
            finally
            {
                File.Delete(path);
            }
        }

        private sealed record Step(PropertyInfo Property, int? Index)
        {
            public string Name => Index is { } index ? $"{Property.Name}[{index}]" : Property.Name;
        }
    }
}
