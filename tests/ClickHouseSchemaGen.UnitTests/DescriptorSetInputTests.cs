namespace ClickHouseSchemaGen.UnitTests;

public sealed class DescriptorSetInputTests : IDisposable
{
    private const string OrderEventClrType = "ClickHouseSchemaGen.TestContracts.OrderEvent, ClickHouseSchemaGen.TestContracts";

    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"kafka2ch-descriptor-set-{Guid.NewGuid():N}");

    public DescriptorSetInputTests() => Directory.CreateDirectory(_directory);

    public void Dispose() => Directory.Delete(_directory, recursive: true);

    public static TheoryData<string> TopologyConfigs => new()
    {
        "clickhouse.codegen.json",
        "clickhouse.codegen.cluster.json",
        "clickhouse.codegen.cluster.replicated-db.json"
    };

    [Theory]
    [MemberData(nameof(TopologyConfigs))]
    public void GivenConfigWithDescriptorSetPath_WhenInitAndGenerate_ThenWrittenFilesAreByteIdenticalToAssemblyPath(
        string configFileName)
    {
        var assemblyDirectory = CopyFixtures("assembly");
        var descriptorSetDirectory = CopyFixtures("descriptor-set");
        SetBaseDescriptorSetPath(descriptorSetDirectory, "test-contracts.binpb");

        InitAndGenerate(Path.Combine(assemblyDirectory, configFileName));
        InitAndGenerate(Path.Combine(descriptorSetDirectory, configFileName));

        var expected = GeneratedFiles(assemblyDirectory);
        var actual = GeneratedFiles(descriptorSetDirectory);
        expected.Keys.Should().Contain(path => path.EndsWith("schema.snapshot.json", StringComparison.Ordinal));
        expected.Keys.Count(path => path.EndsWith(".sql", StringComparison.Ordinal)).Should().BeGreaterThanOrEqualTo(3);
        actual.Keys.Should().BeEquivalentTo(expected.Keys);
        foreach (var (path, bytes) in expected)
            actual[path].Should().Equal(bytes, $"'{path}' must not depend on the descriptor source");
    }

    [Theory]
    [MemberData(nameof(TopologyConfigs))]
    public void GivenDescriptorSetSource_WhenBuildPlan_ThenInitSqlAndSnapshotMatchAssemblyPath(string configFileName)
    {
        var config = CodegenConfigLoader.Load(Path.Combine(TestContractsPaths.RootDirectory, configFileName));
        var source = DescriptorSetMessageDescriptorSource.Load(TestContractsPaths.DescriptorSetPath);

        var expected = SchemaGeneratorFactory.Create().BuildPlan(config);
        var actual = new ClickHouseSchemaGenerator(new DenormalizationPlanner(), descriptorSource: source).BuildPlan(config);

        SchemaPlanRenderer.RenderInitScripts(actual).Should().Equal(SchemaPlanRenderer.RenderInitScripts(expected));
        SchemaSnapshotSerializer.SerializeToString(SchemaSnapshotMapper.FromPlan(actual))
            .Should().Be(SchemaSnapshotSerializer.SerializeToString(SchemaSnapshotMapper.FromPlan(expected)));
    }

    [Fact]
    public void GivenEveryCompiledTestContractMessage_WhenResolvedByClrName_ThenMapsToSameColumns()
    {
        var source = DescriptorSetMessageDescriptorSource.Load(TestContractsPaths.DescriptorSetPath);
        var planner = new DenormalizationPlanner();
        var messageTypes = typeof(OrderEvent).Assembly.GetTypes()
            .Where(type => typeof(IMessage).IsAssignableFrom(type) && !type.IsAbstract)
            .ToList();

        messageTypes.Should().Contain(typeof(NestedHolder.Types.Inner));
        foreach (var type in messageTypes)
        {
            var messageType = $"{type.FullName}, {type.Assembly.GetName().Name}";
            var expected = ProtoToClickHouseMapper.ResolveDescriptor(messageType);

            var actual = source.Resolve(messageType);

            actual.FullName.Should().Be(expected.FullName);
            planner.MapMessage(actual, OrdersQueueTestConfig.Defaults, MappingTestSupport.EmptyOverrides)
                .Should().Equal(
                    planner.MapMessage(expected, OrdersQueueTestConfig.Defaults, MappingTestSupport.EmptyOverrides),
                    $"columns of {messageType}");
        }
    }

    [Theory]
    [InlineData("testcontracts.orders.v1.OrderEvent", "testcontracts.orders.v1.OrderEvent")]
    [InlineData(".testcontracts.fixtures.v1.NestedHolder.Inner", "testcontracts.fixtures.v1.NestedHolder.Inner")]
    [InlineData("ClickHouseSchemaGen.TestContracts.Fixtures.NestedHolder+Types+Inner", "testcontracts.fixtures.v1.NestedHolder.Inner")]
    [InlineData(OrderEventClrType, "testcontracts.orders.v1.OrderEvent")]
    public void GivenMessageName_WhenResolve_ThenFindsMessage(string messageType, string expectedFullName)
    {
        var source = DescriptorSetMessageDescriptorSource.Load(TestContractsPaths.DescriptorSetPath);

        source.Resolve(messageType).FullName.Should().Be(expectedFullName);
    }

    [Fact]
    public void GivenUnknownMessage_WhenResolve_ThenThrowsNamingMessageAndDescriptorSet()
    {
        var source = DescriptorSetMessageDescriptorSource.Load(TestContractsPaths.DescriptorSetPath);

        var act = () => source.Resolve("testcontracts.orders.v1.Missing");

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*'testcontracts.orders.v1.Missing'*test-contracts.binpb*");
    }

    [Fact]
    public void GivenDescriptorSetWithoutImportedFile_WhenLoad_ThenThrowsNamingMissingImport()
    {
        var path = WriteFilteredSet("no-money.binpb", file => file.Name != "common/money.proto");

        var act = () => DescriptorSetMessageDescriptorSource.Load(path);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*imports 'common/money.proto'*--include_imports*");
    }

    [Fact]
    public void GivenConfigPointingToDescriptorSetWithoutImportedFile_WhenBuildPlan_ThenThrows()
    {
        var path = WriteFilteredSet("no-money.binpb", file => file.Name != "common/money.proto");
        var config = CodegenConfigLoader.Load(TestContractsPaths.CodegenConfigPath);
        config.DescriptorSetPath = path;

        var act = () => SchemaGeneratorFactory.Create().BuildPlan(config);

        act.Should().Throw<InvalidOperationException>().WithMessage("*imports 'common/money.proto'*");
    }

    [Fact]
    public void GivenDescriptorSetWithoutWellKnownTypes_WhenResolve_ThenUsesBuiltInWellKnownTypes()
    {
        var path = WriteFilteredSet("no-wkt.binpb", file => !file.Name.StartsWith("google/protobuf/", StringComparison.Ordinal));
        var planner = new DenormalizationPlanner();

        var descriptor = DescriptorSetMessageDescriptorSource.Load(path).Resolve(OrderEventClrType);

        planner.MapMessage(descriptor, OrdersQueueTestConfig.Defaults, MappingTestSupport.EmptyOverrides)
            .Should().Equal(planner.MapMessage(OrderEvent.Descriptor, OrdersQueueTestConfig.Defaults, MappingTestSupport.EmptyOverrides));
    }

    [Fact]
    public void GivenMissingDescriptorSetFile_WhenLoad_ThenThrowsFileNotFound()
    {
        var act = () => DescriptorSetMessageDescriptorSource.Load(Path.Combine(_directory, "missing.binpb"));

        act.Should().Throw<FileNotFoundException>().WithMessage("*missing.binpb*");
    }

    [Fact]
    public void GivenDescriptorSetPathInBaseConfig_WhenLoadOverlay_ThenPathIsResolvedAgainstBaseConfig()
    {
        var baseDirectory = Path.Combine(_directory, "base");
        Directory.CreateDirectory(baseDirectory);
        File.WriteAllText(Path.Combine(baseDirectory, "base.json"), """{ "descriptorSetPath": "protos/contracts.binpb" }""");
        var overlayPath = Path.Combine(_directory, "overlay.json");
        File.WriteAllText(overlayPath, """{ "extends": "base/base.json" }""");

        var config = CodegenConfigLoader.Load(overlayPath);

        config.DescriptorSetPath.Should().Be(Path.Combine(baseDirectory, "protos", "contracts.binpb"));
    }

    [Fact]
    public void GivenCompiledTestContracts_WhenComparedWithDescriptorSetFixture_ThenFixtureIsUpToDate()
    {
        var fixture = FileDescriptorSet.Parser.ParseFrom(File.ReadAllBytes(TestContractsPaths.DescriptorSetPath)).File
            .Where(file => !file.Name.StartsWith("google/protobuf/", StringComparison.Ordinal))
            .ToDictionary(file => file.Name, WithoutJsonNames);
        var compiled = typeof(OrderEvent).Assembly.GetTypes()
            .Where(type => typeof(IMessage).IsAssignableFrom(type) && !type.IsAbstract)
            .Select(type => ProtoToClickHouseMapper.ResolveDescriptor($"{type.FullName}, {type.Assembly.GetName().Name}").File)
            .DistinctBy(file => file.Name)
            .ToDictionary(file => file.Name, file => FileDescriptorProto.Parser.ParseFrom(file.SerializedData));

        fixture.Keys.Should().BeEquivalentTo(
            compiled.Keys,
            "regenerate the fixture with scripts/generate-test-descriptor-set.sh");
        foreach (var (name, proto) in compiled)
            fixture[name].Should().Be(proto, $"'{name}' is stale; run scripts/generate-test-descriptor-set.sh");
    }

    private static FileDescriptorProto WithoutJsonNames(FileDescriptorProto file)
    {
        var copy = file.Clone();
        foreach (var message in copy.MessageType)
            ClearJsonNames(message);
        return copy;
    }

    private static void ClearJsonNames(DescriptorProto message)
    {
        foreach (var field in message.Field.Concat(message.Extension))
            field.ClearJsonName();
        foreach (var nested in message.NestedType)
            ClearJsonNames(nested);
    }

    private string CopyFixtures(string name)
    {
        var destination = Path.Combine(_directory, name);
        Directory.CreateDirectory(destination);
        foreach (var file in Directory.GetFiles(TestContractsPaths.RootDirectory))
            File.Copy(file, Path.Combine(destination, Path.GetFileName(file)));
        return destination;
    }

    private static void SetBaseDescriptorSetPath(string directory, string descriptorSetPath)
    {
        var path = Path.Combine(directory, "clickhouse.codegen.json");
        var node = JsonNode.Parse(File.ReadAllText(path))!.AsObject();
        node["descriptorSetPath"] = descriptorSetPath;
        File.WriteAllText(path, node.ToJsonString());
    }

    private static void InitAndGenerate(string configPath)
    {
        var generator = SchemaGeneratorFactory.Create();
        new MigrationOrchestrator(generator).Init(configPath);
        generator.GenerateFromConfigFile(configPath);
    }

    private static Dictionary<string, byte[]> GeneratedFiles(string directory) =>
        Directory.GetFiles(directory, "*", SearchOption.AllDirectories)
            .Where(path => Path.GetDirectoryName(path) != directory)
            .ToDictionary(path => Path.GetRelativePath(directory, path), File.ReadAllBytes);

    private string WriteFilteredSet(string fileName, Func<FileDescriptorProto, bool> keep)
    {
        var set = FileDescriptorSet.Parser.ParseFrom(File.ReadAllBytes(TestContractsPaths.DescriptorSetPath));
        var filtered = new FileDescriptorSet();
        filtered.File.AddRange(set.File.Where(keep));
        var path = Path.Combine(_directory, fileName);
        File.WriteAllBytes(path, filtered.ToByteArray());
        return path;
    }
}
