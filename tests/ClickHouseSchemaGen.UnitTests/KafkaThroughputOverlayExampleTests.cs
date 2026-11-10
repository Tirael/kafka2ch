namespace ClickHouseSchemaGen.UnitTests;

public sealed class KafkaThroughputOverlayExampleTests
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
        AllowTrailingCommas = true
    };

    [Theory]
    [InlineData("clickhouse.codegen.throughput-demo.example.json")]
    [InlineData("clickhouse.codegen.throughput-prod.example.json")]
    public void ThroughputOverlayExample_LocksSteadyProfileAndIsNotATopologyInput(string fileName)
    {
        var root = FindRepositoryRoot();
        var path = Path.Combine(root, "src", "Sandbox.Contracts", fileName);
        File.Exists(path).Should().BeTrue($"expected overlay example at {path}");

        using var document = JsonDocument.Parse(
            File.ReadAllText(path),
            new JsonDocumentOptions { CommentHandling = JsonCommentHandling.Skip, AllowTrailingCommas = true });

        document.RootElement.TryGetProperty("kafka", out var kafkaNode).Should().BeTrue();
        var kafka = kafkaNode.Deserialize<KafkaSettingsConfig>(JsonOptions)
            ?? throw new InvalidOperationException($"Failed to bind kafka from {fileName}");

        kafka.NumConsumers.Should().Be(1);
        kafka.FlushIntervalMs.Should().Be(1000);
        kafka.MaxBlockSize.Should().BeNull();
        kafka.PollMaxBatchSize.Should().BeNull();
        kafka.ThreadPerConsumer.Should().BeNull();

        var topologies = File.ReadAllText(Path.Combine(root, "src", "Sandbox.Contracts", "clickhouse.topologies.json"));
        topologies.Should().NotContain(fileName, "overlay examples must not be build inputs");
    }

    [Fact]
    public void LiveSandboxCodegen_KeepsOptionalThroughputFieldsUnset()
    {
        var path = Path.Combine(FindRepositoryRoot(), "src", "Sandbox.Contracts", "clickhouse.codegen.json");
        var config = CodegenConfigLoader.Load(path);
        var kafka = config.KafkaTables.Should().ContainSingle().Subject.Kafka;

        kafka.NumConsumers.Should().Be(1);
        kafka.FlushIntervalMs.Should().BeNull();
        kafka.MaxBlockSize.Should().BeNull();
        kafka.PollMaxBatchSize.Should().BeNull();
        kafka.ThreadPerConsumer.Should().BeNull();
    }

    private static string FindRepositoryRoot()
    {
        for (var directory = new DirectoryInfo(AppContext.BaseDirectory); directory is not null; directory = directory.Parent)
        {
            if (File.Exists(Path.Combine(directory.FullName, "kafka2ch.sln")))
            {
                return directory.FullName;
            }
        }

        throw new InvalidOperationException("Repository root with kafka2ch.sln was not found.");
    }
}
