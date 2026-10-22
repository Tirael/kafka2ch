namespace ClickHouseSchemaGen.UnitTests;

public sealed class KafkaImageVersionTests
{
    [Theory]
    [InlineData(KafkaTestImages.KafkaImageVariable, KafkaTestImages.KafkaImageName)]
    [InlineData(KafkaTestImages.SchemaRegistryImageVariable, KafkaTestImages.SchemaRegistryImageName)]
    public void TestContainersImage_MatchesEnvExample(string variable, string testImage)
    {
        var envExample = Path.Combine(FindRepositoryRoot(), ".env.example");

        var image = File.ReadAllLines(envExample)
            .Select(line => line.Trim())
            .Where(line => line.StartsWith(variable + "=", StringComparison.Ordinal))
            .Select(line => line[(variable.Length + 1)..].Trim())
            .Single();

        testImage.Should().Be(image);
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
