namespace ClickHouseSchemaGen.UnitTests;

public sealed class ClickHouseImageVersionTests
{
    [Fact]
    public void TestContainersDefaultImage_MatchesEnvExample()
    {
        var envExample = Path.Combine(FindRepositoryRoot(), ".env.example");

        var image = File.ReadAllLines(envExample)
            .Select(line => line.Trim())
            .Where(line => line.StartsWith(TestContainersEnv.ClickHouseImageVariable + "=", StringComparison.Ordinal))
            .Select(line => line[(TestContainersEnv.ClickHouseImageVariable.Length + 1)..].Trim())
            .Single();

        TestContainersEnv.DefaultClickHouseImageName.Should().Be(image);
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
