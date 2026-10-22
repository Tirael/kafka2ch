namespace ClickHouseSchemaGen.UnitTests;

public sealed class ObservabilityConfigTests
{
    private const string PrometheusConfigMount =
        "./docker/clickhouse/config/prometheus.xml:/etc/clickhouse-server/config.d/prometheus.xml:ro";

    [Fact]
    public void PrometheusConfig_MatchesTestFixture()
    {
        var dockerConfig = Path.Combine(FindRepositoryRoot(), "docker", "clickhouse", "config", "prometheus.xml");

        Normalize(File.ReadAllText(dockerConfig))
            .Should().Be(Normalize(File.ReadAllText(TestContractsPaths.PrometheusConfigPath)));
        File.ReadAllText(dockerConfig).Should().Contain("<port>9363</port>");
    }

    [Theory]
    [InlineData("docker-compose.yml", 1)]
    [InlineData("docker-compose.cluster.yml", 2)]
    public void ComposeClickHouseNodes_MountPrometheusConfig(string composeFile, int expectedMounts)
    {
        var compose = File.ReadAllText(Path.Combine(FindRepositoryRoot(), composeFile));

        CountOccurrences(compose, PrometheusConfigMount).Should().Be(expectedMounts);
    }

    [Theory]
    [InlineData("ClickHouseKafkaNoCommits")]
    [InlineData("ClickHouseKafkaParseErrors")]
    [InlineData("ClickHouseKafkaNoAssignments")]
    public void PrometheusAlertRules_DefineAlert(string alert)
    {
        var rules = File.ReadAllText(
            Path.Combine(FindRepositoryRoot(), "docker", "prometheus", "alerts", "clickhouse-kafka.rules.yml"));

        rules.Should().Contain($"- alert: {alert}");
    }

    private static string Normalize(string text) => text.Replace("\r\n", "\n").Trim();

    private static int CountOccurrences(string text, string value)
    {
        var count = 0;
        for (var index = text.IndexOf(value, StringComparison.Ordinal);
             index >= 0;
             index = text.IndexOf(value, index + value.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
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
