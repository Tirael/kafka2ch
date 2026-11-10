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
    [InlineData("ClickHouseOrdersCrossShardDuplicates")]
    [InlineData("ClickHouseViewRefreshStuck")]
    [InlineData("ClickHouseViewRefreshException")]
    [InlineData("ClickHouseOrdersQueueErrorsGrowth")]
    [InlineData("ClickHouseKafkaQueueWithoutMv")]
    [InlineData("ClickHouseKafkaQueueMissing")]
    public void PrometheusAlertRules_DefineAlert(string alert)
    {
        var rules = File.ReadAllText(
            Path.Combine(FindRepositoryRoot(), "docker", "prometheus", "alerts", "clickhouse-kafka.rules.yml"));

        rules.Should().Contain($"- alert: {alert}");
    }

    [Fact]
    public void MigrateRunbook_DocumentsProdDiscipline()
    {
        var runbook = File.ReadAllText(Path.Combine(FindRepositoryRoot(), "docs", "migrate-runbook.md"));

        runbook.Should().Contain("--skip-preflight");
        runbook.Should().Contain("schema_migrations");
        runbook.Should().Contain("DETACH");
        runbook.Should().Contain("Staging drill");
        runbook.Should().Contain("параллельн");
        runbook.Should().Contain("run-verify-gate.sh");
        runbook.Should().Contain("VERIFY_ASSUME_STACK=1");
        runbook.Should().Contain("Data-quality gate");
        runbook.Should().Contain("neither attached nor detached");
        runbook.Should().Contain("ClickHouseKafkaQueueWithoutMv");
        runbook.Should().Contain("ClickHouseKafkaQueueMissing");
    }

    [Fact]
    public void VerifyClusterScript_GatesCountVsUniqExactOrderId()
    {
        var script = File.ReadAllText(Path.Combine(FindRepositoryRoot(), "scripts", "verify-cluster.sh"));

        script.Should().Contain("uniqExact(order_id)");
        script.Should().Contain("FROM orders FINAL");
        script.Should().Contain("N1 A1");
    }

    [Fact]
    public void VerifySingleScript_GatesCountVsUniqExactOrderId()
    {
        var script = File.ReadAllText(Path.Combine(FindRepositoryRoot(), "scripts", "verify-single.sh"));

        script.Should().Contain("uniqExact(order_id)");
        script.Should().Contain("FROM orders FINAL");
        script.Should().Contain("fail");
    }

    [Fact]
    public void RunVerifyGateScript_SupportsAssumeStackAndTopologies()
    {
        var script = File.ReadAllText(Path.Combine(FindRepositoryRoot(), "scripts", "run-verify-gate.sh"));

        script.Should().Contain("VERIFY_ASSUME_STACK");
        script.Should().Contain("verify-cluster.sh");
        script.Should().Contain("verify-single.sh");
        script.Should().Contain("wait_for_rows");
    }

    [Fact]
    public void VerifyDataGateWorkflow_IsNightlyNotPullRequest()
    {
        var workflow = File.ReadAllText(
            Path.Combine(FindRepositoryRoot(), ".github", "workflows", "verify-data-gate.yml"));

        workflow.Should().Contain("schedule:");
        workflow.Should().Contain("workflow_dispatch:");
        workflow.Should().Contain("run-verify-gate.sh");
        workflow.Should().Contain("VERIFY_TOPOLOGY: cluster");
        workflow.Should().NotContain("pull_request:");
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
