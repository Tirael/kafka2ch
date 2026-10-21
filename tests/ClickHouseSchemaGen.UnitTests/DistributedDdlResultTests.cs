using System.Data;

namespace ClickHouseSchemaGen.UnitTests;

public sealed class DistributedDdlResultTests
{
    private const string Statement = "CREATE TABLE probe ON CLUSTER kafka2ch (x UInt8) ENGINE = MergeTree ORDER BY x";

    [Theory]
    [InlineData("ALTER TABLE orders_local ON CLUSTER kafka2ch MODIFY COLUMN a Int64", true)]
    [InlineData("DROP VIEW IF EXISTS orders_mv ON CLUSTER kafka2ch", true)]
    [InlineData("ALTER TABLE orders MODIFY COLUMN a Int64", false)]
    [InlineData("INSERT INTO t VALUES ('on cluster')", false)]
    public void GivenStatement_WhenIsDistributedDdl_ThenDetectsOnCluster(string statement, bool expected)
        => DistributedDdlResult.IsDistributedDdl(statement).Should().Be(expected);

    [Fact]
    public void GivenAllHostsSucceeded_WhenEnsureSucceeded_ThenDoesNotThrow()
    {
        var act = () => DistributedDdlResult.EnsureSucceeded(Statement,
        [
            new DistributedDdlHostStatus("ch1:9000", 0, string.Empty, 1),
            new DistributedDdlHostStatus("ch2:9000", 0, string.Empty, 0)
        ]);

        act.Should().NotThrow();
    }

    [Fact]
    public void GivenSecondHostFailed_WhenEnsureSucceeded_ThenThrowsWithFailedAndSucceededHosts()
    {
        var act = () => DistributedDdlResult.EnsureSucceeded(Statement,
        [
            new DistributedDdlHostStatus("ch1:9000", 0, string.Empty, 1),
            new DistributedDdlHostStatus("ch2:9000", 57, "Code: 57. DB::Exception: Table default.probe already exists.", 0)
        ]);

        var exception = act.Should().Throw<DistributedDdlException>().Which;
        exception.FailedHosts.Should().Equal("ch2:9000");
        exception.Message.Should().Contain("Failed: ch2:9000 (code 57: Code: 57. DB::Exception: Table default.probe already exists.)")
            .And.Contain("Succeeded: ch1:9000")
            .And.Contain("Statement: CREATE TABLE probe ON CLUSTER kafka2ch");
    }

    [Fact]
    public void GivenHostTimedOut_WhenEnsureSucceeded_ThenThrowsNamingUnfinishedHost()
    {
        var act = () => DistributedDdlResult.EnsureSucceeded(Statement,
        [
            new DistributedDdlHostStatus("ch1:9000", 0, string.Empty, 1),
            new DistributedDdlHostStatus("ch2:9000", null, null, 1)
        ]);

        act.Should().Throw<DistributedDdlException>()
            .Which.Message.Should().Contain("ch2:9000 (not finished before distributed_ddl_task_timeout)");
    }

    [Fact]
    public void GivenHostsRemainingWithoutRows_WhenEnsureSucceeded_ThenThrows()
    {
        var act = () => DistributedDdlResult.EnsureSucceeded(Statement,
        [
            new DistributedDdlHostStatus("ch1:9000", 0, string.Empty, 1)
        ]);

        act.Should().Throw<DistributedDdlException>().Which.Message.Should().Contain("1 host(s) did not report");
    }

    [Fact]
    public void GivenOnClusterStatusRow_WhenReadHost_ThenMapsHostPortStatusAndError()
    {
        var table = new DataTable();
        table.Columns.Add("host", typeof(string));
        table.Columns.Add("port", typeof(ushort));
        table.Columns.Add("status", typeof(long));
        table.Columns.Add("error", typeof(string));
        table.Columns.Add("num_hosts_remaining", typeof(ulong));
        table.Columns.Add("num_hosts_active", typeof(ulong));
        table.Rows.Add("ch2", (ushort)9000, 57L, "boom", 0UL, 0UL);
        table.Rows.Add("ch3", (ushort)9000, DBNull.Value, DBNull.Value, 1UL, 1UL);
        using var reader = table.CreateDataReader();

        reader.Read().Should().BeTrue();
        DistributedDdlResult.ReadHost(reader).Should().Be(new DistributedDdlHostStatus("ch2:9000", 57, "boom", 0));
        reader.Read().Should().BeTrue();
        DistributedDdlResult.ReadHost(reader).Should().Be(new DistributedDdlHostStatus("ch3:9000", null, null, 1));
    }

    [Fact]
    public void GivenReplicatedDatabaseStatusRow_WhenReadHost_ThenUsesShardAndReplica()
    {
        var table = new DataTable();
        table.Columns.Add("shard", typeof(string));
        table.Columns.Add("replica", typeof(string));
        table.Columns.Add("status", typeof(long));
        table.Columns.Add("num_hosts_remaining", typeof(ulong));
        table.Rows.Add("01", "ch1", 0L, 0UL);
        using var reader = table.CreateDataReader();

        reader.Read().Should().BeTrue();
        DistributedDdlResult.ReadHost(reader).Should().Be(new DistributedDdlHostStatus("01|ch1", 0, null, 0));
    }
}

public sealed class RestoreReportTests
{
    [Fact]
    public void GivenEmptyReport_WhenSummary_ThenReportsNothing()
    {
        var report = new RestoreReport();

        report.IsEmpty.Should().BeTrue();
        report.Summary.Should().Be("restored: none; not restored: none");
    }

    [Fact]
    public void GivenRestoredAndSkippedObjects_WhenWrappedInMigrationFailure_ThenMessageListsBoth()
    {
        var report = new RestoreReport();
        report.AddRestored("orders_queue");
        report.AddNotRestored("orders_mv", "source 'orders_queue' schema changed");

        var exception = new MigrationFailedException("2099_broken.sql", report, new InvalidOperationException("boom"));

        report.IsEmpty.Should().BeFalse();
        exception.InnerException!.Message.Should().Be("boom");
        exception.Message.Should().Be(
            "Migration '2099_broken.sql' failed: boom Recovery report: "
                + "restored: orders_queue; not restored: orders_mv (source 'orders_queue' schema changed).");
    }
}
