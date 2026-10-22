namespace ClickHouseSchemaGen.UnitTests;

public sealed class MigrationLockTests
{
    private static readonly DateTime Now = new(2026, 10, 21, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void GivenNoOtherActiveLocks_WhenEvaluateRowLock_ThenWins() =>
        MigrationLock.RowLockWins(Now, []).Should().BeTrue();

    [Fact]
    public void GivenOwnLeaseIsStrictlyEarliest_WhenEvaluateRowLock_ThenWins() =>
        MigrationLock.RowLockWins(Now, [Now.AddSeconds(1), Now.AddMinutes(5)]).Should().BeTrue();

    [Fact]
    public void GivenEarlierLeaseOfAnotherOwner_WhenEvaluateRowLock_ThenLoses() =>
        MigrationLock.RowLockWins(Now, [Now.AddSeconds(-1)]).Should().BeFalse();

    [Fact]
    public void GivenTieWithAnotherOwner_WhenEvaluateRowLock_ThenBothLose() =>
        MigrationLock.RowLockWins(Now, [Now]).Should().BeFalse();

    [Fact]
    public void GivenSingleNode_WhenCreateKeeperMapTable_ThenUsesDatabaseScopedKeeperPath()
    {
        var sql = MigrationLock.CreateKeeperMapTableSql(ClusterDdl.SingleNode, "kafka2ch");

        sql.Should().Be(
            "CREATE TABLE IF NOT EXISTS schema_migrations_lock "
                + "(key String, owner String, expires_at DateTime) "
                + "ENGINE = KeeperMap('/kafka2ch/schema_migrations_lock') PRIMARY KEY key");
    }

    [Fact]
    public void GivenReplicatedDatabase_WhenCreateKeeperMapTable_ThenLivesNextToHistoryInDefaultDatabase()
    {
        var cluster = new ClusterDdl(new ClusterConfig
        {
            Name = "kafka2ch",
            DdlMode = ClusterDdlModes.ReplicatedDatabase,
            ReplicatedDatabaseName = "kafka2ch"
        });

        MigrationLock.KeeperMapTableName(cluster).Should().Be("default.schema_migrations_lock");
        MigrationLock.CreateKeeperMapTableSql(cluster, "kafka2ch")
            .Should().StartWith("CREATE TABLE IF NOT EXISTS default.schema_migrations_lock ")
            .And.Contain("KeeperMap('/kafka2ch/schema_migrations_lock')")
            .And.NotContain("ON CLUSTER");
    }

    [Fact]
    public void GivenOnCluster_WhenKeeperMapTableName_ThenUsesCurrentDatabase()
    {
        var cluster = new ClusterDdl(new ClusterConfig { Name = "kafka2ch" });

        MigrationLock.KeeperMapTableName(cluster).Should().Be("schema_migrations_lock");
    }

    [Fact]
    public void GivenDefaultOptions_WhenCreated_ThenOwnersAreUnique() =>
        new MigrationLockOptions().Owner.Should().NotBe(new MigrationLockOptions().Owner);

    [Fact]
    public void GivenStartedWithoutMigration_WhenFindInterrupted_ThenReportsOnlyUnfinishedVersions()
    {
        var interrupted = MigrationRunner.FindInterruptedMigrations(
        [
            ("20260101000000", "20260101000000_a.sql", SchemaMigrationsTable.StartedKind),
            ("20260101000000", "20260101000000_a.sql", SchemaMigrationsTable.MigrationKind),
            ("20260102000000", "20260102000000_b.sql", SchemaMigrationsTable.StartedKind),
            ("20260102000000", "20260102000000_b.sql", SchemaMigrationsTable.StartedKind),
            ("20260103000000", "20260103000000_c.sql", SchemaMigrationsTable.MigrationKind),
            ("01", "01_orders_queue.sql", SchemaMigrationsTable.InitKind)
        ]);

        interrupted.Should().Equal("20260102000000_b.sql");
    }
}
