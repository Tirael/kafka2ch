namespace ClickHouseSchemaGen.UnitTests;

public sealed class MigratorArgumentsTests
{
    [Fact]
    public void GivenOnlyMigrations_WhenParse_ThenUsesDefaultLockSettings()
    {
        var parsed = MigratorArguments.TryParse(["--migrations", "/m"], out var arguments, out var error);

        parsed.Should().BeTrue(error);
        arguments!.MigrationsDirectory.Should().Be("/m");
        arguments.LockTimeout.Should().Be(MigrationLockOptions.DefaultTimeout);
        arguments.LockTtl.Should().Be(MigrationLockOptions.DefaultTtl);
        arguments.Cluster.Should().BeNull();
        arguments.DdlMode.Should().BeNull();
    }

    [Theory]
    [InlineData("0", 0)]
    [InlineData("30", 30)]
    [InlineData("00:02:00", 120)]
    [InlineData("1.00:00:00", 86400)]
    public void GivenLockTimeout_WhenParse_ThenAcceptsSecondsAndTimeSpan(string value, int expectedSeconds)
    {
        var parsed = MigratorArguments.TryParse(
            ["--migrations", "/m", "--lock-timeout", value],
            out var arguments,
            out var error);

        parsed.Should().BeTrue(error);
        arguments!.LockTimeout.Should().Be(TimeSpan.FromSeconds(expectedSeconds));
    }

    [Fact]
    public void GivenAllOptions_WhenParse_ThenReadsEveryValue()
    {
        var parsed = MigratorArguments.TryParse(
            ["--cluster", "kafka2ch", "--lock-ttl", "600", "--ddl-mode", "replicatedDatabase", "--migrations", "/m", "--lock-timeout", "15"],
            out var arguments,
            out var error);

        parsed.Should().BeTrue(error);
        arguments.Should().Be(new MigratorArguments(
            "/m",
            "kafka2ch",
            "replicatedDatabase",
            TimeSpan.FromSeconds(15),
            TimeSpan.FromMinutes(10)));
    }

    [Theory]
    [InlineData("abc")]
    [InlineData("-5")]
    [InlineData("-00:00:05")]
    [InlineData("")]
    public void GivenInvalidLockTimeout_WhenParse_ThenFailsWithMessage(string value)
    {
        var parsed = MigratorArguments.TryParse(
            ["--migrations", "/m", "--lock-timeout", value],
            out var arguments,
            out var error);

        parsed.Should().BeFalse();
        arguments.Should().BeNull();
        error.Should().Contain("--lock-timeout");
    }

    [Fact]
    public void GivenZeroLockTtl_WhenParse_ThenFails()
    {
        var parsed = MigratorArguments.TryParse(
            ["--migrations", "/m", "--lock-ttl", "0"],
            out _,
            out var error);

        parsed.Should().BeFalse();
        error.Should().Contain("--lock-ttl");
    }

    [Theory]
    [InlineData("--lock-timeout")]
    [InlineData("--lock-ttl")]
    [InlineData("--cluster")]
    [InlineData("--ddl-mode")]
    [InlineData("--migrations")]
    public void GivenOptionWithoutValue_WhenParse_ThenFails(string option)
    {
        var parsed = MigratorArguments.TryParse(
            option == "--migrations" ? [option] : ["--migrations", "/m", option],
            out _,
            out var error);

        parsed.Should().BeFalse();
        error.Should().Contain(option);
    }

    [Fact]
    public void GivenInvalidDdlMode_WhenParse_ThenFails()
    {
        var parsed = MigratorArguments.TryParse(
            ["--migrations", "/m", "--ddl-mode", "sharded"],
            out _,
            out var error);

        parsed.Should().BeFalse();
        error.Should().Contain("--ddl-mode");
    }
}
