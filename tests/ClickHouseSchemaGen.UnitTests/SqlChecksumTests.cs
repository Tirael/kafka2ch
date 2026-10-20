using System.Text.RegularExpressions;

namespace ClickHouseSchemaGen.UnitTests;

public sealed class SqlChecksumTests
{
    private const string LfSql =
        "CREATE TABLE t\n(\n    id String\n)\nENGINE = MergeTree\nORDER BY id;\n\nSELECT 1;\n";

    private static readonly Regex InitRecord = new(
        @"^INSERT INTO \S+ \(version, name, checksum, applied_at, kind\) VALUES \('[^']*', '[^']*', '(?<checksum>[0-9a-f]{64})', now\(\), 'init'\);\r?$",
        RegexOptions.Multiline);

    public static TheoryData<string> CodegenConfigs => new()
    {
        RepoPaths.CodegenConfigPath,
        RepoPaths.ClusterCodegenConfigPath,
        RepoPaths.ClusterReplicatedDbCodegenConfigPath
    };

    [Fact]
    public void GivenCrLfAndLfVariants_WhenCompute_ThenChecksumsAreEqual()
        => SqlChecksum.Compute(LfSql.Replace("\n", "\r\n", StringComparison.Ordinal))
            .Should().Be(SqlChecksum.Compute(LfSql));

    [Theory]
    [InlineData("\uFEFF")]
    [InlineData("\r\n\r\n")]
    public void GivenBomOrLeadingBlankLines_WhenCompute_ThenChecksumIsUnchanged(string prefix)
        => SqlChecksum.Compute(prefix + LfSql).Should().Be(SqlChecksum.Compute(LfSql));

    [Fact]
    public void GivenTrailingWhitespaceAndNewlineDrift_WhenCompute_ThenChecksumIsUnchanged()
    {
        var drifted = LfSql.Replace(";\n", ";  \t\r\n", StringComparison.Ordinal).TrimEnd('\n', '\r') + "\n\n\n";
        var noTrailingNewline = LfSql.TrimEnd('\n');

        SqlChecksum.Compute(drifted).Should().Be(SqlChecksum.Compute(LfSql));
        SqlChecksum.Compute(noTrailingNewline).Should().Be(SqlChecksum.Compute(LfSql));
    }

    [Fact]
    public void GivenDifferentStatements_WhenCompute_ThenChecksumsDiffer()
        => SqlChecksum.Compute(LfSql.Replace("SELECT 1", "SELECT 2", StringComparison.Ordinal))
            .Should().NotBe(SqlChecksum.Compute(LfSql));

    [Fact]
    public void GivenSql_WhenCanonicalize_ThenUsesLfTrimsLinesAndKeepsSingleTrailingNewline()
        => SqlChecksum.Canonicalize("\uFEFFSELECT 1;   \r\n\r\n\r\nSELECT 2;\r\n\r\n")
            .Should().Be("SELECT 1;\n\n\nSELECT 2;\n");

    [Fact]
    public void GivenCrLfMigrationFiles_WhenReadFromDirectory_ThenChecksumsMatchLfFiles()
    {
        var root = Path.Combine(Path.GetTempPath(), $"sql-checksum-{Guid.NewGuid():N}");
        var lf = Directory.CreateDirectory(Path.Combine(root, "lf")).FullName;
        var crlf = Directory.CreateDirectory(Path.Combine(root, "crlf")).FullName;
        try
        {
            File.WriteAllText(Path.Combine(lf, "20260101000000_add.sql"), LfSql);
            File.WriteAllText(
                Path.Combine(crlf, "20260101000000_add.sql"),
                LfSql.Replace("\n", "\r\n", StringComparison.Ordinal),
                new UTF8Encoding(encoderShouldEmitUTF8Identifier: true));

            SchemaMigrationsScriptGenerator.ReadFromDirectory(crlf).Single().Checksum
                .Should().Be(SchemaMigrationsScriptGenerator.ReadFromDirectory(lf).Single().Checksum)
                .And.Be(SqlChecksum.Compute(LfSql));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Theory]
    [MemberData(nameof(CodegenConfigs))]
    public void GivenRepoConfig_WhenGenerateVersionsScript_ThenIsIdempotent(string configPath)
    {
        var config = CodegenConfigLoader.Load(configPath);
        var migrations = new[]
        {
            new MigrationFileInfo("20990101000000", "fixture", SqlChecksum.Compute("SELECT 1;\n"), "unused")
        };

        var first = SchemaMigrationsScriptGenerator.Generate(migrations, ClusterDdl.For(config));
        var second = SchemaMigrationsScriptGenerator.Generate(migrations, ClusterDdl.For(config));

        SqlText.Normalize(first).Should().Be(SqlText.Normalize(second));
        first.Should().Contain("CREATE TABLE IF NOT EXISTS");
        first.Should().Contain("VALUES ('20990101000000', 'fixture'");
    }

    [Theory]
    [MemberData(nameof(CodegenConfigs))]
    public void GivenGeneratedInitScripts_WhenRecomputeChecksum_ThenMatchesSelfRecord(string configPath)
    {
        foreach (var (fileName, text) in InitScriptsFixture.Generate(configPath).Skip(1))
        {
            var record = InitRecord.Match(text);
            record.Success.Should().BeTrue(fileName);

            var body = text[..record.Index];
            SqlChecksum.Compute(body).Should().Be(record.Groups["checksum"].Value, fileName);
            SqlChecksum.Compute(body.ReplaceLineEndings("\r\n")).Should().Be(record.Groups["checksum"].Value, fileName);
        }
    }
}
