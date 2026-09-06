namespace ClickHouseSchemaGen.Tests.Support;

internal static class RepoPaths
{
    public static string RepositoryRoot =>
        Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", ".."));

    public static string FormatSchemasDirectory =>
        Path.Combine(RepositoryRoot, "docker", "clickhouse", "format_schemas");

    public static string CodegenConfigPath =>
        Path.Combine(RepositoryRoot, "src", "Sandbox.Contracts", "clickhouse.codegen.json");
}
