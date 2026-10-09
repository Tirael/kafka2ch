namespace ClickHouseSchemaGen.Shared;

public static class RepoPaths
{
    public static string RepositoryRoot =>
        Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", ".."));

    public static string FormatSchemasDirectory =>
        Path.Combine(RepositoryRoot, "docker", "clickhouse", "format_schemas");

    public static string CodegenConfigPath =>
        Path.Combine(RepositoryRoot, "src", "Sandbox.Contracts", "clickhouse.codegen.json");

    public static string ClusterCodegenConfigPath =>
        Path.Combine(RepositoryRoot, "src", "Sandbox.Contracts", "clickhouse.codegen.cluster.json");

    public static string ClusterInitDirectory =>
        Path.Combine(RepositoryRoot, "docker", "clickhouse-cluster", "init");

    public static string ClusterReplicatedDbCodegenConfigPath =>
        Path.Combine(RepositoryRoot, "src", "Sandbox.Contracts", "clickhouse.codegen.cluster.replicated-db.json");

    public static string ClusterReplicatedDbInitDirectory =>
        Path.Combine(RepositoryRoot, "docker", "clickhouse-cluster", "init-replicated-db");

    public static string KeeperTestConfigPath =>
        Path.Combine(RepositoryRoot, "docker", "clickhouse-cluster", "config", "keeper-test.xml");
}
