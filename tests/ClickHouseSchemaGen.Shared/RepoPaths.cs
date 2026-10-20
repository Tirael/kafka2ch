namespace ClickHouseSchemaGen.Shared;

public static class RepoPaths
{
    public static string RepositoryRoot =>
        Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", ".."));

    public static string ProtosDirectory =>
        Path.Combine(RepositoryRoot, "src", "Sandbox.Contracts", "protos");

    public static string CodegenConfigPath =>
        Path.Combine(RepositoryRoot, "src", "Sandbox.Contracts", "clickhouse.codegen.json");

    public static string ClusterCodegenConfigPath =>
        Path.Combine(RepositoryRoot, "src", "Sandbox.Contracts", "clickhouse.codegen.cluster.json");

    public static string ClusterReplicatedDbCodegenConfigPath =>
        Path.Combine(RepositoryRoot, "src", "Sandbox.Contracts", "clickhouse.codegen.cluster.replicated-db.json");

    public static string KeeperTestConfigPath =>
        Path.Combine(RepositoryRoot, "tests", "ClickHouseSchemaGen.Shared", "Fixtures", "keeper-test.xml");
}
