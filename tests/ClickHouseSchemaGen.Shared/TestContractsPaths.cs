namespace ClickHouseSchemaGen.Shared;

public static class TestContractsPaths
{
    public static string RootDirectory => Path.Combine(AppContext.BaseDirectory, "TestContracts");

    public static string ProtosDirectory => Path.Combine(RootDirectory, "protos");

    public static string CodegenConfigPath => Path.Combine(RootDirectory, "clickhouse.codegen.json");

    public static string ClusterCodegenConfigPath => Path.Combine(RootDirectory, "clickhouse.codegen.cluster.json");

    public static string ClusterReplicatedDbCodegenConfigPath =>
        Path.Combine(RootDirectory, "clickhouse.codegen.cluster.replicated-db.json");

    public static string KeeperTestConfigPath => Path.Combine(RootDirectory, "keeper-test.xml");
}
