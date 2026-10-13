namespace ClickHouseSchemaGen.IntegrationTests;

public static class TestContainersEnv
{
    public const string HubImageNamePrefixVariable = "TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX";
    public const string RyukContainerImageVariable = "TESTCONTAINERS_RYUK_CONTAINER_IMAGE";

    public const string HubImageNamePrefix = "";
    public const string RyukImageName = "testcontainers/ryuk:0.11.0";
    public const string ClickHouseImageName = "clickhouse/clickhouse-server:26.5.6.64-checked";

    public static string FullClickHouseImageName => HubImageNamePrefix + ClickHouseImageName;

    public static void SetVariables()
    {
        Environment.SetEnvironmentVariable(HubImageNamePrefixVariable, HubImageNamePrefix);
        Environment.SetEnvironmentVariable(RyukContainerImageVariable, RyukImageName);
    }
}
