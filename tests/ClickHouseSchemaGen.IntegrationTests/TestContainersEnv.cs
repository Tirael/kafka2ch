namespace ClickHouseSchemaGen.IntegrationTests;

public static class TestContainersEnv
{
    public const string HubImageNamePrefixVariable = "TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX";
    public const string RyukContainerImageVariable = "TESTCONTAINERS_RYUK_CONTAINER_IMAGE";

    public const string HubImageNamePrefix = "";
    public const string RyukImageName = "testcontainers/ryuk:0.11.0";
    // 26.5.6.64-checked is not published on Docker Hub; pin the public 26.5.6.64 tag.
    public const string ClickHouseImageName = "clickhouse/clickhouse-server:26.5.6.64";

    public static string FullClickHouseImageName => HubImageNamePrefix + ClickHouseImageName;

    public static void SetVariables()
    {
        Environment.SetEnvironmentVariable(HubImageNamePrefixVariable, HubImageNamePrefix);
        Environment.SetEnvironmentVariable(RyukContainerImageVariable, RyukImageName);
    }
}
