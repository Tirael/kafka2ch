namespace ClickHouseSchemaGen.Shared;

public static class TestContainersEnv
{
    public const string HubImageNamePrefixVariable = "TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX";
    public const string RyukContainerImageVariable = "TESTCONTAINERS_RYUK_CONTAINER_IMAGE";
    public const string DockerApiVersionVariable = "DOCKER_API_VERSION";
    public const string ClickHouseImageVariable = "CLICKHOUSE_IMAGE";

    public const string HubImageNamePrefix = "";
    public const string RyukImageName = "testcontainers/ryuk:0.11.0";
    public const string DefaultClickHouseImageName = "clickhouse/clickhouse-server:26.6.3.62";
    public const string DockerApiVersion = "1.41";

    public static string ClickHouseImageName
    {
        get
        {
            var image = Environment.GetEnvironmentVariable(ClickHouseImageVariable);
            return string.IsNullOrWhiteSpace(image) ? DefaultClickHouseImageName : image.Trim();
        }
    }

    public static string FullClickHouseImageName => HubImageNamePrefix + ClickHouseImageName;

    public static void SetVariables()
    {
        Environment.SetEnvironmentVariable(HubImageNamePrefixVariable, HubImageNamePrefix);
        Environment.SetEnvironmentVariable(RyukContainerImageVariable, RyukImageName);
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable(DockerApiVersionVariable)))
        {
            Environment.SetEnvironmentVariable(DockerApiVersionVariable, DockerApiVersion);
        }
    }
}
