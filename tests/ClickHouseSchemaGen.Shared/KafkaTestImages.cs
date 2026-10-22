namespace ClickHouseSchemaGen.Shared;

public static class KafkaTestImages
{
    public const string KafkaImageVariable = "KAFKA_IMAGE";
    public const string SchemaRegistryImageVariable = "SCHEMA_REGISTRY_IMAGE";

    public const string KafkaImageName = "confluentinc/cp-kafka:7.8.0";
    public const string SchemaRegistryImageName = "confluentinc/cp-schema-registry:7.8.0";

    public static string FullKafkaImageName => TestContainersEnv.HubImageNamePrefix + KafkaImageName;

    public static string FullSchemaRegistryImageName => TestContainersEnv.HubImageNamePrefix + SchemaRegistryImageName;
}
