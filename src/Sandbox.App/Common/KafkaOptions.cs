namespace Sandbox.App.Common;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = "kafka:9092";

    public string SchemaRegistryUrl { get; set; } = "http://schema-registry:8081";
}
