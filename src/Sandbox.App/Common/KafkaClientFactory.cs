namespace Sandbox.App.Common;

public sealed class KafkaClientFactory(IOptions<KafkaOptions> options)
{
    private readonly KafkaOptions _options = options.Value;

    public CachedSchemaRegistryClient CreateSchemaRegistryClient() =>
        new(new SchemaRegistryConfig { Url = _options.SchemaRegistryUrl });

    public IProducer<OrderKey, OrderEvent> CreateProducer(ISchemaRegistryClient schemaRegistry)
    {
        var serializerConfig = new ProtobufSerializerConfig
        {
            AutoRegisterSchemas = true,
            SkipKnownTypes = true
        };

        return new ProducerBuilder<OrderKey, OrderEvent>(new ProducerConfig
        {
            BootstrapServers = _options.BootstrapServers
        })
            .SetKeySerializer(new ProtobufSerializer<OrderKey>(schemaRegistry, serializerConfig))
            .SetValueSerializer(new ProtobufSerializer<OrderEvent>(schemaRegistry, serializerConfig))
            .Build();
    }
}
