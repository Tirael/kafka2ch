namespace Sandbox.App.Common;

public sealed class KafkaClientFactory(IOptions<KafkaOptions> options)
{
    private readonly KafkaOptions _options = options.Value;

    public CachedSchemaRegistryClient CreateSchemaRegistryClient() =>
        new(new SchemaRegistryConfig { Url = _options.SchemaRegistryUrl });

    public IProducer<TKey, TValue> CreateProducer<TKey, TValue>(ISchemaRegistryClient schemaRegistry)
        where TKey : class, IMessage<TKey>, new()
        where TValue : class, IMessage<TValue>, new()
    {
        var serializerConfig = new ProtobufSerializerConfig
        {
            AutoRegisterSchemas = true,
            SkipKnownTypes = true
        };

        return new ProducerBuilder<TKey, TValue>(new ProducerConfig
        {
            BootstrapServers = _options.BootstrapServers
        })
            .SetKeySerializer(new ProtobufSerializer<TKey>(schemaRegistry, serializerConfig))
            .SetValueSerializer(new ProtobufSerializer<TValue>(schemaRegistry, serializerConfig))
            .Build();
    }
}
