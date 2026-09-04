using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Options;
using Sandbox.Contracts;

namespace Sandbox.App.Common;

public sealed class KafkaClientFactory
{
    private readonly KafkaOptions _options;

    public KafkaClientFactory(IOptions<KafkaOptions> options) => _options = options.Value;

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
