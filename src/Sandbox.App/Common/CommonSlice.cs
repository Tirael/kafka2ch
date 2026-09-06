using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Sandbox.Contracts;

namespace Sandbox.App.Common;

public static class CommonSlice
{
    public static IServiceCollection AddCommon(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<KafkaOptions>(configuration.GetSection(KafkaOptions.SectionName));
        services.Configure<ClickHouseOptions>(configuration.GetSection(ClickHouseOptions.SectionName));
        services.AddSingleton<KafkaClientFactory>();
        services.AddSingleton<ClickHouseClientFactory>();

        services.AddSingleton<ISchemaRegistryClient>(sp =>
        {
            var factory = sp.GetRequiredService<KafkaClientFactory>();
            var logger = sp.GetRequiredService<ILoggerFactory>().CreateLogger("StartupRetry");
            var timeProvider = sp.GetRequiredService<TimeProvider>();

            return StartupRetry.Execute(factory.CreateSchemaRegistryClient, logger, timeProvider);
        });

        services.AddSingleton<IProducer<OrderKey, OrderEvent>>(sp =>
        {
            var factory = sp.GetRequiredService<KafkaClientFactory>();
            var schemaRegistry = sp.GetRequiredService<ISchemaRegistryClient>();
            var logger = sp.GetRequiredService<ILoggerFactory>().CreateLogger("StartupRetry");
            var timeProvider = sp.GetRequiredService<TimeProvider>();

            return StartupRetry.Execute(() => factory.CreateProducer(schemaRegistry), logger, timeProvider);
        });

        return services;
    }
}
