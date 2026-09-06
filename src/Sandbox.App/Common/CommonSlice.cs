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
            ExecuteWithStartupRetry(sp, sp.GetRequiredService<KafkaClientFactory>().CreateSchemaRegistryClient));

        services.AddSingleton(sp =>
            ExecuteWithStartupRetry(
                sp,
                () => sp.GetRequiredService<KafkaClientFactory>()
                    .CreateProducer<OrderKey, OrderEvent>(sp.GetRequiredService<ISchemaRegistryClient>())));

        services.AddSingleton(sp =>
            ExecuteWithStartupRetry(
                sp,
                () => sp.GetRequiredService<KafkaClientFactory>()
                    .CreateProducer<ShipmentKey, ShipmentEvent>(sp.GetRequiredService<ISchemaRegistryClient>())));

        return services;
    }

    private static T ExecuteWithStartupRetry<T>(IServiceProvider sp, Func<T> action) =>
        StartupRetry.Execute(action, CreateRetryContext(sp));

    private static RetryContext CreateRetryContext(IServiceProvider sp) =>
        new(
            sp.GetRequiredService<ILoggerFactory>().CreateLogger("StartupRetry"),
            sp.GetRequiredService<TimeProvider>());
}
