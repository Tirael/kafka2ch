namespace Sandbox.App.Features.ConsumerHealth;

public static class ConsumerHealthSlice
{
    public static IServiceCollection AddConsumerHealth(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<ConsumerHealthOptions>(configuration.GetSection(ConsumerHealthOptions.SectionName));
        services.AddHostedService<ConsumerHealthWorker>();
        return services;
    }
}
