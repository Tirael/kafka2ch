namespace Sandbox.App.Features.ReadAggregates;

public static class ReadAggregatesSlice
{
    public static IServiceCollection AddReadAggregates(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<ReadAggregatesOptions>(configuration.GetSection(ReadAggregatesOptions.SectionName));
        services.AddHostedService<ReadAggregatesWorker>();
        return services;
    }
}
