namespace Sandbox.App.Features.ReadAggregates;

public static class ReadAggregatesSlice
{
    public static IServiceCollection AddReadAggregates(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddOptions<ReadAggregatesOptions>()
            .Bind(configuration.GetSection(ReadAggregatesOptions.SectionName))
            .Validate(o => o.HasValidDatabase, "ReadAggregates:Database must be a plain ClickHouse identifier.")
            .ValidateOnStart();
        services.AddHostedService<ReadAggregatesWorker>();
        return services;
    }
}
