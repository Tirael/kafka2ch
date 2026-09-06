namespace Sandbox.App.Features.PublishShipments;

public static class PublishShipmentsSlice
{
    public static IServiceCollection AddPublishShipments(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<PublishShipmentsOptions>(configuration.GetSection(PublishShipmentsOptions.SectionName));
        services.AddHostedService<PublishShipmentsWorker>();
        return services;
    }
}
