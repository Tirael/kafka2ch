using Microsoft.Extensions.DependencyInjection;

namespace Sandbox.App.Features.PublishOrders;

public static class PublishOrdersSlice
{
    public static IServiceCollection AddPublishOrders(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<PublishOrdersOptions>(configuration.GetSection(PublishOrdersOptions.SectionName));
        services.AddHostedService<PublishOrdersWorker>();
        return services;
    }
}
