namespace Sandbox.App.Features.VerifyStoredMessages;

public static class VerifyStoredMessagesSlice
{
    public static IServiceCollection AddVerifyStoredMessages(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services.Configure<VerifyStoredMessagesOptions>(
            configuration.GetSection(VerifyStoredMessagesOptions.SectionName));
        services.AddSingleton<StoredMessageReader>();
        services.AddHostedService<VerifyStoredMessagesWorker>();
        return services;
    }
}
