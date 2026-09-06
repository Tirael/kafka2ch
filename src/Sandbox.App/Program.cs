var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSingleton(TimeProvider.System);
builder.Services
    .AddCommon(builder.Configuration)
    .AddPublishOrders(builder.Configuration)
    .AddPublishShipments(builder.Configuration)
    .AddReadAggregates(builder.Configuration);

var host = builder.Build();
host.Run();
