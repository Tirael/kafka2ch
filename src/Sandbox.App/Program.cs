using Sandbox.App.Common;
using Sandbox.App.Features.PublishOrders;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddCommon(builder.Configuration).AddPublishOrders(builder.Configuration);

var host = builder.Build();
host.Run();
