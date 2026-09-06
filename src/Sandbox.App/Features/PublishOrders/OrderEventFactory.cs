using Sandbox.Contracts.Common;

namespace Sandbox.App.Features.PublishOrders;

public static class OrderEventFactory
{
    private static readonly string[] Categories = ["electronics", "books", "food"];
    private static readonly string[] Currencies = ["USD", "EUR", "RUB"];
    private static readonly string[] Tags = ["promo", "vip", "new", "sale"];
    private static readonly string[] Skus = ["SKU-100", "SKU-200", "SKU-300"];
    private static readonly string[] MetadataKeys = ["source", "campaign", "channel"];
    private static readonly OrderStatus[] Statuses =
    [
        OrderStatus.Created,
        OrderStatus.Paid
    ];

    public static (OrderKey Key, OrderEvent Event) CreateRandom(DateTimeOffset now)
    {
        var orderId = Guid.NewGuid().ToString();
        var key = new OrderKey { OrderId = orderId };
        var orderEvent = new OrderEvent
        {
            OrderId = orderId,
            Category = Categories[Random.Shared.Next(Categories.Length)],
            Price = new Money
            {
                Currency = Currencies[Random.Shared.Next(Currencies.Length)],
                Amount = Random.Shared.NextDouble() * 1000
            },
            Quantity = (uint)Random.Shared.Next(1, 10),
            EventTimeUnixMs = now.ToUnixTimeMilliseconds(),
            Status = Statuses[Random.Shared.Next(Statuses.Length)]
        };

        var tagCount = Random.Shared.Next(0, 3);
        for (var i = 0; i < tagCount; i++)
            orderEvent.Tags.Add(Tags[Random.Shared.Next(Tags.Length)]);

        var itemCount = Random.Shared.Next(1, 3);
        for (var i = 0; i < itemCount; i++)
        {
            orderEvent.Items.Add(new LineItem
            {
                Sku = Skus[Random.Shared.Next(Skus.Length)],
                Qty = (uint)Random.Shared.Next(1, 5),
                UnitPrice = Random.Shared.NextDouble() * 100
            });
        }

        orderEvent.Metadata["source"] = "sandbox-app";
        orderEvent.Metadata[MetadataKeys[Random.Shared.Next(MetadataKeys.Length)]] = "demo";

        if (Random.Shared.Next(0, 2) == 0)
            orderEvent.Note = "generated-by-sandbox";

        return (key, orderEvent);
    }
}
