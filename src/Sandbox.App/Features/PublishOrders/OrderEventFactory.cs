namespace Sandbox.App.Features.PublishOrders;

public static class OrderEventFactory
{
    private static readonly string[] Categories = ["electronics", "books", "food"];
    private static readonly string[] Currencies = ["USD", "EUR", "RUB"];
    private static readonly string[] Tags = ["promo", "vip", "new", "sale"];
    private static readonly string[] Skus = ["SKU-100", "SKU-200", "SKU-300"];
    private static readonly string[] MetadataKeys = ["source", "campaign", "channel"];
    private static readonly string[] CardNetworks = ["visa", "mastercard", "mir"];
    private static readonly string[] WalletProviders = ["apple-pay", "google-pay", "yoomoney"];
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
            EventTime = Google.Protobuf.WellKnownTypes.Timestamp.FromDateTimeOffset(now),
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
                UnitPrice = Random.Shared.NextDouble() * 100,
                LineStatus = Statuses[Random.Shared.Next(Statuses.Length)]
            });
        }

        orderEvent.Metadata["source"] = "sandbox-app";
        orderEvent.Metadata[MetadataKeys[Random.Shared.Next(MetadataKeys.Length)]] = "demo";

        if (Random.Shared.Next(0, 2) == 0)
            orderEvent.Note = "generated-by-sandbox";

        ApplyPayment(orderEvent);
        ApplyPromoCode(orderEvent);
        ApplyStatusHistory(orderEvent);

        if (Random.Shared.Next(0, 2) == 0)
            orderEvent.LoyaltyPoints = Random.Shared.Next(10, 500);

        return (key, orderEvent);
    }

    private static void ApplyPayment(OrderEvent orderEvent)
    {
        switch (Random.Shared.Next(3))
        {
            case 0:
                orderEvent.Card = new CardPayment
                {
                    Last4 = Random.Shared.Next(1000, 9999).ToString(),
                    Network = CardNetworks[Random.Shared.Next(CardNetworks.Length)]
                };
                break;
            case 1:
                orderEvent.Cash = new CashPayment { Received = orderEvent.Price.Amount * 1.1 };
                break;
            default:
                orderEvent.Wallet = new WalletPayment
                {
                    Provider = WalletProviders[Random.Shared.Next(WalletProviders.Length)],
                    WalletId = Guid.NewGuid().ToString("N")[..12]
                };
                break;
        }
    }

    private static void ApplyPromoCode(OrderEvent orderEvent)
    {
        if (Random.Shared.Next(0, 2) == 0)
            orderEvent.PromoCode = $"PROMO-{Random.Shared.Next(100, 999)}";
    }

    private static void ApplyStatusHistory(OrderEvent orderEvent)
    {
        orderEvent.StatusHistory.Add(OrderStatus.Unspecified);
        orderEvent.StatusHistory.Add(orderEvent.Status);
    }
}
