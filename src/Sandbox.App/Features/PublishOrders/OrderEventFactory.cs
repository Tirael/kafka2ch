using Sandbox.Contracts;
using Sandbox.Contracts.Common;

namespace Sandbox.App.Features.PublishOrders;

public static class OrderEventFactory
{
    private static readonly string[] Categories = ["electronics", "books", "food"];
    private static readonly string[] Currencies = ["USD", "EUR", "RUB"];
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

        return (key, orderEvent);
    }
}
