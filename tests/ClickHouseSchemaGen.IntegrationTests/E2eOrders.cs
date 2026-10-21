namespace ClickHouseSchemaGen.IntegrationTests;

public static class E2eOrders
{
    private static readonly string[] Categories = ["electronics", "books", "food"];

    public static uint QuantityOf(int sequence) => (uint)(sequence % 5 + 1);

    public static (OrderKey Key, OrderEvent Value) Create(int sequence)
    {
        var orderId = $"order-{sequence:D6}-{Guid.NewGuid():N}";
        var eventTime = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow);
        var value = new OrderEvent
        {
            OrderId = orderId,
            Category = Categories[sequence % Categories.Length],
            Price = new Money { Currency = "USD", Amount = sequence % 100 + 0.5 },
            Quantity = QuantityOf(sequence),
            EventTime = eventTime,
            Status = OrderStatus.Paid,
            Card = new CardPayment { Last4 = "4242", Network = "visa" },
            PromoCode = sequence % 2 == 0 ? "PROMO-E2E" : null,
            Attachments = new OrderAttachments()
        };

        if (sequence % 3 == 0)
            value.LoyaltyPoints = sequence;

        value.Tags.Add("e2e");
        value.Items.Add(new LineItem
        {
            Sku = "SKU-100",
            Qty = 1,
            UnitPrice = 9.99,
            LineStatus = OrderStatus.Created,
            Parts = { new ItemPart { Sku = "SKU-100-PART", Qty = 1, Weight = 0.5 } }
        });
        value.Metadata["source"] = "e2e";
        value.StatusHistory.Add(OrderStatus.Created);
        value.StatusHistory.Add(OrderStatus.Paid);
        value.Attachments.Invoices.Add(new Attachment { Name = "invoice.pdf", Note = "generated" });
        value.OrderLines2ByLineId[sequence] = new OrderLine2
        {
            Line2Id = sequence,
            IsActive2 = true,
            Value2 = new PriceValue2 { Price2 = 1.5, Quantity2 = 2, CreatedAt2 = eventTime, ChangedAt2 = eventTime }
        };

        return (new OrderKey { OrderId = orderId }, value);
    }
}
