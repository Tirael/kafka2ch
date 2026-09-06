namespace Sandbox.App.Features.ReadAggregates;

public sealed record OrderAggregateRow(
    DateTime Minute,
    string Category,
    ulong OrdersCount,
    double TotalAmount,
    ulong TotalQty);

public sealed record ShipmentAggregateRow(
    DateTime Minute,
    string Status,
    ulong ShipmentsCount);
