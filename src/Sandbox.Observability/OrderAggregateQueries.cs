using System.Text.RegularExpressions;

namespace Sandbox.Observability;

/// <summary>
/// Canonical read-path SQL for order aggregates. Production consumers must use
/// <c>orders_agg_*</c> (built from <c>orders FINAL</c>) or <c>orders FINAL</c> —
/// never raw <c>orders</c> without FINAL.
/// </summary>
public static partial class OrderAggregateQueries
{
    public const string AggregatesTable = "orders_agg_1m";
    public const string AggregatesView = "orders_agg_mv";

    public static string LastRefreshSuccess(string? database) =>
        $"""
        SELECT max(last_success_time)
        FROM system.view_refreshes
        WHERE database = {DatabasePredicate(database)} AND view = '{AggregatesView}'
        """;

    public static string AggregatesWindow(string? database, int windowMinutes)
    {
        if (windowMinutes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(windowMinutes), windowMinutes, "Window must be positive.");
        }

        var table = QualifiedTable(database, AggregatesTable);
        return
            $"""
            SELECT minute, category,
                   sum(orders_count) AS orders_count,
                   sum(total_amount) AS total_amount,
                   sum(total_qty) AS total_qty
            FROM {table}
            WHERE minute >= now() - INTERVAL {windowMinutes} MINUTE
            GROUP BY minute, category
            ORDER BY minute DESC, category
            """;
    }

    /// <summary>
    /// Exact row count for debugging. Always includes FINAL so duplicates before merge are collapsed.
    /// </summary>
    public static string OrdersExactCount(string? database = null) =>
        $"SELECT count() FROM {QualifiedTable(database, "orders")} FINAL";

    private static string DatabasePredicate(string? database) =>
        database is null ? "currentDatabase()" : $"'{Identifier(database, nameof(database))}'";

    private static string QualifiedTable(string? database, string table)
    {
        var safeTable = Identifier(table, nameof(table));
        return database is null
            ? safeTable
            : $"{Identifier(database, nameof(database))}.{safeTable}";
    }

    private static string Identifier(string value, string parameterName) =>
        IdentifierPattern().IsMatch(value)
            ? value
            : throw new ArgumentException($"'{value}' is not a valid ClickHouse identifier.", parameterName);

    [GeneratedRegex("^[A-Za-z_][A-Za-z0-9_]*$")]
    private static partial Regex IdentifierPattern();
}
