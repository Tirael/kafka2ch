using System.Text.RegularExpressions;

namespace ClickHouseSchemaGen.Generation;

/// <summary>
/// Adapts hand-written <c>trailingSql</c> (aggregate tables and their views) to cluster mode with the same
/// topology as generated tables: MergeTree-family tables become <c>Replicated*</c> local tables plus a
/// <c>Distributed</c> table, materialized views read from and write to local tables, and every
/// <c>CREATE</c> runs <c>ON CLUSTER</c>. Other DDL (ALTER, DROP, ...) has no unambiguous cluster form and is rejected.
/// </summary>
public static partial class ClusterTrailingSqlRewriter
{
    public static string Rewrite(string sql, ClusterDdl cluster, IEnumerable<string> storageTables)
    {
        ArgumentNullException.ThrowIfNull(sql);
        ArgumentNullException.ThrowIfNull(cluster);

        if (!cluster.Enabled)
            return sql;

        var localTables = new HashSet<string>(storageTables, StringComparer.OrdinalIgnoreCase);
        var builder = new StringBuilder();
        foreach (var statement in SqlStatementSplitter.Split(sql))
        {
            if (statement.StartsWith("--", StringComparison.Ordinal))
            {
                builder.AppendLine(statement);
                continue;
            }

            builder.Append(RewriteStatement(statement, cluster, localTables)).AppendLine();
        }

        return builder.ToString();
    }

    private static string RewriteStatement(string statement, ClusterDdl cluster, HashSet<string> localTables)
    {
        var table = CreateTableRegex().Match(statement);
        if (table.Success)
            return RewriteCreateTable(table, cluster, localTables);

        var view = CreateMaterializedViewRegex().Match(statement);
        if (view.Success)
            return RewriteCreateMaterializedView(view, cluster, localTables);

        var other = CreateOtherRegex().Match(statement);
        if (other.Success)
            return $"{other.Groups["head"].Value}{cluster.OnCluster}{other.Groups["rest"].Value};" + Environment.NewLine;

        if (InsertRegex().IsMatch(statement))
            return statement + ";" + Environment.NewLine;

        throw new InvalidOperationException(
            "trailingSql statement is not supported in cluster mode (only CREATE TABLE / MATERIALIZED VIEW / VIEW / " +
            $"FUNCTION / DICTIONARY and INSERT are rewritten): '{FirstLine(statement)}'. Use a migration instead.");
    }

    private static string RewriteCreateTable(Match match, ClusterDdl cluster, HashSet<string> localTables)
    {
        var ifNotExists = match.Groups["ifne"].Success;
        var name = match.Groups["name"].Value;
        var rest = match.Groups["rest"].Value;
        var create = ifNotExists ? "CREATE TABLE IF NOT EXISTS" : "CREATE TABLE";

        if (!TryReplaceMergeTreeEngine(rest, cluster, out var replicatedRest))
            return $"{create} {name}{cluster.OnCluster}{rest};" + Environment.NewLine;

        localTables.Add(name);
        return $"{create} {cluster.StorageTable(name)}{cluster.OnCluster}{replicatedRest};" + Environment.NewLine +
               Environment.NewLine +
               cluster.DistributedTableStatement(name, shardingKey: null, ifNotExists);
    }

    private static string RewriteCreateMaterializedView(Match match, ClusterDdl cluster, HashSet<string> localTables)
    {
        var head = match.Groups["head"].Value;
        var rest = match.Groups["rest"].Value;

        rest = ToClauseRegex().Replace(rest, to =>
            localTables.Contains(to.Groups["table"].Value)
                ? $"{to.Groups["prefix"].Value}{cluster.StorageTable(to.Groups["table"].Value)}"
                : to.Value,
            count: 1);

        rest = SourceTableRegex().Replace(rest, source =>
            localTables.Contains(source.Groups["table"].Value)
                ? $"{source.Groups["keyword"].Value}{cluster.StorageTable(source.Groups["table"].Value)}"
                : source.Value);

        if (TryReplaceMergeTreeEngine(rest, cluster, out var replicatedRest))
            rest = replicatedRest;

        return $"{head}{cluster.OnCluster}{rest};" + Environment.NewLine;
    }

    private static bool TryReplaceMergeTreeEngine(string sql, ClusterDdl cluster, out string rewritten)
    {
        rewritten = sql;
        var engine = MergeTreeEngineRegex().Match(sql);
        if (!engine.Success)
            return false;

        var engineStart = engine.Groups["engine"].Index;
        var engineEnd = engine.Index + engine.Length;
        var argumentsStart = engineEnd;
        while (argumentsStart < sql.Length && char.IsWhiteSpace(sql[argumentsStart]))
            argumentsStart++;

        if (argumentsStart < sql.Length && sql[argumentsStart] == '(')
            engineEnd = FindClosingParenthesis(sql, argumentsStart) + 1;

        rewritten = sql[..engineStart] + cluster.StorageEngine(sql[engineStart..engineEnd]) + sql[engineEnd..];
        return true;
    }

    private static int FindClosingParenthesis(string sql, int openIndex)
    {
        var depth = 0;
        var inQuote = false;
        for (var i = openIndex; i < sql.Length; i++)
        {
            var current = sql[i];
            if (current == '\'')
                inQuote = !inQuote;
            else if (!inQuote && current == '(')
                depth++;
            else if (!inQuote && current == ')' && --depth == 0)
                return i;
        }

        throw new InvalidOperationException($"Unbalanced parentheses in engine clause: '{FirstLine(sql[openIndex..])}'.");
    }

    private static string FirstLine(string statement)
    {
        var trimmed = statement.Trim();
        var newline = trimmed.IndexOf('\n');
        return newline < 0 ? trimmed : trimmed[..newline].TrimEnd();
    }

    private const string Identifier = "[A-Za-z_][A-Za-z0-9_]*";

    [GeneratedRegex(
        $@"^CREATE\s+TABLE\s+(?<ifne>IF\s+NOT\s+EXISTS\s+)?(?<name>{Identifier})(?<rest>[\s\S]*)$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex CreateTableRegex();

    [GeneratedRegex(
        $@"^(?<head>CREATE\s+MATERIALIZED\s+VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?{Identifier})(?<rest>[\s\S]*)$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex CreateMaterializedViewRegex();

    [GeneratedRegex(
        $@"^(?<head>CREATE\s+(?:OR\s+REPLACE\s+)?(?:VIEW|FUNCTION|DICTIONARY)\s+(?:IF\s+NOT\s+EXISTS\s+)?{Identifier})(?<rest>[\s\S]*)$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex CreateOtherRegex();

    [GeneratedRegex(@"^INSERT\s", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex InsertRegex();

    [GeneratedRegex($@"^(?<prefix>\s+TO\s+)(?<table>{Identifier})\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex ToClauseRegex();

    [GeneratedRegex($@"(?<keyword>\b(?:FROM|JOIN)\s+)(?<table>{Identifier})\b(?!\s*\()", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex SourceTableRegex();

    [GeneratedRegex(@"\bENGINE\s*=\s*(?<engine>[A-Za-z]*MergeTree)\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex MergeTreeEngineRegex();
}
