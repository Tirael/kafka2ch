namespace ClickHouseSchemaGen.Generation;

public static partial class ClusterTrailingSqlRewriter
{
    public static string Rewrite(string sql, ClusterDdl cluster, IEnumerable<string> storageTables)
    {
        ArgumentNullException.ThrowIfNull(sql);
        ArgumentNullException.ThrowIfNull(cluster);

        if (!cluster.Enabled)
            return sql;

        var localTables = new HashSet<string>(storageTables, StringComparer.OrdinalIgnoreCase);
        var statements = SqlStatementSplitter.Split(sql);
        var refreshTargets = CollectRefreshTargets(statements);
        var builder = new StringBuilder();
        foreach (var statement in statements)
        {
            if (statement.StartsWith("--", StringComparison.Ordinal))
            {
                builder.AppendLine(statement);
                continue;
            }

            builder.Append(RewriteStatement(statement, cluster, localTables, refreshTargets)).AppendLine();
        }

        return SqlText.Normalize(builder.ToString());
    }

    private static HashSet<string> CollectRefreshTargets(IEnumerable<string> statements)
    {
        var targets = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var statement in statements)
        {
            var view = CreateMaterializedViewRegex().Match(statement);
            if (!view.Success)
                continue;

            var refresh = RefreshToClauseRegex().Match(view.Groups["rest"].Value);
            if (refresh.Success)
                targets.Add(refresh.Groups["table"].Value);
        }

        return targets;
    }

    private static string RewriteStatement(
        string statement,
        ClusterDdl cluster,
        HashSet<string> localTables,
        HashSet<string> refreshTargets)
    {
        var table = CreateTableRegex().Match(statement);
        if (table.Success)
            return RewriteCreateTable(table, cluster, localTables, refreshTargets);

        var view = CreateMaterializedViewRegex().Match(statement);
        if (view.Success)
            return RewriteCreateMaterializedView(view, cluster, localTables);

        var other = CreateOtherRegex().Match(statement);
        if (other.Success)
            return $"{other.Groups["head"].Value}{cluster.OnCluster}{other.Groups["rest"].Value};" + SqlText.NewLine;

        if (InsertRegex().IsMatch(statement))
            return statement + ";" + SqlText.NewLine;

        throw new InvalidOperationException(
            "trailingSql statement is not supported in cluster mode (only CREATE TABLE / MATERIALIZED VIEW / VIEW / " +
            $"FUNCTION / DICTIONARY and INSERT are rewritten): '{FirstLine(statement)}'. Use a migration instead.");
    }

    private static string RewriteCreateTable(
        Match match,
        ClusterDdl cluster,
        HashSet<string> localTables,
        HashSet<string> refreshTargets)
    {
        var ifNotExists = match.Groups["ifne"].Success;
        var name = match.Groups["name"].Value;
        var rest = match.Groups["rest"].Value;
        var create = ifNotExists ? "CREATE TABLE IF NOT EXISTS" : "CREATE TABLE";

        if (!TryReplaceMergeTreeEngine(rest, cluster, out var replicatedRest))
            return $"{create} {name}{cluster.OnCluster}{rest};" + SqlText.NewLine;

        if (cluster.UsesOnCluster && refreshTargets.Contains(name))
            replicatedRest = rest;

        localTables.Add(name);
        return $"{create} {cluster.StorageTable(name)}{cluster.OnCluster}{replicatedRest};" + SqlText.NewLine +
               SqlText.NewLine +
               cluster.DistributedTableStatement(name, shardingKey: null, ifNotExists);
    }

    private static string RewriteCreateMaterializedView(Match match, ClusterDdl cluster, HashSet<string> localTables)
    {
        var head = match.Groups["head"].Value;
        var rest = match.Groups["rest"].Value;

        var refreshable = RefreshToClauseRegex().IsMatch(rest);
        rest = refreshable
            ? RefreshToClauseRegex().Replace(rest, to =>
                    localTables.Contains(to.Groups["table"].Value)
                        ? $"{to.Groups["prefix"].Value}{cluster.StorageTable(to.Groups["table"].Value)}"
                        : to.Value,
                count: 1)
            : ToClauseRegex().Replace(rest, to =>
                    localTables.Contains(to.Groups["table"].Value)
                        ? $"{to.Groups["prefix"].Value}{cluster.MaterializedViewTarget(to.Groups["table"].Value)}"
                        : to.Value,
                count: 1);

        var readsDistributed = refreshable && cluster.UsesReplicatedDatabase;
        rest = SourceTableRegex().Replace(rest, source =>
            !readsDistributed && localTables.Contains(source.Groups["table"].Value)
                ? $"{source.Groups["keyword"].Value}{cluster.StorageTable(source.Groups["table"].Value)}"
                : source.Value);

        if (TryReplaceMergeTreeEngine(rest, cluster, out var replicatedRest))
            rest = replicatedRest;

        return $"{head}{cluster.OnCluster}{rest};" + SqlText.NewLine;
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

    [GeneratedRegex(
        $@"^(?<prefix>\s+REFRESH\s(?:(?!\sAS\s)[\s\S])*?\sTO\s+)(?<table>{Identifier})\b",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex RefreshToClauseRegex();

    [GeneratedRegex($@"(?<keyword>\b(?:FROM|JOIN)\s+)(?<table>{Identifier})\b(?!\s*\()", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex SourceTableRegex();

    [GeneratedRegex(@"\bENGINE\s*=\s*(?<engine>[A-Za-z]*MergeTree)\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex MergeTreeEngineRegex();
}
