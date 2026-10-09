namespace ClickHouseSchemaGen.Generation;

public static class SqlStatementSplitter
{


    public static IReadOnlyList<string> Split(string sql)
    {
        var statements = new List<string>();
        var builder = new StringBuilder();
        var inSingleQuote = false;
        var inLineComment = false;

        for (var i = 0; i < sql.Length; i++)
        {
            var current = sql[i];
            var next = i + 1 < sql.Length ? sql[i + 1] : '\0';

            if (inLineComment)
            {
                builder.Append(current);
                if (current == '\n')
                    inLineComment = false;
                continue;
            }

            if (!inSingleQuote && current == '-' && next == '-')
            {


                if (string.IsNullOrWhiteSpace(builder.ToString()))
                {
                    var lineEnd = sql.IndexOf('\n', i);
                    var end = lineEnd < 0 ? sql.Length : lineEnd;
                    statements.Add(sql[i..end].TrimEnd());
                    builder.Clear();
                    i = end;
                    continue;
                }

                inLineComment = true;
                builder.Append(current);
                continue;
            }

            if (current == '\'' && !inSingleQuote)
            {
                inSingleQuote = true;
                builder.Append(current);
                continue;
            }

            if (current == '\'' && inSingleQuote)
            {
                builder.Append(current);
                if (next == '\'')
                {
                    builder.Append(next);
                    i++;
                    continue;
                }

                inSingleQuote = false;
                continue;
            }

            if (current == ';' && !inSingleQuote)
            {
                var statement = builder.ToString().Trim();
                if (statement.Length > 0)
                    statements.Add(statement);
                builder.Clear();
                continue;
            }

            builder.Append(current);
        }

        var tail = builder.ToString().Trim();
        if (tail.Length > 0)
            statements.Add(tail);

        return statements;
    }
}
