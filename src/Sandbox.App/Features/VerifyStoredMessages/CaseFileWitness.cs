namespace Sandbox.App.Features.VerifyStoredMessages;

public static class CaseFileWitness
{
    public static string SelectSql(string table, string idColumn, string id) =>
        $"""
        SELECT
            case_file.file_id,
            toString(length(case_file.journal)),
            toString(arraySum(arrayMap(x -> length(x.body), case_file.journal))),
            toString(length(case_file.obligations)),
            toString(length(case_file.evidence.exhibits)),
            toString(arraySum(arrayMap(exhibit -> arraySum(arrayMap(excerpt -> length(excerpt.text), exhibit.excerpts)), case_file.evidence.exhibits))),
            case_file.schedules[1].windows[1].slots[1].label,
            case_file.journal[1].body,
            case_file.journal[length(case_file.journal)].body,
            case_file.evidence.exhibits[1].excerpts[1].text,
            case_file.counterparties[1].legal_name,
            case_file.labels['owner'],
            toString(case_file.instrument),
            toString(length(case_file.seal))
        FROM {table}
        WHERE {idColumn} = '{id}'
        LIMIT 1
        """;

    public static string[] Expected(CaseFile file)
    {
        var firstBody = file.Journal.Count == 0 ? "" : file.Journal[0].Body;
        var lastBody = file.Journal.Count == 0 ? "" : file.Journal[^1].Body;
        return
        [
            file.FileId,
            file.Journal.Count.ToString(),
            file.Journal.Sum(entry => (long)entry.Body.Length).ToString(),
            file.Obligations.Count.ToString(),
            file.Evidence.Exhibits.Count.ToString(),
            file.Evidence.Exhibits.Sum(exhibit => exhibit.Excerpts.Sum(excerpt => (long)excerpt.Text.Length)).ToString(),
            file.Schedules[0].Windows[0].Slots[0].Label,
            firstBody,
            lastBody,
            file.Evidence.Exhibits[0].Excerpts[0].Text,
            file.Counterparties[0].LegalName,
            file.Labels["owner"],
            Instrument(file),
            file.Seal.Length.ToString()
        ];
    }

    public static IReadOnlyList<string> Compare(CaseFile file, IReadOnlyList<string> stored)
    {
        var expected = Expected(file);
        if (stored.Count != expected.Length)
        {
            return [$"case file column count kafka={expected.Length} clickhouse={stored.Count}"];
        }

        string[] names =
        [
            "file_id",
            "journal_count",
            "journal_chars",
            "obligations",
            "exhibits",
            "excerpt_chars",
            "first_slot",
            "first_journal_body",
            "last_journal_body",
            "first_excerpt",
            "first_counterparty",
            "label_owner",
            "instrument",
            "seal_bytes"
        ];

        List<string> diffs = [];
        for (var i = 0; i < expected.Length; i++)
        {
            if (stored[i] != expected[i])
            {
                diffs.Add(
                    $"case_file.{names[i]}: kafka='{Trim(expected[i])}' clickhouse='{Trim(stored[i])}'");
            }
        }

        return diffs;
    }

    private static string Instrument(CaseFile file) => file.InstrumentCase switch
    {
        CaseFile.InstrumentOneofCase.Bank => "bank",
        CaseFile.InstrumentOneofCase.Wallet => "wallet",
        CaseFile.InstrumentOneofCase.Ledger => "ledger",
        _ => "absent"
    };

    private static string Trim(string value) =>
        value.Length <= 120 ? value : string.Concat(value.AsSpan(0, 117), "...");
}
