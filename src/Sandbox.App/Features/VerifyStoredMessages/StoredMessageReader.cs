using System.Globalization;
using System.Text;

namespace Sandbox.App.Features.VerifyStoredMessages;

public sealed class StoredMessageReader(IOptions<ClickHouseOptions> options) : IDisposable
{
    private readonly ClickHouseOptions _options = options.Value;
    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(60) };

    public async Task<byte[]?> ReadProtobufAsync(string sql, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(
            HttpMethod.Post,
            $"http://{_options.Host}:{_options.Port}/")
        {
            Content = new StringContent(sql)
        };
        request.Headers.TryAddWithoutValidation("X-ClickHouse-User", _options.Username);
        if (!string.IsNullOrEmpty(_options.Password))
            request.Headers.TryAddWithoutValidation("X-ClickHouse-Key", _options.Password);

        request.Headers.TryAddWithoutValidation("X-ClickHouse-Database", _options.Database);

        using var response = await _http.SendAsync(request, cancellationToken);
        var body = await response.Content.ReadAsByteArrayAsync(cancellationToken);
        if (!response.IsSuccessStatusCode)
            throw new InvalidOperationException(ErrorText(body));

        if (body.Length == 0)
            return null;

        if (LooksLikeClickHouseError(body))
            throw new InvalidOperationException(ErrorText(body));

        return body;
    }

    public async Task<string[]> ReadRowAsync(string sql, CancellationToken cancellationToken)
    {
        await using var connection = new ClickHouseConnection(_options.ConnectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            return [];

        var values = new string[reader.FieldCount];
        for (var i = 0; i < reader.FieldCount; i++)
        {
            values[i] = reader.IsDBNull(i)
                ? ""
                : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture) ?? "";
        }

        return values;
    }

    public void Dispose() => _http.Dispose();

    private static bool LooksLikeClickHouseError(byte[] body) =>
        body.Length >= 5 && Encoding.UTF8.GetString(body, 0, 5) == "Code:";

    private static string ErrorText(byte[] body) =>
        Encoding.UTF8.GetString(body, 0, Math.Min(body.Length, 2000));
}
