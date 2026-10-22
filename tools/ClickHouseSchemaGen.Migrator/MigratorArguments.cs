using System.Diagnostics.CodeAnalysis;
using System.Globalization;

namespace ClickHouseSchemaGen.Migrator;

public sealed record MigratorArguments(
    string MigrationsDirectory,
    string? Cluster,
    string? DdlMode,
    TimeSpan LockTimeout,
    TimeSpan LockTtl)
{
    public static bool TryParse(
        IReadOnlyList<string> args,
        [NotNullWhen(true)] out MigratorArguments? arguments,
        [NotNullWhen(false)] out string? error)
    {
        arguments = null;
        string? migrationsDirectory = null;
        string? cluster = null;
        string? ddlMode = null;
        var lockTimeout = MigrationLockOptions.DefaultTimeout;
        var lockTtl = MigrationLockOptions.DefaultTtl;

        for (var i = 0; i < args.Count; i++)
        {
            var option = args[i];
            if (option is not ("--migrations" or "--cluster" or "--ddl-mode" or "--lock-timeout" or "--lock-ttl"))
                continue;

            if (i + 1 >= args.Count)
            {
                error = $"Missing value for {option}.";
                return false;
            }

            var value = args[++i];
            switch (option)
            {
                case "--migrations":
                    migrationsDirectory = value;
                    break;
                case "--cluster":
                    cluster = value;
                    break;
                case "--ddl-mode":
                    if (!ClusterDdlModes.All.Contains(value, StringComparer.OrdinalIgnoreCase))
                    {
                        error = $"Invalid --ddl-mode '{value}'. Expected: {string.Join(", ", ClusterDdlModes.All)}.";
                        return false;
                    }

                    ddlMode = value;
                    break;
                case "--lock-timeout":
                    if (!TryParseDuration(value, out lockTimeout))
                    {
                        error = $"Invalid --lock-timeout '{value}'. Expected seconds or hh:mm:ss, not negative.";
                        return false;
                    }

                    break;
                case "--lock-ttl":
                    if (!TryParseDuration(value, out lockTtl) || lockTtl < TimeSpan.FromSeconds(1))
                    {
                        error = $"Invalid --lock-ttl '{value}'. Expected seconds or hh:mm:ss, at least one second.";
                        return false;
                    }

                    break;
            }
        }

        if (string.IsNullOrWhiteSpace(migrationsDirectory))
        {
            error = "Missing required --migrations argument.";
            return false;
        }

        arguments = new MigratorArguments(migrationsDirectory, cluster, ddlMode, lockTimeout, lockTtl);
        error = null;
        return true;
    }

    public static bool TryParseDuration(string value, out TimeSpan duration)
    {
        if (long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var seconds))
        {
            var valid = seconds <= (long)TimeSpan.MaxValue.TotalSeconds;
            duration = valid ? TimeSpan.FromSeconds(seconds) : default;
            return valid;
        }

        return TimeSpan.TryParseExact(value, "c", CultureInfo.InvariantCulture, out duration)
            && duration >= TimeSpan.Zero;
    }
}
