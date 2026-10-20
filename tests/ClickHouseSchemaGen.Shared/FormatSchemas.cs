namespace ClickHouseSchemaGen.Shared;

/// <summary>
/// Prepares a ClickHouse format_schemas tree from <c>protos/</c> plus embedded google.protobuf stubs,
/// without depending on <c>docker/clickhouse/format_schemas</c>.
/// </summary>
public static class FormatSchemas
{
    private static readonly object Gate = new();
    private static string? _directory;

    private const string TimestampProto = """
        syntax = "proto3";

        package google.protobuf;

        message Timestamp {
          int64 seconds = 1;
          int32 nanos = 2;
        }
        """;

    private const string WrappersProto = """
        syntax = "proto3";

        package google.protobuf;

        message DoubleValue {
          double value = 1;
        }

        message FloatValue {
          float value = 1;
        }

        message Int64Value {
          int64 value = 1;
        }

        message UInt64Value {
          uint64 value = 1;
        }

        message Int32Value {
          int32 value = 1;
        }

        message UInt32Value {
          uint32 value = 1;
        }

        message BoolValue {
          bool value = 1;
        }

        message StringValue {
          string value = 1;
        }

        message BytesValue {
          bytes value = 1;
        }
        """;

    public static string Directory
    {
        get
        {
            if (_directory is not null)
                return _directory;

            lock (Gate)
            {
                if (_directory is not null)
                    return _directory;

                var dest = Path.Combine(Path.GetTempPath(), "kafka2ch-test-format-schemas");
                if (System.IO.Directory.Exists(dest))
                    System.IO.Directory.Delete(dest, recursive: true);

                CopyTree(RepoPaths.ProtosDirectory, dest);

                var google = Path.Combine(dest, "google", "protobuf");
                System.IO.Directory.CreateDirectory(google);
                File.WriteAllText(Path.Combine(google, "timestamp.proto"), TimestampProto + "\n");
                File.WriteAllText(Path.Combine(google, "wrappers.proto"), WrappersProto + "\n");

                _directory = dest;
                return dest;
            }
        }
    }

    private static void CopyTree(string source, string destination)
    {
        System.IO.Directory.CreateDirectory(destination);
        foreach (var directory in System.IO.Directory.GetDirectories(source, "*", SearchOption.AllDirectories))
        {
            System.IO.Directory.CreateDirectory(directory.Replace(source, destination, StringComparison.Ordinal));
        }

        foreach (var file in System.IO.Directory.GetFiles(source, "*", SearchOption.AllDirectories))
        {
            var target = file.Replace(source, destination, StringComparison.Ordinal);
            File.Copy(file, target, overwrite: true);
        }
    }
}
