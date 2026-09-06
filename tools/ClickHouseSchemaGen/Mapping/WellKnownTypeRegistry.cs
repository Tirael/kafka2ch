namespace ClickHouseSchemaGen.Mapping;

public static class WellKnownTypeRegistry
{
    private const string GoogleProtobufPrefix = "google.protobuf.";

    public static bool IsTimestamp(MessageDescriptor descriptor) =>
        descriptor.FullName == "google.protobuf.Timestamp";

    public static bool IsDuration(MessageDescriptor descriptor) =>
        descriptor.FullName == "google.protobuf.Duration";

    public static bool IsStruct(MessageDescriptor descriptor) =>
        descriptor.FullName == "google.protobuf.Struct";

    public static bool IsAny(MessageDescriptor descriptor) =>
        descriptor.FullName == "google.protobuf.Any";

    public static bool IsWrapper(MessageDescriptor descriptor) =>
        descriptor.FullName.StartsWith(GoogleProtobufPrefix, StringComparison.Ordinal)
        && descriptor.Name.EndsWith("Value", StringComparison.Ordinal);

    public static bool ShouldFlattenWellKnownMessage(MessageDescriptor descriptor) =>
        IsTimestamp(descriptor) || IsDuration(descriptor);

    public static string? MapMessageType(MessageDescriptor descriptor)
    {
        if (ShouldFlattenWellKnownMessage(descriptor))
            return null;

        return descriptor.FullName switch
        {
            _ when IsWrapper(descriptor) => MapWrapperType(descriptor),
            _ when IsStruct(descriptor) || IsAny(descriptor) => "String",
            _ => null
        };
    }

    private static string MapWrapperType(MessageDescriptor descriptor) =>
        descriptor.Name switch
        {
            "DoubleValue" => "Nullable(Float64)",
            "FloatValue" => "Nullable(Float32)",
            "Int64Value" => "Nullable(Int64)",
            "UInt64Value" => "Nullable(UInt64)",
            "Int32Value" => "Nullable(Int32)",
            "UInt32Value" => "Nullable(UInt32)",
            "BoolValue" => "Nullable(UInt8)",
            "StringValue" => "Nullable(String)",
            "BytesValue" => "Nullable(String)",
            _ => "Nullable(String)"
        };
}
