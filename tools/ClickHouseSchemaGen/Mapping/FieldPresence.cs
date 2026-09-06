namespace ClickHouseSchemaGen.Mapping;

internal static class FieldPresence
{
    public static bool HasSyntheticOneof(FieldDescriptor field) =>
        field.ContainingOneof?.IsSynthetic == true;

    public static bool HasExplicitPresence(FieldDescriptor field) =>
        field.HasPresence && field.ContainingOneof is null;

    public static bool IsProtoOptional(FieldMappingRequest request) =>
        HasSyntheticOneof(request.Field)
        || (request.Field.HasPresence && request.Context.Defaults.OptionalAsNullable);
}
