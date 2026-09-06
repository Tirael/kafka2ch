namespace ClickHouseSchemaGen.Mapping;

public sealed class ScalarFieldStrategy : IFieldMappingStrategy
{
    public bool CanMap(FieldMappingRequest request) => request.Field.IsScalarField();

    public IEnumerable<ClickHouseColumn> Map(FieldMappingRequest request)
    {
        var strategy = ResolveStrategy(request);

        return
        [
            ClickHouseColumn.Create(
                request.ColumnPath,
                ClickHouseTypeResolver.ResolveScalar(request),
                strategy,
                ResolveComment(request.Field.FieldType, strategy))
        ];
    }

    private static MappingStrategy ResolveStrategy(FieldMappingRequest request) =>
        IsOptionalScalar(request) ? MappingStrategy.Optional : MappingStrategy.Direct;

    private static bool IsOptionalScalar(FieldMappingRequest request) =>
        FieldPresence.IsProtoOptional(request);

    private static string? ResolveComment(FieldType fieldType, MappingStrategy strategy) =>
        fieldType == FieldType.Enum ? "proto enum"
        : strategy == MappingStrategy.Optional ? "proto optional"
        : null;
}
