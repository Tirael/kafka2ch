namespace ClickHouseSchemaGen.Mapping;

public sealed class DenormalizationPlanner
{
    private readonly IReadOnlyList<IFieldMappingStrategy> _strategies;

    public DenormalizationPlanner()
    {
        _strategies =
        [
            new MapFieldStrategy(),
            new RepeatedFieldStrategy(this),
            new MessageFieldStrategy(this),
            new ScalarFieldStrategy()
        ];
    }

    public IReadOnlyList<ClickHouseColumn> MapMessage(
        MessageDescriptor descriptor,
        MappingContext context)
    {
        List<ClickHouseColumn> columns = [];
        var processedOneofs = new HashSet<OneofDescriptor>();

        foreach (var field in descriptor.Fields.InDeclarationOrder())
        {
            if (field.ContainingOneof is not null && !field.ContainingOneof.IsSynthetic)
            {
                if (!processedOneofs.Add(field.ContainingOneof))
                    continue;

                columns.AddRange(MapOneof(field.ContainingOneof, context));
                continue;
            }

            columns.AddRange(MapField(field, field.Name, context));
        }

        return columns;
    }

    public IReadOnlyList<ClickHouseColumn> MapMessage(
        MessageDescriptor descriptor,
        CodegenDefaults defaults,
        IReadOnlyDictionary<string, FieldOverrideConfig> fieldOverrides) =>
        MapMessage(descriptor, new MappingContext
        {
            Defaults = defaults,
            FieldOverrides = fieldOverrides,
            Depth = 0
        });

    internal IEnumerable<ClickHouseColumn> MapNestedFields(
        MessageDescriptor descriptor,
        MappingContext context) =>
        MapMessage(descriptor, context with { Depth = context.Depth + 1 });

    private IEnumerable<ClickHouseColumn> MapField(
        FieldDescriptor field,
        string columnPath,
        MappingContext context)
    {
        if (context.GetOverrideStrategy(columnPath) == MappingStrategy.JsonFallback)
        {
            var type = context.GetOverride(columnPath)?.Type ?? "String";
            yield return new ClickHouseColumn(columnPath, type, "json fallback", columnPath, MappingStrategy.JsonFallback);
            yield break;
        }

        var strategy = _strategies.FirstOrDefault(candidate => candidate.CanMap(field, context))
            ?? throw new NotSupportedException($"No mapping strategy for field '{columnPath}'.");

        foreach (var column in strategy.Map(field, columnPath, context))
            yield return column;
    }

    private static IEnumerable<ClickHouseColumn> MapOneof(OneofDescriptor oneof, MappingContext context)
    {
        if (!context.Defaults.OneofPresence)
            throw new NotSupportedException(
                $"oneof '{oneof.Name}' requires defaults.oneofPresence=true for ClickHouse ProtobufSingle.");

        foreach (var branch in oneof.Fields)
        {
            var branchType = ClickHouseTypeResolver.ResolveScalar(branch, branch.Name, context, forceNullable: true);
            yield return new ClickHouseColumn(
                branch.Name,
                branchType,
                $"oneof {oneof.Name}",
                branch.Name,
                MappingStrategy.Oneof);
        }

        var presenceEnum = string.Join(
            ", ",
            ["'absent' = 0", .. oneof.Fields.Select(branch => $"'{branch.Name}' = {branch.FieldNumber}")]);

        yield return new ClickHouseColumn(
            oneof.Name,
            $"Enum8({presenceEnum})",
            "oneof presence",
            oneof.Name,
            MappingStrategy.Oneof);
    }

    internal static string BuildNestedType(IReadOnlyList<ClickHouseColumn> innerColumns) =>
        $"Nested({string.Join(", ", innerColumns.Select(column => $"{column.Name} {column.Type}"))})";

    internal static string BuildTupleType(IReadOnlyList<ClickHouseColumn> innerColumns) =>
        $"Tuple({string.Join(", ", innerColumns.Select(column => $"{column.Name} {column.Type}"))})";
}
