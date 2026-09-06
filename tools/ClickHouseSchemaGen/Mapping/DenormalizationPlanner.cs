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
            if (IsRealOneofField(field))
            {
                if (processedOneofs.Add(field.ContainingOneof!))
                    columns.AddRange(MapOneof(field.ContainingOneof!, context));

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
        var request = new FieldMappingRequest(field, columnPath, context);

        return TryCreateJsonFallbackColumns(columnPath, context, MappingStrategy.JsonFallback, "json fallback")
            ?? MapWithStrategy(request, $"field '{columnPath}'");
    }

    private IEnumerable<ClickHouseColumn> MapOneofBranch(
        FieldDescriptor branch,
        string oneofName,
        MappingContext context)
    {
        var request = new FieldMappingRequest(branch, branch.Name, context).WithForceNullable();

        return TryCreateJsonFallbackColumns(branch.Name, context, MappingStrategy.Oneof, $"oneof {oneofName}")
            ?? MapWithStrategy(
                request,
                $"oneof branch '{branch.Name}'",
                column => column with
                {
                    Strategy = MappingStrategy.Oneof,
                    Comment = column.Comment ?? $"oneof {oneofName}"
                });
    }

    private static IEnumerable<ClickHouseColumn>? TryCreateJsonFallbackColumns(
        string columnPath,
        MappingContext context,
        MappingStrategy strategy,
        string comment) =>
        context.GetOverrideStrategy(columnPath) == MappingStrategy.JsonFallback
            ? [ClickHouseColumn.Create(
                columnPath,
                context.GetOverride(columnPath)?.Type ?? "String",
                strategy,
                comment)]
            : null;

    private IEnumerable<ClickHouseColumn> MapWithStrategy(
        FieldMappingRequest request,
        string errorContext,
        Func<ClickHouseColumn, ClickHouseColumn>? transform = null)
    {
        var strategy = _strategies.FirstOrDefault(candidate => candidate.CanMap(request))
            ?? throw new NotSupportedException($"No mapping strategy for {errorContext}.");

        var columns = strategy.Map(request);
        return transform is null ? columns : columns.Select(transform);
    }

    private IEnumerable<ClickHouseColumn> MapOneof(OneofDescriptor oneof, MappingContext context)
    {
        if (!context.Defaults.OneofPresence)
            throw new NotSupportedException(
                $"oneof '{oneof.Name}' requires defaults.oneofPresence=true for ClickHouse ProtobufSingle.");

        List<ClickHouseColumn> columns = [];

        foreach (var branch in oneof.Fields)
            columns.AddRange(MapOneofBranch(branch, oneof.Name, context));

        var presenceEnum = ClickHouseEnumFormatter.JoinValues(
            [("absent", 0), .. oneof.Fields.Select(branch => (branch.Name, branch.FieldNumber))]);

        columns.Add(ClickHouseColumn.Create(
            oneof.Name,
            $"Enum8({presenceEnum})",
            MappingStrategy.Oneof,
            "oneof presence"));

        return columns;
    }

    internal static string BuildNestedType(IReadOnlyList<ClickHouseColumn> innerColumns) =>
        BuildCompositeType("Nested", innerColumns);

    internal static string BuildTupleType(IReadOnlyList<ClickHouseColumn> innerColumns) =>
        BuildCompositeType("Tuple", innerColumns);

    private static string BuildCompositeType(string keyword, IReadOnlyList<ClickHouseColumn> innerColumns) =>
        $"{keyword}({string.Join(", ", innerColumns.Select(column => $"{SanitizeCompositeColumnName(column.Name)} {column.Type}"))})";

    private static string SanitizeCompositeColumnName(string name) =>
        name.Replace('.', '_');

    private static bool IsRealOneofField(FieldDescriptor field) =>
        field.ContainingOneof is not null && !field.ContainingOneof.IsSynthetic;
}
