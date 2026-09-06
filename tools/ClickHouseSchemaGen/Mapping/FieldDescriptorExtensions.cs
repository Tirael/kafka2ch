namespace ClickHouseSchemaGen.Mapping;

internal static class FieldDescriptorExtensions
{
    public static bool IsRepeatedOrMap(this FieldDescriptor field) =>
        field.IsRepeated || field.IsMap;

    public static bool IsScalarField(this FieldDescriptor field) =>
        !field.IsRepeatedOrMap() && field.FieldType != FieldType.Message;
}
