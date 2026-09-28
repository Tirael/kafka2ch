namespace ClickHouseSchemaGen.Generation;

public sealed record ProtobufKeyField(
    string Name,
    int FieldNumber,
    string ClickHouseType,
    string TupleValueExpression);
