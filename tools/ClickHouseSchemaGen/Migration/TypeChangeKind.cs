namespace ClickHouseSchemaGen.Migration;

public enum TypeChangeKind
{
    Safe,
    Rewrite,
    Destructive,
    Manual
}
