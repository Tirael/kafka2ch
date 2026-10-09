using ClickHouseSchemaGen.Migration;

namespace ClickHouseSchemaGen.Shared;

/// <summary>
/// Rebuilds the "persist protobuf kafka key" migration (new <c>kafka_key.order_id</c> column + view recreate with a
/// detached queue) against any config, so single-node and cluster rendering can be compared and applied.
/// </summary>
public static class PersistKeyMigrationFixture
{
    public const string AddedColumn = "kafka_key.order_id";

    public static string GenerateSql(string configPath)
    {
        var config = CodegenConfigLoader.Load(configPath);
        var newPlan = SchemaGeneratorFactory.Create().BuildPlan(config);
        var oldPlan = WithoutOrderKeyColumn(newPlan);

        var plan = MigrationPolicy.Apply(SchemaDiffer.Diff(oldPlan, newPlan), oldPlan, newPlan);
        return MigrationSqlGenerator.Generate(
            plan,
            oldPlan,
            newPlan,
            "20990101000000_persist_order_key",
            parentChecksum: null,
            targetChecksum: "target",
            new DateTimeOffset(2099, 1, 1, 0, 0, 0, TimeSpan.Zero));
    }

    private static ResolvedSchemaPlan WithoutOrderKeyColumn(ResolvedSchemaPlan plan) =>
        plan with
        {
            MergeTreeTables = plan.MergeTreeTables
                .Select(table => table.Config.TableName != "orders"
                    ? table
                    : table with
                    {
                        Config = new MergeTreeTableConfig
                        {
                            TableName = table.Config.TableName,
                            OrderBy = table.Config.OrderBy,
                            Ttl = table.Config.Ttl,
                            ShardingKey = table.Config.ShardingKey,
                            SourceTable = table.Config.SourceTable,
                            IncludeKafkaMeta = table.Config.IncludeKafkaMeta,
                            Columns = table.Config.Columns.Where(column => column.Name != AddedColumn).ToList()
                        }
                    })
                .ToList(),
            MaterializedViews = plan.MaterializedViews
                .Select(view => view.Config.Name != "orders_mv"
                    ? view
                    : view with
                    {
                        Config = new MaterializedViewConfig
                        {
                            Name = view.Config.Name,
                            TargetTable = view.Config.TargetTable,
                            SourceTable = view.Config.SourceTable,
                            IncludeKafkaMeta = view.Config.IncludeKafkaMeta,
                            Columns = view.Config.Columns.Where(mapping => mapping.Target != AddedColumn).ToList()
                        }
                    })
                .ToList()
        };
}
