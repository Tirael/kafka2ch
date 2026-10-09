using System.Text.RegularExpressions;

namespace ClickHouseSchemaGen.Migration;

public static partial class MigrationSqlGenerator
{
    public static string Generate(
        MigrationPlan plan,
        ResolvedSchemaPlan oldPlan,
        ResolvedSchemaPlan newPlan,
        string migrationName,
        string? parentChecksum,
        string targetChecksum,
        DateTimeOffset generatedAt)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(oldPlan);
        ArgumentNullException.ThrowIfNull(newPlan);
        ArgumentException.ThrowIfNullOrWhiteSpace(migrationName);
        ArgumentException.ThrowIfNullOrWhiteSpace(targetChecksum);

        var cluster = ClusterDdl.For(newPlan.Config);
        var builder = new StringBuilder();
        AppendHeader(builder, migrationName, generatedAt, parentChecksum, targetChecksum, plan.Warnings);

        var kafkaRecreates = CollectKafkaRecreates(plan);
        var metaOnlyQueues = CollectMetaOnlyQueues(plan, kafkaRecreates, newPlan);
        var detachedQueues = kafkaRecreates
            .Concat(metaOnlyQueues)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(q => q, StringComparer.Ordinal)
            .ToList();

        foreach (var queue in detachedQueues)
        {
            builder.AppendLine($"DETACH TABLE IF EXISTS {queue}{cluster.OnCluster};");
            builder.AppendLine(cluster.AwaitKafkaConsumersEmpty(queue));
        }

        if (detachedQueues.Count > 0)
            builder.AppendLine();

        AppendMergeTreeAutomaticChanges(builder, plan, cluster);
        AppendNewMergeTreeTables(builder, plan, newPlan, cluster);
        AppendPipelineRecreates(builder, plan, newPlan, kafkaRecreates, metaOnlyQueues, cluster);

        AppendManualBlock(builder, plan, cluster);

        return builder.ToString();
    }

    private static void AppendHeader(
        StringBuilder builder,
        string migrationName,
        DateTimeOffset generatedAt,
        string? parentChecksum,
        string targetChecksum,
        IReadOnlyList<string> warnings)
    {
        builder.AppendLine(SqlScriptWriter.GeneratedHeader);
        builder.AppendLine($"-- Migration: {migrationName}");
        builder.AppendLine($"-- Generated at: {generatedAt:O}");
        builder.AppendLine($"-- Parent checksum: {parentChecksum ?? "<none>"}");
        builder.AppendLine($"-- Target checksum: {targetChecksum}");

        foreach (var warning in warnings)
            builder.AppendLine($"-- Warning: {warning}");

        builder.AppendLine();
    }

    private static void AppendMergeTreeAutomaticChanges(
        StringBuilder builder,
        MigrationPlan plan,
        ClusterDdl cluster)
    {
        foreach (var change in plan.AutomaticChanges.OrderBy(c => c, ChangeComparer.Instance))
        {
            switch (change)
            {
                case ColumnAdded added:
                    AppendAlter(builder, cluster, added.TableName, BuildAddColumnClause(added));
                    break;
                case ColumnRenamed renamed:
                    AppendAlter(
                        builder,
                        cluster,
                        renamed.TableName,
                        $"RENAME COLUMN IF EXISTS {SqlColumnFormatter.FormatColumnName(renamed.OldName)} TO {SqlColumnFormatter.FormatColumnName(renamed.NewName)}");
                    break;
                case ColumnTypeChanged { Kind: TypeChangeKind.Safe or TypeChangeKind.Rewrite } typeChanged:
                    AppendAlter(
                        builder,
                        cluster,
                        typeChanged.TableName,
                        $"MODIFY COLUMN {SqlColumnFormatter.FormatColumnName(typeChanged.ColumnName)} {typeChanged.NewType}");
                    break;
            }
        }

        if (plan.AutomaticChanges.Any(c => c is ColumnAdded or ColumnRenamed or ColumnTypeChanged))
            builder.AppendLine();
    }

    /// <summary>
    /// A column change of MergeTree table <c>t</c>: on a cluster the storage table <c>t_local</c> is altered
    /// first, then the <c>Distributed</c> table <c>t</c> whose structure must match it.
    /// </summary>
    private static void AppendAlter(StringBuilder builder, ClusterDdl cluster, string tableName, string clause)
    {
        foreach (var target in AlterTargets(cluster, tableName))
            builder.AppendLine($"ALTER TABLE {target}{cluster.OnCluster} {clause};");
    }

    private static IEnumerable<string> AlterTargets(ClusterDdl cluster, string tableName) =>
        cluster.Enabled ? [cluster.StorageTable(tableName), tableName] : [tableName];

    private static void AppendNewMergeTreeTables(
        StringBuilder builder,
        MigrationPlan plan,
        ResolvedSchemaPlan newPlan,
        ClusterDdl cluster)
    {
        foreach (var added in plan.AutomaticChanges.OfType<TableAdded>().Where(t => t.Engine == "MergeTree"))
        {
            var table = newPlan.MergeTreeTables.First(t =>
                string.Equals(t.Config.TableName, added.TableName, StringComparison.OrdinalIgnoreCase));
            builder.AppendLine("-- New MergeTree table; consider a new kafka_group_name for backfill if needed.");
            builder.Append(MergeTreeTableGenerator.Generate(table.Config, ifNotExists: true, cluster));
        }
    }

    private static void AppendPipelineRecreates(
        StringBuilder builder,
        MigrationPlan plan,
        ResolvedSchemaPlan newPlan,
        HashSet<string> kafkaRecreates,
        HashSet<string> metaOnlyQueues,
        ClusterDdl cluster)
    {
        var viewsToRecreate = plan.AutomaticChanges
            .OfType<ViewChanged>()
            .Select(v => v.ViewName)
            .Concat(plan.ManualChanges.OfType<ViewChanged>().Select(v => v.ViewName))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(v => v, StringComparer.Ordinal)
            .ToList();
        var attachedQueues = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var viewName in viewsToRecreate)
        {
            var view = newPlan.MaterializedViews.First(v =>
                string.Equals(v.Config.Name, viewName, StringComparison.OrdinalIgnoreCase));
            var queue = view.Config.SourceTable;
            var recreateKafka = kafkaRecreates.Contains(queue);

            builder.AppendLine($"DROP VIEW IF EXISTS {view.Config.Name}{cluster.OnCluster};");

            if (recreateKafka)
            {
                builder.AppendLine($"DROP TABLE IF EXISTS {queue}{cluster.OnCluster};");

                var kafkaPlan = newPlan.KafkaTables.First(t =>
                    string.Equals(t.Config.TableName, queue, StringComparison.OrdinalIgnoreCase));
                builder.Append(KafkaTableGenerator.Generate(kafkaPlan.Config, kafkaPlan.Columns, ifNotExists: false, cluster));
                builder.AppendLine();
            }

            // ClickHouse cannot create a view over a detached table; an attached Kafka table without views
            // has no consumers, so attaching before CREATE does not start consumption early.
            if (!recreateKafka && metaOnlyQueues.Contains(queue) && attachedQueues.Add(queue))
                builder.AppendLine($"ATTACH TABLE {queue}{cluster.OnCluster};");

            builder.Append(MaterializedViewGenerator.Generate(view.Config, cluster: cluster));

            if (recreateKafka)
                builder.AppendLine($"ATTACH TABLE {queue}{cluster.OnCluster};");

            builder.AppendLine();
        }

        foreach (var queue in kafkaRecreates.Except(viewsToRecreate.SelectMany(v =>
                     newPlan.MaterializedViews.Where(mv => string.Equals(mv.Config.Name, v, StringComparison.OrdinalIgnoreCase))
                         .Select(mv => mv.Config.SourceTable))))
        {
            if (viewsToRecreate.Any(v =>
                {
                    var source = newPlan.MaterializedViews
                        .First(m => string.Equals(m.Config.Name, v, StringComparison.OrdinalIgnoreCase))
                        .Config.SourceTable;
                    return string.Equals(source, queue, StringComparison.OrdinalIgnoreCase);
                }))
            {
                continue;
            }

            builder.AppendLine($"DROP TABLE IF EXISTS {queue}{cluster.OnCluster};");
            var kafkaPlan = newPlan.KafkaTables.First(t =>
                string.Equals(t.Config.TableName, queue, StringComparison.OrdinalIgnoreCase));
            builder.Append(KafkaTableGenerator.Generate(kafkaPlan.Config, kafkaPlan.Columns, ifNotExists: false, cluster));
            builder.AppendLine($"ATTACH TABLE {queue}{cluster.OnCluster};");
            builder.AppendLine();
        }
    }

    private static void AppendManualBlock(StringBuilder builder, MigrationPlan plan, ClusterDdl cluster)
    {
        if (plan.ManualChanges.Count == 0)
            return;

        builder.AppendLine("-- MANUAL / DESTRUCTIVE (review and uncomment before applying)");
        foreach (var change in plan.ManualChanges)
        {
            switch (change)
            {
                case ColumnRemoved removed:
                    foreach (var target in AlterTargets(cluster, removed.TableName))
                    {
                        builder.AppendLine(
                            $"-- ALTER TABLE {target}{cluster.OnCluster} DROP COLUMN IF EXISTS {SqlColumnFormatter.FormatColumnName(removed.ColumnName)};");
                    }

                    break;
                case ColumnTypeChanged typeChanged:
                    foreach (var target in AlterTargets(cluster, typeChanged.TableName))
                    {
                        builder.AppendLine(
                            $"-- ALTER TABLE {target}{cluster.OnCluster} MODIFY COLUMN {SqlColumnFormatter.FormatColumnName(typeChanged.ColumnName)} {typeChanged.NewType}; -- {typeChanged.Kind}");
                    }

                    break;
                case OrderByOrTtlChanged order:
                    builder.AppendLine(
                        $"-- Manual ORDER BY/TTL change for {order.TableName}: '{order.OldOrderBy}' -> '{order.NewOrderBy}', TTL '{order.OldTtl}' -> '{order.NewTtl}'.");
                    break;
                case OriginChanged origin:
                    builder.AppendLine(
                        $"-- Manual origin change for {origin.TableName}: {origin.OldOrigin} -> {origin.NewOrigin}.");
                    break;
                case TrailingSqlChanged trailing:
                    builder.AppendLine(
                        $"-- Manual trailingSql update required (hash {trailing.OldHash ?? "<empty>"} -> {trailing.NewHash ?? "<empty>"}).");
                    break;
                case TableRemoved { Engine: "MergeTree" } removed when cluster.Enabled:
                    builder.AppendLine($"-- DROP TABLE IF EXISTS {removed.TableName}{cluster.OnCluster};");
                    builder.AppendLine($"-- DROP TABLE IF EXISTS {cluster.StorageTable(removed.TableName)}{cluster.OnCluster} SYNC;");
                    break;
                case TableRemoved removed:
                    builder.AppendLine($"-- DROP TABLE IF EXISTS {removed.TableName}{cluster.OnCluster};");
                    break;
                default:
                    builder.AppendLine($"-- {change.GetType().Name}: {change}");
                    break;
            }
        }

        builder.AppendLine();
    }

    private static string BuildAddColumnClause(ColumnAdded added)
    {
        var defaultClause = BuildDefaultClause(added.ColumnType);
        var afterClause = string.IsNullOrWhiteSpace(added.AfterColumn)
            ? string.Empty
            : $" AFTER {SqlColumnFormatter.FormatColumnName(added.AfterColumn)}";

        return defaultClause is null
            ? $"ADD COLUMN IF NOT EXISTS {SqlColumnFormatter.FormatBareDefinition(added.ColumnName, added.ColumnType)}{afterClause}"
            : $"ADD COLUMN IF NOT EXISTS {SqlColumnFormatter.FormatBareDefinition(added.ColumnName, added.ColumnType)} DEFAULT {defaultClause}{afterClause}";
    }

    internal static string? BuildDefaultClause(string columnType)
    {
        if (columnType.StartsWith("Nullable(", StringComparison.Ordinal))
            return null;

        if (columnType is "String" or "LowCardinality(String)")
            return "''";

        if (IntegerTypes.Contains(columnType))
            return "0";

        if (columnType is "Float32" or "Float64")
            return "0";

        if (columnType.StartsWith("Array(", StringComparison.Ordinal))
            return "[]";

        if (columnType.StartsWith("Map(", StringComparison.Ordinal))
            return "map()";

        if (columnType.StartsWith("Enum8(", StringComparison.Ordinal) || columnType.StartsWith("Enum16(", StringComparison.Ordinal))
            return FirstEnumValue(columnType);

        if (columnType is "DateTime" or "DateTime64(3)" or "DateTime64(6)" or "DateTime64(9)")
            return "toDateTime(0)";

        if (columnType == "Bool")
            return "0";

        return null;
    }

    private static string FirstEnumValue(string enumType)
    {
        var match = EnumValueRegex().Match(enumType);
        if (!match.Success)
            return "0";

        var first = enumType[(enumType.IndexOf('\'') + 1)..];
        var end = first.IndexOf('\'');
        return end < 0 ? "0" : $"'{first[..end]}'";
    }

    private static HashSet<string> CollectKafkaRecreates(MigrationPlan plan) =>
        plan.AutomaticChanges
            .Concat(plan.ManualChanges)
            .OfType<KafkaTableChanged>()
            .Select(c => c.TableName)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

    private static HashSet<string> CollectMetaOnlyQueues(
        MigrationPlan plan,
        HashSet<string> kafkaRecreates,
        ResolvedSchemaPlan newPlan)
    {
        HashSet<string> queues = new(StringComparer.OrdinalIgnoreCase);

        foreach (var viewChanged in plan.AutomaticChanges.OfType<ViewChanged>())
        {
            var source = viewChanged.SourceTable;
            if (!kafkaRecreates.Contains(source))
                queues.Add(source);
        }

        foreach (var added in plan.AutomaticChanges.OfType<ColumnAdded>())
        {
            if (KafkaMetaColumnFactory.IsKafkaMetaPath(added.FieldNumberPath))
            {
                var queue = newPlan.MaterializedViews
                    .FirstOrDefault(v => string.Equals(v.Config.TargetTable, added.TableName, StringComparison.OrdinalIgnoreCase))
                    ?.Config.SourceTable;
                if (!string.IsNullOrWhiteSpace(queue) && !kafkaRecreates.Contains(queue))
                    queues.Add(queue);
            }
        }

        return queues;
    }

    private static readonly HashSet<string> IntegerTypes = new(StringComparer.Ordinal)
    {
        "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64"
    };

    [GeneratedRegex(@"Enum(?:8|16)\('(?<value>[^']+)'", RegexOptions.CultureInvariant)]
    private static partial Regex EnumValueRegex();

    private sealed class ChangeComparer : IComparer<SchemaChange>
    {
        public static ChangeComparer Instance { get; } = new();

        public int Compare(SchemaChange? x, SchemaChange? y)
        {
            if (ReferenceEquals(x, y))
                return 0;
            if (x is null)
                return -1;
            if (y is null)
                return 1;

            var tableX = TableName(x);
            var tableY = TableName(y);
            var tableCompare = string.Compare(tableX, tableY, StringComparison.OrdinalIgnoreCase);
            if (tableCompare != 0)
                return tableCompare;

            return Rank(x).CompareTo(Rank(y));
        }

        private static string TableName(SchemaChange change) =>
            change switch
            {
                ColumnAdded a => a.TableName,
                ColumnRenamed r => r.TableName,
                ColumnTypeChanged t => t.TableName,
                _ => string.Empty
            };

        private static int Rank(SchemaChange change) =>
            change switch
            {
                ColumnAdded => 0,
                ColumnRenamed => 1,
                ColumnTypeChanged => 2,
                _ => 99
            };
    }
}
