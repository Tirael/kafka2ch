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
        _ = (generatedAt, parentChecksum, plan.Warnings);

        var cluster = ClusterDdl.For(newPlan.Config);
        var builder = new StringBuilder();

        var kafkaRecreates = CollectKafkaRecreates(plan);
        var addedKafkaTables = CollectAddedKafkaTables(plan);
        var metaOnlyQueues = CollectMetaOnlyQueues(plan, kafkaRecreates, addedKafkaTables, oldPlan, newPlan);
        var detachedQueues = kafkaRecreates
            .Concat(metaOnlyQueues)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(q => q, StringComparer.Ordinal)
            .ToList();

        foreach (var queue in detachedQueues)
        {
            builder.AppendLine(cluster.DetachTableSql(queue));
            builder.AppendLine(cluster.AwaitKafkaConsumersEmpty(queue));
        }

        if (detachedQueues.Count > 0)
            builder.AppendLine();

        AppendMergeTreeAutomaticChanges(builder, plan, cluster);
        AppendNewMergeTreeTables(builder, plan, newPlan, cluster);
        AppendNewKafkaTables(builder, plan, newPlan, cluster);
        AppendPipelineRecreates(builder, plan, newPlan, kafkaRecreates, metaOnlyQueues, cluster);
        AppendAggregateChanges(builder, plan, newPlan, cluster);

        AppendManualBlock(builder, plan, newPlan, cluster);

        return SqlText.Normalize(builder.ToString());
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
                        $"RENAME COLUMN IF EXISTS {SqlColumnFormatter.FormatColumnName(renamed.OldName)} "
                            + $"TO {SqlColumnFormatter.FormatColumnName(renamed.NewName)}");
                    break;
                case ColumnTypeChanged { Kind: TypeChangeKind.Safe or TypeChangeKind.Rewrite } typeChanged:
                    AppendAlter(
                        builder,
                        cluster,
                        typeChanged.TableName,
                        $"MODIFY COLUMN {SqlColumnFormatter.FormatColumnName(typeChanged.ColumnName)} {typeChanged.NewType}");
                    break;
                case SourceTableChanged sourceChanged:
                    builder.AppendLine(
                        $"-- MergeTree table {sourceChanged.TableName} sourceTable changed: "
                            + $"'{sourceChanged.OldSourceTable ?? "<none>"}' -> '{sourceChanged.NewSourceTable ?? "<none>"}'.");
                    break;
            }
        }

        if (plan.AutomaticChanges.Any(c => c is ColumnAdded or ColumnRenamed or ColumnTypeChanged or SourceTableChanged))
            builder.AppendLine();
    }

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
            builder.Append(MergeTreeTableGenerator.Generate(table.Config, ifNotExists: true, cluster));
        }
    }

    private static void AppendNewKafkaTables(
        StringBuilder builder,
        MigrationPlan plan,
        ResolvedSchemaPlan newPlan,
        ClusterDdl cluster)
    {
        foreach (var added in plan.AutomaticChanges.OfType<TableAdded>().Where(t => t.Engine == "Kafka"))
        {
            var kafkaPlan = newPlan.KafkaTables.First(t =>
                string.Equals(t.Config.TableName, added.TableName, StringComparison.OrdinalIgnoreCase));
            builder.Append(KafkaTableGenerator.Generate(
                kafkaPlan.Config,
                kafkaPlan.Columns,
                ifNotExists: true,
                cluster));
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
        var viewsToDrop = plan.AutomaticChanges
            .OfType<ViewChanged>()
            .Select(v => v.ViewName)
            .Concat(plan.ManualChanges.OfType<ViewChanged>().Select(v => v.ViewName))
            .Concat(newPlan.MaterializedViews
                .Where(v => kafkaRecreates.Contains(v.Config.SourceTable))
                .Select(v => v.Config.Name))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(v => v, StringComparer.Ordinal)
            .ToList();
        var viewsToCreate = viewsToDrop
            .Select(name => newPlan.MaterializedViews.FirstOrDefault(v =>
                string.Equals(v.Config.Name, name, StringComparison.OrdinalIgnoreCase)))
            .OfType<MaterializedViewPlan>()
            .ToList();

        foreach (var viewName in viewsToDrop)
            builder.AppendLine($"DROP VIEW IF EXISTS {viewName}{cluster.OnCluster};");

        foreach (var queue in kafkaRecreates.OrderBy(q => q, StringComparer.Ordinal))
        {
            builder.AppendLine(cluster.AttachTableSql(queue));
            builder.AppendLine($"DROP TABLE IF EXISTS {queue}{cluster.OnCluster};");

            var kafkaPlan = newPlan.KafkaTables.First(t =>
                string.Equals(t.Config.TableName, queue, StringComparison.OrdinalIgnoreCase));
            builder.Append(KafkaTableGenerator.Generate(
                kafkaPlan.Config,
                kafkaPlan.Columns,
                ifNotExists: false,
                cluster));
        }

        foreach (var queue in metaOnlyQueues.OrderBy(q => q, StringComparer.Ordinal))
            builder.AppendLine(cluster.AttachTableSql(queue));

        if (metaOnlyQueues.Count > 0 || (viewsToDrop.Count > 0 && kafkaRecreates.Count == 0))
            builder.AppendLine();

        foreach (var view in viewsToCreate)
        {
            var writeThrough = newPlan.MergeTreeTables
                .FirstOrDefault(table => string.Equals(
                    table.Config.TableName,
                    view.Config.TargetTable,
                    StringComparison.OrdinalIgnoreCase))
                ?.Config.MaterializedViewsWriteThroughDistributed;
            builder.Append(MaterializedViewGenerator.Generate(
                view.Config,
                cluster: cluster,
                writeThroughDistributed: writeThrough));
        }
    }

    private static void AppendAggregateChanges(
        StringBuilder builder,
        MigrationPlan plan,
        ResolvedSchemaPlan newPlan,
        ClusterDdl cluster)
    {
        foreach (var change in plan.AutomaticChanges)
        {
            switch (change)
            {
                case AggregateAdded added:
                    builder.Append(AggregateGenerator.Generate(FindAggregate(newPlan, added.Name), cluster, ifNotExists: true));
                    break;
                case AggregateChanged changed:
                    builder.AppendLine($"DROP VIEW IF EXISTS {changed.OldViewName}{cluster.OnCluster};");
                    builder.Append(AggregateGenerator.GenerateView(FindAggregate(newPlan, changed.Name), cluster));
                    break;
                case AggregatesAdoptedFromTrailingSql adopted:
                    builder.AppendLine(
                        $"-- trailingSql (hash {adopted.TrailingSqlHash}) is now declared as aggregates "
                            + $"{string.Join(", ", adopted.Names)}; the generated DDL is unchanged.");
                    builder.AppendLine();
                    break;
            }
        }
    }

    private static AggregatePlan FindAggregate(ResolvedSchemaPlan plan, string name) =>
        plan.Aggregates.First(aggregate => string.Equals(aggregate.Name, name, StringComparison.OrdinalIgnoreCase));

    private static void AppendAggregateRecreateTemplate(
        StringBuilder builder,
        AggregateChanged changed,
        ResolvedSchemaPlan newPlan,
        ClusterDdl cluster)
    {
        builder.AppendLine($"-- Manual aggregate change for {changed.Name}: {changed.Reason}.");
        builder.AppendLine(
            "-- The target table is recreated; the next refresh repopulates it from the source table.");
        builder.AppendLine($"-- DROP VIEW IF EXISTS {changed.OldViewName}{cluster.OnCluster};");
        AppendDropAggregateTables(builder, changed.Name, cluster);
        foreach (var line in AggregateGenerator.Generate(FindAggregate(newPlan, changed.Name), cluster)
                     .Split(SqlText.NewLine)
                     .Where(line => line.Length > 0))
        {
            builder.AppendLine($"-- {line}");
        }
    }

    private static void AppendDropAggregateTables(StringBuilder builder, string name, ClusterDdl cluster)
    {
        builder.AppendLine($"-- DROP TABLE IF EXISTS {name}{cluster.OnCluster};");
        if (cluster.Enabled)
            builder.AppendLine($"-- DROP TABLE IF EXISTS {cluster.StorageTable(name)}{cluster.OnCluster} SYNC;");
    }

    private static void AppendManualBlock(
        StringBuilder builder,
        MigrationPlan plan,
        ResolvedSchemaPlan newPlan,
        ClusterDdl cluster)
    {
        if (plan.ManualChanges.Count == 0)
            return;

        foreach (var change in plan.ManualChanges)
        {
            switch (change)
            {
                case ColumnRemoved removed:
                    foreach (var target in AlterTargets(cluster, removed.TableName))
                    {
                        builder.AppendLine(
                            $"-- ALTER TABLE {target}{cluster.OnCluster} DROP COLUMN IF EXISTS "
                                + $"{SqlColumnFormatter.FormatColumnName(removed.ColumnName)};");
                    }

                    break;
                case ColumnTypeChanged typeChanged:
                    foreach (var target in AlterTargets(cluster, typeChanged.TableName))
                    {
                        builder.AppendLine(
                            $"-- ALTER TABLE {target}{cluster.OnCluster} MODIFY COLUMN "
                                + $"{SqlColumnFormatter.FormatColumnName(typeChanged.ColumnName)} {typeChanged.NewType};");
                    }

                    break;
                case OrderByOrTtlChanged order:
                    builder.AppendLine(
                        $"-- Manual ORDER BY/TTL change for {order.TableName}: "
                            + $"'{order.OldOrderBy}' -> '{order.NewOrderBy}', "
                            + $"TTL '{order.OldTtl}' -> '{order.NewTtl}'.");
                    break;
                case EngineChanged engine:
                    AppendEngineChangeTemplate(builder, engine, newPlan, cluster);
                    break;
                case OriginChanged origin:
                    builder.AppendLine(
                        $"-- Manual origin change for {origin.TableName}: "
                            + $"{origin.OldOrigin} -> {origin.NewOrigin}.");
                    break;
                case ClusterTopologyChanged topology:
                    builder.AppendLine(
                        $"-- Manual cluster topology change ({topology.Scope}): {topology.Reason}.");
                    builder.AppendLine(
                        "-- Requires a data/topology migration: create the objects for the new topology "
                            + "(local/Distributed tables or Replicated database), copy the data, switch Kafka queues "
                            + "and materialized views, then drop the old objects. It cannot be generated automatically.");
                    break;
                case AggregateChanged changed:
                    AppendAggregateRecreateTemplate(builder, changed, newPlan, cluster);
                    break;
                case AggregateRemoved removed:
                    builder.AppendLine($"-- DROP VIEW IF EXISTS {removed.ViewName}{cluster.OnCluster};");
                    AppendDropAggregateTables(builder, removed.Name, cluster);
                    break;
                case TrailingSqlChanged trailing:
                    builder.AppendLine(
                        $"-- Manual trailingSql update required "
                            + $"(hash {trailing.OldHash ?? "<empty>"} -> {trailing.NewHash ?? "<empty>"}).");
                    break;
                case TableRemoved { Engine: "MergeTree" } removed when cluster.Enabled:
                    builder.AppendLine($"-- DROP TABLE IF EXISTS {removed.TableName}{cluster.OnCluster};");
                    builder.AppendLine(
                        $"-- DROP TABLE IF EXISTS {cluster.StorageTable(removed.TableName)}"
                            + $"{cluster.OnCluster} SYNC;");
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

    private static void AppendEngineChangeTemplate(
        StringBuilder builder,
        EngineChanged engine,
        ResolvedSchemaPlan newPlan,
        ClusterDdl cluster)
    {
        var table = newPlan.MergeTreeTables.First(t =>
            string.Equals(t.Config.TableName, engine.TableName, StringComparison.OrdinalIgnoreCase)).Config;
        var storage = cluster.StorageTable(table.TableName);
        var copy = storage + "__engine_new";
        var ttl = string.IsNullOrWhiteSpace(table.Ttl) ? null : table.Ttl.Trim();

        builder.AppendLine(
            $"-- Manual engine change for {engine.TableName}: '{engine.OldEngine}' -> '{engine.NewEngine}'.");
        builder.AppendLine(
            $"-- Stop writers to {engine.TableName} (DETACH the Kafka queues feeding it) before copying.");
        builder.AppendLine($"-- CREATE TABLE {copy}{cluster.OnCluster} AS {storage}");
        builder.AppendLine($"-- ENGINE = {cluster.StorageEngine(engine.NewEngine)}");
        builder.AppendLine($"-- ORDER BY {table.OrderBy}{(ttl is null ? ";" : string.Empty)}");
        if (ttl is not null)
            builder.AppendLine($"-- TTL {ttl};");
        builder.AppendLine(
            $"-- INSERT INTO {copy} SELECT * FROM {storage};{(cluster.Enabled ? " -- once per shard" : string.Empty)}");
        builder.AppendLine($"-- EXCHANGE TABLES {storage} AND {copy}{cluster.OnCluster};");
        builder.AppendLine($"-- DROP TABLE {copy}{cluster.OnCluster} SYNC;");
    }

    private static string BuildAddColumnClause(ColumnAdded added)
    {
        var defaultClause = BuildDefaultClause(added.ColumnType);
        var afterClause = string.IsNullOrWhiteSpace(added.AfterColumn)
            ? " FIRST"
            : $" AFTER {SqlColumnFormatter.FormatColumnName(added.AfterColumn)}";

        var definition = SqlColumnFormatter.FormatBareDefinition(added.ColumnName, added.ColumnType);
        return defaultClause is null
            ? $"ADD COLUMN IF NOT EXISTS {definition}{afterClause}"
            : $"ADD COLUMN IF NOT EXISTS {definition} DEFAULT {defaultClause}{afterClause}";
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

        if (columnType.StartsWith("Enum8(", StringComparison.Ordinal)
            || columnType.StartsWith("Enum16(", StringComparison.Ordinal))
            return FirstEnumValue(columnType);

        if (columnType is "DateTime" or "DateTime64(3)" or "DateTime64(6)" or "DateTime64(9)")
            return "toDateTime(0)";

        if (columnType is "Bool")
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

    private static HashSet<string> CollectAddedKafkaTables(MigrationPlan plan) =>
        plan.AutomaticChanges
            .OfType<TableAdded>()
            .Where(t => t.Engine == "Kafka")
            .Select(t => t.TableName)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

    private static HashSet<string> CollectMetaOnlyQueues(
        MigrationPlan plan,
        HashSet<string> kafkaRecreates,
        HashSet<string> addedKafkaTables,
        ResolvedSchemaPlan oldPlan,
        ResolvedSchemaPlan newPlan)
    {
        HashSet<string> queues = new(StringComparer.OrdinalIgnoreCase);
        var existingQueues = oldPlan.KafkaTables
            .Select(t => t.Config.TableName)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        foreach (var viewChanged in plan.AutomaticChanges.OfType<ViewChanged>())
        {
            var source = viewChanged.SourceTable;
            if (!kafkaRecreates.Contains(source)
                && !addedKafkaTables.Contains(source)
                && (existingQueues.Count == 0 || existingQueues.Contains(source)))
            {
                queues.Add(source);
            }
        }

        foreach (var added in plan.AutomaticChanges.OfType<ColumnAdded>())
        {
            if (KafkaMetaColumnFactory.IsKafkaMetaPath(added.FieldNumberPath))
            {
                var queue = newPlan.MaterializedViews
                    .FirstOrDefault(v => string.Equals(v.Config.TargetTable, added.TableName, StringComparison.OrdinalIgnoreCase))
                    ?.Config.SourceTable;
                if (!string.IsNullOrWhiteSpace(queue)
                    && !kafkaRecreates.Contains(queue)
                    && !addedKafkaTables.Contains(queue)
                    && (existingQueues.Count == 0 || existingQueues.Contains(queue)))
                {
                    queues.Add(queue);
                }
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
