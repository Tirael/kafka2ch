namespace ClickHouseSchemaGen.Shared;

/// <summary>
/// Renders ordered ClickHouse init scripts from a codegen config without reading docker/ artifacts.
/// </summary>
public static class InitScriptsFixture
{
    public static IReadOnlyList<(string FileName, string Sql)> Generate(
        string configPath,
        IReadOnlyList<MigrationFileInfo>? baselineMigrations = null)
    {
        var config = CodegenConfigLoader.Load(configPath);
        var cluster = ClusterDdl.For(config);
        var versionsSql = SchemaMigrationsScriptGenerator.Generate(baselineMigrations ?? [], cluster);
        var plan = SchemaGeneratorFactory.Create().BuildPlan(config);
        var rendered = SchemaPlanRenderer.RenderInitScripts(plan);

        var scripts = new List<(string FileName, string Sql)>
        {
            (SchemaMigrationsTable.DefaultScriptFileName, versionsSql)
        };

        foreach (var (outputPath, sql) in rendered
                     .OrderBy(pair => Path.GetFileName(pair.Key), StringComparer.Ordinal))
        {
            scripts.Add((Path.GetFileName(outputPath), sql));
        }

        return scripts;
    }
}
