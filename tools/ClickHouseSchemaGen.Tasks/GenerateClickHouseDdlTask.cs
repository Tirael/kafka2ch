using FluentValidation;
using Microsoft.Build.Framework;
using MsBuildTask = Microsoft.Build.Utilities.Task;

namespace ClickHouseSchemaGen.Tasks;

/// <summary>
/// Runs ClickHouse DDL codegen inside the MSBuild/`dotnet` process.
/// Avoids spawning a separate <c>dotnet …Cli.dll</c> process that AppLocker/SRP
/// often blocks in closed corporate environments ("заблокирована групповой политикой").
/// </summary>
public sealed class GenerateClickHouseDdlTask : MsBuildTask
{
    [Required]
    public string ConfigPath { get; set; } = "";

    public override bool Execute()
    {
        try
        {
            new ClickHouseSchemaGenerator(new DenormalizationPlanner()).GenerateFromConfigFile(ConfigPath);
            Log.LogMessage(MessageImportance.High, "Generated ClickHouse DDL from '{0}'.", ConfigPath);
            return true;
        }
        catch (ValidationException exception)
        {
            Log.LogError("Invalid codegen config '{0}':", ConfigPath);
            foreach (var error in exception.Errors)
                Log.LogError("  {0}: {1}", error.PropertyName, error.ErrorMessage);

            return false;
        }
        catch (Exception exception)
        {
            Log.LogErrorFromException(exception, showStackTrace: true);
            return false;
        }
    }
}
