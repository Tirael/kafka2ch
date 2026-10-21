namespace ClickHouseSchemaGen.Migration;

internal sealed class AtomicFileSet
{
    private readonly List<(string Path, string Content)> _files = [];

    public void Add(string path, string content) => _files.Add((Path.GetFullPath(path), content));

    public void Commit()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var createdDirectories = new List<string>();
        var temporaries = new List<string>();
        var backups = new List<(string Path, string Backup)>();
        var placed = new List<string>();
        try
        {
            foreach (var (path, content) in _files)
            {
                CreateDirectory(Path.GetDirectoryName(path)!, createdDirectories);
                var temporary = $"{path}.tmp-{suffix}";
                temporaries.Add(temporary);
                File.WriteAllText(temporary, content);
            }

            for (var i = 0; i < _files.Count; i++)
            {
                var path = _files[i].Path;
                if (File.Exists(path))
                {
                    var backup = $"{path}.bak-{suffix}";
                    File.Move(path, backup);
                    backups.Add((path, backup));
                }

                File.Move(temporaries[i], path);
                placed.Add(path);
            }
        }
        catch
        {
            Rollback(placed, backups, temporaries, createdDirectories);
            throw;
        }

        foreach (var (_, backup) in backups)
            File.Delete(backup);
    }

    private static void Rollback(
        List<string> placed,
        List<(string Path, string Backup)> backups,
        List<string> temporaries,
        List<string> createdDirectories)
    {
        foreach (var path in Enumerable.Reverse(placed))
            File.Delete(path);

        foreach (var (path, backup) in Enumerable.Reverse(backups))
            File.Move(backup, path);

        foreach (var temporary in temporaries.Where(File.Exists))
            File.Delete(temporary);

        foreach (var directory in Enumerable.Reverse(createdDirectories))
        {
            if (Directory.Exists(directory) && !Directory.EnumerateFileSystemEntries(directory).Any())
                Directory.Delete(directory);
        }
    }

    private static void CreateDirectory(string directory, List<string> createdDirectories)
    {
        var missing = new Stack<string>();
        for (var current = directory; !Directory.Exists(current); current = Path.GetDirectoryName(current)!)
            missing.Push(current);

        foreach (var path in missing)
        {
            Directory.CreateDirectory(path);
            createdDirectories.Add(path);
        }
    }
}
