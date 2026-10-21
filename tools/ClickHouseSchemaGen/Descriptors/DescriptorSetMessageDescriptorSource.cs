namespace ClickHouseSchemaGen.Descriptors;

public sealed class DescriptorSetMessageDescriptorSource : IMessageDescriptorSource
{
    private static readonly IReadOnlyDictionary<string, FileDescriptor> BuiltInFiles = new[]
        {
            Google.Protobuf.WellKnownTypes.AnyReflection.Descriptor,
            Google.Protobuf.WellKnownTypes.ApiReflection.Descriptor,
            Google.Protobuf.WellKnownTypes.DurationReflection.Descriptor,
            Google.Protobuf.WellKnownTypes.EmptyReflection.Descriptor,
            Google.Protobuf.WellKnownTypes.FieldMaskReflection.Descriptor,
            Google.Protobuf.WellKnownTypes.SourceContextReflection.Descriptor,
            Google.Protobuf.WellKnownTypes.StructReflection.Descriptor,
            Google.Protobuf.WellKnownTypes.TimestampReflection.Descriptor,
            Google.Protobuf.WellKnownTypes.TypeReflection.Descriptor,
            Google.Protobuf.WellKnownTypes.WrappersReflection.Descriptor,
            DescriptorReflection.Descriptor
        }
        .ToDictionary(file => file.Name, StringComparer.Ordinal);

    private readonly string _sourceName;
    private readonly Dictionary<string, MessageDescriptor> _byFullName = new(StringComparer.Ordinal);
    private readonly Dictionary<string, MessageDescriptor> _byClrName = new(StringComparer.Ordinal);

    private DescriptorSetMessageDescriptorSource(string sourceName, IReadOnlyList<FileDescriptor> files)
    {
        _sourceName = sourceName;
        Files = files;
        foreach (var file in files)
        {
            var clrNamespace = ClrNamespace(file);
            foreach (var message in file.MessageTypes)
                Index(message, string.IsNullOrEmpty(clrNamespace) ? message.Name : $"{clrNamespace}.{message.Name}");
        }
    }

    public IReadOnlyList<FileDescriptor> Files { get; }

    public static DescriptorSetMessageDescriptorSource Load(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        var fullPath = Path.GetFullPath(path);
        if (!File.Exists(fullPath))
            throw new FileNotFoundException($"Descriptor set not found: {fullPath}", fullPath);

        FileDescriptorSet set;
        try
        {
            set = FileDescriptorSet.Parser.ParseFrom(File.ReadAllBytes(fullPath));
        }
        catch (InvalidProtocolBufferException exception)
        {
            throw new InvalidOperationException(
                $"'{fullPath}' is not a FileDescriptorSet (protoc --descriptor_set_out / buf build -o): {exception.Message}",
                exception);
        }

        return FromFileDescriptorSet(set, fullPath);
    }

    public static DescriptorSetMessageDescriptorSource FromFileDescriptorSet(FileDescriptorSet set, string sourceName)
    {
        ArgumentNullException.ThrowIfNull(set);
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceName);

        var files = new Dictionary<string, FileDescriptorProto>(StringComparer.Ordinal);
        foreach (var file in set.File)
        {
            if (!files.TryAdd(file.Name, file))
                throw new InvalidOperationException($"Descriptor set '{sourceName}' contains '{file.Name}' more than once.");
        }

        var ordered = new List<FileDescriptorProto>();
        var visited = new HashSet<string>(StringComparer.Ordinal);
        foreach (var file in set.File)
            AddWithDependencies(file, files, visited, [], ordered, sourceName);

        var built = FileDescriptor.BuildFromByteStrings(ordered.Select(file => file.ToByteString()));
        return new DescriptorSetMessageDescriptorSource(sourceName, built);
    }

    public MessageDescriptor Resolve(string messageType)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageType);

        var name = messageType.Trim();
        var typeName = name.Split(',', 2)[0].Trim();
        if (_byFullName.TryGetValue(name.TrimStart('.'), out var descriptor)
            || _byClrName.TryGetValue(typeName, out descriptor))
        {
            return descriptor;
        }

        throw new InvalidOperationException(
            $"Message type '{messageType}' was not found in descriptor set '{_sourceName}'. " +
            "Use the protobuf full name (package.Message) or the generated C# type name.");
    }

    private void Index(MessageDescriptor message, string clrName)
    {
        _byFullName.TryAdd(message.FullName, message);
        _byClrName.TryAdd(clrName, message);
        foreach (var nested in message.NestedTypes)
            Index(nested, $"{clrName}+Types+{nested.Name}");
    }

    private static void AddWithDependencies(
        FileDescriptorProto file,
        IReadOnlyDictionary<string, FileDescriptorProto> files,
        HashSet<string> visited,
        HashSet<string> inProgress,
        List<FileDescriptorProto> ordered,
        string sourceName)
    {
        if (visited.Contains(file.Name))
            return;
        if (!inProgress.Add(file.Name))
            throw new InvalidOperationException($"Descriptor set '{sourceName}' has an import cycle through '{file.Name}'.");

        foreach (var dependency in file.Dependency)
        {
            var dependencyFile = files.GetValueOrDefault(dependency) ?? BuiltInFile(dependency)
                ?? throw new InvalidOperationException(
                    $"Descriptor set '{sourceName}': '{file.Name}' imports '{dependency}', which is not in the set. " +
                    "Regenerate it with 'protoc --include_imports --descriptor_set_out=...' or 'buf build -o ...'.");
            AddWithDependencies(dependencyFile, files, visited, inProgress, ordered, sourceName);
        }

        inProgress.Remove(file.Name);
        visited.Add(file.Name);
        ordered.Add(file);
    }

    private static FileDescriptorProto? BuiltInFile(string name) =>
        BuiltInFiles.TryGetValue(name, out var file)
            ? FileDescriptorProto.Parser.ParseFrom(file.SerializedData)
            : null;

    private static string ClrNamespace(FileDescriptor file)
    {
        var options = file.GetOptions();
        return options is { HasCsharpNamespace: true }
            ? options.CsharpNamespace
            : PascalCasePackage(file.Package);
    }

    private static string PascalCasePackage(string package)
    {
        var builder = new StringBuilder(package.Length);
        var capitalizeNext = true;
        foreach (var character in package)
        {
            if (char.IsAsciiLetterLower(character))
            {
                builder.Append(capitalizeNext ? char.ToUpperInvariant(character) : character);
                capitalizeNext = false;
            }
            else if (char.IsAsciiLetterUpper(character))
            {
                builder.Append(character);
                capitalizeNext = false;
            }
            else if (char.IsAsciiDigit(character))
            {
                builder.Append(character);
                capitalizeNext = true;
            }
            else
            {
                capitalizeNext = true;
                if (character == '.')
                    builder.Append(character);
            }
        }

        return builder.ToString();
    }
}
