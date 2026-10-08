# Tool Playbook

Run commands from the repository root. Select the smallest command that can prove the change.

## Inspect and edit

- Use file search and content search to locate projects, call sites, tests, and configuration before editing.
- Read the complete relevant method/type and its neighboring tests; do not infer behavior from a search result alone.
- Apply focused patches and preserve unrelated user changes.
- Check diagnostics for edited files after substantive edits.
- Use Git diff/status only for inspection unless the user explicitly requests a Git write operation.

## Restore and build

Restore when dependencies or assets are missing:

```bash
dotnet restore kafka2ch.sln
```

Build the affected project during iteration:

```bash
dotnet build tools/ClickHouseSchemaGen/ClickHouseSchemaGen.csproj --no-restore
dotnet build src/Sandbox.App/Sandbox.App.csproj --no-restore
```

Build the solution before handoff for a cross-project or MSBuild change:

```bash
dotnet build kafka2ch.sln --no-restore
```

Do not add `--no-restore` when package references changed or restore has not succeeded in the current workspace.

## Test

Run one relevant test class or namespace first:

```bash
dotnet test tests/ClickHouseSchemaGen.UnitTests/ClickHouseSchemaGen.UnitTests.csproj \
  --filter "FullyQualifiedName~MergeTreeTableGeneratorTests"
```

Run the complete generator suite when shared mapping, generation, validation, or test infrastructure changed:

```bash
dotnet test tests/ClickHouseSchemaGen.UnitTests/ClickHouseSchemaGen.UnitTests.csproj
dotnet test tests/ClickHouseSchemaGen.IntegrationTests/ClickHouseSchemaGen.IntegrationTests.csproj
```

Integration tests require a working Docker daemon because they use Testcontainers. Do not replace a failed real integration test with mocks merely to make verification green.

For diagnosis, increase verbosity before changing code based on incomplete output:

```bash
dotnet test tests/ClickHouseSchemaGen.UnitTests/ClickHouseSchemaGen.UnitTests.csproj \
  --filter "FullyQualifiedName~TestName" \
  --logger "console;verbosity=detailed"
```

## Format

Check formatting without rewriting unrelated files:

```bash
dotnet format kafka2ch.sln --verify-no-changes --no-restore
```

If formatting is required, target changed files or the smallest project possible. Inspect the diff afterward.

## Protobuf and ClickHouse code generation

After editing source schemas, code-generation config, or the generator, run:

```bash
dotnet build src/Sandbox.Contracts/Sandbox.Contracts.csproj
```

This intentionally updates:

- `docker/clickhouse/format_schemas`
- `docker/clickhouse/init/01_orders_queue.sql`
- `docker/clickhouse/init/02_shipments_queue.sql`
- `docker/clickhouse/init/03_pipeline.sql`

If `schema.snapshot.json` exists and the plan drifted, the build fails. Generate a migration:

```bash
dotnet build tools/ClickHouseSchemaGen.Cli/ClickHouseSchemaGen.Cli.csproj
dotnet exec tools/ClickHouseSchemaGen.Cli/bin/Debug/net8.0/ClickHouseSchemaGen.Cli.dll \
  migrate --config src/Sandbox.Contracts/clickhouse.codegen.json --name <change_name>
```

Bootstrap snapshot (once):

```bash
dotnet exec tools/ClickHouseSchemaGen.Cli/bin/Debug/net8.0/ClickHouseSchemaGen.Cli.dll \
  migrate init --config src/Sandbox.Contracts/clickhouse.codegen.json
```

Apply pending SQL migrations:

```bash
dotnet exec tools/ClickHouseSchemaGen.Migrator/bin/Debug/net8.0/ClickHouseSchemaGen.Migrator.dll \
  --migrations docker/clickhouse/migrations
```

Also generated/updated by migrate: `docker/clickhouse/init/schema.snapshot.json`, `00_schema_migrations.sql`, files under `docker/clickhouse/migrations/`.

Escape hatches:

```bash
dotnet build kafka2ch.sln -p:SkipClickHouseCodegen=true
dotnet build src/Sandbox.Contracts -p:SkipClickHouseSnapshotCheck=true
```

Do not use the escape hatch as final verification for a schema, generator, or MSBuild integration change.

## Docker and end-to-end checks

Check service state before starting or recreating containers:

```bash
docker compose ps
```

Start the full stack only when end-to-end behavior must be verified:

```bash
docker compose up -d --build
./scripts/verify-pipeline.sh
```

Use targeted logs:

```bash
docker compose logs sandbox-app
docker compose logs clickhouse
```

Never run `docker compose down -v` unless the user explicitly approves deleting local ClickHouse data. Remember that changed init SQL is applied only to a fresh ClickHouse volume.

## Dependencies

Before adding or upgrading a NuGet package:

1. Confirm the BCL, framework, or an existing package does not already provide the capability.
2. Check current compatibility with `net8.0`.
3. Check known vulnerabilities, maintenance health, transitive risk, and license.
4. Pin an intentional version in the affected project and run restore, build, and relevant tests.
5. Explain why the dependency is needed in the handoff.

## Failure handling

- Separate source failures from environment failures such as unavailable Docker, package feeds, credentials, or ports.
- Read the first causal compiler/test error, not only the final summary.
- Reproduce before fixing when practical.
- Do not weaken assertions, skip tests, suppress warnings, or bypass code generation to hide a failure.
- Report the exact command and blocker when verification cannot complete.
