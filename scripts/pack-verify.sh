#!/usr/bin/env bash
# Pack ClickHouseSchemaGen NuGet packages to artifacts/nuget and smoke-test restore + codegen.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${ROOT}/artifacts/nuget"
export DOTNET_ROOT="${DOTNET_ROOT:-$HOME/.dotnet}"
export PATH="${DOTNET_ROOT}:${PATH}"

mkdir -p "${OUT}"
rm -f "${OUT}"/ClickHouseSchemaGen*.nupkg "${OUT}"/ClickHouseSchemaGen*.snupkg

echo "==> Build (Release)"
dotnet build "${ROOT}/tools/ClickHouseSchemaGen/ClickHouseSchemaGen.csproj" -c Release --nologo
dotnet build "${ROOT}/tools/ClickHouseSchemaGen.Tasks/ClickHouseSchemaGen.Tasks.csproj" -c Release --nologo
dotnet build "${ROOT}/tools/ClickHouseSchemaGen.Cli/ClickHouseSchemaGen.Cli.csproj" -c Release --nologo
dotnet build "${ROOT}/tools/ClickHouseSchemaGen.Migrator/ClickHouseSchemaGen.Migrator.csproj" -c Release --nologo

# --no-build matches CI that packs after a separate build; Tasks must not invoke Build (NETSDK1085).
echo "==> Pack (--no-build)"
dotnet pack "${ROOT}/tools/ClickHouseSchemaGen/ClickHouseSchemaGen.csproj" -c Release -o "${OUT}" --no-build --nologo
dotnet pack "${ROOT}/tools/ClickHouseSchemaGen.Tasks/ClickHouseSchemaGen.Tasks.csproj" -c Release -o "${OUT}" --no-build --nologo
dotnet pack "${ROOT}/tools/ClickHouseSchemaGen.Cli/ClickHouseSchemaGen.Cli.csproj" -c Release -o "${OUT}" --no-build --nologo
dotnet pack "${ROOT}/tools/ClickHouseSchemaGen.Migrator/ClickHouseSchemaGen.Migrator.csproj" -c Release -o "${OUT}" --no-build --nologo

echo "==> Packages:"
ls -1 "${OUT}"/ClickHouseSchemaGen*.nupkg

echo "==> In-repo Contracts codegen (ProjectReference / local Tasks)"
dotnet build "${ROOT}/src/Sandbox.Contracts/Sandbox.Contracts.csproj" -c Release --nologo

echo "==> NuGet smoke: restore Tasks package + generate"
dotnet build "${ROOT}/samples/NuGetCodegenSmoke/NuGetCodegenSmoke.csproj" -c Release --nologo
test -f "${ROOT}/samples/NuGetCodegenSmoke/generated/01_orders_queue.sql"
echo "Generated: samples/NuGetCodegenSmoke/generated/01_orders_queue.sql"

echo "==> Unit tests"
dotnet test "${ROOT}/tests/ClickHouseSchemaGen.UnitTests/ClickHouseSchemaGen.UnitTests.csproj" -c Release --nologo

echo "==> pack-verify OK"
