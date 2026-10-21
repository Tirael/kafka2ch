#!/usr/bin/env bash
# Regenerate the FileDescriptorSet fixture for tests/ClickHouseSchemaGen.TestContracts/protos
# with protoc from the Grpc.Tools NuGet package (restore the solution first) or PROTOC=/path/to/protoc.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONTRACTS="${ROOT}/tests/ClickHouseSchemaGen.TestContracts"
OUT="${CONTRACTS}/Fixtures/test-contracts.binpb"
NUGET_ROOT="${NUGET_PACKAGES:-$HOME/.nuget/packages}"
GRPC_TOOLS_VERSION="$(sed -n 's/.*PackageVersion Include="Grpc.Tools" Version="\([^"]*\)".*/\1/p' "${ROOT}/Directory.Packages.props")"
GRPC_TOOLS="${NUGET_ROOT}/grpc.tools/${GRPC_TOOLS_VERSION}"

case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) PLATFORM=linux_x64 ;;
  Linux-aarch64 | Linux-arm64) PLATFORM=linux_arm64 ;;
  Darwin-*) PLATFORM=macosx_x64 ;;
  MINGW* | MSYS* | CYGWIN*) PLATFORM=windows_x64 ;;
  *) echo "Unsupported platform $(uname -s)-$(uname -m); set PROTOC." >&2; exit 1 ;;
esac

PROTOC="${PROTOC:-${GRPC_TOOLS}/tools/${PLATFORM}/protoc}"
WKT_INCLUDE="${GRPC_TOOLS}/build/native/include"

if [[ ! -x "${PROTOC}" ]]; then
  echo "protoc not found at ${PROTOC}. Run 'dotnet restore kafka2ch.sln' or set PROTOC." >&2
  exit 1
fi

cd "${CONTRACTS}/protos"
mapfile -t PROTOS < <(find . -name '*.proto' | sed 's|^\./||' | LC_ALL=C sort)

"${PROTOC}" \
  --proto_path=. \
  --proto_path="${WKT_INCLUDE}" \
  --include_imports \
  --descriptor_set_out="${OUT}" \
  "${PROTOS[@]}"

echo "Wrote ${OUT}"
