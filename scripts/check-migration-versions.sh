#!/usr/bin/env bash
# Fail when a migration added on this branch is not newer than every migration on the base branch,
# or when a changed schema.snapshot.json does not descend (parentChecksum chain) from the base snapshot.
# Usage: scripts/check-migration-versions.sh [base-ref]   (default: origin/$GITHUB_BASE_REF or origin/main)
set -euo pipefail

BASE_REF="${1:-origin/${GITHUB_BASE_REF:-main}}"
MIGRATION_PATTERN='(^|/)migrations[^/]*/[0-9]{14}_[^/]+\.sql$'
SNAPSHOT_PATTERN='(^|/)schema\.snapshot\.json$'

if ! git rev-parse --verify --quiet "${BASE_REF}^{commit}" > /dev/null; then
  echo "Base ref '${BASE_REF}' not found; fetch it first (git fetch origin <branch>)." >&2
  exit 2
fi

MERGE_BASE="$(git merge-base "${BASE_REF}" HEAD)"
errors=0

report() {
  local file="$1" message="$2"
  if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
    echo "::error file=${file}::${message//$'\n'/%0A}"
  fi
  echo "ERROR: ${message}" >&2
  errors=$((errors + 1))
}

version_of() {
  local name
  name="$(basename "$1")"
  printf '%s' "${name%%_*}"
}

parent_checksum_of() {
  sed -n 's/^[[:space:]]*"parentChecksum":[[:space:]]*"\([0-9a-fA-F]*\)".*/\1/p' | head -n 1
}

base_max=""
while IFS= read -r path; do
  version="$(version_of "${path}")"
  if [[ "${version}" > "${base_max}" ]]; then
    base_max="${version}"
  fi
done < <(git ls-tree -r --name-only "${BASE_REF}" | grep -E "${MIGRATION_PATTERN}" || true)

added=0
while IFS= read -r path; do
  added=$((added + 1))
  version="$(version_of "${path}")"
  if [[ -n "${base_max}" && ! "${version}" > "${base_max}" ]]; then
    report "${path}" "Migration '${path}' has version ${version}, which is not newer than the latest migration on ${BASE_REF} (${base_max}).
The Migrator rejects out-of-order migrations. Rename it to a fresh timestamp (for every topology config that has it):
  clickhouse-schema-gen migrate rebase --config <codegen.json> --name $(basename "${path}")"
  fi
done < <(git diff --name-only --diff-filter=AR "${MERGE_BASE}" HEAD | grep -E "${MIGRATION_PATTERN}" || true)

while IFS= read -r path; do
  git cat-file -e "${BASE_REF}:${path}" 2> /dev/null || continue
  head_parent="$(git show "HEAD:${path}" | parent_checksum_of)"
  [[ -n "${head_parent}" ]] || continue

  base_checksum="$(git show "${BASE_REF}:${path}" | sha256sum | cut -c1-64)"
  declare -A parents=()
  while IFS= read -r commit; do
    git cat-file -e "${commit}:${path}" 2> /dev/null || continue
    parents["$(git show "${commit}:${path}" | sha256sum | cut -c1-64)"]="$(git show "${commit}:${path}" | parent_checksum_of)"
  done < <(git rev-list "${BASE_REF}..HEAD" -- "${path}")

  current="${head_parent}"
  steps=0
  while [[ "${current}" != "${base_checksum}" ]]; do
    if [[ -z "${current}" || -z "${parents[${current}]+set}" || ${steps} -gt 1000 ]]; then
      report "${path}" "Snapshot '${path}' does not descend from ${BASE_REF}: its parentChecksum chain does not reach ${base_checksum}.
The migration was generated over an outdated snapshot. Take the snapshot from ${BASE_REF}, delete the branch migration and regenerate it (README: schema.snapshot.json conflicts)."
      break
    fi
    current="${parents[${current}]}"
    steps=$((steps + 1))
  done
  unset parents
done < <(git diff --name-only --diff-filter=M "${MERGE_BASE}" HEAD | grep -E "${SNAPSHOT_PATTERN}" || true)

if [[ ${errors} -gt 0 ]]; then
  exit 1
fi
echo "Migration versions OK: ${added} added migration(s) newer than ${BASE_REF} (${base_max:-none}); snapshot chains descend from ${BASE_REF}."
