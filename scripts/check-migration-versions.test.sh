#!/usr/bin/env bash
# Fixture tests for check-migration-versions.sh: builds throwaway git repos with main/feature branches.
set -euo pipefail

SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/check-migration-versions.sh"
WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT

failures=0

snapshot() {
  printf '{\n  "version": 1,\n  "parentChecksum": "%s",\n  "trailingSqlHash": "%s"\n}' "$1" "$2"
}

sha_of() {
  sha256sum "$1" | cut -c1-64
}

commit_all() {
  git add -A
  git commit -q -m "$1"
}

new_repo() {
  local repo="${WORK}/$1"
  git init -q "${repo}"
  cd "${repo}"
  git checkout -q -b main
  git config user.email test@example.com
  git config user.name test
  git config commit.gpgsign false
  git config core.fsmonitor false
  mkdir -p docker/clickhouse/migrations docker/clickhouse-cluster/migrations-replicated-db docker/clickhouse/init
  echo "SELECT 1;" > docker/clickhouse/migrations/20261001000000_initial.sql
  snapshot "" "s0" > docker/clickhouse/init/schema.snapshot.json
  commit_all "base"
}

main_adds_migration() {
  git checkout -q main
  echo "SELECT 2;" > docker/clickhouse/migrations/20261020000000_from_main.sql
  snapshot "$(sha_of docker/clickhouse/init/schema.snapshot.json)" "main" > docker/clickhouse/init/schema.snapshot.json
  commit_all "main migration"
}

run_check() {
  output="$("${SCRIPT}" main 2>&1)" && status=0 || status=$?
}

expect_pass() {
  run_check
  if [[ ${status} -ne 0 ]]; then
    echo "FAIL $1: expected success, got ${status}:"
    echo "${output}"
    failures=$((failures + 1))
  else
    echo "ok   $1"
  fi
}

expect_fail() {
  run_check
  if [[ ${status} -eq 0 || "${output}" != *"$2"* ]]; then
    echo "FAIL $1: expected failure containing '$2', got ${status}:"
    echo "${output}"
    failures=$((failures + 1))
  else
    echo "ok   $1"
  fi
}

new_repo newer-version
git checkout -q -b feature
echo "SELECT 3;" > docker/clickhouse/migrations/20261021000000_feature.sql
commit_all "feature"
expect_pass "migration newer than base passes"

new_repo stale-version
git checkout -q -b feature
echo "SELECT 3;" > docker/clickhouse/migrations/20261010000000_feature.sql
commit_all "feature"
main_adds_migration
git checkout -q feature
git merge -q --no-edit -X ours main > /dev/null
expect_fail "stale migration fails with rebase hint" "migrate rebase --config <codegen.json> --name 20261010000000_feature.sql"
git mv docker/clickhouse/migrations/20261010000000_feature.sql docker/clickhouse/migrations/20261021000000_feature.sql
commit_all "rebase"
expect_pass "rebased migration passes"

new_repo stale-in-other-directory
main_adds_migration
git checkout -q -b feature
echo "SELECT 3;" > docker/clickhouse-cluster/migrations-replicated-db/20261019000000_feature.sql
commit_all "feature"
expect_fail "stale migration in another directory fails" "20261019000000_feature.sql"

new_repo equal-version
main_adds_migration
git checkout -q -b feature
echo "SELECT 3;" > docker/clickhouse-cluster/migrations-replicated-db/20261020000000_feature.sql
commit_all "feature"
expect_fail "migration equal to base max fails" "20261020000000"

new_repo snapshot-over-stale-parent
base_sha="$(sha_of docker/clickhouse/init/schema.snapshot.json)"
git checkout -q -b feature
echo "SELECT 3;" > docker/clickhouse/migrations/20261021000000_feature.sql
snapshot "${base_sha}" "feature" > docker/clickhouse/init/schema.snapshot.json
commit_all "feature"
main_adds_migration
git checkout -q feature
git merge -q --no-edit -X ours main > /dev/null
expect_fail "snapshot generated over stale parent fails" "does not descend from main"
main_sha="$(git show main:docker/clickhouse/init/schema.snapshot.json | sha256sum | cut -c1-64)"
snapshot "${main_sha}" "feature-regenerated" > docker/clickhouse/init/schema.snapshot.json
commit_all "regenerate over main"
expect_pass "snapshot regenerated over main passes"

new_repo snapshot-chain-through-branch-commits
main_adds_migration
git checkout -q -b feature
snapshot "$(sha_of docker/clickhouse/init/schema.snapshot.json)" "feature-1" > docker/clickhouse/init/schema.snapshot.json
echo "SELECT 3;" > docker/clickhouse/migrations/20261021000000_feature_one.sql
commit_all "feature one"
snapshot "$(sha_of docker/clickhouse/init/schema.snapshot.json)" "feature-2" > docker/clickhouse/init/schema.snapshot.json
echo "SELECT 4;" > docker/clickhouse/migrations/20261021000001_feature_two.sql
commit_all "feature two"
expect_pass "snapshot chain through branch commits passes"

new_repo snapshot-baseline-reset
main_adds_migration
git checkout -q -b feature
snapshot "" "reinit" > docker/clickhouse/init/schema.snapshot.json
commit_all "re-init baseline"
expect_pass "snapshot without parentChecksum (baseline) is skipped"

if [[ ${failures} -gt 0 ]]; then
  echo "${failures} check-migration-versions test(s) failed"
  exit 1
fi
echo "All check-migration-versions tests passed"
