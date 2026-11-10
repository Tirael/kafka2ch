#!/usr/bin/env bash
# Obligatory data-quality gate (P1-2): bring up a compose topology (optional),
# wait until orders has rows, run verify-cluster.sh or verify-single.sh, tear down.
#
# Post-deploy against an already-running stack:
#   VERIFY_ASSUME_STACK=1 ./scripts/run-verify-gate.sh
#   VERIFY_ASSUME_STACK=1 VERIFY_TOPOLOGY=single ./scripts/run-verify-gate.sh
#
# Nightly / compose smoke (default): starts cluster compose, verifies, tears down.
# See .github/workflows/verify-data-gate.yml and docs/migrate-runbook.md.
set -euo pipefail

cd "$(dirname "$0")/.."

ENV_FILE=${ENV_FILE:-.env.example}
TOPOLOGY=${VERIFY_TOPOLOGY:-cluster}
ASSUME_STACK=${VERIFY_ASSUME_STACK:-0}
KEEP_STACK=${VERIFY_KEEP_STACK:-0}
WAIT_SECONDS=${VERIFY_WAIT_SECONDS:-180}
PASSWORD=$(grep '^CLICKHOUSE_PASSWORD=' "$ENV_FILE" | cut -d= -f2-)
PASSWORD=${PASSWORD:-sandbox}

case "$TOPOLOGY" in
  cluster)
    COMPOSE_FILES=( -f docker-compose.yml -f docker-compose.cluster.yml )
    VERIFY_SCRIPT=./scripts/verify-cluster.sh
    ;;
  single)
    COMPOSE_FILES=( -f docker-compose.yml )
    VERIFY_SCRIPT=./scripts/verify-single.sh
    ;;
  *)
    echo "FAIL: VERIFY_TOPOLOGY must be cluster or single (got: ${TOPOLOGY})" >&2
    exit 1
    ;;
esac

COMPOSE=( docker compose --env-file "$ENV_FILE" "${COMPOSE_FILES[@]}" )

log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }

ch() {
  docker exec clickhouse clickhouse-client --password "$PASSWORD" --query "$1"
}

wait_for_rows() {
  local deadline=$(( SECONDS + WAIT_SECONDS )) rows=0
  log "waiting up to ${WAIT_SECONDS}s for rows in orders"
  while (( SECONDS < deadline )); do
    if rows=$(ch "SELECT count() FROM orders" 2>/dev/null); then
      if [[ "$rows" =~ ^[0-9]+$ ]] && (( rows > 0 )); then
        log "orders has ${rows} row(s)"
        return 0
      fi
    fi
    sleep 5
  done
  echo "FAIL: no rows in orders within ${WAIT_SECONDS}s" >&2
  return 1
}

cleanup() {
  local status=$?
  if (( status != 0 )) && (( ASSUME_STACK == 0 )); then
    "${COMPOSE[@]}" logs --no-color --tail 200 clickhouse sandbox-app kafka 2>/dev/null || true
  fi
  if (( ASSUME_STACK == 0 )) && (( KEEP_STACK == 0 )); then
    log "tearing down compose stack"
    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  fi
  exit "$status"
}

main() {
  export CLICKHOUSE_PASSWORD="$PASSWORD"

  if (( ASSUME_STACK == 0 )); then
    trap cleanup EXIT
    log "starting ${TOPOLOGY} stack"
    "${COMPOSE[@]}" up -d --build --wait
    # sandbox-app depends on migrate; --wait covers healthchecks / completed init.
    "${COMPOSE[@]}" up -d sandbox-app
  else
    log "using existing stack (VERIFY_ASSUME_STACK=1), topology=${TOPOLOGY}"
  fi

  wait_for_rows
  log "running ${VERIFY_SCRIPT}"
  "$VERIFY_SCRIPT"
  log "gate OK (${TOPOLOGY})"
}

main "$@"
