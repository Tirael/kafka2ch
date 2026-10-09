#!/bin/sh
# Applies the generated ON CLUSTER init scripts once, from a single node.
# docker-entrypoint-initdb.d is not used: it would run the same ON CLUSTER DDL from every node.
set -eu

CLUSTER="${CLICKHOUSE_CLUSTER:-kafka2ch}"
INIT_DIR="${INIT_DIR:-/cluster/init}"

ch() {
    clickhouse-client --host "${CLICKHOUSE_HOST:-clickhouse}" --password "${CLICKHOUSE_PASSWORD}" "$@"
}

echo "Waiting for every replica of cluster '${CLUSTER}'..."
attempt=0
until ch --query "SELECT count() FROM clusterAllReplicas('${CLUSTER}', system.one)" >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ "${attempt}" -ge 90 ]; then
        echo "Cluster '${CLUSTER}' is not reachable." >&2
        exit 1
    fi
    sleep 2
done

if [ "$(ch --query "EXISTS TABLE schema_migrations")" = "1" ] \
    && [ "$(ch --query "SELECT count() FROM schema_migrations WHERE kind = 'init'")" -gt 0 ]; then
    echo "Init scripts already applied; skipping."
    exit 0
fi

for script in $(ls "${INIT_DIR}"/*.sql | sort); do
    echo "Applying $(basename "${script}")"
    ch --multiquery < "${script}"
done

echo "Cluster '${CLUSTER}' initialized."
