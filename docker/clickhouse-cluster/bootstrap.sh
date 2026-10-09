#!/bin/sh
set -eu

CLUSTER="${CLICKHOUSE_CLUSTER:-kafka2ch}"
INIT_DIR="${INIT_DIR:-/cluster/init}"
DATABASE="${CLICKHOUSE_DATABASE:-default}"

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

history_db="$DATABASE"
if [ "$(ch --query "EXISTS TABLE ${DATABASE}.schema_migrations")" = "1" ] \
    && [ "$(ch --query "SELECT count() FROM ${DATABASE}.schema_migrations WHERE kind = 'init'")" -gt 0 ]; then
    echo "Init scripts already applied; skipping."
    exit 0
fi

first=1
for script in $(ls "${INIT_DIR}"/*.sql | sort); do
    echo "Applying $(basename "${script}")"
    if [ "${first}" -eq 1 ]; then
        ch --multiquery < "${script}"
        first=0
    else
        ch --database "${DATABASE}" --multiquery < "${script}"
    fi
done

echo "Cluster '${CLUSTER}' initialized (database=${DATABASE})."
