#!/bin/sh
# Lance le worker edge ; si des arguments sont passés, les exécute à la place (ex. pytest, bash).
set -e
if [ "$#" -gt 0 ]; then
  exec "$@"
fi
exec airflow edge worker -q "${EDGE_QUEUE:-laptop}" -c "${EDGE_CONCURRENCY:-2}" -H "${EDGE_HOSTNAME:-laptop-dev}"
