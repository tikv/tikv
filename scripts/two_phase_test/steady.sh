#!/bin/bash
# Steady workload: 4 concurrent workers doing full-table scans with slight
# variations to defeat the coprocessor result cache.
# Run for 12+ minutes before firing spike.sh to build RuTracker history.

WORKER_ID=${1:-0}
COUNT=0
while true; do
  # Small random offset defeats TiDB coprocessor result cache
  OFFSET=$((RANDOM % 1000))
  mysql -h 127.0.0.1 -P 4000 -u root bench -e \
    "SET RESOURCE GROUP steady; SELECT COUNT(*), SUM(LENGTH(pad)) FROM t WHERE id > ${OFFSET};" 2>/dev/null
  COUNT=$((COUNT+1))
  if (( COUNT % 10 == 0 )); then echo "steady[$WORKER_ID]: $COUNT queries sent"; fi
done
