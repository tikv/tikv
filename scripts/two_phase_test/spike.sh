#!/bin/bash
# Spike workload: 50 concurrent full-table scans from 'spike' resource group.
# Each query uses a unique OFFSET to defeat TiDB's coprocessor result cache.
# Fire this after steady group has built 12+ minutes of history.
echo "=== SPIKE START $(date +%T) ==="
for i in $(seq 1 50); do
  OFFSET=$((i * 1000 + RANDOM % 500))
  mysql -h 127.0.0.1 -P 4000 -u root bench -e \
    "SET RESOURCE GROUP spike; SELECT COUNT(*), SUM(LENGTH(pad)) FROM t WHERE id > ${OFFSET};" 2>/dev/null &
done
wait
echo "=== SPIKE DONE $(date +%T) ==="
