#!/bin/bash
# Monitor slow query latency per resource group in real time.
# Run in a separate terminal alongside steady.sh and spike.sh.
while true; do
  echo "--- $(date +%T) ---"
  mysql -h 127.0.0.1 -P 4000 -u root bench -e "
    SELECT Resource_group                   AS rg,
           COUNT(*)                         AS queries,
           ROUND(AVG(Query_time)*1000, 1)   AS avg_ms,
           ROUND(MAX(Query_time)*1000, 1)   AS max_ms
    FROM   information_schema.slow_query
    WHERE  Time > NOW() - INTERVAL 2 SECOND
      AND  Resource_group IN ('steady','spike')
    GROUP  BY Resource_group;" 2>/dev/null
  sleep 2
done
