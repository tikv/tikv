#!/bin/bash
# One-time setup: create bench database, table (500K rows), and resource groups.
# Safe to re-run; uses IF NOT EXISTS / INSERT IGNORE.
set -e

echo "Creating schema..."
mysql -h 127.0.0.1 -P 4000 -u root -e "
CREATE DATABASE IF NOT EXISTS bench;
"
mysql -h 127.0.0.1 -P 4000 -u root bench -e "
CREATE TABLE IF NOT EXISTS t (id INT PRIMARY KEY AUTO_INCREMENT, pad VARCHAR(500));
CREATE RESOURCE GROUP IF NOT EXISTS steady RU_PER_SEC=UNLIMITED PRIORITY=HIGH BURSTABLE;
CREATE RESOURCE GROUP IF NOT EXISTS spike  RU_PER_SEC=UNLIMITED PRIORITY=HIGH BURSTABLE;
"

ROW_COUNT=$(mysql -h 127.0.0.1 -P 4000 -u root bench -sNe "SELECT COUNT(*) FROM t")
if [ "$ROW_COUNT" -lt 500000 ]; then
  echo "Inserting 500K rows (current: $ROW_COUNT)..."
  python3 "$(dirname "$0")/insert_data.py"
else
  echo "Table already has $ROW_COUNT rows, skipping insert."
fi
echo "Setup complete."
