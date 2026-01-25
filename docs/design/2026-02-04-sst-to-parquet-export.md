# Design: BR SST to Parquet Export with Optional Iceberg Manifest

## Summary
This document describes the TiKV-side pipeline that converts BR backup SSTs into
Parquet files and optionally emits Iceberg-compatible JSON manifests. The
conversion runs offline via `tikv-ctl`, reads `backupmeta` from the backup
storage, and writes Parquet data to a separate output storage prefix. The
pipeline is restartable via a checkpoint directory in the output prefix.

## Goals
- Export BR transactional backups (default CF + write CF short values) into Parquet files.
- Preserve table schema using `backupmeta` and TiDB row decoding.
- Support bounded parallel export for large backups.
- Provide optional Iceberg manifest output for lakehouse ingestion.
- Expose progress and health via structured logs and Prometheus metrics.
- Allow safe resume after interruption via checkpoints.

## Non-goals
- Distributed conversion across TiKV nodes.
- Raw KV backup support.
- Encrypted backupmeta index support.
- Schema evolution or transformation during export.

## CLI Surface
`tikv-ctl br-parquet-export` accepts base64 `StorageBackend` configs.

Required flags:
- `--input-storage-base64` base64-encoded storage backend pointing at the BR SST backup
- `--output-storage-base64` base64-encoded storage backend for Parquet output

Optional flags:
- `--output-prefix` (default `parquet`) prefix under the output storage
- `--row-group-size` (default `8192`)
- `--sst-concurrency` number of SST files to export concurrently (defaults to available CPU count)
- `--compression` (default `snappy`, supports `snappy|zstd|gzip|brotli|lz4|lz4raw|none`)
- `--filter` table filter rules (same syntax as BR `--filter`, repeatable; supports `!` and `@file`)
- `--table-ids` comma-separated list of physical table IDs
- `--use-checkpoint` (default `true`, set `--use-checkpoint=false` to disable resume)
- `--write-iceberg-manifest` plus `--iceberg-warehouse`, `--iceberg-namespace`, `--iceberg-table`
- `--iceberg-manifest-prefix` (default `manifest`)

## Data Flow
1. `tikv-ctl br-parquet-export` receives base64 `StorageBackend` configs for
   input (BR backup) and output (Parquet).
2. The exporter loads `backupmeta` (v1 or v2 with schema/file index) and expands
   index references into full schema and file lists. The `backupmeta` path is
   fixed to `backupmeta` at the root of the input storage.
3. Tables are filtered using BR syntax and optional explicit table IDs.
4. SSTs are grouped into per-file tasks (tables are derived from table metas
   when present, or inferred from key ranges when not present).
5. Checkpoint state is loaded from `<output-prefix>/_checkpoint` (unless
   `--use-checkpoint=false`). Already-exported Parquet objects are skipped.
6. For each SST (in a bounded worker pool controlled by `--sst-concurrency`):
   - Download once to a local temp file.
   - Iterate keys, strip the `z` data prefix, remove MVCC commit ts, decode
     memcomparable keys, and skip non-record keys (indexes).
   - For write CF SSTs, emit rows only when the write record is `Put` and has a
     short value; otherwise skip and rely on default CF.
   - Decode row format v1/v2 into datums, fill clustered PK columns from the
     handle when missing, and write to a Parquet file.
   - Upload the Parquet object to output storage.
   - Record a checkpoint entry for the uploaded object.
7. (Optional) Emit Iceberg JSON manifest entries for all Parquet files.

## Parallelism
- Concurrency is per-SST task. Each SST is downloaded once and reused for all
  matching tables within that SST.
- A bounded Rayon thread pool enforces `--sst-concurrency` and avoids runaway
  memory/IO usage.
- Uploads happen within the same tasks using the Tokio runtime handle.

## Checkpoint & Resume
Checkpoint data lives under:
`{output-prefix}/_checkpoint/`

Layout:
- `meta.json` (checkpoint metadata)
- `entries/` (one JSON entry per Parquet object)

The checkpoint config hash includes:
- `backupmeta` content digest
- `start_version` and `end_version`
- `--filter` rules and `--table-ids`
- `--row-group-size` and `--compression`

On startup, the exporter loads checkpoint metadata and entries and logs the
number of reusable outputs. If the config hash mismatches, the export fails and
asks the user to remove the checkpoint directory or use a new output prefix.
Disable checkpoints with `--use-checkpoint=false` (this ignores checkpoint data
but does not delete it).

## Output Layout
Parquet objects are written to:
`{output_prefix}/{db}/{table}/{sst_stem}.parquet`

Name sanitization:
- Database/table names are normalized to `[A-Za-z0-9_]`.
- If sanitization changes the name, a short hash suffix is appended to prevent
  collisions (for example, `table-name` and `table_name` remain distinct).

## Schema Mapping
- Each Parquet schema includes `_tidb_table_id` and `_tidb_handle` columns.
- Handles use integer or common-handle bytes depending on table schema.
- TiDB column types are mapped to Parquet types:
  - Signed integers -> `INT64`
  - Unsigned integers -> `UTF8` string
  - Floats -> `DOUBLE`
  - String-like, JSON, time, decimal, enum/set -> `UTF8`
  - Blob/bit -> `BINARY`

## Parquet Writer Tuning
- Dictionary encoding is enabled, with page-level statistics to aid predicate pushdown.
- Writer batch size is tuned based on `--row-group-size` (bounded between 1024 and 8192 rows).
- The Parquet writer row group limit is aligned with `--row-group-size`.
- Bloom filters are not emitted; engines rely on statistics and dictionary encoding for pruning.

## Filtering
The exporter supports BR-style filters (same syntax as `br --filter`) and table
ID filters:
- `--filter 'db.table' --filter '!sys.*'`
- `--table-ids 45,1024`

Filters are applied before file iteration; if no table matches, the command
fails fast. Matching is case-insensitive for schema/table names, consistent with
BR behavior.

## Iceberg Manifest
When `--write-iceberg-manifest` is set, the exporter writes a JSON manifest with
per-file metadata (name, range keys, size, checksums, snapshot timestamps). The
manifest location is derived from:
`--iceberg-warehouse`, `--iceberg-namespace`, `--iceberg-table`, and
`--iceberg-manifest-prefix`.

## Observability
- Structured logs on download, convert, upload, and checkpoint load stages.
- Prometheus counters and histograms:
  - `tikv_br_parquet_sst_events_total{event=...}`
  - `tikv_br_parquet_rows_emitted_total`
  - `tikv_br_parquet_output_bytes_total`
  - `tikv_br_parquet_stage_duration_seconds{stage=...}`
- `tikv-ctl` prints a progress delta snapshot at the end of a run.

## Failure Handling
- Missing schema or empty table set results in an error.
- Encrypted meta index files are rejected.
- Checksum mismatches in indexed meta files are validated and rejected.
- Crashes can leave partial output; rerun with checkpoints to resume or disable
  checkpoints and use a fresh output prefix.

## Testing
- Unit tests cover schema conversion, backupmeta v1/v2 expansion, Parquet output,
  BR-style filter parsing, and checkpoint resume.
- Integration tests cover row-group sizing, table filters, and resume behavior.
- Manual smoke test (local TiUP, saved under `/Users/brian.w/projects/testdata`):
  - `tiup playground --tag parquet-smoke --db 1 --pd 1 --kv 1 --without-monitor --host 127.0.0.1`
  - Connect and load the full type table:
    ```bash
    mysql -h 127.0.0.1 -P 4000 -u root <<'SQL'
    DROP DATABASE IF EXISTS parquet_smoke;
    CREATE DATABASE parquet_smoke;
    USE parquet_smoke;
    CREATE TABLE all_types (
      id INT NOT NULL PRIMARY KEY,
      i_tiny TINYINT NULL,
      i_small SMALLINT NOT NULL,
      i_medium MEDIUMINT NULL,
      i_int INT NOT NULL,
      i_big BIGINT NULL,
      u_tiny TINYINT UNSIGNED NOT NULL,
      u_small SMALLINT UNSIGNED NULL,
      u_int INT UNSIGNED NOT NULL,
      u_big BIGINT UNSIGNED NULL,
      f_float FLOAT NULL,
      f_double DOUBLE NOT NULL,
      f_decimal DECIMAL(10,2) NULL,
      c_char CHAR(4) NOT NULL,
      c_varchar VARCHAR(20) NULL,
      c_text TEXT NULL,
      b_binary BINARY(4) NOT NULL,
      b_varbinary VARBINARY(8) NULL,
      b_blob BLOB NULL,
      t_date DATE NULL,
      t_datetime DATETIME NOT NULL,
      t_timestamp TIMESTAMP NULL,
      t_time TIME NULL,
      t_year YEAR NULL,
      j_json JSON NOT NULL,
      e_enum ENUM('a','b') NOT NULL,
      s_set SET('x','y') NULL,
      b_bit BIT(4) NULL
    );
    INSERT INTO all_types VALUES (
      1, -8, -16, -32, -64, -128,
      8, 16, 64, 128,
      1.25, 2.5, 1234.56,
      'abcd', 'hello', 'text',
      X'61626364', X'01020304', X'BEEF',
      '2024-01-02', '2024-01-02 03:04:05', '2024-01-02 03:04:05', '12:34:56', 2024,
      JSON_OBJECT('k','v'), 'a', 'x,y', b'1010'
    ),
    (
      2, NULL, 1, NULL, 2, NULL,
      1, NULL, 2, NULL,
      NULL, 3.5, NULL,
      'wxyz', NULL, NULL,
      X'00000000', NULL, NULL,
      NULL, '2024-02-03 04:05:06', NULL, NULL, NULL,
      JSON_OBJECT('k','v2'), 'b', NULL, NULL
    );
    SQL
    ```
  - `tiup br backup full --pd 127.0.0.1:2379 --storage local:///Users/brian.w/projects/testdata/br-backup-smoke-all-types --filter 'parquet_smoke.*'`
  - Generate local storage base64 configs:
    ```bash
    BASE64_INPUT=$(tiup br operator base64ify -s "local:///Users/brian.w/projects/testdata/br-backup-smoke-all-types")
    BASE64_OUTPUT=$(tiup br operator base64ify -s "local:///Users/brian.w/projects/testdata/parquet-export-smoke-all-types")
    ```
  - `tikv-ctl br-parquet-export --input-storage-base64 "$BASE64_INPUT" --output-storage-base64 "$BASE64_OUTPUT" --filter 'parquet_smoke.*' --write-iceberg-manifest --iceberg-warehouse "warehouse" --iceberg-namespace "analytics" --iceberg-table "all_types"`
  - Validate export output:
    - `parquet-tools schema /Users/brian.w/projects/testdata/parquet-export-smoke-all-types/parquet/parquet_smoke/all_types/*.parquet`
    - `jq '.files[0] | {file_name, cf, total_kvs}' /Users/brian.w/projects/testdata/parquet-export-smoke-all-types/warehouse/analytics/all_types/metadata/manifest_*_0.json`
  - Row group size case (force small row groups):
    - `tiup br backup full --pd 127.0.0.1:2379 --storage local:///Users/brian.w/projects/testdata/br-backup-smoke-rowgroups --filter 'parquet_smoke.all_types'`
    - `BASE64_INPUT=$(tiup br operator base64ify -s "local:///Users/brian.w/projects/testdata/br-backup-smoke-rowgroups")`
    - `BASE64_OUTPUT=$(tiup br operator base64ify -s "local:///Users/brian.w/projects/testdata/parquet-export-smoke-rowgroups")`
    - `tikv-ctl br-parquet-export --input-storage-base64 "$BASE64_INPUT" --output-storage-base64 "$BASE64_OUTPUT" --filter 'parquet_smoke.all_types' --row-group-size 1`
    - Validate row groups:
      - `parquet-tools meta /Users/brian.w/projects/testdata/parquet-export-smoke-rowgroups/parquet/parquet_smoke/all_types/*.parquet | rg -n 'row group|num_rows'`
  - Grouping case (single SST spans multiple tables in one region):
    ```bash
    mysql -h 127.0.0.1 -P 4000 -u root <<'SQL'
    USE parquet_smoke;
    DROP TABLE IF EXISTS group_a;
    DROP TABLE IF EXISTS group_b;
    CREATE TABLE group_a (id INT PRIMARY KEY, v INT);
    CREATE TABLE group_b (id INT PRIMARY KEY, v INT);
    INSERT INTO group_a VALUES (1, 10), (2, 20);
    INSERT INTO group_b VALUES (1, 100), (2, 200);
    SQL
    ```
  - Run a full backup so a single region SST can include multiple tables:
    - `tiup br backup full --pd 127.0.0.1:2379 --storage local:///Users/brian.w/projects/testdata/br-backup-smoke-grouping`
  - `BASE64_INPUT=$(tiup br operator base64ify -s "local:///Users/brian.w/projects/testdata/br-backup-smoke-grouping")`
  - `BASE64_OUTPUT=$(tiup br operator base64ify -s "local:///Users/brian.w/projects/testdata/parquet-export-smoke-grouping")`
  - `tikv-ctl br-parquet-export --input-storage-base64 "$BASE64_INPUT" --output-storage-base64 "$BASE64_OUTPUT" --filter 'parquet_smoke.group_*'`
  - Validate grouped output:
    - `ls /Users/brian.w/projects/testdata/parquet-export-smoke-grouping/parquet/parquet_smoke/group_a`
    - `ls /Users/brian.w/projects/testdata/parquet-export-smoke-grouping/parquet/parquet_smoke/group_b`

## Future Work
- Distributed export across TiKV nodes.
- Extended type mapping (for example, unsigned integer Parquet logical types).
