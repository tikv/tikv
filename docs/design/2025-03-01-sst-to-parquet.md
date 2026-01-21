# Proposal: BR SST to Parquet Lakehouse Pipeline

- Author(s): TiKV Data Services
- Last updated: 2025-03-01
- Discussion: TBD

## Abstract

Build a production-ready pipeline that turns TiDB BR artifacts (RocksDB SST files plus
schema metadata) into Parquet files accompanied by Iceberg-compatible manifests. The
pipeline lets backup operators feed Lakehouse/AI stacks directly without restoring data
back into TiDB.

## Background and Motivation

- BR stores data as SSTs and schema as TiDB-specific JSON. Lakehouse engines cannot read
  SSTs.
- Existing Iceberg manifests only describe SST metadata; they do not provide columnar data.
- Users want a single workflow: run BR, convert output to Parquet, register the result in
  an Iceberg/Delta catalog.

## Goals

1. Decode SST payloads offline using TiDB codecs and produce typed rows.
2. Stream rows into Parquet writers with bounded memory and tunable compression.
3. Produce Iceberg manifest entries that reference the generated Parquet files.
4. Design for reuse: shared row projection, pluggable writers (Parquet today, ORC later).

## Non-goals

- Replacing BR or changing SST format.
- Automatically registering tables in external catalogs (initially we only emit manifests).
- Full SQL semantics (we start with raw key/value rows; column decoding can evolve).

## Architecture Overview

```
+--------------+      +---------------------+      +-------------------+
| backupmeta    |      | Schema + SST Reader |      | Parquet Writer    |
| (BR storage)  |----->|  decode table JSON  |----->|  upload to remote |
+--------------+      |  stream SST rows    |      |  emit Iceberg     |
                      +---------------------+      +-------------------+
```

### Components

1. **Metadata loader**
   - Reads `backupmeta` from any `ExternalStorage` endpoint.
   - Parses `Schema` entries (TiDB DB/Table JSON) into `TableSchema` structs.
   - Groups `kvproto::brpb::File` records by physical table id. We currently rely on
     the `files` field; multi-level indices are recorded for future work.
2. **Row decoder**
   - Streams SSTs with `RocksSstReader`, stripping the TiKV data prefix.
   - Verifies table id from each key, decodes either integer or common handles, and uses
     `tidb_query_datatype::codec::table::decode_row` to materialize column `Datum`s.
   - Projects rows into `_tidb_table_id`, `_tidb_handle`, and the TiDB column set
     (strings, integers, floats, binary) while preserving nullability.
3. **Parquet writer**
   - Builds a per-table schema (required vs optional columns, row group size, compression).
   - Buffers values per column, flushes to row groups, and writes to local temp files that
     are hashed (SHA256) before being uploaded through the configured `ExternalStorage`.
4. **CLI controller (`tikv-ctl br parquet-export`)**
   - Accepts base64 encoded `StorageBackend`s for input/output, chooses compression
     (snappy, zstd, gzip, brotli, lz4raw), and forwards options to the exporter.
   - Prints a summary of exported rows/files. When `--write-iceberg-manifest` is set the
     command reuses `backup::iceberg::IcebergCatalog` to emit manifest JSON records into
     the same output storage.
   - The BR CLI wires through the same controller via `br backup ... --export-parquet`,
     automatically generating the input/output descriptors so operators can opt into
     Parquet output without running two separate commands. `--parquet-only` skips the
     SST phase entirely and just drives the exporter against an existing backup.

## Data Flow

1. `tikv-ctl` downloads `backupmeta`, validates that the backup is transactional, and
   instantiates `SstParquetExporter`.
2. Each SST listed in `backupmeta.files` is streamed into a `TempDir`, decoded, and
   converted into a Parquet file located at
   `<prefix>/<db>/<table>/<sst name>.parquet`.
3. SHA256 and row-count statistics are collected for every Parquet file and returned in
   the export report. Optional Iceberg manifests are written with the original snapshot
   timestamps and Parquet file metadata.

## Error Handling

- All network reads and writes use the unified `ExternalStorage` interface and are rate
  limited by `Limiter`. Failures bubble up to the CLI, which exits with a descriptive
  message.
- Temporary files live inside a per-run `TempDir` to guarantee cleanup even on crashes.
- Schema/codec errors (for example invalid JSON or unsupported TiDB types) are surfaced
  as `Schema` errors and abort the run; future work can allow best-effort skipping.

## Next Steps

- Support metadata fan-out when `backupmeta.files` is empty but file indexes exist.
- Add a partition aware planner so large tables can be processed concurrently.
- Expose progress reporting in BR/status server and add strict/lenient row decoding modes.
- Build additional writers (ORC/Delta) reusing the row projector, and integrate catalog
  registration through Hive/REST APIs.
