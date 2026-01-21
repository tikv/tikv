# Proposal: Iceberg-Compatible Backup Metadata

- Author(s): Internal TiKV Data Services
- Last updated: 2025-02-28
- Discussion at: (TBD)

## Abstract

Expose a first-class backup option that mirrors TiKV snapshot metadata into
[Apache Iceberg] manifests. BR today writes SST files into an external bucket,
but downstream users must reverse-engineer the contents to build Iceberg tables.
This feature adds structured manifests describing every SST produced by BR,
enabling catalog-side ingestion workflows without modifying the SST payload.

[Apache Iceberg]: https://iceberg.apache.org

## Motivation

- AI/analytics platforms increasingly standardize on Iceberg catalogs as the
  exchange language for object storage. TiKV backups should be consumable by
  these ecosystems without bespoke parsers.
- Enterprises want to track snapshot-version metadata (ts ranges, regions, CFs)
  and tie them back to lineage or compliance pipelines. Writing Iceberg
  manifests provides a portable, queryable index without altering storage
  engines.
- Operators need a way to opt-in at runtime, specify catalog layout, and ensure
  manifests are created atomically with existing BR flows.

## Goals

1. Provide configuration knobs (`backup.iceberg.*`) to enable/disable Iceberg
   manifest generation and describe catalog coordinates (warehouse, namespace,
   table, manifest prefix).
2. Emit a manifest JSON document each time BR flushes an SST batch, capturing
   file names, key ranges, column family, checksums, and snapshot timestamps.
3. Keep the solution backend-agnostic by reusing the configured external storage
   bucket and path.
4. Gate all work behind the feature flag so existing deployments remain
   unaffected.

## Non-Goals

- This change does **not** rewrite SST contents into Iceberg table files.
- No catalog registration is attempted (e.g., Hive Metastore or REST calls).
- We do not attempt to compact or delta-log the manifests; each batch is
  independent.

## Detailed Design

### Configuration Surface

`BackupConfig` gains a nested `BackupIcebergConfig` (online-config capable):

| Field | Description |
| ----- | ----------- |
| `enable` | Feature flag (default `false`). |
| `warehouse` | Logical warehouse root inside the bucket. |
| `namespace` | Slash- or dot-delimited namespace; dots become `/` when stored. |
| `table` | Target table name. |
| `manifest_prefix` | Prefix for generated manifest filenames. |

The manifest path resolves to:

```
<warehouse>/<namespace>/<table>/metadata/<manifest_prefix>_<snapshot_end>_<seq>.json
```

Where `snapshot_end` is the backup `end_version` and `seq` is a counter scoped
to the BR endpoint instance.

### Execution Flow

1. `Endpoint::handle_backup_task` clones the current `BackupConfig` and, if
   enabled, builds an `IcebergCatalog`.
2. Every `InMemBackupFiles` flushed by worker threads is still persisted to the
   configured `ExternalStorage`.
3. After rewriting key ranges to the destination API version but before
   returning the response to the caller, the `save_backup_file_worker` checks
   for an active catalog and invokes `IcebergCatalog::write_manifest`.
4. `IcebergCatalog` converts each `kvproto::brpb::File` to an `IcebergDataFile`
   struct. Keys are base64 encoded, SHA256 digests are hex encoded when
   available, and timestamps are recorded in physical units.
5. The manifest is serialized via `serde_json` and written into the same
   backend using `ExternalStorage::write`, inheriting encryption and rate limits.

### Failure Handling

- Manifest writes reuse the same resource limiter as file writes, so quota
  controls still apply.
- If manifest generation fails, the worker logs a warning but does **not**
  abort the main backup flow, mirroring the optional nature of the feature.
- Because manifests are derived from already-successful SST flushes, a retry
  simply creates an additional JSON file; consumers are expected to dedupe by
  filename if necessary.

### Compatibility and Security

- Disabled by default, so existing BR behavior is unchanged.
- Manifests are stored alongside existing SSTs and therefore inherit the
  external storage backend’s authentication (S3, GCS, Azure, HDFS, local).
- The JSON schema nests only metadata; no row-level or CF payload is copied.
- Namespace normalization replaces dots with slashes to align with Iceberg’s
  multi-level namespaces.

## Testing Plan

1. **Unit Tests**
   - Validate catalog enable/disable logic and path normalization.
   - Ensure manifest JSON contains encoded keys and snapshot bounds.
2. **Integration Tests**
   - Extend the existing `tests/integrations/backup` suite to build a cluster
     with Iceberg config enabled, run a backup to a local path, and assert that
     manifest files exist with correct header fields and file counts.

## Rollout & Observability

- Exposed via online config so cloud operators can toggle at runtime.
- Manifest creation uses existing backup logging (`info!`) with manifest paths
  to aid troubleshooting.
- Future work can extend metrics (e.g., count manifests) if adoption requires.

## Alternatives Considered

- Embedding Iceberg writers into TiKV to emit Parquet/ORC data directly was
  rejected because it duplicates TiSpark/TiFlash capabilities and complicates
  the BR binary.
- Having the BR client stitch manifests post hoc was dismissed; writing inside
  TiKV ensures manifests are co-located with the exact SST set that was written.

## References

- [Apache Iceberg Spec](https://iceberg.apache.org/spec/)
- [BR design doc (2020-04-20)](./2020-04-20-brie.md)
