# `components/sst_importer` Maintenance Guide

## Purpose And Scope

`components/sst_importer` is the shared library behind TiKV's SST import and
restore flows. It owns:

- import-directory layout and file lifecycle
- upload target creation and validation
- download from external storage
- rewrite / merge / prepare-for-ingest helpers
- importer-side caching and memory quota
- import-mode switching for classic and partitioned-raft-kv engines

The RPC-facing import service lives in `src/import`. This crate is the storage
and file-management backend used by that service.

## Architectural Views

### File lifecycle view

1. `ImportDir` defines the root, `.temp`, and `.clone` directories.
2. `ImportFile` writes to a temporary file, validates CRC32, and renames it
   into the durable import directory.
3. `SstImporter` manages already-uploaded files plus externally downloaded
   files and in-memory buffers.
4. Import service or raftstore ingest code later asks for preparation,
   verification, or ingest-specific clones.

### Import-mode view

- `import_mode.rs` is the classic single-RocksDB switcher. Import mode is
  global for the shared KV engine and modifies RocksDB options.
- `import_mode2.rs` is the `RaftKv2` / partitioned-raft-kv switcher. Import
  mode is range-scoped and does not rewrite RocksDB options globally.

## Process Lifecycle And Startup Sequencing

- `SstImporter` instances are created during server bootstrap in:
  - `components/server/src/server.rs`
  - `components/server/src/server2.rs`
- Creation initializes:
  - import directory structure
  - download/runtime infrastructure
  - cache GC loop
  - memory quota derived from `import.memory_use_ratio`
- `src/import::ImportSstService` later calls
  `start_switch_mode_check(...)` and periodically refreshes dynamic settings.

Shutdown rule:

- The importer's background runtime must outlive in-flight downloads and cache
  cleanup work. Avoid extracting ad hoc download helpers that bypass the
  importer's owned runtime.

## Data Model And Metadata Contracts

- `ImportDir` path layout is part of the maintenance surface:
  - `<root>/`
  - `<root>/.temp/`
  - `<root>/.clone/`
- `ImportPath` tracks three paths for one SST:
  - `temp`: upload in progress
  - `save`: durable imported file
  - `clone`: ingest-side clone or prepared path
- `ImportFile` validates CRC32 against `SstMeta` before finalizing the file.
- `SstImporter` couples:
  - `BackendConfig`
  - encryption key manager / master keys
  - API version expectations
  - cache entries and file-lock state
- `CacheKvFile` has multiple states, including deduplicated in-progress
  download state. That state machine is correctness-sensitive, not just a
  cache optimization.

## Start Here

- `components/sst_importer/src/lib.rs`
- `components/sst_importer/src/sst_importer.rs`
- `components/sst_importer/src/import_file.rs`
- `components/sst_importer/src/import_mode.rs`
- `components/sst_importer/src/import_mode2.rs`
- `components/sst_importer/src/sst_writer.rs`
- `components/sst_importer/src/metrics.rs`
- `components/sst_importer/src/config.rs`
- `components/sst_importer/src/errors.rs`

## Must-Read File Order

1. `components/sst_importer/src/sst_importer.rs`
2. `components/sst_importer/src/import_file.rs`
3. `components/sst_importer/src/import_mode.rs`
4. `components/sst_importer/src/import_mode2.rs`
5. `components/sst_importer/src/sst_writer.rs`
6. `components/sst_importer/src/config.rs`
7. `components/sst_importer/src/errors.rs`
8. `components/sst_importer/src/metrics.rs`

## Core Concepts

- `SstImporter`: the main owner of import files, downloads, cache, and mode
  switching
- `ImportDir`: the on-disk directory contract
- `ImportModeSwitcher` / `ImportModeSwitcherV2`: classic vs range-scoped import
  mode
- `CacheKvFile`: importer-side cache entry and in-flight dedup state
- `DownloadedSstFile`: disk-backed or memory-backed downloaded SST

## Why It Matters

This crate sits on the boundary between external restore data and TiKV local
storage. Bugs here commonly surface as file corruption, stale downloads, wrong
API-version handling, or import-mode behavior that silently distorts ingest
performance.

## Critical Invariants

- `ImportFile::finish` must validate CRC32 before rename.
- Temporary-file cleanup must not delete successfully finalized files or leave
  stale encryption metadata behind.
- Classic import mode must restore RocksDB options after timeout.
- Range-scoped import mode must only report overlap for the intended ranges.
- Downloaded SST readers must verify checksum before use.
- Cache state transitions must prevent duplicate downloads without turning
  transient failures into permanent stuck state.
- `import.memory_use_ratio` is intentionally bounded; callers should not bypass
  that control with ad hoc large in-memory allocations.

## Observability And Operational Signals

Open `components/sst_importer/src/metrics.rs` first. The main signals are:

- `tikv_import_download_duration`
- `tikv_import_download_bytes`
- `tikv_import_apply_bytes`
- `tikv_import_ingest_duration`
- `tikv_import_ingest_bytes`
- `tikv_import_error_counter`
- `tikv_import_storage_cache`
- `tikv_import_apply_cached_bytes`
- `tikv_import_apply_cache_event`
- `tikv_import_applier_event`
- `tikv_import_engine_request`

The error taxonomy in `errors.rs::error_inc` is also part of the operational
surface; patches that introduce new failure classes should decide whether a new
metric label is needed.

## Change Management Guidance

- If a patch changes path layout, rename behavior, or checksum rules, update
  this guide and review restore/import callers together.
- If a patch changes import mode semantics, audit both classic `RaftKv` and
  `RaftKv2` behavior explicitly; the two modes are intentionally different.
- If a patch changes download caching or encryption handling, review both local
  and external-storage code paths.
- If a patch changes API-version checks, review `src/import` RPC validation and
  restore tooling assumptions together.

## Change-Impact Matrix

- File lifecycle changes:
  inspect `import_file.rs`, cleanup-on-drop, and encryption metadata handling
- Download/cache changes:
  inspect `sst_importer.rs`, `caching/*`, and external-storage wrappers
- Import-mode changes:
  inspect `import_mode.rs`, `import_mode2.rs`, and server bootstrap/service
  callers
- API-version or key rewrite changes:
  inspect `sst_writer.rs`, `sst_importer.rs`, and `src/import`

## Review Checklist

- Does the patch change temp/save/clone path semantics?
- Does it change checksum or integrity verification before ingest?
- Does it change classic import-mode option restoration?
- Does it change range-overlap behavior for `RaftKv2` import mode?
- Does it change download deduplication, cache expiry, or encryption wrapping?
- Does it change API-version compatibility or key-prefix rewriting?

## Observability And Tests

- Test helpers live in `components/test_sst_importer/src/lib.rs`.
- `components/sst_importer/src/sst_importer.rs` contains extensive unit coverage.
- Many regressions are easiest to reproduce through `src/import` service tests
  or higher-level restore flows because RPC chunking and engine interaction are
  outside this crate.

## Common Failure Modes

- CRC mismatch or malformed imported file
- leaked temp files or orphaned encryption metadata
- import mode not reverting after timeout
- range-scoped import mode overlapping too much or too little
- stale in-progress cache entries suppressing retries
- incompatible API-version data entering the ingest path

## Reading Map And Companion Docs

Suggested reading order:

1. `sst_importer.rs`
2. `import_file.rs`
3. `import_mode.rs`
4. `import_mode2.rs`
5. `errors.rs`

Companion docs:

- `repo-overview.md`
- `src/import.md`
- `components/backup.md`
- BR overview:
  `https://docs.pingcap.com/tidb/stable/backup-and-restore-overview/`

## Glossary

- Import dir:
  the local directory tree used to stage imported SSTs
- Import mode:
  a temporary mode optimized for ingest-heavy workloads
- Clone path:
  a prepared file path used for ingest-specific handling
- Download state:
  importer cache entry representing an in-progress external download

## Related Components

- `src/import`
- `components/external_storage`
- `components/encryption`
- `components/engine_rocks`
