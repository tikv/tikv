# `src/import` Maintenance Guide

## Purpose And Scope

`src/import` owns TiKV's RPC-facing import and ingest service. It exposes the
APIs used by Lightning and restore workflows to:

- upload SSTs to a TiKV node
- download and rewrite remote SST/KV files
- apply KV data through raft writes
- ingest prepared SST files into region engines
- detect duplicates for conflict checks
- suspend or gate import traffic
- switch import mode and manage force-partition ranges

This module is the service layer above `components/sst_importer`.

## Architectural Views

### Service-layer view

`ImportSstService` in `sst_service.rs` is the main entry point. It owns RPC
handlers for:

- `upload`
- `download`
- `batch_download`
- `batch_download_latest_mvcc`
- `apply`
- `ingest`
- `multi_ingest`
- `write` / `raw_write`
- `duplicate_detect`
- `switch_mode`
- `suspend_import_rpc`
- force-partition-range management

### Apply / ingest split

- `apply` rewrites downloaded KV data into regular raft writes. It is heavy on
  request batching, TLS-engine apply workers, and txn-source tagging.
- `ingest` / `multi_ingest` ingest already prepared SSTs into the target region
  after region and snapshot checks.

## Process Lifecycle And Startup Sequencing

- Service bootstrap is in:
  - `components/server/src/server.rs`
  - `components/server/src/server2.rs`
- It starts after the shared `SstImporter` exists and after the main storage
  engine/tablet registry is ready.
- `ImportSstService::new` creates:
  - a resizable Tokio runtime whose worker threads install the TLS engine
  - a `ConfigManager` that can resize the runtime dynamically
  - a background ticker that refreshes importer memory settings and cache
    shrink behavior
  - a `ThrottledTlsEngineWriter` GC loop

Shutdown rule:

- The runtime worker threads own the TLS engine registration used by
  `raft_writer.rs`. Do not move apply work onto threads that have not installed
  the expected engine type.

## Data Model And Metadata Contracts

- `RequestCollector` in `sst_service.rs` is the main apply-path batching
  contract.
  - It de-duplicates write-CF keys.
  - It batches requests conservatively against raft-entry size.
  - It emits `WriteData` units consumed by `raft_writer.rs`.
- `prepare_write` sets the Lightning physical-import txn-source bit. CDC relies
  on that bit to suppress imported traffic from normal TiCDC semantics.
- `IngestLatch` in `ingest.rs` prevents duplicate ingest of the same SST path
  within one CF.
- `SuspendDeadline` is the service-wide temporary admission gate for import
  requests.
- `duplicate_detect.rs` scans `CF_WRITE` and, when needed, `CF_DEFAULT` to
  report conflicting historical versions after a given `min_commit_ts`.
- Region freshness checks compare the request epoch with local region info
  before heavy work begins.

## Start Here

- `src/import/mod.rs`
- `src/import/sst_service.rs`
- `src/import/ingest.rs`
- `src/import/raft_writer.rs`
- `src/import/duplicate_detect.rs`

## Must-Read File Order

1. `src/import/sst_service.rs`
2. `src/import/ingest.rs`
3. `src/import/raft_writer.rs`
4. `src/import/duplicate_detect.rs`
5. `components/sst_importer/src/sst_importer.rs`
6. `components/sst_importer/src/import_mode.rs`
7. `components/sst_importer/src/import_mode2.rs`

## Core Concepts

- `ImportSstService`: the RPC facade
- `RequestCollector`: apply-path batcher and deduplicator
- `ThrottledTlsEngineWriter`: per-region apply throttling over TLS engine state
- `IngestLatch`: duplicate-ingest guard
- `SuspendDeadline`: temporary request rejection gate
- `DuplicateDetector`: conflict scanner over MVCC history

## Why It Matters

This module is where remote restore/import traffic becomes real local writes or
ingest operations. Small mistakes here can create Raft-entry blowups, duplicate
apply, stalled regions, unexpected CDC side effects, or subtle restore
corruption.

## Critical Invariants

- Write-CF apply batching must not send duplicate keys in one request. The
  resolved-ts and apply path assume this.
- Lightning physical import writes must preserve the txn-source bit masking
  expected by CDC.
- Ingest must acquire a fresh snapshot before checking file existence and
  submitting ingest writes, so the leader is current-term and the region view is
  coherent.
- Region epoch checks must happen before expensive upload/download/apply work.
- Resource checks for memory and disk are part of admission control, not just
  best-effort warnings.
- `RaftKv2` import mode requires explicit ranges; do not silently fall back to
  classic global import-mode assumptions.
- TLS-engine apply workers must run only on threads where the matching engine
  type was installed.

## Observability And Operational Signals

Most metrics live in `components/sst_importer/src/metrics.rs`. For this module,
the most useful ones are:

- `tikv_import_rpc_duration`
- `tikv_import_rpc_count`
- `tikv_import_upload_chunk_bytes`
- `tikv_import_upload_chunk_duration`
- `tikv_import_download_duration`
- `tikv_import_ingest_duration`
- `tikv_import_apply_duration`
- `tikv_import_error_counter`
- `tikv_import_applier_event`
- `tikv_import_engine_request`

Operationally useful logs are concentrated around:

- region-staleness checks
- apply batching and request-size behavior
- resource rejection
- ingest slowdowns / `server_is_busy`
- import suspension and resume

## Change Management Guidance

- If a patch changes batching, deduplication, or txn-source tagging, review CDC
  and resolved-ts interactions together with import logic.
- If a patch changes ingest stalling or import-mode behavior, review both
  classic and `RaftKv2` paths explicitly.
- If a patch changes admission control, keep the user-visible retry semantics in
  `Error::ResourceNotEnough`, `Error::Suspended`, and epoch-staleness errors
  coherent.
- If a patch changes force-partition-range or region-info access, audit split
  and region-movement scenarios.

## Change-Impact Matrix

- Upload/download RPC changes:
  inspect `sst_service.rs` plus `components/sst_importer`
- Apply batching or throttling changes:
  inspect `RequestCollector`, `raft_writer.rs`, and raft-entry sizing
- Ingest correctness changes:
  inspect `ingest.rs`, snapshot acquisition, and importer file state
- Duplicate detection changes:
  inspect `duplicate_detect.rs` and MVCC/value lookup rules
- Import mode or suspension changes:
  inspect `switch_mode`, `SuspendDeadline`, and server bootstrap/config wiring

## Review Checklist

- Does the patch change write-CF deduplication or batch splitting?
- Does it change how the Lightning physical-import txn-source bit is set?
- Does it change region epoch freshness checks?
- Does it change ingest ordering around snapshot acquisition and file existence?
- Does it change import-mode assumptions for `RaftKv2`?
- Does it change resource rejection or suspension behavior seen by clients?

## Observability And Tests

- `src/import/sst_service.rs`, `raft_writer.rs`, and `duplicate_detect.rs` all
  carry significant unit coverage.
- `components/test_sst_importer` provides engine-side helpers.
- Cluster-level import/restore behavior is also exercised indirectly through BR
  and Lightning test suites outside this module.

## Common Failure Modes

- duplicate write-CF keys causing downstream panic or rejected apply
- oversized raft requests due to incorrect batch accounting
- stale region epoch errors after region movement
- ingest conflict or missing file after retries
- import service rejecting traffic because memory or disk thresholds are crossed
- imported writes surfacing incorrectly in CDC because txn-source tagging broke

## Reading Map And Companion Docs

Suggested reading order:

1. `sst_service.rs`
2. `ingest.rs`
3. `raft_writer.rs`
4. `duplicate_detect.rs`
5. `components/sst_importer/src/sst_importer.rs`

Companion docs:

- `repo-overview.md`
- `components/sst_importer.md`
- `components/cdc.md`
- `components/resolved_ts.md`
- `src/gc_worker.md`
- `components/backup.md`
- BR overview:
  `https://docs.pingcap.com/tidb/stable/backup-and-restore-overview/`

## Glossary

- Apply:
  rewrite remote KV records into normal raft writes
- Ingest:
  install prepared SST files through the engine ingest path
- TLS engine:
  the thread-local storage engine handle used by async apply workers
- Physical import bit:
  the txn-source bit marking Lightning physical import writes

## Related Components

- `components/sst_importer`
- `components/cdc`
- `components/raftstore`
- `components/raftstore-v2`
- `src/storage`
