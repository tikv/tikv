# `components/backup` Maintenance Guide

## Purpose And Scope

`components/backup` owns TiKV's BR-facing backup service. It covers:

- transactional range backup
- raw KV backup
- streaming RPC orchestration for `Backup`
- disk-snapshot backup preparation RPCs used by newer BR flows
- backup-side concurrency tuning, SST building, encryption, and external
  storage upload

This crate does not own restore or ingest. Those live in `src/import` and
`components/sst_importer`.

## Architectural Views

### Request execution view

1. `service.rs` accepts `Backup` or `PrepareSnapshotBackup` RPCs.
2. `Task::new` in `endpoint.rs` validates the request and captures the backup
   parameters, lock-bypass lists, rate limiting, and cancel handle.
3. `Endpoint::run` turns the request range into region-scoped `BackupRange`
   units through `Progress`.
4. `BackupRange::backup` or `BackupRange::backup_raw_kv_to_file` takes a
   snapshot, scans the region fragment, and builds in-memory SST files.
5. `save_backup_file_worker` uploads the built SSTs to external storage and
   emits `BackupResponse` records.

### Snapshot-backup control view

- `service.rs::prepare_snapshot_backup` owns the long-lived duplex stream used
  for disk snapshot orchestration.
- `disk_snap.rs` mediates lease updates, wait-apply fanout, and abort/error
  mapping around `raftstore::store::snapshot_backup`.
- On classic `RaftKv`, `PrepareDiskSnapObserver` in raftstore is the
  cluster-local guard that controls whether snapshot backup can proceed.
- On `RaftKv2`, the `Backup` gRPC service still exists, but the snapshot-backup
  handle is currently unimplemented, so disk snapshot backup is not available
  there yet.

## Process Lifecycle And Startup Sequencing

- Classic `RaftKv` wiring lives in `components/server/src/server.rs`.
- `RaftKv2` wiring lives in `components/server/src/server2.rs`.
- Both paths register the gRPC `Backup` service before starting the backup
  endpoint worker.
- The endpoint owns:
  - a worker scheduler for `Task`
  - a separate Tokio IO runtime for file upload
  - a `SoftLimitKeeper` background task for adaptive concurrency
  - a config manager registered under `tikv::config::Module::Backup`
- The snapshot-backup stream additionally requires, on classic `RaftKv`:
  - a `PrepareDiskSnapObserver` already registered to the coprocessor host
  - a `SnapshotBrHandle` from the raftstore/router side
  - an async runtime handle for stream-side fanout
- On `RaftKv2`, do not assume the same disk-snapshot flow exists yet; the
  stream-side handle is intentionally unimplemented today.

Shutdown rule:

- Stop new RPC admission first, then let the endpoint worker and its upload
  runtime drain. Do not reorder this casually; the response stream and cancel
  handling assume the worker outlives in-flight upload tasks.

## Data Model And Metadata Contracts

- `Task.request` is the request contract. The fields that tend to matter during
  maintenance are:
  - key range and optional sub-ranges
  - `start_ts` / `end_ts`
  - raw vs transactional mode
  - destination API version and CF
  - replica-read intent
  - resolved/committed lock bypass lists
  - resource-group and request-origin metadata
- `Progress` slices the user request into region-aligned `BackupRange` work
  items. Reviewers should treat region slicing as a correctness boundary.
- `BackupRange` binds:
  - concrete region and peer
  - effective `[start_key, end_key)` inside that region
  - codec rules for API-version conversion
  - whether the range is served by leader or follower
- Output file metadata is `kvproto::brpb::File`. The file name, key range,
  versions, checksum, SHA-256, CF name, and encryption IV must stay aligned
  with the uploaded bytes.
- Disk snapshot preparation maintains a lease-like state in
  `PrepareDiskSnapObserver`; the stream protocol in `disk_snap.rs` assumes the
  lease transitions and wait-apply responses stay coherent across retries.

## Start Here

- `components/backup/src/lib.rs`
- `components/backup/src/service.rs`
- `components/backup/src/endpoint.rs`
- `components/backup/src/writer.rs`
- `components/backup/src/disk_snap.rs`
- `components/backup/src/softlimit.rs`
- `components/backup/src/errors.rs`
- `components/backup/src/metrics.rs`

## Must-Read File Order

1. `components/backup/src/endpoint.rs`
2. `components/backup/src/service.rs`
3. `components/backup/src/writer.rs`
4. `components/backup/src/disk_snap.rs`
5. `components/backup/src/softlimit.rs`
6. `components/backup/src/errors.rs`
7. `components/backup/src/metrics.rs`

## Core Concepts

- `Task`: one backup RPC request plus its cancel handle and response stream
- `Progress`: the region-walking state machine for a request
- `BackupRange`: one region-scoped backup fragment
- `BackupWriter` / `BackupRawKvWriter`: SST builders for transactional and raw
  backup
- `SoftLimitKeeper`: backup-specific adaptive concurrency controller
- `Env` / `StreamHandleLoop`: disk-snapshot backup stream coordination

## Why It Matters

Backup is a correctness-sensitive export path. Regressions here can silently
produce incomplete data, stale snapshots, wrong key encoding, or files that are
well-formed but semantically unusable by restore.

## Critical Invariants

- Transactional backup must read from a snapshot consistent with `end_ts` and
  the intended lock-bypass rules.
- Replica-read backup still has to perform request-origin-aware max-ts
  validation before taking the snapshot.
- Region slicing must preserve the exact request range without gaps or overlap.
- The response stream may report partial region results; error paths must not
  hide already-sent success records.
- File metadata must describe the uploaded encrypted/compressed SST accurately.
- Cancellation must be observed both before scheduling and inside long-running
  scan loops.
- Disk snapshot backup must not treat expired leases or aborted wait-apply
  results as success.

## Observability And Operational Signals

Open `components/backup/src/metrics.rs` first. The main signals are:

- `tikv_backup_range_duration_seconds`
- `tikv_backup_range_size_bytes`
- `tikv_backup_thread_pool_size`
- `tikv_backup_error_counter`
- `tikv_backup_softlimit`
- `tikv_backup_scan_wait_for_writer_seconds`
- `tikv_backup_scan_kv_count`
- `tikv_backup_scan_kv_size_bytes`
- `tikv_backup_raw_expired_count`

Operationally useful logs live mostly in `endpoint.rs` and `disk_snap.rs`,
especially around:

- long-running scans
- region seek failures
- upload failures
- snapshot wait/apply aborts
- client-side cancellation

## Change Management Guidance

- Treat snapshot-taking, lock-checking, key codec conversion, and file-metadata
  changes as correctness changes, not just performance changes.
- If a patch changes the snapshot-backup stream protocol or lease semantics,
  audit `raftstore::store::snapshot_backup` and the server bootstrap path in
  the same review.
- If a patch changes request context handling, review resource-control and
  request-origin behavior together with `src/server/service/kv.rs`.
- If backup output format or encryption handling changes, audit restore/import
  consumers and update this guide.

## Change-Impact Matrix

- Range splitting or region iteration changes:
  inspect `Progress`, `BackupRange`, and `RegionInfoProvider` call sites
- Snapshot or lock-check behavior changes:
  inspect `BackupRange::backup`, `ConcurrencyManager`, and storage snapshot
  APIs
- Output SST format or checksum changes:
  inspect `writer.rs`, `brpb::File`, and restore/import consumers
- Disk snapshot protocol changes:
  inspect `service.rs`, `disk_snap.rs`, and raftstore snapshot-backup code
- Concurrency or auto-tuning changes:
  inspect `softlimit.rs`, endpoint scheduling, and upload runtime usage

## Review Checklist

- Does the patch change which peer or role can serve backup?
- Does it change how region boundaries are converted into backup ranges?
- Does it change `BackupResponse` partial-success or error behavior?
- Does it change snapshot lifetime or lock-check semantics?
- Does it change SST metadata, encryption, checksum, or upload ordering?
- Does it change disk snapshot lease/update/wait-apply sequencing?

## Observability And Tests

- Unit-heavy coverage lives in `components/backup/src/endpoint.rs`,
  `service.rs`, and `disk_snap.rs`.
- Integration-style helpers live in `components/test_backup/src/lib.rs` and
  `components/test_backup/src/disk_snap.rs`.
- Regressions often require cluster-level validation because the failure modes
  are usually tied to leadership, region movement, or snapshot timing.

## Common Failure Modes

- not-leader / epoch-not-match loops while walking regions
- stale or missing lock handling around backup ts
- canceled clients leaving long-running scans alive
- mismatched API-version conversion for raw or transactional keys
- output file metadata not matching uploaded bytes
- disk snapshot wait-apply aborts caused by leadership or epoch changes

## Reading Map And Companion Docs

Suggested reading order:

1. `endpoint.rs`
2. `writer.rs`
3. `service.rs`
4. `disk_snap.rs`
5. `softlimit.rs`

Companion docs:

- `repo-overview.md`
- `src/storage.md`
- `src/gc_worker.md`
- `components/server.md`
- `src/import.md`
- BR overview:
  `https://docs.pingcap.com/tidb/stable/backup-and-restore-overview/`

## Glossary

- Backup ts:
  the snapshot timestamp exported by transactional backup
- Sub-range:
  a caller-provided sub-interval inside the top-level backup range
- Replica read backup:
  backup served by a follower after the required snapshot checks
- Wait-apply:
  the snapshot-backup step that waits for leaders to apply to the target point

## Related Components

- `src/storage`
- `src/server/gc_worker`
- `components/raftstore`
- `components/raftstore-v2`
- `components/external_storage`
- `components/encryption`
- `src/import`
