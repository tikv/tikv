# `src/coprocessor` Maintenance Guide

## Purpose And Scope

This module implements the classic TiDB coprocessor path for built-in request
types:

- DAG requests
- analyze requests
- checksum requests

It is a read-heavy hot path and directly impacts query latency.

## Architectural Views

### Request pipeline view

- parse protobuf request
- build request context
- get snapshot
- build request handler
- execute in read pool
- collect stats and emit response

### Batched unary result merging

- Unary handlers return `Result<HandlerOutput>`. Response data is either ready
  or kept as an unserialized `MergeableResult`. Currently only full-sampling
  analyze produces mergeable results. All mergeable outputs of one request must
  have the same concrete type and must produce the same logical result
  regardless of merge order.
- Merging is enabled only when the client sets
  `Request::allow_batch_task_data_merge` and supplies batched tasks. Otherwise,
  every task is serialized in its own read-pool task, preserving the existing
  wire behavior.
- Execution scheduling is negotiated independently. When the client sets
  `Request::execute_batch_tasks_serially`, the top task and batched tasks are
  polled one at a time so batching does not increase scan concurrency. This
  relies on the `ReadPoolHandle::spawn` contract: on both backends a task is
  admitted and enqueued only when the returned future is first polled. Because
  neither admission nor queueing observes the deadline, serial collection is
  bounded by the top task's deadline: on expiry the stream is dropped, which
  abandons the in-flight child and never submits the rest, and only a
  top-level timeout is returned. When the field is unset, batched tasks retain
  the legacy concurrent polling behavior.
  TiDB correlates child responses by task ID, so scheduling does not depend on
  response order.
- A successful mergeable batched result is folded into an error-free mergeable
  top result. Its batch response contains no data, sets
  `data_merged_into_response`, and keeps its execution details. Failed or
  non-mergeable tasks keep normal per-task responses.
- Final merging and serialization run in the read pool under the request's
  deadline, resource-control settings, selected semaphore group, and tracker.
  Outputs are buffered until finalization, so they contribute to peak request
  memory; each buffered output rides in its memory-trace guard, and attachment
  rebuilds the combined response's guard (adopting a batch response's node when
  the top response is untracked, e.g. a top task error) so the retained data
  stays accounted until the response drops.
- Data, acknowledgments, response-byte accounting, and memory tracing are
  published only after the final deadline check. Admission failure, deadline
  expiry, or failure to serialize a top result that already consumed child
  results returns no partial data or acknowledgments, allowing every task to be
  retried safely.

The main contracts live in `HandlerOutput` and `MergeableResult` in
`src/coprocessor/mod.rs`; orchestration is in `src/coprocessor/endpoint.rs`;
collection and finalization are in `src/coprocessor/batch.rs`.

## Process Lifecycle And Startup Sequencing

- Endpoint and read pools are created during server startup.
- Runtime behavior depends on read-pool setup, memory quota, concurrency
  controls, and storage snapshot access already being available.
- The main runtime anchors are:
  `src/coprocessor/readpool_impl.rs::build_read_pool`,
  `src/coprocessor/endpoint.rs::Endpoint::new`, and
  `src/server/service/kv.rs` as the RPC entry path.
- On the Yatp path, unary and streaming heavy tasks are admitted through
  semaphores created in `Endpoint::new`: a shared semaphore for ordinary
  coprocessor work and a dedicated semaphore for Analyze requests that are
  intentionally throttled by the background quota limiter.
- The shared semaphore is controlled by
  `server.end-point-max-concurrency`. The dedicated background-limited
  semaphore is enabled only when
  `server.end-point-max-bg-concurrency` is explicitly set to a positive value;
  its capacity is then controlled by that value.
- When the dedicated setting is absent or `0`, Analyze and ordinary Cop
  requests share the legacy semaphore. When it is positive, the two semaphores
  are independent and both lanes can make progress concurrently.
- The dedicated cap does not automatically track unified read-pool worker
  autoscaling at runtime.
- `build_read_pool` sets TLS engine state and marks threads as
  `IoType::ForegroundRead`. Any change that moves blocking work into or out of
  this path should be reviewed against foreground IO expectations.
- Online config is limited but real: `Endpoint::config_manager()` exposes
  `CopConfigManager`, which currently updates memory quota. If config scope
  expands, update lifecycle and operational sections in this guide together.

## Data Model And Metadata Contracts

- `ReqContext` is the key runtime metadata contract:
  context, ranges, deadline, peer, start ts, lock-bypass sets, bounds, cache
  version, perf level.
- Request parsing contract differs by request type:
  DAG, analyze, checksum.
- `HandlerOutput` always owns the traced response and records separately
  whether its data is ready or remains as a mergeable result until
  finalization.
- `ReqContextInner::new` is where deadline, bypass/access locks, and derived
  lower/upper bounds are normalized. Reviewers should treat changes there as
  cross-cutting request-semantic changes.
- `endpoint.rs::parse_request_and_check_memory_locks` is the main admission and
  normalization contract. Request parsing, memory-lock checks, API-version
  dispatch, and handler construction are deliberately coupled there.
- The hot request handlers differ materially:
  DAG requests use `dag/*`, analyze requests use `statistics/analyze_context.rs`
  and `statistics/analyze.rs`, and checksum requests use `checksum.rs`.
- Cache-match version, flashback allowance, and lock-bypass/access sets are all
  correctness-sensitive metadata, not optional optimization flags.

## Start Here

- `src/coprocessor/mod.rs`
- `src/coprocessor/endpoint.rs`
- `src/coprocessor/batch.rs`
- `src/coprocessor/readpool_impl.rs`
- `src/coprocessor/dag/*`
- `src/coprocessor/statistics/*`
- `src/coprocessor/interceptors/*`
- `src/coprocessor/config_manager.rs`

## Must-Read File Order

1. `src/coprocessor/mod.rs`
2. `src/coprocessor/endpoint.rs`
3. `src/coprocessor/batch.rs`
4. `src/coprocessor/tracker.rs`
5. `src/coprocessor/readpool_impl.rs`
6. `src/coprocessor/interceptors/deadline.rs`
7. `src/coprocessor/interceptors/concurrency_limiter.rs`
8. `src/coprocessor/dag/mod.rs`
9. `src/coprocessor/statistics/analyze_context.rs`

## Main Responsibilities

- parse coprocessor protobuf payloads
- build `ReqContext`
- acquire snapshots and perform memory-lock checks
- construct request handlers
- execute handlers on read pools
- enforce request deadlines, concurrency limits, and memory quotas
- collect execution stats and produce coprocessor responses

## Critical Invariants

- Range bounds in `ReqContext` must stay aligned with the actual request.
- Memory-lock checks must happen before serving reads that could violate lock
  semantics.
- Handler execution must respect request deadline and cancellation behavior.
- Memory quota and concurrency limiters must remain cheap and correct.
- Request parsing and admission must stay aligned: when the dedicated setting
  is enabled, a request class that reports quota samples to the background
  quota limiter should use the dedicated background-limited semaphore instead
  of bypassing heavy-task admission. With the setting disabled, it intentionally
  shares the ordinary semaphore.
- When enabled, the dedicated background-limited semaphore protects all
  Analyze variants, including index, common-handle, column, mixed, and
  full-sampling Analyze, from unlimited fan-out. It is not part of the ordinary
  shared heavy-task budget; when disabled, these requests intentionally use the
  shared semaphore.
- Streaming and unary response handling must preserve stats and partial-progress
  semantics.
- Batched unary result merging must preserve task identity, retry semantics,
  deadline enforcement, response-byte accounting, and memory tracing.
- Serial batch execution must keep at most one task from the request active
  without changing result-merging or response-order semantics.

## Observability And Operational Signals

- wait-time and snapshot-time metrics
- request-type metrics and execution summaries
- slow-log behavior driven by endpoint thresholds
- Resource metering / TopSQL records the per-request RocksDB PerfContext
  `block_read_count` delta as `rocksdb_block_read_count` when both
  `resource-metering.enable-network-io-collection` and
  `resource-metering.enable-detailed-io-collection` are enabled. The field is
  used for the downstream `read_iops` dimension and relative attribution; it is
  not a device-level IOPS measurement. Unary and streaming handler futures must
  keep this PerfContext accounting poll-scoped so TLS metrics cannot be
  attributed to another request. Keep that poll observer separate from the
  streaming item lifecycle: one item can span multiple polls, but its
  `ExecDetails` process time must still cover the complete item.
- Start with `src/coprocessor/metrics.rs`. High-value signals include:
  `tikv_coprocessor_request_duration_seconds` family,
  `tikv_coprocessor_request_wait_seconds`,
  `tikv_coprocessor_request_handler_build_seconds`,
  `tikv_coprocessor_request_error`,
  `tikv_coprocessor_scan_keys`,
  `tikv_coprocessor_scan_details`,
  `tikv_coprocessor_response_bytes`,
  `tikv_coprocessor_waiting_for_semaphore`, and
  `tikv_coprocessor_semaphore_wait_time_duration_seconds`.
- The semaphore wait metrics use `group=shared|background_limited` to
  distinguish ordinary Cop request pressure from Analyze background-limited
  throttling. Dashboard queries should preserve this label when diagnosing an
  individual lane and aggregate it only when displaying total semaphore
  pressure.
- `tracker.rs` is the best place to understand slow logs, exec details, request
  lifetime accounting, and the distinction between schedule wait, snapshot
  wait, suspend time, and processing time.
- Triage starting points:
  `endpoint.rs`, `tracker.rs`, `readpool_impl.rs`, `metrics.rs`,
  `interceptors/deadline.rs`, `interceptors/concurrency_limiter.rs`.

## Change Management Guidance

- If `ReqContext`, request parsing, timeout behavior, or resource-control
  integration changes, update this guide in the same patch.
- Hot-path changes should be reviewed together with performance-critical-path
  expectations.
- Treat `endpoint.rs` as both a correctness and latency hotspot. Extra parsing,
  allocation, or logging there needs justification.
- If lock checking or extra snapshot access logic changes, review the change
  with `src/storage` and concurrency-manager semantics in mind, not as a
  coprocessor-only patch.
- If a new request type or major execution mode is added, document its parser,
  handler builder, resource admission path, and observability surface here.

## Change-Impact Matrix

- Request parsing or context changes:
  inspect `mod.rs`, `endpoint.rs`, and request-type-specific builders
- Timeout or concurrency admission changes:
  inspect interceptors, `tracker.rs`, metrics, and read-pool behavior
- DAG execution changes:
  inspect `dag/*`, snapshot/store setup, and query-side statistics paths
- Analyze or checksum changes:
  inspect `statistics/*` or `checksum.rs` plus exec-detail accounting

## Review Checklist

- Does the change touch `endpoint.rs` parsing or request-type dispatch?
- Does it affect `ReqContext`, deadline handling, or lock bypass/access sets?
- Does it change read-pool wiring or per-request resource control?
- Does it add extra allocation, parsing, or logging to the hot path?
- Does it change handler stats collection or slow-log behavior?

## Observability And Tests

- Inline tests exist across handler and statistics modules.
- Performance-sensitive behavior often needs bench or end-to-end query testing.
- Metrics live in `metrics.rs`, trackers, and read-pool tickers.
- `endpoint.rs` itself contains many targeted tests for parsing, lock checking,
  timeout, and snapshot-access behavior. It is one of the most important files
  to consult when changing request admission semantics.

## Common Failure Modes

- wrong request classification or parser fallback
- lock-check bypass on paths that should block
- deadline handling drift between streaming and unary paths
- memory quota leaks on early-return/error paths
- concurrency limiter behavior only applied to one pool mode

## Reading Map And Companion Docs

Suggested reading order:

1. `mod.rs`
2. `endpoint.rs`
3. `batch.rs`
4. `readpool_impl.rs`
5. `dag/mod.rs`
6. `statistics/analyze_context.rs`
7. `interceptors/*`

Companion docs:

- `repo-overview.md`
- `src/server.md`
- `src/storage.md`

## Glossary

- DAG request:
  built-in coprocessor request for pushed-down query execution
- ReqContext:
  immutable runtime request context shared through execution
- Light task threshold:
  the execution-time budget before a coprocessor future must acquire a
  semaphore permit in the Yatp path
- Snapshot wait:
  request time spent between scheduling and obtaining the storage snapshot

## Related Components

- `src/server/service/kv.rs` is the RPC entry point.
- `src/storage` provides snapshots and the lock-related behavior.
- `components/in_memory_engine` can accelerate snapshot-backed reads indirectly.
