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

## Process Lifecycle And Startup Sequencing

- Endpoint and read pools are created during server startup.
- Runtime behavior depends on read-pool setup, memory quota, concurrency
  controls, and storage snapshot access already being available.
- The main runtime anchors are:
  `src/coprocessor/readpool_impl.rs::build_read_pool`,
  `src/coprocessor/endpoint.rs::Endpoint::new`, and
  `src/server/service/kv.rs` as the RPC entry path.
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

### Typed full-sampling Analyze batching

- The ordinary `RequestHandler::handle_request` contract still returns a
  serialized `Response`. The endpoint has a separate, concrete
  `FullSamplingAnalyzeHandler` path whose scan phase returns an
  `AnalyzeSamplingResult`; generic handlers and other request types cannot opt
  into result merging.
- Merging is enabled only for a valid full-sampling Analyze request that sets
  `Request.allow_batch_task_data_merge`, disables cache lookup, and carries at
  least one batched task. All other requests keep the ordinary one-response-
  per-task behavior. A negotiated request may carry at most four child tasks;
  a wider request is rejected before any scan starts so the whole batch remains
  retryable.
- The top scan and child scans are polled concurrently because read-pool work
  is enqueued only when its future is first polled, while its deadline starts
  at parse time. At most four child scans run concurrently (five physical
  scans including the top task). Completed children are restored to request
  ordinal before merging and attachment; protocol task IDs may be absent or
  duplicated and are not used for ordering.
- Wire contract: the top response carries the sampling result merged in
  request order with every successful child result. Each merged child becomes
  a data-less acknowledgement with `data_merged_into_response` set. Failed or
  non-mergeable children keep normal responses so the client can retry or
  consume them independently.
- The top response is the single accounting owner for merged work: its
  execution details combine the top scan with every acknowledged child.
  Acknowledgements retain their per-child details for diagnostics, but a
  supporting client must not charge those details again.
- Merging and protobuf encoding run in a second read-pool task under the
  preserved top tracker, resource-metering tag, deadline, resource control,
  and an eagerly acquired coprocessor semaphore permit. The finalizer never
  falls back to inline execution. Pool rejection, permit/deadline failure, or
  encode failure returns one top-level error with no child acknowledgements,
  so the client retries the whole batch. Response bytes and acknowledgements
  are committed only after encoding finishes and the deadline is rechecked.
- The canonical contracts live on `FullSamplingAnalyzeHandler` and
  `AnalyzeSamplingResult` in `src/coprocessor/statistics/`, and on
  `collect_batch_task_outputs`, `merge_and_encode_analyze_batch`,
  `finish_analyze_batch`, and `AnalyzeBatchFinalizer` in
  `src/coprocessor/endpoint.rs`.

## Start Here

- `src/coprocessor/mod.rs`
- `src/coprocessor/endpoint.rs`
- `src/coprocessor/readpool_impl.rs`
- `src/coprocessor/dag/*`
- `src/coprocessor/statistics/*`
- `src/coprocessor/interceptors/*`
- `src/coprocessor/config_manager.rs`

## Must-Read File Order

1. `src/coprocessor/mod.rs`
2. `src/coprocessor/endpoint.rs`
3. `src/coprocessor/tracker.rs`
4. `src/coprocessor/readpool_impl.rs`
5. `src/coprocessor/interceptors/deadline.rs`
6. `src/coprocessor/interceptors/concurrency_limiter.rs`
7. `src/coprocessor/dag/mod.rs`
8. `src/coprocessor/statistics/analyze_context.rs`

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
- Streaming and unary response handling must preserve stats and partial-progress
  semantics.
- Merged batched responses must keep the wire contract described in
  "Handler outcomes and batched-task merging".

## Observability And Operational Signals

- wait-time and snapshot-time metrics
- request-type metrics and execution summaries
- slow-log behavior driven by endpoint thresholds
- Start with `src/coprocessor/metrics.rs`. High-value signals include:
  `tikv_coprocessor_request_duration_seconds` family,
  `tikv_coprocessor_request_wait_seconds`,
  `tikv_coprocessor_request_handler_build_seconds`,
  `tikv_coprocessor_request_error`,
  `tikv_coprocessor_scan_keys`,
  `tikv_coprocessor_scan_details`,
  `tikv_coprocessor_response_bytes`,
  `tikv_coprocessor_waiting_for_semaphore`, and
  `tikv_coprocessor_semaphore_wait_seconds`.
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
3. `readpool_impl.rs`
4. `dag/mod.rs`
5. `statistics/analyze_context.rs`
6. `interceptors/*`

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
