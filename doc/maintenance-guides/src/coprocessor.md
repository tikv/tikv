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
  admitted and enqueued only when the returned future is first polled. The top
  task and children share a shorter execution deadline, computed before parsing
  from `serial_batch_task_budget`, leaving a best-effort margin for finalization.
  Every task's internal deadline matches this shared deadline. Because
  neither admission nor queueing observes the deadline, the sequential collector
  bounds the top wait and the serial stream (`serial_batch_task_outputs`) bounds
  child waits: no task starts without time left, and the first task that runs out
  of time is abandoned. An abandoned task still waiting for admission is dropped;
  one already in the pool fails its next deadline check. The tasks that completed
  are kept, and the abandoned and unstarted ones receive deadline-exceeded
  responses in their respective top or child response slots so the client retries
  only those. The finalization reserve remains best effort because running tasks
  check deadlines cooperatively and finalization has no deadline. When the field
  is unset, batched tasks retain the legacy concurrent polling behavior. Requests
  without children keep their original execution timeout.
  TiDB correlates child responses by task ID, so scheduling does not depend on
  response order.
- A successful mergeable batched result is folded into an error-free mergeable
  top result. Its batch response contains no data, sets
  `data_merged_into_response`, and keeps its execution details. Failed or
  non-mergeable tasks keep normal per-task responses.
- Final merging and serialization run in the read pool under the request's
  selected semaphore group and tracker, but without an execution deadline or
  background-limiter admission. Semaphore selection matches execution: fair
  pools bypass the legacy shared semaphore, while an explicitly configured
  Analyze semaphore still applies. Fair-pool live-task limits and the global CPU
  gate still apply. Completed work is not discarded merely because the
  execution budget expires while waiting for the pool, acquiring a permit,
  merging, or serializing. Every result was already admitted and charged when
  produced; the merge's own CPU is not charged to the group's limiter.
  For the same reason finalization is queued as delivery of completed work,
  ahead of new scans: at high command priority, which pins the top yatp level,
  and with the highest resource-group override priority, which places it in
  the top scheduling tier so a throttled group's completed results are not
  starved behind other groups' scans in the priority queue. The request's
  group name is kept for CPU attribution. In the fair pool its CPU advances
  that group's high-priority scheduling lane's virtual time; this does not
  charge the virtual time of its other priority lanes. Pool-full
  rejection remains possible and returns no partial data.
  This applies to both serial and concurrent batches with merging enabled.
  Pool and semaphore waits have no time limit, so they may retain buffered
  results after the client times out. Dropping the caller cancels a merge still
  queued in the pool, releasing the fair-pool live-task slot as well as the
  buffered results. Taking the results and their cancellation guard is one
  atomic handoff; once the pool task takes them, dropping the caller does not
  interrupt permit acquisition or finalization.
  Outputs are buffered until finalization, so they contribute to peak request
  memory; each buffered output rides in its memory-trace guard, and attachment
  rebuilds the combined response's guard (adopting a batch response's node when
  the top response is untracked, e.g. a top task error) so the retained data
  stays accounted until the response drops.
- Data and acknowledgments are published together after materialization;
  response bytes are accounted only when the caller accepts the response.
  Pool rejection or failure to serialize a top result that already consumed
  child results returns no partial data or acknowledgments, allowing every task
  to be retried safely.

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
- `Endpoint::new` selects semaphores from the actual read-pool mode. Legacy
  Yatp pools use a shared semaphore controlled by
  `server.end-point-max-concurrency`. Resource-group fair pools omit it:
  whole-request permits held across yields would prevent other groups from
  reaching the fair scheduler. Fair-pool CPU and live-task limits still apply.
- In both Yatp modes, the dedicated background-limited Analyze semaphore is
  enabled only when
  `server.end-point-max-bg-concurrency` is explicitly set to a positive value;
  its capacity is then controlled by that value.
- When the dedicated setting is absent or `0`, Analyze follows the ordinary
  request policy: shared semaphore in legacy pools, no semaphore in fair pools.
  A positive setting is independent of the ordinary request policy. Unary,
  streaming, and batch finalization all use the same selection. FuturePools
  retain their existing behavior without these semaphores.
- The dedicated cap does not automatically track unified read-pool worker
  autoscaling at runtime.
- `build_read_pool` sets TLS engine state and marks threads as
  `IoType::ForegroundRead`. Any change that moves blocking work into or out of
  this path should be reviewed against foreground IO expectations.
- Online config is limited but real: `Endpoint::config_manager()` exposes
  `CopConfigManager`, which currently updates memory quota. If config scope
  expands, update lifecycle and operational sections in this guide together.
- In an RC unified read pool, the unary response future and the entire
  streaming response own a `ReadPoolCancellationGuard`. Dropping either
  cancels its queued task without waiting for the CPU gate to reopen and
  releases the task's live-capacity and memory permits. An IO-waiting inner
  future can also be dropped without waiting for its external operation to
  complete. A running poll sees cancellation when it returns; synchronous
  execution is not forcibly interrupted. Non-RC pools keep their existing
  detached behavior.

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
  cross-cutting request-semantic changes. One path rewrites the deadline
  afterwards: a serial batch lowers `max_execution_duration_ms` before parsing,
  then overwrites the parsed deadline of the top task and every child with the
  shared serial deadline (`parse_and_handle_unary_request`,
  `process_batch_tasks`) so they all expire at the same instant. The override
  happens before a task is scheduled, and handlers are built later from the
  overridden context.
- `endpoint.rs::parse_request_and_check_memory_locks` is the main admission and
  normalization contract. Request parsing, memory-lock checks, API-version
  dispatch, and handler construction are deliberately coupled there.
- The hot request handlers differ materially:
  DAG requests use `dag/*`, analyze requests use `statistics/analyze_context.rs`
  and `statistics/analyze.rs`, and checksum requests use `checksum.rs`.
- Cache-match version, flashback allowance, and lock-bypass/access sets are all
  correctness-sensitive metadata, not optional optimization flags.
- The DAG `flags` bitmask is a network-facing contract. Bit 12,
  `Flag::ENABLE_SHORT_CIRCUIT_EXPRESSION`, enables lazy `LogicalAnd`/`LogicalOr`
  evaluation through `EvalConfig::from_request` and `RpnExpressionBuilder`.
- Lazy evaluation is left-to-right, row-selective, and must preserve SQL
  three-valued logic; short-circuit nesting is capped at 32. If the bit is
  absent or unknown to the server, the expression is not eligible/profitable,
  or the cap is exceeded, the existing eager `FnCall` path is used.
- In short-circuit mode, skipped argument functions are not invoked, so their
  warnings/errors are suppressed, although referenced columns may still be
  eagerly decoded; when unavailable, the existing eager path and SQL-mode
  warning/error behavior are preserved.
- Full-sampling Analyze requests with `ndv_rate` below 1 select NDV rows
  after MVCC visibility checks and before filling column vectors. Histogram
  rows come from the selected rows: Bernoulli sampling keeps each at
  `sample_rate / ndv_rate`, and fixed-size reservoirs see only selected rows.
  Scan batches count skipped rows and remain bounded by visible rows.
  Sampled Analyze charges background read bandwidth for all scanned KV bytes,
  including rows not selected for NDV.
- Sampled responses set `RowSampleCollector.ndv_sample_count`, including zero.
  `count` counts all visible rows. TiKV scales NULL counts and sizes by the
  actual visible-row / selected-row ratio when serializing the response, after
  any batch merges. TiDB adds these population estimates as usual.
  Each FMSketch keeps disjoint singleton and repeated hash sets under one mask
  and size limit. TiDB estimates NDV after merging the sketches. Missing
  `ndv_rate` or a rate of one keeps the full-input path and omits
  `ndv_sample_count`.

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

## Cooperative Read Scheduling

- Boxed `BatchExecutor::next_batch` calls share a post-batch checkpoint in
  `components/tidb_query_executors/src/interface.rs`. IndexLookup's concrete
  table-scan child has the corresponding explicit checkpoint.
- `RangesScanner` checks every 32 completed get/scan operations, including
  empty ranges, when a poll-scoped cooperative context is installed. Other
  callers retain the legacy scanner rescheduling policy.
- The read-pool wrapper owns the poll time slice and CPU-budget context;
  executor/scanner code owns safe-point placement. Never keep a poll guard
  across an await. A synchronous storage operation or one compute batch can
  still overrun the slice; this is not a hard execution-time bound.
- Checkpoint changes must preserve streaming range accounting, row order and
  execution/storage statistics, including empty ranges and resumed scans.
- The fair wrapper checks CPU debt before every inner poll, including inline
  repolls that bypass queue pop. Exhaustion must use a true scheduler yield.
- Fair-task cancellation owns the inner future through a per-task
  `FutureSlot`; cancel must not wait for a running poll or drop that future
  while holding the scheduler or task-slot lock.

## Critical Invariants

- Range bounds in `ReqContext` must stay aligned with the actual request.
- Memory-lock checks must happen before serving reads that could violate lock
  semantics.
- Handler execution must respect request deadline and cancellation behavior.
- Range scanners must check the cooperative scheduling budget even when point
  lookups or interval scans produce no rows, so empty multi-range requests do
  not monopolize a read-pool worker.
- Memory quota and concurrency limiters must remain cheap and correct.
- Keep cancellation attached to the complete streaming response, not only the
  first future that submits it. A guard dropped before admission must also
  prevent a later-polled submission from creating a live task.
- Request parsing and admission must stay aligned: when the dedicated setting
  is enabled, a request class that reports quota samples to the background
  quota limiter should use the dedicated background-limited semaphore instead
  of bypassing heavy-task admission. With the setting disabled, it follows the
  ordinary request policy for the pool mode.
- When enabled, the dedicated background-limited semaphore protects all
  Analyze variants, including index, common-handle, column, mixed, and
  full-sampling Analyze, from unlimited fan-out. It is not part of the ordinary
  shared heavy-task budget; when disabled, these requests use the shared
  semaphore only in legacy Yatp pools.
- Streaming and unary response handling must preserve stats and partial-progress
  semantics.
- Batched unary result merging must preserve task identity, retry semantics,
  response-byte accounting, and memory tracing. Execution deadlines stop scans
  and serial collection, not finalization of completed results. Finalization
  is delivery of admitted work, not new work: it neither waits for
  background-limiter admission nor queues below lower-priority new scans. Its
  CPU stays attributed to the request's group and, in the fair pool, is charged
  to the high-priority lane. Fair-pool capacity and CPU gating still apply.
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
  pressure. Fair-pool requests that bypass the semaphore do not contribute to
  these metrics; explicitly capped Analyze requests still do.
- `tracker.rs` is the best place to understand slow logs, exec details, request
  lifetime accounting, and the distinction between schedule wait, snapshot
  wait, suspend time, and processing time.
- RU-v2 batch-selection work uses expression work units plus column-reference
  count. Expression work units expand flattened `AND`/`OR` chains back to their
  conceptual binary logical operations, so flattening must not reduce
  `tikv_coprocessor_executor_work_total_batch_selection`.
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
  inspect `dag/*`, expression evaluation, snapshot/store setup, and query-side
  statistics paths
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
  runtime request context shared through execution; immutable once the request
  is scheduled (a serial batch overrides its deadline before that)
- Light task threshold:
  the execution-time budget before a unary coprocessor future must acquire a
  permit, when its selected pool/request policy enables a semaphore
- Snapshot wait:
  request time spent between scheduling and obtaining the storage snapshot

## Related Components

- `src/server/service/kv.rs` is the RPC entry point.
- `src/storage` provides snapshots and the lock-related behavior.
- `components/in_memory_engine` can accelerate snapshot-backed reads indirectly.
