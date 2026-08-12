# Preserve Sub-Millisecond Durations in Online Configuration

## Status

Proposed design for [TiKV issue #19956](https://github.com/tikv/tikv/issues/19956).

## Summary

TiKV parses online duration values into `ReadableDuration`, but then converts them through `ConfigValue::Duration(u64)`, where the integer is interpreted as whole milliseconds. This silently changes `200us` into a zero duration before validation and dispatch.

Change `ConfigValue::Duration` to carry `std::time::Duration`. Keep `ReadableDuration` as the configuration-facing parser, formatter, and serde type. This removes the lossy unit conversion without changing SQL input, status JSON, TOML, or the generated `OnlineConfig` protocol.

The same change must correct raftstore's dynamic config metric projection. Most raftstore duration metrics use seconds, but `raft_write_wait_duration` intentionally uses microseconds. Online updates must preserve those existing units.

## Background

`ReadableDuration` is a tuple newtype around `std::time::Duration` and supports `d`, `h`, `m`, `s`, `ms`, and `us` in configuration text (`components/tikv_util/src/config.rs:417`, `components/tikv_util/src/config.rs:498`). Microsecond support was intentionally added in commit `7b677d60ab3b60b3ef4750da09718ba8114060ed` because millisecond precision was too coarse for fine-grained configuration, explicitly including delayed raft-message flushing. The millisecond-based `ConfigValue` representation predates that change and was not updated with it.

`raftstore.raft-write-wait-duration` is one such setting:

- its type is `ReadableDuration` (`components/raftstore/src/store/config.rs:371`);
- its default is `20us` (`components/raftstore/src/store/config.rs:627`);
- values above `1ms` are rejected (`components/raftstore/src/store/config.rs:943`);
- the async write worker consumes the underlying `Duration` at startup and after online updates (`components/raftstore/src/store/async_io/write.rs:825`, `components/raftstore/src/store/async_io/write.rs:1294`);
- its config gauge is expressed in microseconds (`components/raftstore/src/store/config.rs:1342`).

The field is marked `#[doc(hidden)]`, which affects generated Rust documentation only. It is not marked `#[online_config(hidden)]`. The existing derive therefore includes it in `typed`, `diff`, `update`, and `get_encoder`; ordinary `GET /config` exposes it to TiDB `SHOW CONFIG`, and `POST /config` accepts updates. No field-attribute or derive change is needed.

The static TOML path already preserves these values. For example, the integration fixture contains `raft-write-wait-duration = "444us"` (`tests/integrations/config/test-custom.toml:225`). The bug is specific to the online-config intermediate representation.

## Current Behavior

`ConfigValue` stores durations as an undocumented primitive whose effective unit is milliseconds:

```rust
pub enum ConfigValue {
    Duration(u64),
    // ...
}
```

`components/online_config/src/lib.rs:14`

The conversion pair is lossy (`components/tikv_util/src/config.rs:482`):

```rust
ReadableDuration(duration)
    -> ConfigValue::Duration(duration.as_millis() as u64)

ConfigValue::Duration(millis)
    -> ReadableDuration(Duration::from_millis(millis))
```

Therefore:

```text
"200us"
   |
   v
ReadableDuration(200us)
   |
   | as_millis()
   v
ConfigValue::Duration(0)
   |
   | Duration::from_millis(0)
   v
ReadableDuration::ZERO (displayed as "0s")
   |
   +--> validation, module dispatch, runtime config, metrics
```

The online update is accepted but changes the effective value. Persistence then writes the caller's original string (`src/config/mod.rs:5018`, `src/config/mod.rs:5221`), so runtime can hold `ReadableDuration::ZERO` while the file contains `"200us"`. A restart loads the static TOML path and restores `200us`, making behavior differ before and after restart.

## Requirements

1. Preserve every duration representable by the current `ReadableDuration` parser through `ConfigValue`, validation, dispatch, and typed runtime configuration.
2. Preserve existing SQL/config input syntax and `ReadableDuration` output syntax.
3. Preserve original-string TOML patching and static TOML compatibility.
4. Preserve existing online-updatable raftstore duration metric names and units. Where a startup series exists, online updates must target the same series.
5. Avoid a new crate, compatibility variant, feature flag, or duration abstraction.
6. Leave non-duration `ConfigValue` variants and generated `OnlineConfig` behavior unchanged.

## Proposed Design

### 1. Store a typed duration in `ConfigValue`

Change the enum payload in `components/online_config/src/lib.rs`:

```rust
use std::time::Duration;

pub enum ConfigValue {
    Duration(Duration),
    // unchanged variants
}
```

Add the same conversion support used by primitive variants:

```rust
impl From<Duration> for ConfigValue
impl From<ConfigValue> for Duration
impl From<&ConfigValue> for Duration
```

These implementations are legal because `ConfigValue` is a local type in the trait signature. Add `impl_from!(Duration, Duration)` and `impl_into!(Duration, Duration)` alongside the existing primitive macro invocations.

`ConfigValue` is an in-process value. It does not implement serde and is not a persisted or network wire type. Increasing the payload from a scalar to `Duration` therefore requires no data migration or protocol versioning.

`Duration` is `Copy` and adds no allocation. Its physical layout and the layout of `ConfigValue` are Rust implementation details, not compatibility contracts. A probe with the repository's x86_64 toolchain measured both the current and proposed `ConfigValue` at 56 bytes because the `Module(HashMap<...>)` variant already determines the enum size. This measurement is performance evidence, not an ABI guarantee, and should not become a size assertion.

### 2. Make `ReadableDuration` conversion lossless

Update `components/tikv_util/src/config.rs` so the wrapper is copied directly into and out of the payload:

```rust
impl From<ReadableDuration> for ConfigValue {
    fn from(duration: ReadableDuration) -> ConfigValue {
        ConfigValue::Duration(duration.0)
    }
}

impl From<ConfigValue> for ReadableDuration {
    fn from(value: ConfigValue) -> ReadableDuration {
        ReadableDuration(Duration::from(value))
    }
}
```

No parsing, validation, serde, or arithmetic behavior in `ReadableDuration` changes.

### 3. Preserve existing display where it already had a value

`ConfigValue::Display` is used for logs and debug output, not status JSON or TOML. Preserve the exact historical output for millisecond-aligned values whose total milliseconds fit the old `u64` payload, and add precise output for values the old representation could not express:

```text
0s          -> 0ms
1ms         -> 1ms
1s          -> 1000ms
200us       -> 200us
1ms200us    -> 1200us
1ns         -> 1ns
```

The formatter selects the coarsest exact unit without allocating:

```rust
let subsec_nanos = duration.subsec_nanos();
if subsec_nanos % 1_000_000 == 0 {
    write!(f, "{}ms", duration.as_millis())
} else if subsec_nanos % 1_000 == 0 {
    write!(f, "{}us", duration.as_micros())
} else {
    write!(f, "{}ns", duration.as_nanos())
}
```

Checking the fractional nanoseconds is sufficient because every whole second is divisible by all three units. The total-unit accessors return `u128`; the formatter must write those values directly and must not narrow them to `u64`. This keeps `Duration::MAX` and other direct typed values total even though the public parser remains microsecond-granular.

This small formatter avoids importing `ReadableDuration` into `online_config` and avoids relying on the unstable textual form of `Duration`'s `Debug` implementation.

### 4. Keep public configuration boundaries unchanged

The online parser continues to parse strings as `ReadableDuration` (`src/config/mod.rs:4997`). The only difference is that `ConfigValue::from(parsed_duration)` no longer truncates.

No change is required to:

- generated `OnlineConfig::typed`, `diff`, or `update`; the derive delegates to `From` and `TryInto` (`components/online_config/online_config_derive/src/lib.rs:158`, `components/online_config/online_config_derive/src/lib.rs:201`, `components/online_config/online_config_derive/src/lib.rs:250`);
- status `/config` JSON, which serializes typed config fields through `ReadableDuration` (`src/server/status_server/mod.rs:188`);
- canonical TOML serde;
- online TOML patching, which writes the original input string;
- the accepted duration grammar. Nanosecond input is not added by this change.

Both ordinary `GET /config` and `GET /config?full=true` continue to serialize the typed `ReadableDuration`. Tests must exercise the ordinary endpoint because TiDB `SHOW CONFIG` uses it and because only that path would expose an accidental `#[online_config(hidden)]` regression.

### 5. Update direct payload consumers

#### RocksDB option adapter

`src/config/mod.rs:2429` converts a duration payload back to `ReadableDuration` and then to whole seconds for RocksDB options. Keep this behavior. The updated conversion implementation makes the existing branch lossless until the intentional RocksDB seconds boundary.

#### Intentional downstream unit adapters

The shared online-config path will preserve the typed duration, but several subsystem consumers intentionally operate in coarser units. This design does not change those subsystem contracts:

- lock-manager `wake_up_delay_duration` stores whole milliseconds in an `AtomicU64` (`src/server/lock_manager/config.rs:123`);
- resource-metering recorder precision stores whole milliseconds (`components/resource_metering/src/recorder/mod.rs:130`);
- resolved-ts slow-region threshold arithmetic uses whole milliseconds (`components/resolved_ts/src/endpoint.rs:598`);
- in-memory-engine GC physical-timestamp arithmetic uses whole milliseconds (`components/in_memory_engine/src/background.rs:401`);
- storage `max-ts.max-drift` is forwarded precisely by `StorageConfigManger`, then `ConcurrencyManager` stores whole milliseconds because it adds the allowance to TSO physical timestamps (`src/storage/config_manager.rs:120`, `components/concurrency_manager/src/lib.rs:589`, `components/concurrency_manager/src/lib.rs:469`);
- existing raftstore tick and timeout calculations that call `ReadableDuration::as_millis()` remain millisecond-based.

These are downstream domain boundaries, not `ConfigValue` representation requirements. The reported `raft_write_wait_duration` path is different: the async writer consumes `std::time::Duration` directly and therefore receives the newly preserved value.

#### Raftstore dynamic metrics

`Config::write_change_into_metrics` currently assumes that the payload is milliseconds and uses integer division (`components/raftstore/src/store/config.rs:1381`). Replace that assumption with typed conversion:

```text
metric label:
    consistency_check_interval
        -> consistency_check_interval_seconds
    every other field
        -> unchanged field name

metric value:
    raft_write_wait_duration
        -> duration.as_micros() as f64
    every other duration
        -> duration.as_secs_f64()
```

This matches `Config::write_into_metrics`:

- ordinary duration labels remain seconds;
- `raft_write_wait_duration` remains microseconds.
- the historical `consistency_check_interval_seconds` label is updated instead of creating a new `consistency_check_interval` series.

Both mappings are required. Converting every duration to seconds would change the existing unit of `tikv_config_raftstore{name="raft_write_wait_duration"}` after an online update. Reusing every incoming duration field name would create a second series for `consistency_check_interval` and leave the startup series stale (`components/raftstore/src/store/config.rs:1241`). A source audit found no other directly online-updatable raftstore duration whose startup metric label differs from its field name.

Pre-existing metric drift for nested non-duration batch-system fields is outside this fix. Those changes arrive as `ConfigValue::Module` and are ignored by the current scalar metric projection; correcting that broader behavior is unrelated to duration precision.

Some online-updatable duration fields have no startup series, including `disk_hang_timeout`, `check_long_uncommitted_interval`, and `long_uncommitted_base_threshold`. They remain dynamic-only if updated; this fix corrects their typed duration-to-seconds projection but does not add new startup metrics.

#### Direct test constructors

Replace manual millisecond construction in `components/resolved_ts/tests/mod.rs:54` with the typed payload:

```rust
ConfigValue::Duration(new_interval)
```

Update the duration type marker in `src/config/mod.rs:5913` from an integer to `Duration::ZERO` or construct it through `ConfigValue::from(Duration::ZERO)`.

### 6. Preserve existing update ordering and failure semantics

This change preserves precision; it does not make online configuration transactional. `ConfigController::update_impl` keeps its existing order (`src/config/mod.rs:5177`):

```text
parse typed changes
        |
        v
clone current config -> apply -> validate/normalize
        |
        | validation error: return without dispatch or persistence
        v
dispatch changed modules
        |
        | manager error: prior manager side effects are not rolled back
        v
commit controller's in-memory config
        |
        v
patch the node-local TOML file from the original input strings
        |
        | persistence error: runtime/controller state may already be newer
        v
success
```

The precision fix depends on the first boundary: validation receives the exact duration before any runtime side effect. An exact invalid value such as `1001us` must be rejected with controller, runtime, and persisted state unchanged. Existing partial-dispatch and post-commit persistence-failure behavior remains unchanged and is not redesigned here.

Raftstore's async writer observes `VersionTrack` lazily after completing a database write (`components/raftstore/src/store/async_io/write.rs:1294`). The refresh updates the configured `wait_duration` and `wait_duration_hint`. It intentionally preserves `wait_duration_adaptive`, which is learned state introduced by commit `41c78f204a4eadedb867891cb3bdcff18de28703`; resetting that state would be a separate adaptive-batching behavior change. The regression test therefore runs with adaptive batching disabled when asserting the immediate configured wait and does not add a reset rule.

## Why Not `ReadableDuration` in `ConfigValue`?

`ReadableDuration` is defined in `tikv_util`, and `tikv_util` already depends on `online_config` (`components/tikv_util/Cargo.toml:44`). Making `online_config` depend on `tikv_util` would create a Cargo dependency cycle:

```text
online_config
     ^
     |
  tikv_util
     |
     +---- proposed dependency back to online_config
```

Moving `ReadableDuration` into another crate would solve the cycle but adds a new ownership boundary and migration unrelated to the precision bug. `std::time::Duration` provides the required typed, lossless representation without structural changes.

## Alternatives Considered

### Keep milliseconds and reject sub-millisecond values

This prevents silent corruption but leaves static and online configuration with different capabilities. It also makes the affected `20us`-default setting impossible to configure online. Rejected.

### Change the `u64` unit to microseconds or nanoseconds

This retains primitive unit ambiguity and silently changes every direct constructor and consumer. A typed standard duration is safer and simpler. Rejected.

### Add a second precise-duration variant

`ConfigValue` is not a serialized compatibility protocol, so a legacy variant has no consumer to protect. Two variants would duplicate matching and conversion logic and permit inconsistent internal states. Rejected.

### Move `ReadableDuration` to a lower-level crate

This would allow `ConfigValue::Duration(ReadableDuration)` but expands the change into crate ownership and dependency restructuring. It provides no functional advantage over a standard duration payload. Rejected.

## Compatibility and Rollout

### User-visible compatibility

Unchanged:

- SQL `SET CONFIG` values remain strings such as `200us`.
- `SHOW CONFIG` and status JSON remain formatted by `ReadableDuration`.
- static and updated TOML remain string-valued and accept the same units.
- parser-derived, millisecond-aligned `ConfigValue` debug text remains byte-for-byte unchanged. Values whose total milliseconds exceed `u64::MAX` intentionally stop wrapping and become exact.
- all non-duration variants remain unchanged.

Changed intentionally:

- sub-millisecond online updates now affect runtime state instead of becoming zero;
- internal debug text can now show `us` or `ns` for values that previously appeared as a truncated millisecond value;
- dynamic raftstore metrics become consistent with startup metrics.
- any duration that is not an exact multiple of `1ms` now reaches validation and normalization unchanged. Depending on the existing rule, an update may be newly rejected, newly accepted, no longer replaced by a default, or newly enter a clamp-and-warn path.

The validation-impact audit identified these representative behavior changes:

| Outcome | Field and value | Old behavior | Fixed behavior |
|---|---|---|---|
| Newly rejected | `raft-write-wait-duration = 1001us` | rounds to valid `1ms` | rejected above the exact `1ms` maximum |
| Newly rejected | resource-metering `precision = 1h1us` | rounds to valid `1h` | rejected above the exact `1h` maximum |
| Newly rejected | resource-metering `report-receiver-interval = 500s1us` with `precision = 1s` | rounds to valid `500s` | rejected above `precision * 500` |
| Newly rejected | enabled in-memory-engine `gc-run-interval = 10m1us` | rounds to valid `10m` | rejected above the exact `10m` maximum |
| Newly rejected | `quota.max-delay-duration = ReadableDuration::micros(u64::MAX / 1000 + 1)` through the typed update path | rounds below the maximum | rejected above the exact maximum |
| Newly accepted | `storage.max-ts.max-drift = 15s1us` with the default `15s` cache interval | rounds to `15s` and fails the `<=` rule | remains greater than `15s` and is accepted |
| Newly accepted | `resolved-ts.advance-ts-interval = 200us` | rounds to zero and is rejected | remains nonzero and is accepted |
| Default substitution removed | `cdc.min-ts-interval = 200us` | rounds to zero and is replaced with the `1s` default | remains `200us` |
| Normalization becomes visible | `raftstore.max-entry-cache-warmup-duration = 10m1us` | rounds to `10m` before validation | reaches validation exactly, then clamps to `10m` with the existing warning |

For the max-ts case, the fixed typed configuration retains `15s1us`, while runtime enforcement intentionally uses a `15000ms` drift allowance. Validation operates on the precise configured durations; enforcement remains millisecond-based because TSO physical timestamps are milliseconds. This typed/runtime difference is a downstream domain boundary, not residual online-config truncation.

### Mixed versions and rollback

No cross-node or cross-version `ConfigValue` exchange exists. TiDB sends independent HTTP requests containing string values, and each TiKV node constructs its own in-process value, so the representation change requires no wire migration.

`SET CONFIG` is best-effort and non-atomic across nodes: TiDB sends one request per selected node, continues after failures, and reports node failures as statement warnings. Behavior is not uniform while old and fixed TiKV nodes coexist. Old nodes round every non-millisecond-aligned duration down; fixed nodes preserve it, which can change runtime values, validation outcomes, normalization, and persisted node-local files.

During a rolling upgrade, operators must defer every duration update that is not an exact multiple of `1ms` until every TiKV node has been upgraded. Exact millisecond alignment only avoids the representation difference introduced by this fix; it is not a general compatibility guarantee. Any update still requires the field name, parser, validation, and runtime semantics to be equivalent on every serving version.

After any cluster-wide update, operators must inspect `SHOW WARNINGS` and verify the result for every node with `SHOW CONFIG`. A retry resends the update to all selected nodes; there is no automatic per-node retry ledger. Deployment-managed configuration must also be reconciled because each TiKV persists to its own local configuration file.

Rolling back to an old binary does not require data conversion. A persisted `"200us"` remains valid TOML and is preserved at startup. However, subsequent online updates on the old binary regain the original truncation bug until the fixed binary is restored.

No feature flag is needed.

## Implementation Plan

Execute the change in test-first increments. Do not write all tests after the representation change.

### Increment 1: Lock the existing boundary behavior

Add these tests before production changes:

- failing `ReadableDuration::micros(200) -> ConfigValue -> ReadableDuration` equality test;
- failing `to_change_value("200us", ...)` exact-payload test;
- failing controller, raftstore-manager, persistence/reload, async-writer, and dynamic-metric tests listed below;
- one exact scalar-bound test (`1001us` wait duration) and one exact relational-bound test (`15s1us` max-ts drift), which cover the distinct validation classes exposed by the representation fix;
- failing formatter precision/maximum tests, constructed through the existing `ConfigValue::from(ReadableDuration)` boundary;
- currently-green characterization tests for millisecond-aligned display, ordinary/full status JSON, existing TOML text, max-ts's millisecond enforcement boundary, and non-duration variants.

The compatibility table is an impact audit, not a requirement for one test per field. Resource-metering, in-memory-engine, quota, resolved-ts, CDC, and warmup examples repeat the scalar, relational, nonzero, default, or clamp classes already exercised through the shared conversion seam. Add subsystem-specific tests only if implementation work changes those subsystem validators.

The async-writer test scaffolding must retain the `Arc<VersionTrack<Config>>` currently created inline and discarded by `TestWorker::new` (`components/raftstore/src/store/async_io/write_tests.rs:201`). Its hot update must call `Config::update` with a `ConfigChange` containing `ConfigValue::from(ReadableDuration::micros(200))`; directly assigning `ReadableDuration::micros(200)` would bypass the bug and incorrectly pass before the fix.

The formatter tests use `ConfigValue::from(ReadableDuration(...))`, so all can compile against the old payload. Exact expected maximum outputs are `18446744073709551615ms` for `Duration::from_millis(u64::MAX)` and `18446744073709551615999999999ns` for `Duration::MAX` on the current toolchain. Use `Duration::MAX` in code rather than treating its current internal representation as a portable ABI guarantee.

### Increment 2: Change the payload and every type-dependent consumer atomically

The enum shape cannot compile while old payload consumers remain. Make one production increment containing all of the following:

1. Change `ConfigValue::Duration`, its standard-duration conversions, and its exact `ms`/`us`/`ns` formatter in `components/online_config/src/lib.rs`.
2. Replace the lossy `ReadableDuration` conversion pair in `components/tikv_util/src/config.rs`.
3. Update explicit duration construction/type markers in `components/resolved_ts/tests/mod.rs` and `src/config/mod.rs`.
4. Preserve the RocksDB whole-second adapter behavior.
5. Implement the duration value and `consistency_check_interval_seconds` label mappings in `components/raftstore/src/store/config.rs`.

Run every Increment 1 regression. All must now compile and turn green.

### Increment 3: Document and validate

1. Update `doc/maintenance-guides/components/raftstore.md` with the `VersionTrack` to async-writer refresh path, seconds-based duration metrics, the microsecond wait-duration exception, and the consistency-check label alias.
2. Update `doc/maintenance-guides/src/storage.md` with the max-ts dynamic-config path and the deliberate precise-typed-config to millisecond-TSO-enforcement boundary.
3. Include an operator-facing PR release note: non-millisecond-aligned updates are now exact and may be newly accepted, rejected, defaulted differently, or normalized differently by existing rules; none must be issued during a mixed-version rolling upgrade.
4. Run focused tests followed by the repository-wide `make dev` gate.

The derive macro source should be reviewed but not modified. A change there would add complexity without addressing the representation seam.

## Test Plan

Bug-regression tests must be written before their production fix and observed failing for the precision bug. Characterization tests that preserve existing millisecond, TOML, status, and non-duration behavior must be observed passing before and after the change.

| Scenario | Location | Pre-fix result | Required result |
|---|---|---|---|
| `ReadableDuration -> ConfigValue -> ReadableDuration` for `200us` | `components/tikv_util/src/config.rs::test_config_value_duration_round_trip_preserves_sub_millisecond` | equals `ReadableDuration::ZERO` (display `0s`) | exactly `200us` |
| Duration display through the existing wrapper conversion | `components/tikv_util/src/config.rs::test_config_value_duration_display_preserves_precision` | `200us` becomes `0ms`; maximum duration narrows | exact outputs for aligned milliseconds, `200us`, `1200us`, `1ns`, `18446744073709551615ms`, and `18446744073709551615999999999ns` |
| String parsing to typed change | `src/config/mod.rs::test_to_config_change_preserves_sub_millisecond_duration` | payload is zero | payload contains `Duration::from_micros(200)` |
| Controller state | `tests/integrations/config/test_config_client.rs::test_update_config_preserves_sub_millisecond_duration` | affected field becomes zero | field equals `ReadableDuration::micros(200)` |
| Live raftstore config dispatch | `tests/integrations/config/dynamic/raftstore.rs::test_update_raftstore_config_preserves_sub_millisecond_duration` | callback observes zero | callback observes `200us` |
| Async writer cached value through `ConfigValue` | `components/raftstore/src/store/async_io/write_tests.rs::test_async_writer_duration_hot_update_preserves_sub_millisecond` | `Config::update` truncates the track update, so writer refresh stores zero | recorder's `wait_duration` and `wait_duration_hint` become `200us` |
| Persist online update | `tests/integrations/config/test_config_client.rs::test_write_update_to_file_preserves_sub_millisecond_duration` | file says `200us`, controller state is zero | file and controller state both represent `200us` |
| Reload persisted update | `tests/integrations/config/test_config_client.rs::test_update_from_toml_file_preserves_sub_millisecond_duration` | `diff()` converts incoming `200us` to zero before applying it | reloaded runtime is exactly `200us`; initialize current state to a distinct non-`200us` value |
| Status JSON text and online visibility | `src/server/status_server/mod.rs::test_config_endpoint_preserves_duration_text` | static typed config is already precise | ordinary and full endpoints both contain literal fields `raft-write-wait-duration = "200us"` and `snap-wait-split-duration = "1ms"`; ordinary output proves `#[doc(hidden)]` does not suppress online visibility |
| Metric values and startup/dynamic identity | `components/raftstore/src/store/config.rs::test_write_change_into_metrics_preserves_duration_units_and_labels` | duration values are truncated and consistency-check writes the wrong label | ordinary `1ms` is `0.001` seconds, wait `200us` is `200` microseconds, and startup `consistency_check_interval_seconds` is updated without an unaliased series |
| Millisecond regression | conversion/controller tests with `1ms`, `1s` | passes | remains unchanged |
| Newly precise raftstore validation | `tests/integrations/config/test_config_client.rs::test_update_config_rejects_exact_over_limit_duration` with `1001us` | accepted after truncating to `1ms` | rejected and state remains unchanged |
| Max-ts relational acceptance | `src/storage/config.rs::test_online_max_ts_drift_preserves_relational_fraction` | `15s1us` rounds to `15s` and is rejected | exact value is accepted against the default `15s` cache interval |
| Max-ts runtime boundary after dispatch | `src/storage/config_manager.rs::test_online_max_ts_drift_dispatch_uses_millisecond_tso_boundary` | runtime allowance is `15000ms` | remains a green characterization: dispatching `15s1us` installs an effective `15000ms` drift allowance |
| Whole-config TOML characterization | existing `tests/integrations/config/mod.rs::test_toml_serde` and `test_serde_custom_tikv_config` | pass | remain passing with unchanged text semantics |
| Non-duration `ConfigValue` characterization | existing `components/online_config/src/lib.rs::test_update_fields` and `test_not_update` | pass | remain passing unchanged |

The live raftstore manager test should use the existing bounded `StoreMsg::Validate` callback rather than sleeps (`tests/integrations/config/dynamic/raftstore.rs:140`). Capture the callback result without asserting it, call `system.shutdown()`, and only then assert the captured result so panic or assertion failure cannot leave raftstore workers running. This proves dispatch into the raftstore `VersionTrack`, but not the async writer's cached recorder.

The max-ts runtime-boundary test should dispatch a nested `max_ts.max_drift` change containing `15s1us` through `StorageConfigManger`, retain a clone of its `ConcurrencyManager`, and exercise the public max-ts limit behavior with a deterministic TSO provider and `ActionOnInvalidMaxTs::Error`: after setting a base TSO limit, base plus `15000ms` is allowed and base plus `15001ms` is rejected. Coupled with the red-to-green validation test proving that typed `15s1us` survives online conversion, this green characterization proves that the deliberate enforcement boundary remains whole milliseconds. Do not add a production getter solely for the test.

Add a separate worker-level test using the existing `TestWorker` and write-task setup in `components/raftstore/src/store/async_io/write_tests.rs`. Retain its configuration track as a test-struct field and disable adaptive batching. Initialize the worker with one wait duration, apply a `ConfigChange` to that track through `Config::update` and `ConfigValue::from(ReadableDuration::micros(200))`, enqueue one existing test `WriteTask`, call `worker.write_to_db(true)`, and assert after the write that both `worker.batch.recorder.wait_duration` and `wait_duration_hint` equal `Duration::from_micros(200)`. Preserve `wait_duration_adaptive` as the existing learned state. This crosses the lossy seam before the fix and executes the production refresh block at `components/raftstore/src/store/async_io/write.rs:1294` without sleeps or a test-only production helper.

The metric regression must be one deterministic unit test in the `store/config.rs` test module:

1. reset `CONFIG_RAFTSTORE_GAUGE` once;
2. call `Config::write_into_metrics` and record the startup values and collected label set;
3. call `Config::write_change_into_metrics` with `200us` for `raft_write_wait_duration`, `1ms` for an ordinary seconds-based duration, and a changed `consistency_check_interval`;
4. collect the gauge again and assert that the same startup series changed to `200`, `0.001`, and the requested consistency-check seconds value;
5. inspect collected label pairs or series count to prove `consistency_check_interval` was not created. Do not call `with_label_values(["consistency_check_interval"])` for the absence assertion because that call would create the forbidden series.

The status characterization must start `StatusServer` on an ephemeral port with a `ConfigController` whose typed config contains `raft_write_wait_duration = 200us` and `snap_wait_split_duration = 1ms`. Request both ordinary `/config` and `/config?full=true`, capture and parse both responses without asserting them, call `StatusServer::stop()`, and only then compare `raftstore.raft-write-wait-duration` and `raftstore.snap-wait-split-duration` to the independent literals `"200us"` and `"1ms"`. Do not derive expected JSON with `get_encoder()` or the same serializer under test.

Suggested focused commands:

```bash
./scripts/test test_config_value_duration_round_trip_preserves_sub_millisecond -- --nocapture
./scripts/test test_config_value_duration_display_preserves_precision -- --nocapture
./scripts/test test_to_config_change_preserves_sub_millisecond_duration -- --nocapture
./scripts/test test_update_config_preserves_sub_millisecond_duration -- --nocapture
./scripts/test test_update_raftstore_config_preserves_sub_millisecond_duration -- --nocapture
./scripts/test test_async_writer_duration_hot_update_preserves_sub_millisecond -- --nocapture
./scripts/test test_write_update_to_file_preserves_sub_millisecond_duration -- --nocapture
./scripts/test test_update_from_toml_file_preserves_sub_millisecond_duration -- --nocapture
./scripts/test test_config_endpoint_preserves_duration_text -- --nocapture
./scripts/test test_write_change_into_metrics_preserves_duration_units_and_labels -- --nocapture
./scripts/test test_update_config_rejects_exact_over_limit_duration -- --nocapture
./scripts/test test_online_max_ts_drift_preserves_relational_fraction -- --nocapture
./scripts/test test_online_max_ts_drift_dispatch_uses_millisecond_tso_boundary -- --nocapture
./scripts/test test_toml_serde -- --nocapture
./scripts/test test_serde_custom_tikv_config -- --nocapture
./scripts/test test_update_fields -- --nocapture
./scripts/test test_not_update -- --nocapture
cargo check -p online_config -p tikv_util -p raftstore -p tikv
make dev
```

Each focused command must report at least one executed test and the named test as passed. Treat `running 0 tests` as failure rather than successful verification.

## Manual Verification

Use a one-TiKV TiUP playground with an explicit config file. A single TiKV is sufficient for this behavior; mixed-version behavior is governed by the rollout restriction above. The same file is reused across a full playground restart, so the restart check exercises persisted configuration rather than a new default.

Prerequisites are `tiup`, `tmux`, a MySQL client, `curl`, and `jq`, with default playground ports `4000`, `20160`, `20180`, and `2379` available. Run the script from the TiKV repository root.

```bash
set -euo pipefail

ROOT=$PWD
TAG="issue-19956-$USER-$$"
SESSION="$TAG"
CFG=$(mktemp -t issue-19956.XXXXXX.toml)
LOG=$(mktemp -t issue-19956.XXXXXX.log)
ORIGINAL=""
TIUP_DATA_HOME="${TIUP_HOME:-$HOME/.tiup}"
INTERNAL_CFG="$TIUP_DATA_HOME/data/$TAG/tikv-0/tikv.toml"

start_cluster() {
  tmux new-session -d -s "$SESSION" \
    "bash -lc 'tiup playground nightly --tag \"$TAG\" --db 1 --pd 1 --kv 1 --tiflash 0 --without-monitor --kv.binpath \"$ROOT/target/debug/tikv-server\" --kv.config \"$CFG\" >\"$LOG\" 2>&1'"
  for _ in $(seq 1 120); do
    mysqladmin -u root -h 127.0.0.1 -P 4000 ping >/dev/null 2>&1 && return
    sleep 1
  done
  return 1
}

stop_cluster() {
  tmux has-session -t "$SESSION" 2>/dev/null || return 0
  tmux send-keys -t "$SESSION" C-c || true
  for _ in $(seq 1 60); do
    tmux has-session -t "$SESSION" 2>/dev/null || return 0
    sleep 1
  done
  tmux kill-session -t "$SESSION" 2>/dev/null || true
  ! tmux has-session -t "$SESSION" 2>/dev/null
}

restore_and_cleanup() {
  cleanup_status=0
  if [ -n "$ORIGINAL" ] && mysqladmin -u root -h 127.0.0.1 -P 4000 ping >/dev/null 2>&1; then
    mysql -u root -h 127.0.0.1 -P 4000 -e \
      "SET CONFIG tikv \`raftstore.raft-write-wait-duration\` = '$ORIGINAL';" || true
  fi
  stop_cluster || cleanup_status=$?
  if ! tmux has-session -t "$SESSION" 2>/dev/null; then
    tiup clean "$TAG" >/dev/null 2>&1 || cleanup_status=$?
  else
    cleanup_status=1
  fi
  if [ "$cleanup_status" -eq 0 ]; then
    rm -f "$CFG" "$LOG"
  fi
  return "$cleanup_status"
}
trap restore_and_cleanup EXIT

make build
start_cluster

ORIGINAL=$(mysql -u root -h 127.0.0.1 -P 4000 -Nse \
  "SHOW CONFIG WHERE type='tikv' AND name='raftstore.raft-write-wait-duration'" \
  | awk -F '\t' 'NR == 1 { print $4 }')
test -n "$ORIGINAL"

WARNINGS=$(mysql -u root -h 127.0.0.1 -P 4000 -Nse \
  "SET CONFIG tikv \`raftstore.raft-write-wait-duration\` = '200us'; SHOW WARNINGS;")
test -z "$WARNINGS"

mysql -u root -h 127.0.0.1 -P 4000 -Nse \
  "SHOW CONFIG WHERE type='tikv' AND name='raftstore.raft-write-wait-duration'" \
  | awk -F '\t' '$4 == "200us" { found=1 } END { exit !found }'

curl -fsS 'http://127.0.0.1:20180/config' \
  | jq -e '.raftstore["raft-write-wait-duration"] == "200us"'

curl -fsS 'http://127.0.0.1:20180/metrics' \
  | awk '$1 == "tikv_config_raftstore{name=\"raft_write_wait_duration\"}" && $2 == 200 { found=1 } END { exit !found }'

# TiUP copies --kv.config into its tagged instance directory. Copy the actual
# online-updated file back to the input path before recreating the playground.
test -s "$INTERNAL_CFG"
cp "$INTERNAL_CFG" "$CFG"

# Restart the entire playground while reusing the persisted TiKV config.
stop_cluster
tiup clean "$TAG"
start_cluster

mysql -u root -h 127.0.0.1 -P 4000 -Nse \
  "SHOW CONFIG WHERE type='tikv' AND name='raftstore.raft-write-wait-duration'" \
  | awk -F '\t' '$4 == "200us" { found=1 } END { exit !found }'
curl -fsS 'http://127.0.0.1:20180/config' \
  | jq -e '.raftstore["raft-write-wait-duration"] == "200us"'
curl -fsS 'http://127.0.0.1:20180/metrics' \
  | awk '$1 == "tikv_config_raftstore{name=\"raft_write_wait_duration\"}" && $2 == 200 { found=1 } END { exit !found }'

# Restore before normal exit; the trap repeats this safely on failures.
mysql -u root -h 127.0.0.1 -P 4000 -e \
  "SET CONFIG tikv \`raftstore.raft-write-wait-duration\` = '$ORIGINAL';"
ORIGINAL=""
```

Pass conditions are encoded as command exit statuses: `SET CONFIG` produces no node warning; SQL, ordinary status JSON, and the metric all report `200us`/`200` before and after restart; the captured original value is restored on success and best-effort restored on failure. Teardown first requests graceful termination, forcibly kills a session that survives the bounded wait, verifies that no tmux session remains, and removes temporary files only after `tiup clean` succeeds.

## Risks and Mitigations

| Risk | Mitigation |
|---|---|
| A direct millisecond assumption is missed | Search every `ConfigValue::Duration` constructor/match and audit downstream `ReadableDuration::as_millis()` boundaries. The compiler catches payload arithmetic and constructors, but wildcard matches and downstream unit adapters require review. |
| Internal duration formatting changes unexpectedly | Preserve exact `Nms` output for parser-derived millisecond-aligned values in the old representable range and test the new `us`/`ns` and maximum cases. |
| Dynamic metrics change units | Special-case `raft_write_wait_duration`; test it alongside an ordinary seconds-based duration label. |
| A dynamic metric updates the wrong label | Map `consistency_check_interval` to its established `consistency_check_interval_seconds` label and test that the unaliased series is not created. |
| Persistence and runtime diverge after a successful precise update | Test one online update through controller state, persisted TOML, and reload. Preserve the existing contract that a later persistence failure can leave runtime state newer than the file. |
| Mixed versions or partial fanout apply the same string differently | Defer non-millisecond-aligned values until all nodes are upgraded, inspect `SHOW WARNINGS`, and verify `SHOW CONFIG` per node. Do not treat millisecond alignment as a general compatibility guarantee. |
| Async adaptive state is changed unintentionally | Update only the configured base and hint; preserve the existing learned `wait_duration_adaptive` state and test with adaptive batching disabled. |
| The larger duration payload affects performance | The conversion allocates nothing and runs only on configuration paths. The current x86_64 toolchain measures the enum unchanged at 56 bytes because another variant dominates; do not rely on that layout as an ABI contract. |

## Non-Goals

- Adding nanosecond syntax to `ReadableDuration`.
- Changing `ReadableDuration` parsing, canonical serialization, or validation rules.
- Redesigning `ConfigValue` error handling or replacing existing panic-based conversions.
- Changing RocksDB duration option units.
- Correcting unrelated validation message formatting, including the current rounded millisecond text for a rejected `1001us` wait duration.
- Correcting pre-existing dynamic metric drift for nested non-duration batch-system fields.
- Refactoring the `OnlineConfig` derive or config-manager architecture.

## Success Criteria

The change is complete when:

- `200us` survives every online-config conversion and reaches the live raftstore configuration exactly;
- public JSON and TOML formats are unchanged;
- parser-derived millisecond-aligned behavior remains unchanged, while formerly overflowing debug values become exact;
- dynamically projected raftstore durations preserve existing units; where a startup series exists, the update targets the same label;
- the async write worker's cached batching duration refreshes to the exact online value;
- max-ts retains precise typed configuration while its documented TSO enforcement boundary remains whole milliseconds;
- rolling-upgrade guidance prevents every non-millisecond-aligned duration update while old and fixed nodes coexist;
- the raftstore and storage maintenance guides document their respective runtime refresh, metric, and max-ts unit-boundary behavior;
- focused tests, affected package checks, formatting, and clippy pass;
- the SQL/status/metric restart scenario passes on a real test cluster.
