# `src/coprocessor_v2` Maintenance Guide

## Purpose And Scope

`coprocessor_v2` is the plugin-oriented coprocessor framework. It is distinct
from the classic built-in coprocessor path. It dynamically loads plugins and
dispatches raw coprocessor requests to them.

## Architectural Views

### Plugin execution view

- request arrives
- endpoint resolves plugin and version constraints
- plugin registry supplies loaded plugin
- raw storage adapter exposes TiKV raw APIs
- plugin executes user code

## Process Lifecycle And Startup Sequencing

- Plugin registry is created during server startup if configured.
- Hot-reloading watcher setup must succeed or fail cleanly without corrupting
  endpoint state.
- The lifecycle anchors are `src/coprocessor_v2/endpoint.rs::Endpoint::new`
  and `plugin_registry.rs::PluginRegistry::start_hot_reloading`.
- Startup is configuration-driven through
  `Config::coprocessor_plugin_directory`. If that directory is unset, the whole
  subsystem is intentionally disabled and requests should fail clearly.
- The file watcher is long-lived state. Any change to watcher ownership,
  registry lifetime, or background-thread termination should be reviewed as a
  lifecycle change, not only as a plugin-loading change.

## Data Model And Metadata Contracts

- plugin name and version constraint
- plugin build-info compatibility contract
- raw-storage adapter contract for context, range, and error mapping
- `plugin_registry.rs::err_on_mismatch` enforces the core ABI contract:
  plugin API version, `rustc`, and target triple must all match TiKV.
- `endpoint.rs` adds a second contract layer through semver constraint checking
  on `copr_version_req`.
- `raw_storage_impl.rs` deliberately exposes only raw KV semantics. It forwards
  the request `Context`, uses raw APIs, and maps storage errors into
  `PluginError` forms such as `KeyNotInRegion`, `Timeout`, `Canceled`, or a
  wrapped opaque error.
- Hot-reload semantics are intentionally limited. Deletes and overwrites do not
  imply safe in-place replacement of running plugin code; the current watcher
  mostly updates path bookkeeping and warns.

## Start Here

- `src/coprocessor_v2/mod.rs`
- `src/coprocessor_v2/endpoint.rs`
- `src/coprocessor_v2/plugin_registry.rs`
- `src/coprocessor_v2/raw_storage_impl.rs`
- `src/coprocessor_v2/config.rs`

## Must-Read File Order

1. `src/coprocessor_v2/config.rs`
2. `src/coprocessor_v2/endpoint.rs`
3. `src/coprocessor_v2/plugin_registry.rs`
4. `src/coprocessor_v2/raw_storage_impl.rs`
5. `src/server/service/kv.rs`

## Main Responsibilities

- locate plugins by name
- validate plugin version constraints
- watch a configured plugin directory and attempt dynamic plugin loading
- adapt TiKV raw storage APIs to the plugin API
- translate storage errors into plugin-facing error forms

## Critical Invariants

- Plugin ABI compatibility checks must remain strict:
  target, rustc, and coprocessor API version.
- Plugin-path watching must not leave registry state internally inconsistent,
  especially because overwrite/delete handling is limited.
- Request-to-plugin dispatch must preserve region errors and cancellation
  semantics.
- Storage adaptation must not silently widen permissions or semantics beyond the
  raw API it wraps.

## Observability And Operational Signals

- plugin load/reload warnings and failures
- request-time plugin dispatch errors
- raw-storage error translation behavior visible at RPC layer
- There is no large dedicated metric surface in this subsystem today. Logs in
  `plugin_registry.rs` and request failures returned from `endpoint.rs` are the
  main operational signals.
- Triage starting points:
  `endpoint.rs`, `plugin_registry.rs`, `raw_storage_impl.rs`,
  `src/server/service/kv.rs`.
- Distinguish four failure classes during triage:
  disabled subsystem, plugin not found, compatibility/version mismatch, and
  storage/runtime failure inside a loaded plugin.

## Change Management Guidance

- If plugin ABI validation, hot-reload behavior, or raw-storage API mapping
  changes, update this guide in the same patch.
- Because this subsystem executes externally supplied code, changes here should
  be reviewed conservatively.
- Do not weaken compatibility checks for convenience. Rejecting an incompatible
  plugin early is safer than attempting partial compatibility.
- Any expansion of the plugin-facing storage API should explicitly document
  allowed semantics, error mapping, and whether the new call can mutate data.
- If hot-reload behavior is changed, state clearly whether the system now
  supports real replacement or still only supports additive loading plus limited
  path bookkeeping.

## Change-Impact Matrix

- Plugin discovery or directory config changes:
  inspect `config.rs`, `endpoint.rs`, and startup wiring
- ABI or compatibility changes:
  inspect `plugin_registry.rs`, plugin tests, and user-facing failure behavior
- Hot-reload behavior changes:
  inspect watcher lifecycle, path tracking, and operational warnings
- Storage adapter changes:
  inspect `raw_storage_impl.rs`, `src/storage`, and region-error extraction

## Review Checklist

- Does the change alter hot-reload or plugin-path tracking?
- Does it weaken compatibility checks or version matching?
- Does it change raw storage error mapping or region-error extraction?
- Does it introduce trust assumptions about plugin code or plugin inputs?
- Does it require new tests for plugin lifecycle or storage adaptation?

## Observability And Tests

- Local unit tests exist in `raw_storage_impl.rs`.
- Wider validation usually needs `components/test_coprocessor_plugin`.
- `plugin_registry.rs` also contains local tests around loading and hot-reload
  mechanics. Use them when touching compatibility checks or watcher behavior.

## Common Failure Modes

- plugin reload path drift after rename or overwrite
- overwrite/delete behavior assumed to be stronger than the current watcher
  implementation
- version mismatch accepted too early or rejected too late
- storage errors surfaced as opaque plugin failures instead of region errors
- plugin framework enabled but directory/watch behavior not actually active

## Reading Map And Companion Docs

Suggested reading order:

1. `mod.rs`
2. `config.rs`
3. `endpoint.rs`
4. `plugin_registry.rs`
5. `raw_storage_impl.rs`

Companion docs:

- `repo-overview.md`
- `src/server.md`
- `src/storage.md`

## Glossary

- Plugin registry:
  runtime loader and index of coprocessor plugins
- RawStorageImpl:
  adapter exposing TiKV raw KV APIs to plugins
- Hot reloading:
  file-watcher-driven plugin discovery and limited path update behavior, not a
  guarantee of safe in-place code replacement

## Related Components

- `src/server/service/kv.rs` provides the RPC entry point.
- `src/storage` is the underlying raw KV implementation exposed to plugins.
