---
name: rust-upgrade-loop
description: "Upgrade Rust toolchain/edition and related dependencies in Rust monorepos (especially TiKV-style repos), then run an iterative CI fix loop: poll Jenkins/BlueOcean runs, extract clippy/test failures, apply minimal patches, run focused local verification, format, commit with sign-off, and push until CI is green."
---

# Rust Upgrade Loop

## Overview
Use this skill when the task is to perform a Rust upgrade and drive CI to green through repeated failure triage and patching.

Typical triggers:
- Upgrade Rust toolchain or edition (for example 2018 -> 2024).
- Fix new clippy/rustc failures caused by the upgrade.
- Continue Jenkins `pull_unit_test`/lint loop after each push.
- Repeatedly check CI, patch, commit, and push until success.

## Preconditions
- Work inside the target repo branch.
- Keep unrelated user changes intact.
- Prefer focused verification over full-workspace rebuild unless explicitly requested.
- Use signed commits for loop fixes.

## Standard Workflow

### 1) Establish Current State
1. Confirm branch and working tree state.
2. Identify current upgrade intent (toolchain, edition, deps, deny config, CI pipeline target).
3. Capture latest Jenkins run id and status before edits.

Suggested commands:
```bash
git status --short --branch
curl -s '<BLUEOCEAN_RUNS_API>?limit=10' | jq -r '.[] | "\(.id)\t\(.state)\t\(.result)"'
```

### 2) Apply Upgrade Changes
1. Update only required files for this step (for example `Cargo.toml`, `rust-toolchain*`, `deny.toml`, dependency constraints).
2. Keep change scope minimal and reviewable.
3. Run formatting after edits.

Suggested command:
```bash
make format
```

### 3) Verify Locally With Focused Checks
1. Reproduce the exact failing check/test from CI logs.
2. Run only targeted crates/tests first.
3. Expand checks only if needed.

Examples:
```bash
cargo test -p <crate> <module::test_name> -- --nocapture
cargo clippy -p <crate> --all-targets -- -D warnings -A clippy::bool_assert_comparison
```

### 4) Commit and Push
1. Stage only intended files.
2. Commit with sign-off.
3. Push immediately to trigger next CI run.

```bash
git add <files>
git commit -s -m "<focused message>"
git push
```

### 5) Poll Jenkins Run and Extract Failure
Use BlueOcean API in this order:
1. List latest runs.
2. Poll selected run until `FINISHED`.
3. Query run nodes.
4. Query failing node steps.
5. Pull failing step log and inspect last meaningful error.

Core pattern:
```bash
# latest runs
curl -s '<RUNS_API>?limit=10' | jq -r '.[] | "\(.id)\t\(.state)\t\(.result)"'

# run status
curl -s '<RUN_API>/<id>/' | jq -r '.state+"\t"+(.result//"UNKNOWN")'

# nodes
curl -s '<RUN_API>/<id>/nodes/' | jq -r '.[] | "\(.id)\t\(.displayName)\t\(.state)\t\(.result)"'

# steps for failing node
curl -s '<RUN_API>/<id>/nodes/<node_id>/steps/' | jq -r '.[] | "\(.id)\t\(.displayName)\t\(.state)\t\(.result)"'

# step log
curl -s '<RUN_API>/<id>/nodes/<node_id>/steps/<step_id>/log/?start=0'
```

## Triage Rules

### Prefer Deterministic Failures First
- Fix compile/clippy/test assertion failures before flaky retries.
- Ignore secondary tooling noise (for example `gdb: command not found`) unless it is the root cause.

### Common Upgrade Regression Types
- New rustc/clippy denies from newer toolchain.
- Formatting expectation changes in JSON/string rendering tests.
- `Display` vs `to_string` semantic mismatches.
- Recursion/ordering bugs surfacing under stricter checks.
- Edition-2024 unsafe rule changes and cfg lint tightening.

### Patch Strategy
1. Make smallest patch that resolves the concrete failure.
2. Preserve legacy behavior when tests depend on it.
3. Verify with the exact failing test/check locally.
4. Re-run `make format`.

## Loop Control
- Continue: `poll -> isolate failure -> patch -> focused verify -> format -> commit -s -> push`.
- Stop when:
  - Target CI run is `FINISHED SUCCESS`, or
  - User-defined max run count is reached.
- If max count is reached without green CI, report current blocker and the latest failing run id.

## Reporting Back to User
After each loop iteration, report briefly:
1. Run id and stage result.
2. Root failure extracted from logs.
3. Files changed.
4. Local checks executed.
5. Commit id pushed.

Keep updates concise and operational.
