---
name: cherry-pick-resolve-conflict
description: Cherry-pick one specific git commit into the current TiKV branch with strict already-applied pre-checks, conflict resolution based on full code understanding, TiKV-specific validation, and deep review only if the pick is applied. Use when asked to apply a single commit SHA onto this repository, especially when conflicts are likely or the change is production-critical.
---

# Cherry-pick Resolve Conflict

## Goal

Cherry-pick one commit safely and repeatably in TiKV: check if it is already applied, avoid rerere, resolve conflicts with real subsystem understanding, run the repository’s required checks, and deep-review the effective change only if the pick is applied.

## Inputs

- `commit_sha`: the source commit to cherry-pick

## TiKV-Specific Rules

- Require a clean working tree before starting. Do not stash or discard unrelated user changes.
- Never rely on `git rerere` for conflict resolution.
- Read `AGENTS.md` and use TiKV’s repository entrypoints from `Makefile`.
- Treat `make format` and `make clippy` as authoritative repository checks, not raw `cargo fmt` or raw `cargo clippy`.
- If `make clippy` fails in `scripts/deny`, record that separately from Clippy lint results because `scripts/clippy-all` may not have run yet.
- Prefer targeted test execution when you can identify the affected subsystem. If the touched area is broad or the right target is unclear, escalate to broader test coverage.

## Workflow

1. Preconditions

- Require a clean working tree before starting: `git status --porcelain` must be empty.
- If the tree is dirty, stop and ask the user how they want to proceed. Never discard unrelated changes.

2. Collect commit info

- `git show -s --format='%H%n%s' <commit_sha>`
- Pick a stable `grep_key` from the subject line. Prefer PR numbers like `#1234`, otherwise choose another unique token.

3. Pre-check

- `git log --oneline --grep '<grep_key>'`
- If the output clearly shows the change is already on the current branch, skip the cherry-pick.

4. Cherry-pick

- `git cherry-pick -x <commit_sha>`

5. Resolve conflicts if needed

- Inspect conflicts with `git status`.
- Before editing conflict markers, fully understand what the picked commit changes:
  - Read the full patch: `git show <commit_sha>`
  - Identify the intended behavior change, invariant, or bug fix
  - Separate core logic changes from mechanical fallout such as imports or signature propagation
- Read the conflicting TiKV code paths in full before editing. Do not resolve by symbol-name guessing.
- For non-trivial or semantic conflicts, identify which prior commit on the current branch caused the divergence:
  - Use `git blame -w` on `HEAD` for the relevant ours-side lines
  - Record the responsible commit SHA and subject
  - Compare that commit’s intent with the cherry-picked change and resolve to preserve the correct combined behavior
- For trivial or mechanical conflicts, keep resolution localized and do not spend time on provenance unless behavior is ambiguous.
- After resolving:
  - `git add <paths>`
  - `make format`
  - `git cherry-pick --continue`
- If Git reports an empty patch during or after resolution:
  - `git cherry-pick --skip`
  - Treat it as `skipped (empty patch)`

6. Validate if applied

If the result is `picked` or `picked (conflicts resolved)`, validation is required.

- Always run:
  - `cargo check --all`
  - `make clippy`
- Run tests that match the touched subsystem:
  - Prefer `env EXTRA_CARGO_ARGS=$TESTNAME make test_with_nextest` when you can name a focused test target
  - Use `./scripts/test $TESTNAME -- --nocapture` when a specific Rust test name is already known
  - If the change affects shared core paths such as `src/storage`, `src/server`, `components/raftstore`, `components/raftstore-v2`, `components/cdc`, or the correct focused target is unclear, run `make test`
- If any validation fails:
  - investigate the root cause
  - distinguish code regressions from missing prerequisite commits, dependency-security failures, or environment/toolchain blockers
  - do not paper over failures with unrelated edits

7. Deep review only if applied

- Only if the cherry-pick is applied should you perform a deep review of the effective change.
- In this repository, use the local `deep-review` skill after the pick so the final report covers problem summary, solution mechanics, findings, and TiKV-specific validation gaps.

## Output Summary Template

```markdown
- Result: `picked` / `picked (conflicts resolved)` / `skipped (already applied)` / `skipped (empty patch)` / `blocked (dirty worktree)`
- Pre-check:
  - `git log --oneline --grep "<grep_key>"`: [matched / no match]
- Cherry-pick:
  - Command: `git cherry-pick -x <commit_sha>`
  - Conflicts: [none | list files]
- Conflict analysis (only for non-trivial or semantic conflicts):
  - Ours-side provenance: `<prior_commit_sha>` - <prior_subject> (from `git blame -w`)
  - Intent check: [compatible / conflicting] with rationale
- Resolve notes:
  - [What changed, why, and evidence]
- Validation:
  - `make format`: [pass / fail / blocked / not run]
  - `cargo check --all`: [pass / fail / blocked]
  - Focused tests or `make test`: [pass / fail / not run]
  - `make clippy`: [pass / fail / blocked]
  - `scripts/deny` within `make clippy`: [pass / fail / not reached]
  - `scripts/clippy-all` within `make clippy`: [pass / fail / not reached]
```
