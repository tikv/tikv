---
name: deep-review
description: Deep, production-critical review workflow for TiKV changes. Use when asked to review a PR, branch, commit range, or diff in this repository and produce a markdown review report with findings, risk analysis, TiKV-specific validation results, and maintenance-guide consistency checks.
---

# Deep Review

## Goal

Produce a production-critical review for TiKV that:

- explains the problem being solved
- explains how the change works in concrete code terms
- identifies correctness, safety, performance, and operability risks
- follows TiKV repository rules from `AGENTS.md`
- checks whether affected files also require updates to `doc/maintenance-guides`
- writes the review to a markdown file under the target folder
- runs the repo-prescribed formatting and lint checks from `./Makefile`

## Inputs and Defaults

- Determine the repository root and run all commands from that root.
- Determine the target output folder.
- If the target folder is not provided, use `./target`.
- If the filename is not provided, use `review-report-YYYYMMDD-<summary>.md`.
- Do not overwrite an existing report. Choose a distinct filename instead.
- If the review scope is not explicit, prefer the current branch diff against
  its configured upstream tracking branch, not blindly `origin/HEAD`.

## TiKV-Specific Rules

- Treat `make format` and `make clippy` as the authoritative static checks because TiKV’s `Makefile` adds required setup and repository-specific scripts.
- Do not substitute raw `cargo clippy` for `make clippy` unless the user explicitly asks for a narrower check.
- Read `AGENTS.md` before judging engineering-rule compliance.
- Read `doc/maintenance-guides/README.md` and the relevant subsystem guide when the change touches a covered subsystem.
- Treat the maintenance guides as required review context for non-trivial changes in covered subsystems.
- If a change modifies ownership boundaries, startup order, data contracts, invariants, observability, reading maps, must-read file order, or change-impact guidance for a covered subsystem, expect the matching guide under `doc/maintenance-guides` to be updated in the same change.
- If a guide update is required but missing or stale, record that as a review issue or engineering-rule mismatch instead of silently accepting it.
- When unfamiliar code appears, read the surrounding subsystem rather than inferring from symbol names alone.
- Respect a dirty worktree. Never revert unrelated user changes while fixing review-discovered issues.

## Workflow

1. Confirm inputs
   - Identify the repository root, target output folder, and output filename.
   - If the review scope is not explicit, identify the current branch's
     upstream tracking branch first and use that diff.
   - If no upstream tracking branch exists, only fall back to `origin/HEAD`
     when it is clearly the intended review base; otherwise stop and ask for
     guidance.

2. Collect the change set
   - Prefer the diff provided by the user.
   - Otherwise run `git diff <upstream>...HEAD` from the repository root.
   - If the upstream branch is unavailable, or the fallback base is ambiguous,
     stop and ask for guidance instead of reviewing a likely wrong diff.

3. Understand the intent
   - Read the PR description, issue link, commit messages, or nearby code comments when available.
   - State the concrete system problem the change is trying to solve.
   - If intent is still unclear, infer from code and label the inference as an assumption.

4. Read the affected TiKV subsystems
   - Follow changed code into the owning modules, not just the diff hunk.
   - Typical subsystems include:
     - `src/storage`, `src/storage/mvcc`, `src/storage/txn`
     - `src/server`
     - `components/raftstore`, `components/raftstore-v2`
     - `components/cdc`
     - `components/pd_client`
     - `tests/`
   - Read enough nearby code to understand invariants, concurrency assumptions, error propagation, and test coverage.

5. Check maintenance-guide impact
   - Determine whether the changed files map to one or more covered guides under `doc/maintenance-guides`.
   - Always read `doc/maintenance-guides/repo-overview.md` for cross-component changes.
   - For covered subsystems, read the matching guide and use its:
     - purpose and scope
     - data/model contracts
     - observability guidance
     - must-read file order
     - change-impact matrix
   - Decide whether the code change should also update the guide.
   - A guide update is usually required when the change alters:
     - ownership or subsystem boundaries
     - startup or shutdown sequencing
     - metadata or API contracts
     - invariants or ordering rules
     - operational signals, metrics, logs, or health surfaces
     - reading maps, must-read file order, or change-impact guidance
   - If the guide was updated in the same change, review the guide update for accuracy and completeness.
   - If the guide should have been updated but was not, record that explicitly in the review output and tell the user the corresponding guide file should be updated.

6. Explain how the change works
   - Walk through each non-trivial logic change.
   - Explain control flow, data flow, state transitions, and failure paths in plain language.
   - Reference concrete files, symbols, and lines when discussing findings.

7. Evaluate costs and negative impacts
   - Explicitly assess:
     - correctness
     - security
     - robustness and failure modes
     - compatibility and behavioral shifts
     - CPU cost
     - memory cost
     - log volume or log-signal quality
     - operability and debuggability
     - cognitive load and maintainability

8. Run TiKV static validation from `./Makefile`
   - Run `make format` from the repository root.
   - Then run `make clippy` from the repository root.
   - Treat the `Makefile` behavior as part of the review:
     - `make format` runs `pre-format`, which installs `rustfmt` and bootstraps the pinned `cargo-sort` version before running `cargo fmt` and `cargo sort`
     - `make clippy` runs repository checks including redact-log, log-style, dashboards, docker-build, license, deny, and finally `scripts/clippy-all`
   - If `make format` or `make clippy` reports code issues in the working tree:
     - inspect the failing files
     - determine whether the failure belongs to the review target or to
       unrelated dirty-worktree state
     - record the result in the review output
   - Do not silently edit the change under review as part of a review-only
     workflow unless the user explicitly asked for review-plus-fix.
   - If `make clippy` fails in `scripts/deny`, record that separately from Rust lint results because `clippy-all` may not have run yet.
   - If the failure is environmental, toolchain-related, or blocked by unavailable external dependencies:
     - record the exact blocker in the report
     - do not claim the code passed
   - Do not revert unrelated user changes while making fixes.

9. Check engineering rules
   - Verify alignment with `AGENTS.md`, especially:
     - repository-specific build and test entrypoints
     - required validation expectations such as `make dev`
     - PR-title, issue-link, and release-note requirements when relevant to the review
   - Verify alignment with the maintainer contract in `doc/maintenance-guides/README.md` for covered subsystems.

10. Write the review output
   - Write a markdown report under the target folder.
   - If no findings exist, say so explicitly.
   - Still document residual risks, assumptions, and any validation gaps.
   - Always include a maintenance-guide check that states:
     - which guide files were relevant
     - whether guide updates were required
     - whether the change updated them
     - which guide files should be updated if missing

## Review Output Template

```markdown
### Deep Review

#### Problem Summary
- [Explain the concrete problem the change targets]

#### Solution Walkthrough
- [Explain how the change solves the problem; cover all non-obvious logic]

#### Findings (ordered by severity)
- [Issue or risk with file/line references]

#### Maintenance Guide Check
- Relevant guides:
- Guide update required:
- Updated in change:
- If missing, which files should be updated:

#### Costs and Negative Impacts
- Correctness:
- Security:
- Compatibility:
- Robustness:
- Operability:
- Cognitive Load:
- CPU:
- Memory:
- Log Volume:

#### Static Validation
- `make format`:
- `make clippy`:

#### Engineering Rules Check
- [List code or process mismatches against `AGENTS.md`, or "None"]
- [List maintenance-guide contract mismatches from `doc/maintenance-guides/README.md`, or "None"]

#### Questions and Assumptions
- [List unknowns or assumptions made]

#### Suggested Tests / Validation
- [Targeted tests or checks to validate behavior]
```
