---
name: deep-review
description: Deep, production-critical review workflow for TiKV changes. Use when asked to review a PR, branch, commit range, or diff in this repository and produce a markdown review report with findings, risk analysis, and TiKV-specific validation results.
---

# Deep Review

## Goal

Produce a production-critical review for TiKV that:

- explains the problem being solved
- explains how the change works in concrete code terms
- identifies correctness, safety, performance, and operability risks
- follows TiKV repository rules from `AGENTS.md`
- writes the review to a markdown file under the target folder
- runs the repo-prescribed formatting and lint checks from `./Makefile`

## Inputs and Defaults

- Determine the repository root and run all commands from that root.
- Determine the target output folder.
- If the target folder is not provided, use `./target`.
- If the filename is not provided, use `review-report-YYYYMMDD-<summary>.md`.
- Do not overwrite an existing report. Choose a distinct filename instead.

## TiKV-Specific Rules

- Treat `make format` and `make clippy` as the authoritative static checks because TiKV’s `Makefile` adds required setup and repository-specific scripts.
- Do not substitute raw `cargo clippy` for `make clippy` unless the user explicitly asks for a narrower check.
- Read `AGENTS.md` before judging engineering-rule compliance.
- When unfamiliar code appears, read the surrounding subsystem rather than inferring from symbol names alone.
- Respect a dirty worktree. Never revert unrelated user changes while fixing review-discovered issues.

## Workflow

1. Confirm inputs
   - Identify the repository root, target output folder, and output filename.
   - If the review scope is not explicit, use the current branch diff against `origin/HEAD`.

2. Collect the change set
   - Prefer the diff provided by the user.
   - Otherwise run `git diff origin/HEAD...HEAD` from the repository root.
   - If `origin/HEAD` is unavailable or the diff command fails, stop and ask for guidance.

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

5. Explain how the change works
   - Walk through each non-trivial logic change.
   - Explain control flow, data flow, state transitions, and failure paths in plain language.
   - Reference concrete files, symbols, and lines when discussing findings.

6. Evaluate costs and negative impacts
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

7. Run TiKV static validation from `./Makefile`
   - Run `make format` from the repository root.
   - Then run `make clippy` from the repository root.
   - Treat the `Makefile` behavior as part of the review:
     - `make format` runs `pre-format`, which installs `rustfmt` and bootstraps the pinned `cargo-sort` version before running `cargo fmt` and `cargo sort`
     - `make clippy` runs repository checks including redact-log, log-style, dashboards, docker-build, license, deny, and finally `scripts/clippy-all`
   - If `make format` or `make clippy` reports code issues in the working tree:
     - inspect the failing files
     - fix the errors when the fix is local and safe
     - rerun the failing command
   - If `make clippy` fails in `scripts/deny`, record that separately from Rust lint results because `clippy-all` may not have run yet.
   - If the failure is environmental, toolchain-related, or blocked by unavailable external dependencies:
     - record the exact blocker in the report
     - do not claim the code passed
   - Do not revert unrelated user changes while making fixes.

8. Check engineering rules
   - Verify alignment with `AGENTS.md`, especially:
     - repository-specific build and test entrypoints
     - required validation expectations such as `make dev`
     - PR-title, issue-link, and release-note requirements when relevant to the review

9. Write the review output
   - Write a markdown report under the target folder.
   - If no findings exist, say so explicitly.
   - Still document residual risks, assumptions, and any validation gaps.

## Review Output Template

```markdown
### Deep Review

#### Problem Summary
- [Explain the concrete problem the change targets]

#### Solution Walkthrough
- [Explain how the change solves the problem; cover all non-obvious logic]

#### Findings (ordered by severity)
- [Issue or risk with file/line references]

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

#### Questions and Assumptions
- [List unknowns or assumptions made]

#### Suggested Tests / Validation
- [Targeted tests or checks to validate behavior]
```
