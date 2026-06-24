---
name: generate-pr-docs
description: Generate GitHub pull request markdown documents for this TiKV repository. Use when asked to draft or update a PR body, PR summary, or filled markdown document from the current diff, staged changes, branch changes, or a user-provided scope. Always reload `.github/pull_request_template.md` before generating output and preserve every hidden HTML comment block from that template verbatim.
---

# Generate Pr Docs

## Goal

Generate a TiKV-compatible pull request document in markdown that is ready to
paste into GitHub.

## Required Reads

- Read `.github/pull_request_template.md` at the start of every invocation.
- Treat the template file as live input. Never rely on memory of an earlier read.
- Read `AGENTS.md` before proposing the PR title or check-list values.
- If the user asks for a PR document about current code changes, inspect the
  actual diff, staged changes, or changed files before writing the document.

## Workflow

1. Reload the local template
   - Open `.github/pull_request_template.md` fresh before drafting.
   - Copy its structure exactly.
   - Preserve every HTML comment block exactly as written, including wording,
     spacing, and placement.
   - Do not delete, paraphrase, reorder, or compress hidden comments.

2. Determine the PR scope from real evidence
   - Prefer the user-provided summary, diff, branch, or changed-file list.
   - Otherwise inspect the current repository state.
   - Distinguish staged-only, unstaged-only, and full-worktree scopes instead of
     collapsing them together implicitly.
   - When the user asks for the current worktree scope, inspect `git status`
     style evidence and include tracked modifications, staged changes, and
     untracked files that belong to the intended change.
   - Prefer the current branch diff against its configured upstream tracking
     branch.
   - If the upstream base is ambiguous and the user did not provide scope, stop
     and ask instead of drafting from the wrong change set.
   - Read commit messages or touched files when needed so the description is
     concrete and not generic.

3. Propose the PR title
   - Follow `AGENTS.md` title rules:
     - `module: what's changed`
     - `module1, module2: what's changed`
     - `*: what's changed`
   - Use `*:` when the change is repository-wide, mostly documentation/process
     work, or spans multiple unrelated subsystems.
   - Do not invent a misleading narrow module tag.

4. Fill the template
   - Keep the `Issue Number:` line present in the final document.
   - Use the real issue link if the user provided one.
   - If no issue number is known, keep a visible placeholder such as
     `Issue Number: ref #xxx` rather than inventing one.
   - Fill `What's Changed` from the actual diff and intent.
   - Keep the `commit-message` fenced block and make it non-empty.
   - Put the PR commit-message body directly inside that block.
   - If the commit-message body is still unknown, keep a visible placeholder
     line such as `<commit message for PRs>` instead of leaving the block empty.
   - Update `Related changes`, `Check List`, and `Release note` only from known
     facts.
   - Keep unchecked boxes when evidence is missing.
   - Keep the `release-note` fenced block and make it non-empty.
   - Put the release note text directly inside that block.
   - If the release note is unknown, keep a visible placeholder line such as
     `<release notes for PRs>` instead of leaving the block empty.
   - For documentation-only, maintenance-guide-only, or skill-only changes,
     mark `No code` and use `None` in the release note unless the user says
     otherwise.

5. Emit the final output
   - Output `Suggested PR title: ...` first unless the user asked for body-only
     output.
   - Then output the full PR markdown body.
   - The markdown body must still include every hidden template comment.
   - When presenting the PR body in chat, prefer an outer four-backtick
     `markdown` fence so inner triple-backtick blocks remain visible literally.
   - Ensure the literal markers ` ```commit-message ` and
     ` ```release-note ` both appear in the final answer.
   - Do not wrap the whole PR body in an outer triple-backtick fence, because
     that can swallow the inner fenced blocks.
   - Use raw markdown output only when the user explicitly asks for it and the
     renderer will not consume the inner fences.
   - Do not replace the body with a summary or explanation.

## Output Rules

- Preserve the exact hidden context from `.github/pull_request_template.md`.
- Do not edit template headings unless the template itself changed.
- Do not fabricate issue numbers, test evidence, release notes, or side effects.
- Never emit an empty `commit-message` or `release-note` fenced block.
- Never break or hide the inner fenced blocks by wrapping the whole PR body in
  a conflicting outer markdown fence.
- The final answer must preserve the literal visible lines for both fenced
  blocks, including their opening and closing backticks.
- Do not derive current-worktree scope from `git diff` alone when untracked
  files may be part of the change.
- If information is unknown, keep a visible placeholder or brief TODO in the
  visible field instead of changing hidden comments.
- If the user asks to update an existing PR body, still reload the template
  first and then merge the confirmed content into that fresh template shape.

## TiKV-Specific Heuristics

- For maintainer-guide or agent-skill changes, describe the developer/reviewer
  workflow impact explicitly.
- For maintenance-guide additions or updates, mention that the guides are part
  of the repository maintenance surface when that is a core part of the change.
- For `.agents/skills/deep-review` changes, call out review-process or
  maintenance-guide enforcement changes explicitly.
- Leave `PR to update pingcap/docs/pingcap/docs-cn` unchecked unless a matching
  docs PR actually exists.
- Leave `Need to cherry-pick to the release branch` unchecked unless the user
  explicitly indicates that release backporting is needed.

## Minimal Output Shape

Use this shape unless the user asks for something narrower:

1. `Suggested PR title: ...`
2. Full markdown PR body copied from the live template, with filled visible
   fields and unchanged hidden comments.

## Refusal Condition

- If `.github/pull_request_template.md` cannot be read in the current repo,
  stop and report that the template must be available before generating the PR
  document.
