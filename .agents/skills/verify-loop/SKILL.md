---
name: verify-loop
description: User-invoked only bigname verification loop. Use only when the user explicitly invokes `$verify-loop` or asks to run the verify loop after Codex has implemented work; it spawns `verification_reviewer`, triages findings, confirms real issues with failing tests or checks, fixes them, and repeats until the reviewer is clean or all findings are rejected.
metadata:
  kind: command
---

# Verify Loop

Run an explicit reviewer/fix loop over the current worktree. This skill is command-like: do not use it unless the user invokes `$verify-loop` or directly asks to run the verify loop.

## Loop

1. Inspect dirty state enough to know what work exists, but do not pre-review the implementation before spawning the reviewer.
2. Spawn a fresh `verification_reviewer` agent with `fork_context=false`.
   - Do not paste diffs, implementation notes, suspected issues, prior reviewer output, or conversation context.
   - Give only the minimal task: review the current worktree diff in this repo and report concrete findings according to the reviewer role.
3. Wait for the reviewer result.
4. If the reviewer reports no findings, close the reviewer agent and stop.
5. If the reviewer reports findings, triage each one locally:
   - Decide whether the issue is real by reading the cited files, relevant docs, tests, and changed paths.
   - For every real code or behavior issue, write the smallest focused failing test first.
   - For docs, citations, staging, or other non-code findings, create the smallest meaningful validation check when a normal test is not applicable.
   - Run the test or check and confirm it fails for the reported issue before fixing. If it does not fail, refine the test once; otherwise classify the finding as unconfirmed unless direct evidence proves it.
   - Fix only confirmed real issues, keeping the change scoped to the current work.
   - Re-run the confirming tests/checks and any narrow regression checks needed for the fix.
6. If every finding is rejected or unconfirmed, close the reviewer agent and stop with a short explanation.
7. If at least one real issue was fixed, close the reviewer agent, then start again from step 2 with a fresh reviewer.

## Rules

- Keep reviewer agents read-only; never ask them to implement fixes.
- Do not reuse a reviewer agent across loop iterations after fixes. Each iteration starts with a fresh reviewer and no forked conversation context.
- Do not let the loop hide uncertainty. If the same finding repeats after a fix, or a finding cannot be confirmed with code/docs/tests, say so plainly and stop when further progress needs user judgment.
- Never stage or commit unless the user explicitly asks.
