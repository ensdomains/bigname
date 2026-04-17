---
name: phased-continuation
description: Continue phased development in bigname. Use whenever the user asks to keep going, continue implementation, pick the next work, suggest the next slice, continue phased development, or just keep shipping. This skill should route broad execution work through `$orchestrator` instead of letting the root session pick up implementation directly.
---

# Phased Continuation

Use this skill when the user wants steady forward progress without re-specifying the next task.

The job of the current session is not to implement the next slice itself. The job is to decide what should happen next, confirm that it is safe to run, and then use `$orchestrator` so the current session stays in orchestration mode.

## Read first

Start with:

- `AGENTS.md`
- `docs/development-plan.md`
- `docs/workstreams.md`

Read only the additional docs needed to classify the next slice. If the next slice may affect semantics, manifests, shared IDs or enums, coverage meaning, workstream ownership, `crates/domain`, or migrations, use `$change-gate` thinking before delegating execution.

## Goal

Choose the best next thin end-to-end slice for the current state of the repo, then run it through `$orchestrator`.

Do not let the root session absorb broad implementation work just because the next slice looks obvious.

## Continuation workflow

1. Determine the current milestone or phase from `docs/development-plan.md` and the state of the repo.
2. Identify the highest-leverage unfinished slice that is:
   - consistent with milestone order
   - small enough to land cleanly
   - not blocked by unresolved shared-interface changes
   - aligned with `docs/workstreams.md`
3. If the next slice is broad or ambiguous, narrow it into one thin deliverable with explicit boundaries before delegating.
4. If the work may change frozen/shared semantics, treat it as doc-first and make that part of the delegated plan.
5. Invoke `$orchestrator` in the current session with:
   - the chosen slice
   - the relevant docs to anchor on
   - any gating constraints
   - an instruction to execute through subagents rather than doing implementation itself
6. Keep the current session focused on selecting, steering, and evaluating the delegated work.

## Root-session constraints

- Do not implement the next slice locally unless the user explicitly asks you not to delegate.
- Do not write the full task breakdown locally if `$orchestrator` or `task_designer` can do it.
- Do not keep broad repo execution in the root session out of convenience.
- Prefer one clear delegated slice over a vague “continue everything” handoff.

## Next-slice selection rules

Prefer slices that unlock later work without reopening frozen semantics.

Bias toward:

- the next exit criterion in the current phase
- one thin vertical slice over broad scaffolding
- work that fits a single primary workstream with minimal shared-surface edits
- explicit declared-state or verified-execution progress instead of legacy-parity drift

Be conservative around:

- `crates/domain`
- migrations
- fixtures
- manifest schema
- cross-workstream shared IDs or enums

## Output before delegation

Produce a short continuation gate:

1. `current_phase`: the phase or milestone you believe the repo is in
2. `next_slice`: the exact slice to execute now
3. `why_now`: why this is the right next slice
4. `blocking_risks`: anything that would require doc-first or tighter scoping
5. `delegation_target`: `$orchestrator`

Then delegate. Do not stop after analysis unless the repo is blocked or the user asked only for planning.

## When no viable slice is ready

If the repo is blocked by missing semantics, unresolved conflicts, or an unfinished prerequisite, say so plainly and delegate the smallest unblocker instead, usually:

- a doc update
- a task-design pass
- a focused review of the blocking boundary

## Example trigger phrases

- "continue phased development"
- "keep going"
- "what should we do next?"
- "pick up the next slice"
- "just continue shipping"
- "find the next thing to complete and do it"
