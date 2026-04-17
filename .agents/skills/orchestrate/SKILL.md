---
name: orchestrate
description: Run broad bigname execution work in orchestration mode. Use whenever the task is large, multi-slice, parallelizable, or should be delegated instead of implemented directly in the current session. This skill makes the current session orchestrate the work. Read docs, classify the change, create bounded tasks, spawn subagents, steer them, and integrate their results without doing most implementation locally.
---

# Orchestrate

Use this skill when the current session should coordinate execution instead of owning implementation directly.

This is not a separate agent definition. Using `$orchestrate` means the current session orchestrates the work itself.

The job of the current session is to read the docs, classify the change, create concrete tasks, spawn subagents, interact with them, and integrate their results. Keep local implementation to a minimum and prefer delegation whenever a subagent can own the work cleanly.

## Core role

- Do not pick up broad implementation work yourself just because the next step is obvious.
- Do not directly edit product code, tests, manifests, fixtures, migrations, or docs unless delegation is blocked and the task would otherwise stall.
- Prefer delegation even for documentation writing, task breakdown, and analysis when a subagent can own that work cleanly.
- Keep your own work focused on orchestration: scoping, task design, delegation, follow-up, conflict resolution, and synthesis.

## Repo rules

- Treat checked-in docs as the source of truth for semantics. Start from `AGENTS.md` and the minimum relevant docs before assigning work.
- If a task may change public semantics, shared IDs or enums, coverage meaning, manifest schema, workstream ownership, or consumer-replacement meaning, use `$change-gate` before implementation starts.
- Use thin end-to-end slices. Do not create disguised legacy-parity work or extra planning docs unless semantics changed.
- Split work along `docs/workstreams.md` boundaries when possible:
  - `apps/api`
  - `apps/indexer`
  - `apps/worker`
  - `crates/storage`
  - `crates/manifests`
  - `crates/adapters`
  - `crates/execution`
  - `tests/conformance`
- Treat `crates/domain`, migrations, fixtures, and manifest schema as high-conflict. Do not fan them out casually.

## Delegation rules

- Prefer specialist subagents when they fit:
  - `next_slice_researcher` for "what should we work on next?" and phase-aligned thin-slice selection
  - `task_designer` for decomposition, owned slice design, and task prompts
  - `docs_writer` for doc updates, task docs, and other repo documentation changes
  - `verification_reviewer` for cross-slice review and residual risk checking
  - built-in `worker` for bounded implementation tasks
- Prefer read-heavy exploration first when the path is unclear, then hand implementation to focused workers.
- Assign each subagent a narrow goal, explicit file or directory ownership, expected outputs, and validation expectations.
- Tell each subagent it is not alone in the codebase and must not revert others' edits.
- Avoid overlapping write ownership.
- Do not spawn agents for vague work like "figure it out". Give each agent a bounded deliverable.
- Use the smallest number of agents that materially advances the task, but parallelize independent slices aggressively once boundaries are clear.
- Interact with subagents actively: answer blockers, redirect unclear work, and request tighter follow-up when needed.
- Wait only when blocked on a result needed for the next decision.
- Close completed agents when their work is integrated.

## Skill routing

- Use `$change-gate` for doc-first classification and shared-interface changes.
- Use `$manifest-rollout` for manifest, discovery, capability flag, or invalidation changes.
- Use `$capability-slice` for consumer-capability mapping, routes, projections, tests, and rollout criteria.
- Use `$replay-boundaries` for replay, canonicality, projection rebuilds, invalidation, or execution-boundary work.
- Use `$parallel-pickup` when a substantial implementation task should be split into safe owned slices.

## Task template

For each delegated task, specify:

- exact outcome to produce
- files or directories the subagent owns
- what it must not touch
- validation or tests it should run if it edits code
- report back changed files, evidence gathered, unresolved risks, and assumptions

## Preferred fan-out pattern

1. Read the relevant docs and classify the change.
2. If the real question is what to do next, delegate slice selection first to `next_slice_researcher`.
3. If the slice is broad or underspecified, delegate scope mapping or task decomposition next, usually to `task_designer`.
4. Spawn parallel implementation or documentation subagents only after ownership boundaries are explicit.
5. Use `docs_writer` instead of writing docs or task documents yourself when delegation is possible.
6. Use `verification_reviewer` for cross-slice checking when risk is high or multiple workers edited adjacent surfaces.
7. Synthesize results for the user without taking over implementation yourself.

## Documentation rule

If new docs, doc updates, or task write-ups are needed, prefer a dedicated subagent for that writing instead of drafting them yourself.

## Final response

- Summarize which subagents were used and what each one produced.
- Distinguish subagent results from your own synthesis.
- Call out conflicts, unresolved risks, missing validation, and places where user direction is still needed.
