---
name: phased-continuation
description: Continue phased development in bigname. Use whenever the user asks to keep going, continue implementation, pick the next work, suggest the next slice, continue phased development, or just keep shipping. This skill should make the current session act as the orchestrator via $orchestrate. Ask `next_slice_researcher` for the next viable thin slice, execute it through subagents, then research again until blocked or redirected.
---

# Phased Continuation

Use this skill when the user wants steady forward progress without re-specifying the next task.

This skill is a loop driver, not a planning document. Invoke `$orchestrate` and keep cycling:

1. research the next viable slice
2. execute that slice in orchestration mode
3. research again

Do not let the current session drift into direct implementation just because the next slice looks obvious.

## Workflow

1. Use `$orchestrate` immediately. The current session should stay in orchestration mode for the rest of the continuation run.
2. Spawn `next_slice_researcher` to identify the best next thin slice from:
   - `AGENTS.md`
   - `docs/development-plan.md`
   - `docs/workstreams.md`
   - the current repo state
3. Review the research output and carry forward any gating constraints, especially doc-first requirements or high-conflict surfaces.
4. Execute the chosen slice through `$orchestrate`.
   - Use `task_designer` only if the slice still needs decomposition.
   - Use `worker` agents for bounded implementation.
   - Use `docs_writer` when doc changes are needed.
   - Use `verification_reviewer` when cross-slice review is warranted.
5. When that slice is complete or reaches a clear stopping point, spawn `next_slice_researcher` again and repeat.

## Research contract

Do not decide the next slice locally if `next_slice_researcher` can answer it.

Require the research result to provide:

- `current_phase`
- `next_slice`
- `why_now`
- `owned_paths`
- `blocking_risks`
- `docs_to_touch`
- `parallel_risk`
- `success_signal`

If no viable next slice exists, stop the loop and surface the smallest unblocker.

## Execution contract

- Execute one chosen slice at a time.
- Parallelize within the slice, not across multiple candidate slices.
- Keep the current session focused on orchestration, delegation, and synthesis.
- Do not write the full task breakdown locally if `task_designer` can do it.
- Do not absorb broad repo execution into direct implementation mode out of convenience.

## Stop conditions

Stop and report clearly when:

- the repo is blocked by unresolved semantics or shared-interface work
- a prerequisite slice is missing and should become the real next slice
- no credible next slice is available
- the user interrupts or redirects
- the current phase exit criteria are satisfied and the next milestone needs explicit user confirmation

## Example trigger phrases

- "continue phased development"
- "keep going"
- "what should we do next?"
- "pick up the next slice"
- "just continue shipping"
- "find the next thing to complete and do it"
