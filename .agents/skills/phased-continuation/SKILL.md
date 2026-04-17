---
name: phased-continuation
description: Continue phased development in bigname. Use whenever the user asks to keep going, continue implementation, pick the next work, suggest the next slice, continue phased development, or just keep shipping. Runs $orchestrate in continuation mode, asking next_slice_researcher for the next viable slices, executing them, and researching again until blocked or redirected.
metadata:
  kind: coordination
---

# Phased Continuation

Continuation-mode wrapper for `$orchestrate`. Use when the user wants steady forward progress without re-specifying the next task.

Apply `$orchestrate` with the extras below. Do not let the session drift into direct implementation because the next slice looks obvious.

## Loop

1. Read `.agents/state/slices.jsonl` so in-flight and completed slices are visible.
2. Spawn `next_slice_researcher` for the next viable slices. Pass the log contents so it does not re-pick live work. Expect a ranked list of envelopes back.
3. Execute the primary slice through `$orchestrate` using its default fan-out pattern. If follow-on slices in the ranked list are genuinely independent of the primary (non-overlapping `owned_paths`, `parallel_risk: safe`), you may execute them concurrently instead of waiting for the next loop — log each as `picked` / `in_flight` / `completed` in the slice log.
4. When the active slices complete or reach a clear stopping point, loop back to step 2. The researcher will re-evaluate based on the updated slice log and may repeat or drop previously listed follow-ons.

Do not decide the next slice locally if `next_slice_researcher` can answer it.

## Stop conditions

Stop and report when:

- the repo is blocked by unresolved semantics or shared-interface work
- a prerequisite slice is missing and should become the real next slice
- no credible next slice is available
- the user interrupts or redirects
- the current phase exit criteria are satisfied and the next milestone needs explicit user confirmation
