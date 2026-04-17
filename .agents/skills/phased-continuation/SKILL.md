---
name: phased-continuation
description: Continue phased development in bigname. Use whenever the user asks to keep going, continue implementation, pick the next work, suggest the next slice, continue phased development, or just keep shipping. Runs $orchestrate in continuation mode, asking next_slice_researcher for the next viable thin slice, executing it, and researching again until blocked or redirected.
metadata:
  kind: coordination
---

# Phased Continuation

Continuation-mode wrapper for `$orchestrate`. Use when the user wants steady forward progress without re-specifying the next task.

Apply `$orchestrate` with the extras below. Do not let the session drift into direct implementation because the next slice looks obvious.

## Loop

1. Read `.agents/state/slices.jsonl` so in-flight and completed slices are visible.
2. Spawn `next_slice_researcher` for the next viable thin slice. Pass the log contents so it does not re-pick live work.
3. Execute the chosen slice through `$orchestrate` using its default fan-out pattern. Append `picked` / `in_flight` / `completed` events to the slice log as state changes.
4. When the slice completes or reaches a clear stopping point, loop back to step 2.

Do not decide the next slice locally if `next_slice_researcher` can answer it.

## Stop conditions

Stop and report when:

- the repo is blocked by unresolved semantics or shared-interface work
- a prerequisite slice is missing and should become the real next slice
- no credible next slice is available
- the user interrupts or redirects
- the current phase exit criteria are satisfied and the next milestone needs explicit user confirmation
