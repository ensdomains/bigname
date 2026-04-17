---
name: parallel-pickup
description: Take a substantial bigname task, split it into safe owned slices, and parallelize it with subagents. Use whenever the user asks to pick up a task, fan work out, parallelize implementation, or coordinate multiple workstreams.
metadata:
  kind: coordination
---

# Parallel Pickup

Broad-mode wrapper for `$orchestrate`. Use when execution is wanted (not just planning) and the task is large enough to benefit from parallel workers.

Apply `$orchestrate` with the fan-out extras below.

## Extras

1. Form a short top-level plan before delegating.
2. Keep the immediate blocking task local whenever the next step depends on it.
3. Run `$change-gate` first if the task may touch shared semantics, manifests, migrations, `crates/domain`, or parity claims.
4. Split work along `docs/workstreams.md` boundaries.
5. Append each dispatched slice to `.agents/state/slices.jsonl` so continuation loops see in-flight work.

See `AGENTS.md` High Conflict for surfaces that must not be parallelized casually, plus any unresolved shared-interface change.

## Good delegated tasks

- implement one adapter slice in `crates/adapters`
- add one projection or route in `apps/api` or `apps/worker`
- add conformance tests in `tests/conformance`
- wire manifest loading in `crates/manifests`

## Bad delegated tasks

- "figure out the architecture"
- "change whatever is needed"
- overlapping edits to shared type files or the same migration
- blocking semantic decisions that should stay local
