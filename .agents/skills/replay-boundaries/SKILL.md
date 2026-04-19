---
name: replay-boundaries
description: Review bigname storage, replay, projection, and execution-boundary work. Use whenever a task touches canonicality, normalized events, projection rebuilds, reorg repair, invalidation, execution traces, or storage-family write ownership.
metadata:
  kind: playbook
---

# Replay Boundaries

Start with:

- `docs/storage.md`
- `docs/projections.md`
- `docs/execution.md`
- `docs/workstreams.md`

## What to verify

For any design, implementation, or review touching replayable state, check:

1. storage layer affected
2. write owner
3. primary keys or identity anchors
4. canonicality behavior under reorg
5. rebuild path
6. invalidation triggers

## Non-negotiable rules

See `AGENTS.md` Boundaries. Replay-specific additions:

- Reorg repair marks rows noncanonical rather than deleting truth.
- Only projection workers write projection tables.
- Invalidation is deterministic and key-scoped, not broad polling.

## Boundary enforcement

Correct designs that drift into any of these:

- adapters mutating projections
- API reading raw-fact tables for normal reads
- execution code bypassing declared topology or manifest interfaces
- latest-row-wins logic instead of canonicality-aware replay

## Change guidance

- Append-only tables should prefer additive schema changes.
- Projection tables may be recreated only when the rebuild path already exists.
- If a change affects shared ownership or replay semantics, route it back through `$change-gate`.

## Upstream anchor

Canonicality, reorg, and event-ordering claims about ENSv1 or Basenames behavior cite the upstream emitter in `.refs/<key>/` at the pinned commit. Use the `(upstream: .refs/<key>/<path>:L<line> @ <key>@<short-commit>)` format from `AGENTS.md` § Upstream anchors. Paraphrasing upstream event semantics from memory is treated as drift.

Keep the review concrete. Name the affected tables, keys, invalidation sources, rebuild modes, and upstream citations.
