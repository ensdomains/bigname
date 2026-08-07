---
name: replay-safety
description: Review bigname raw facts, normalized events, canonicality, projection rebuilds, reorg repair, invalidation, migrations, and storage-family write ownership.
metadata:
  kind: playbook
---

# Replay Safety

Start with `docs/storage.md`; add `docs/projections.md`, `docs/execution.md`, and `docs/manifests.md` only for affected boundaries.

## Check

For replayable-state work, state:

1. storage layer and write owner
2. identity anchors and primary keys
3. canonicality and reorg behavior
4. rebuild or replay path
5. invalidation effects
6. migration and fixture blast radius

## Non-negotiables

- Raw facts are immutable; reorg repair marks rows noncanonical rather than deleting truth.
- Schema-v2 interpret writes identity rows, discovery edges, and normalized
  events. Adapters provide interpretation behavior and do not write projections.
- Projection workers own projection tables.
- API reads projections, normalized events, and request-scoped lookup output
  except explicit audit endpoints and the guarded resolution divergence ledger.
  Provider responses are never persisted as reusable outcomes.
- Lookup uses declared topology and manifests, not adapter internals.
- Replay or migration semantic changes require `$contract-impact`.
