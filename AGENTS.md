# bigname

`bigname` is in bootstrap. The checked-in docs are the source of truth for semantics.

## Guardrails

- Minimum shared-interface freeze: `docs/architecture.md`, `docs/api-v1.md`, `docs/storage.md`, `docs/manifests.md`, `docs/consumer-capabilities.md`, `docs/adrs/0001-stack.md`, and `docs/adrs/0002-surface-resource-identity.md`.
- If a task changes public semantics, shared IDs or enums, coverage meaning, manifest schema, workstream ownership, or consumer-replacement meaning, update the relevant docs first or in the same change.
- Prefer cohesive end-to-end slices — a full capability with its tests and wiring, not a commit-sized edge. Do not build disguised legacy API parity or new planning docs unless semantics changed.

## Boundaries

- Adapters write identity rows and normalized events, not projection rows.
- API code reads projections and execution output only, except explicit audit endpoints.
- Execution code uses declared topology and manifests, not adapter internals.
- Manifest and discovery code decides what is authoritative.
- Raw facts are immutable. Projections are rebuildable. Canonicality is explicit. Execution artifacts are durable. Unsupported behavior must be explicit.

## High Conflict

- Keep `crates/domain` narrow.
- Coordinate migrations carefully.
- Treat fixture updates as cross-workstream review points.

## Core Skills

- `$change-gate`: classify doc-first vs implementation-only work.
- `$orchestrate`: make the current session orchestrate broad execution work, using subagents instead of doing most implementation directly. Covers fan-out and continuation as modes.
- `$phased-continuation`: run `$orchestrate` in continuation mode, cycling `next_slice_researcher` → execute → research until blocked or redirected.
