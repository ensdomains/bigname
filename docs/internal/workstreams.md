# Ownership Boundaries

Internal reference for splitting implementation work. `AGENTS.md` is the process rulebook; this file only maps review ownership and high-conflict surfaces.

## Boundaries

- Schema-v2 interpret writes identity rows, discovery edges, normalized events,
  and append-only diagnostics for malformed event logs from undeclared
  emitters. At a completed pass boundary, it also atomically replaces the
  [discovery-watch admission snapshot](../glossary.md#discovery-watch-admission-snapshot)
  and may install required Ingest work
  through the shared phase-state installer. That snapshot is coordination
  state, not a work queue; `chain_phase_state` remains the sole work/redo
  authority. Before deleting a redo range, Interpret also preserves bounded
  resolver, path-expiry, and [migration-registry](../glossary.md#migration-registry-wrapperregistry) child identifiers that Project
  consumes to rebuild rows whose source events disappear. Adapters provide interpretation behavior and do not write
  database rows or projections.
- Schema-v2 Project owns projection tables and rebuild behavior. Publication may
  retire stale direct-resolution observations through the guarded projection
  lifecycle trigger; it does not write a live/indexed comparison.
- API code reads phase projections and request-scoped lookup output; lookup may
  write only the guarded divergence ledger. Project may only retire ledger rows
  when publication changes an ENS Mainnet exact resolver to null.
- Storage owns [canonicality](../glossary.md#canonicality), snapshot selection, reusable row reads,
  and database invariants.
  API code owns route-specific joins, pagination, wire shaping, and GraphQL compatibility.
- This documented boundary is authoritative. `scripts/check-query-ownership` is a tripwire for
  known naming patterns, not a complete classification of SQL ownership. Review for every new
  direct-SQL module in `apps/api` must state whether storage or the API owns its query behavior.
- Lookup uses declared topology and manifests, not adapter internals.
- Manifest and discovery code decides what is authoritative.

## Ownership Map

| Surface | Owner | Notes |
| --- | --- | --- |
| `apps/api`, `docs/api-v2.md`, `docs/api-v2-routes.md` | Projections and API | Public route shape, route-specific joins and pagination, wire and GraphQL compatibility, API tests |
| `apps/phase-runner`, `crates/ingest`, `crates/interpret`, `crates/adapters`, `docs/chain-intake.md` | Intake and Adapters | Phase orchestration, raw intake, and schema-v2 interpretation behavior |
| `crates/project`, phase projection modules, `docs/projections.md` | Projections and API | Projection publication, current read models, and redo behavior |
| `crates/storage`, `migrations`, `docs/storage.md` | Storage and Domain | Schema, canonicality, snapshot selection, reusable row reads, database invariants, schema-migrations |
| `crates/domain` | Storage and Domain | Narrow normalization helpers, the projected resolution-topology model and classifier, and their closed wire vocabularies; persisted identity types live in `crates/storage/src/identity/types.rs` |
| `crates/manifests`, `manifests/**`, `docs/manifests.md` | Manifests and Discovery | Source authority, discovery, capability flags, watch-plan inputs |
| `crates/lookup`, `docs/execution.md` | Verified Lookup | Request-scoped resolution/primary lookup and guarded divergence observations; Project may only clear outdated direct observations when it publishes a null exact resolver |
| `docs/consumer-capabilities.md` | Conformance and Fixtures | Replacement meaning, rollout/rollback evidence |
| `.refs/MANIFEST.toml`, `docs/upstream.md` | Upstream Evidence | Pin rotation, citations, known divergences |
| `.agents/**`, `.codex/agents/**`, `.codex/rules/**`, `.codex/config.toml`, `.codex/hooks/**`, `.claude/**`, `AGENTS.md`, `CLAUDE.md` | Agent Process | Skills, subagent definitions, hooks, automation, repo-local process rules |
| `scripts/**`, `.github/**`, root `Cargo.toml`, `Cargo.lock` | Platform and DevEx | Tooling, CI, workspace-wide dependency changes |

## High-Conflict Rules

- Migrations, fixtures, manifest schemas, `crates/domain`, and process definitions are serialized review points.
- Shared public semantics, coverage meaning, source authority, replay behavior, or replacement meaning require docs in the same change.
- Parallel work should split by ownership boundary, with one integrator responsible for final consistency.
- Before staging, inspect dirty state and stage explicit paths only.
