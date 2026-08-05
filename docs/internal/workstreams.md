# Ownership Boundaries

Internal reference for splitting implementation work. `AGENTS.md` is the process rulebook; this file only maps review ownership and high-conflict surfaces.

## Boundaries

- Schema-v2 interpret writes identity rows and normalized events; adapters
  provide interpretation behavior and do not write projection rows.
- Projection workers own projection tables and rebuild behavior.
- API code reads projections and retained execution output only for explicit
  diagnostics; fresh v2 lookup may write the guarded divergence ledger.
- Execution uses declared topology and manifests, not adapter internals.
- Manifest and discovery code decides what is authoritative.

## Ownership Map

| Surface | Owner | Notes |
| --- | --- | --- |
| `apps/api`, `docs/api-v2.md`, `docs/api-v2-routes.md` | Projections and API | Public route shape, response contracts, API tests |
| `apps/phase-runner`, `crates/ingest`, `crates/interpret`, `crates/adapters`, `docs/chain-intake.md` | Intake and Adapters | Phase orchestration, raw intake, and schema-v2 interpretation behavior |
| `apps/worker`, projection modules, `docs/projections.md` | Projections and API | Projection apply/rebuild, current read models, worker-owned operational commands |
| `crates/storage`, `migrations`, `docs/storage.md` | Storage and Domain | Schema, canonicality, migrations, storage helpers |
| `crates/domain` | Storage and Domain | Narrow normalization helpers only; persisted identity types live in `crates/storage/src/identity/types.rs` |
| `crates/manifests`, `manifests/**`, `docs/manifests.md` | Manifests and Discovery | Source authority, discovery, capability flags, watch-plan inputs |
| `crates/execution`, `docs/execution.md` | Verified Execution | Resolution/primary execution, traces, invalidation |
| `docs/consumer-capabilities.md` | Conformance and Fixtures | Replacement meaning, rollout/rollback evidence |
| `.refs/MANIFEST.toml`, `docs/upstream.md` | Upstream Evidence | Pin rotation, citations, known divergences |
| `.agents/**`, `.codex/agents/**`, `.codex/rules/**`, `.codex/config.toml`, `.codex/hooks/**`, `.claude/**`, `AGENTS.md`, `CLAUDE.md` | Agent Process | Skills, subagent definitions, hooks, automation, repo-local process rules |
| `scripts/**`, `.github/**`, root `Cargo.toml`, `Cargo.lock` | Platform and DevEx | Tooling, CI, workspace-wide dependency changes |

## High-Conflict Rules

- Migrations, fixtures, manifest schemas, `crates/domain`, and process definitions are serialized review points.
- Shared public semantics, coverage meaning, source authority, replay behavior, or replacement meaning require docs in the same change.
- Parallel work should split by ownership boundary, with one integrator responsible for final consistency.
- Before staging, inspect dirty state and stage explicit paths only.
