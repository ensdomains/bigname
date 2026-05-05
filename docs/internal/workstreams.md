# Parallel workstreams

Internal reference. Companion to [`development-plan.md`](./development-plan.md). Describes how implementation work splits so multiple engineers can move concurrently without fighting over semantics or storage boundaries.

## Freeze before forking

Minimum interface freeze before parallel work starts:

- `architecture.md`
- `chain-intake.md`
- `api-v1.md`
- `storage.md`
- `manifests.md`
- `consumer-capabilities.md`

## Workstream matrix

| Workstream | Owns | Can start after | Main outputs |
| --- | --- | --- | --- |
| Platform and DevEx | workspace bootstrap, CI, local dev, config | architecture frozen | Rust workspace, compose/dev env, CI gates |
| Storage and Domain | migrations, IDs, storage traits, domain types | `storage.md`, `chain-intake.md` | schema, migration harness, shared types |
| Manifests and Discovery | manifest loader, discovery graph, capability registry | `manifests.md` | manifest crate, discovery persistence, admission logic |
| Intake and Adapters | chain intake, ENSv1/ENSv2/Basenames adapters, normalized events | `storage.md`, `chain-intake.md`, `manifests.md` | raw fact intake, per-chain provider availability, adapter routing, normalized events |
| Projections and API | current-state projections, read handlers, OpenAPI output | `api-v1.md`, `projections.md`, `storage.md` | read models, API routes, contract tests |
| Verified Execution | resolution execution, primary verification, trace persistence | `execution.md`, `manifests.md`, `storage.md` | execution crate, invalidation, explain traces |
| Conformance and Fixtures | fixtures, replay tests, capability checks | `api-v1.md`, `execution.md`, `consumer-capabilities.md` | golden fixtures, contract tests, replay suites, capability mapping |

## Hard boundaries

- Adapters write normalized events, not projection rows.
- API code reads projections and execution output, not raw-fact tables.
- Execution code consumes declared topology and manifests through stable interfaces, not adapter internals.
- Manifest/discovery code decides what is authoritative; adapters consume that decision.
- Changes to shared IDs, enums, or coverage semantics require doc updates before code merges.

## Repository ownership

| Path | Workstream |
| --- | --- |
| `apps/api` | Projections and API |
| `apps/indexer` | Intake and Adapters (selected-profile provider setup, automatic bootstrap) |
| `apps/worker` | Projections, replay, bounded backfill, finalized catch-up, capacity-guarded chunks, execution jobs |
| `crates/domain` | Storage and Domain |
| `crates/storage` | Storage and Domain |
| `crates/manifests` | Manifests and Discovery |
| `crates/adapters` | Intake and Adapters |
| `crates/execution` | Verified Execution |
| `crates/test-support` | Conformance and Fixtures |
| `tests/conformance` | Conformance and Fixtures |

`crates/domain` is the highest-conflict area. Keep it narrow and change it deliberately.

## Recommended startup sequence

Week 1:

1. Bootstrap the Rust workspace, CI, config, dev services.
2. Land the storage skeleton and migrations.
3. Land the manifest loader and discovery schema.

In parallel once storage/domain interfaces are merged:

1. Build Ethereum and Base intake.
2. Build the first ENSv1 adapter slice.
3. Build the first projection worker and `GET /v1/names/{namespace}/{name}`.
4. Build execution-trace persistence and the first verified resolution flow.

## Merge discipline

- Prefer short-lived branches scoped to one workstream.
- Don't share write ownership of migration files without coordination.
- Treat fixture updates as cross-workstream review points.
- If a workstream needs a new shared field or enum, update the relevant doc first.
- Bootstrap backfill and finalized catch-up progress are operational readiness signals — not route-coverage or consumer-replacement evidence without the full admitted-history and conformance gates in `consumer-capabilities.md`.
