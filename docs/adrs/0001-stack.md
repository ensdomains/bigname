# ADR 0001: Stack And Repo Layout

Status: Accepted
Date: 2026-04-16

Amended: 2026-08-06. The phase runner now owns ingest, interpretation,
projection, verification, live follow, and redo. The old indexer, worker, and
legacy execution crate have been deleted.

## Context

`bigname` needs a replay-safe indexing and read system with high schema complexity, strong auditability requirements, and enough modularity for parallel work from an empty repository.

## Decision

Use a Rust modular monolith for the first production version.

Repository shape:

- `apps/api`
- `apps/phase-runner`
- `crates/domain`
- `crates/storage`
- `crates/manifests`
- `crates/adapters`
- `crates/ingest`
- `crates/interpret`
- `crates/lookup`
- `crates/project`
- `tests/e2e`

Baseline technology choices:

- Rust workspace with `tokio`
- `axum` for the HTTP API
- `sqlx` with PostgreSQL for storage access and migrations
- `serde` for wire and persistence serialization
- `clap` for binary entrypoints
- `tracing` and OpenTelemetry-compatible exports for observability

Local development baseline:

- Docker Compose for PostgreSQL
- checked-in migrations
- one command to boot the API and configured phase runner in development

Testing baseline:

- `cargo fmt`
- `cargo clippy`
- `cargo test` or `cargo nextest`
- migration verification in CI
- a standalone Rust end-to-end package under `tests/e2e`

Operational baseline:

- no separate message bus
- phase progress, redo, and heartbeat state are coordinated through PostgreSQL
- introduce separate queueing infrastructure only if measured load demands it

## Consequences

Positive:

- one language across API, intake, projections, and execution
- simpler local setup than a distributed multi-service start
- easier refactoring while semantics are still settling
- clear crate boundaries for parallel implementation

Tradeoffs:

- careful module boundaries are required to avoid a monolith blob
- database-backed coordination may need replacement at higher scale
- `sqlx` query discipline must be maintained to keep schema changes safe
