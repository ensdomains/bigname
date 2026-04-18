# Conformance Harness

Bootstrap supported-read contract harness for already-shipped declared-state routes:

- `GET /v1/namespaces/{namespace}`
- `GET /v1/manifests/{namespace}`
- `GET /v1/names/{namespace}/{name}`
- `GET /v1/history/names/{namespace}/{name}`
- `GET /v1/history/resources/{resource_id}`

The harness is a standalone Rust package rooted in this directory so it can run without changing the workspace root.

Smoke test:

```sh
cargo test shipped_api::conformance::smoke_supported_reads_contract_bootstrap -- --exact
```

Run the full route set:

```sh
cargo test
```

Database:

- uses `BIGNAME_DATABASE_URL` when set
- otherwise falls back to the bootstrap default `postgres://bigname:bigname@127.0.0.1:5432/bigname`
- each test creates and drops its own temporary database
