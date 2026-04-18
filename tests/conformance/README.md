# Conformance Harness

Bootstrap supported-read contract harness for already-shipped declared-state routes and
collections:

- `GET /v1/namespaces/{namespace}`
- `GET /v1/manifests/{namespace}`
- `GET /v1/names/{namespace}/{name}/children`
- `GET /v1/addresses/{address}/names`
- `GET /v1/names/{namespace}/{name}`
- `GET /v1/coverage/{namespace}/{name}`
- `GET /v1/resources/{resource_id}/permissions`
- `GET /v1/resolvers/{chain_id}/{resolver_address}`
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

Execution notes:

- uses `BIGNAME_DATABASE_URL` when set
- otherwise falls back to the bootstrap default `postgres://bigname:bigname@127.0.0.1:5432/bigname`
- each test creates, migrates, and drops its own temporary database
- the child collection contract seeds `children_current` rows and covers both the base
  `GET /v1/names/{namespace}/{name}/children` response and the shipped `include=counts`
  variant; the harness also asserts that unsupported non-`declared` `surface_classes` are
  rejected
- the address-name collection contract seeds `address_names_current` rows plus the backing
  surfaces, resources, token lineage, and bindings; the harness covers the base
  `GET /v1/addresses/{address}/names` response plus shipped `namespace`, `relation`, and
  `dedupe_by` query handling
- the coverage contract reuses the exact-name projection seed and asserts that
  `GET /v1/coverage/{namespace}/{name}` preserves the shipped name payload envelope while moving
  the explain block into `declared_state`
- the resource-permissions contract seeds `permissions_current` rows and covers both the base
  `GET /v1/resources/{resource_id}/permissions` collection response and the shipped `subject` and
  `scope` query filters
- the resolver-overview contract seeds `resolver_current` rows and asserts the shipped declared
  summary sections, projection provenance, coverage, and lowercase address normalization for
  `GET /v1/resolvers/{chain_id}/{resolver_address}`
