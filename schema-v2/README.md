# Fresh product schema

This directory is the Stage A1 baseline. It does not modify `migrations/`. The SQL stores only the product data and phase state authorized below.

## Chain lineage and heads

`chain_lineage`, `chain_header_audit`, and `chain_heads` store block ancestry, explicit chain state, optional raw header fields, and the latest, safe, and finalized markers. A stored block's chain, hash, parent, height, and timestamp are immutable; only its explicit canonicality and observation metadata may change. Head validation locks its referenced lineage rows through commit, so a concurrent canonicality change cannot strand a marker on a noncanonical block. Intake writes these tables. The phase runner, the API status path, and read-only block inspection read them. The [storage census and head-marker finding](../simplification-audit-20260730.md#cratesstorage-fable) authorize all three tables, and [audit entry 9](../simplification-audit-20260730.md#inventory--verdicts) authorizes the stored-header inspection fields.

## Raw facts and inspection

`raw_transactions`, `raw_receipts`, and `raw_logs` store immutable [raw facts](../docs/glossary.md) under a block hash. Intake writes these tables. The interpreter, hydration code, and read-only block or raw-event inspection read them. The [storage census](../simplification-audit-20260730.md#cratesstorage-fable) and the [permanent raw-store decision](../simplification-audit-20260730.md#maintainer-question-list-consolidated-for-decision) authorize these tables; [audit entry 9](../simplification-audit-20260730.md#inventory--verdicts) authorizes their inspection reads.

## Identity and contract admission

`contract_instances`, `contract_instance_addresses`, `discovery_edges`, `token_lineages`, `resources`, `name_surfaces`, and `surface_bindings` store stable contract, token, authority-object, raw-name, and name-to-authority identities. Manifest sync writes declared contract rows. The interpreter writes event-derived identity rows. [Projection](../docs/glossary.md) and execution code read them. The [identity storage census](../simplification-audit-20260730.md#cratesstorage-fable), the [raw-label normalization decision](../simplification-audit-20260730.md#normalization-as-a-gate-not-stored-identity), and the [event-announcement discovery design](../simplification-audit-20260730.md#discovery-design-decided-2026-07-30) authorize these tables.

The logical identity of an on-chain name is `<namespace>:<namehash>`. On chain, a name is its namehash: ENSv1 registry records are keyed by `bytes32` node `(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L13 @ ens_v1@91c966f)`, ENSv2 resolver permissions and records use the namehash/node `(upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L68 @ ens_v2@ccaeb58)` `(upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L133 @ ens_v2@ccaeb58)`, and Basenames defines the resolver node as the namehash of the name `(upstream: .refs/basenames/src/L2/L2Resolver.sol:L88 @ basenames@1809bbc)`. Identity is therefore chain-native and independent of normalization rules. Normalization is only a per-label visibility flag; it never participates in identity. The current [surface and resource identity ADR](../docs/adrs/0002-surface-resource-identity.md) will be amended to match during the doc-first Stage D4 docs rewrite, following the audit's [Normalization as a gate, not stored identity](../simplification-audit-20260730.md#normalization-as-a-gate-not-stored-identity) decision.

## Manifest declarations

`manifest_versions`, `manifest_contract_instances`, and `manifest_discovery_rules` store loaded declarations, declared contracts, start blocks, proxy links, ABI data, and admission rules. Manifest sync writes these tables. Intake, interpretation, projection, and execution read them. The [manifest census](../simplification-audit-20260730.md#cratesmanifests--domain--metrics-fable) authorizes the declaration tables. The [declared-means-supported decision](../simplification-audit-20260730.md#maintainer-question-list-consolidated-for-decision) excludes a capability-flag table.

## Normalized events

`normalized_events` stores plain [normalized events](../docs/glossary.md) with source positions and before-and-after state. Only the interpreter writes this table. Projection builders and read-only history or raw-event inspection read it. The [adapter census](../simplification-audit-20260730.md#cratesadapters-fable) and the [storage census](../simplification-audit-20260730.md#cratesstorage-fable) authorize this table.

## Current projections

`name_current`, `children_current`, `permissions_current`, `permissions_current_resource_summary`, `record_inventory_current`, `resolver_current`, `address_names_current`, and `primary_names_current` are the retained current-state tables written by the retained projection builders. Projection code writes them. The API and GraphQL read them. The [worker census](../simplification-audit-20260730.md#appsworker--cratesexecution-fable) and the [storage census](../simplification-audit-20260730.md#cratesstorage-fable) authorize this enumerated set. The [support-status decision](../simplification-audit-20260730.md#kimi-k3-second-opinion-lenses--adjudicated) keeps explicit support fields and removes exhaustiveness accounting.

## Label data

`label_preimages` stores verified raw labels and the `normalized_under_version` flag. `ens_names` stores the imported rainbow rows. Storage and the interpreter write verified preimages. The import tool writes rainbow rows. Identity and child projection code read both tables. The [normalization decision](../simplification-audit-20260730.md#normalization-as-a-gate-not-stored-identity) and the [label-preimage storage census](../simplification-audit-20260730.md#cratesstorage-fable) authorize both tables.

## Service heartbeats

`service_heartbeats` stores one liveness row for each service instance, chain, and phase. The new phase-runner services write it. Health checks and `/v2/status` read it. The schema leaves `service_name` open because Stage A2 assigns the new names; it does not admit the retired indexer or worker names by default. The [indexer heartbeat absorption](../simplification-audit-20260730.md#appsindexer-fable) and the [service-heartbeat storage census](../simplification-audit-20260730.md#cratesstorage-fable) authorize this table; build-plan amendment F defines its per-chain and per-phase shape.

## Live/indexed resolution differences

`resolution_divergences` stores a row only when a live resolver answer differs from the indexed answer at the recorded positions, and it keeps at most one unresolved row for each exact name, resolver, and request. Every active row must identify a canonical `chain_lineage` block with the same chain, hash, height, and timestamp for each recorded position. The API execution path writes and clears rows; a chain canonicality change also clears every active row that observed the affected block. Position validation locks those lineage rows through commit, so a concurrent canonicality change cannot miss an uncommitted API insert. Projection support logic and operators read the rows. The [no-outcome-cache decision](../simplification-audit-20260730.md#maintainer-question-list-consolidated-for-decision) authorizes this table as the only durable execution-adjacent store.

## Ingest cursors and phase state

`ingest_cursors` stores one cursor for each chain source, and `chain_phase_state` stores one state row for each of the five phases, including the verify phase trust level and an explicit `paused` state for the capacity guard. The phase runner writes both tables. The runner, redo command, health checks, and status path read them. The [indexer absorption census](../simplification-audit-20260730.md#appsindexer-fable) authorizes both tables. [Build-plan amendment B](../simplification-build-plan-20260730.md#b-verify-carried-raw-before-deleting-its-coverage-record) lists the seed inputs as Base block `48,428,000`, the verified historical starts for the three newly watched signature groups, and the observed Ethereum head. The schema does not preload the dynamic starts or Ethereum head. [Build-plan amendment D](../simplification-build-plan-20260730.md#d-status-label-honesty-razor-3) defines provider-trusted, independently cross-checked, and node-checked status. [Build-plan amendment F](../simplification-build-plan-20260730.md#f-specs-pinned) defines the five phase names and the ingest-to-live handoff fields. The [approved phase-runner design](../a2-phase-runner-design-20260731.md#status-and-heartbeats) requires capacity pauses to remain distinguishable from failures.
