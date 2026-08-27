# End-to-end testing plan

Status: current coverage ledger for `tests/e2e`. The harness mechanics, exact
scenario names, local command, and counted inventory live in
[`tests/e2e/README.md`](../../tests/e2e/README.md). This document records what
that inventory proves and, just as importantly, what it does not prove.

## Current evidence boundary

The suite is current evidence for the schema-v2 phase-runner pipeline. It
starts from transactions against pinned ENSv1, ENSv2, and Basenames contracts
on Anvil, then reaches the applicable assertion layer through one of two
supported inputs:

- immutable [raw facts](../glossary.md#raw-fact) materialized from the finished
  Anvil chain before interpretation; or
- the production JSON-RPC ingest redo path for intake parity, provider faults,
  and reorgs.

Every runnable scenario initializes schema-v2 with `phase-runner init-schema`.
Finished-chain fixture scenarios materialize immutable facts and the completed
ingest boundary directly; intake-parity, provider-fault, and reorg scenarios
execute the production ingest redo. Scenarios then execute interpret and
project through the production binary where their assertion needs those
layers. No runnable scenario invokes the verify phase; public lookup and
explain behavior is owned by API crate tests rather than this projection-level
suite.
Assertions read [normalized events](../glossary.md#normalized-event), phase
state, and [projections](../glossary.md#projection) directly. The route-shaped
`ProjectionReader` is a test-only schema-v2 reader; it is not an HTTP server and
is not evidence for public API behavior.

The semantic inventory is fixed at 58 scenarios:

- 55 retargeted and runnable;
- 3 explicitly retired because their runtime semantics were deleted.

The exhaustive list is in the e2e README. The crate also contains 24
harness and support checks, for 82 tests total. The passed-count gate therefore
requires 79 passed, 0 failed, and 3 ignored.

## Coverage vocabulary

- `covered_pipeline(test)` means the named contract-backed scenario ran
  through the real phase-runner binary and asserted every schema-v2 layer
  material to that row. It does not imply public HTTP behavior.
- `covered_ingest(test)` additionally means the scenario exercised the
  production JSON-RPC provider and ingest redo path.
- `known_defect(test; issue)` means a runnable fault scenario reproduces an
  already-recorded production limitation and verifies only the explicit repair
  action that follows. It is evidence of the limitation, not a guarantee.
- `retired(test; reason)` means the scenario remains in the inventory as an
  ignored test with a one-line reason because the asserted behavior was
  intentionally deleted.
- `not_covered(reason)` records a capability risk for which this suite has no
  contract-backed current scenario.

No status in this document implies live checkpoints, in-session discovered
target fetching, a continuously running worker, or v1 API integration. Those
were properties of the deleted runtime.

## Current scenario matrix

The table groups the 55 runnable scenarios by their deepest semantic claim.
The README carries every exact test path and is the count authority.

| Capability group | Current assertion boundary | Evidence |
| --- | --- | --- |
| ENSv1 registration and lifecycle | Registration, renewal, transfer/reclaim divergence, expiry/release, re-registration, resolver omission, and unadmitted-controller behavior reach current schema-v2 identity and registration projections. The one-transaction registration burst reconciles to the registrar [resource](../glossary.md#resource) with populated address/text inventory; its generic reverse-resolver observation remains raw-only rather than creating a primary-name row. The pinned registrar registers, renews, and reclaims registry ownership at the exercised boundaries. (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L157 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171 @ ens_v1@91c966f) | `covered_pipeline(register_eth_name_end_to_end, lifecycle::*, lifecycle_divergence::*, registration_with_records_reverse_and_referrer_derives_single_burst, unadmitted_controller_registration_derives_registry_side_only)` |
| ENSv1 registry hierarchy | Direct children, same-label isolation across parents, zero-owner tombstones, migration between registry instances, and late label-preimage reveal are asserted in normalized events and schema-v2 children/name projections. The deep-hierarchy case asserts no child projection below an unknown parent; it makes no API input-validation claim. The exercised hierarchy mutation is the registry's parent-authorized subnode-owner write. (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75 @ ens_v1@91c966f) | `covered_pipeline(registry_driven_reads::*, registry_migration_legacy_to_current_semantics, label_preimage_revealed_later_upgrades_child_listing)` |
| Resolver and record state | Resolver rotation/zeroing, record-version boundaries, shared-resolver fan-in, delegated writes, admitted and unenumerated record families, and resolver-local reverse-name observations are asserted from normalized rows and current projections. The pinned registry emits resolver-pointer changes, while the resolver emits address and text mutations. (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L59 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/TextResolver.sol:L15 @ ens_v1@91c966f) | `covered_pipeline(resolver_authorization::*, resolver_records::records_route_values_and_version_boundaries_follow_current_resolver, resolver_records::resolver_changes_follow_registry_and_zero_releases, resolver_records::shared_resolver_keeps_per_name_records_and_projection_marks_fan_in_unsupported, record_families::*, reverse_primary::generic_name_record_set_changed_then_cleared_stays_unadmitted)` |
| Reverse and primary names | Missing, malformed, mismatched, unauthorised, and unadmitted reverse claims remain absent from the current primary-name projection while their applicable raw or normalized evidence remains inspectable. The exercised pinned reverse path claims the reverse node and then writes its name through the selected resolver. (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L74 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L123 @ ens_v1@91c966f) | `covered_pipeline(reverse_primary::*, reverse_primary_claims::*)` |
| ENSv1 wrapping | Wrap/unwrap, born-wrapped registration, wrapped child creation, derived wrapped/emancipated/locked state, fuse/expiry changes, wrapper renewal, and single/batch ERC-1155 transfers preserve or rotate schema-v2 resource and [token lineage](../glossary.md#token-lineage) according to the asserted transition. Unsupported effective-control material stays explicit in projections. The pinned wrapper emits wrap/fuse/expiry transitions and implements born-wrapped registration, renewal, and unwrap. (upstream: .refs/ens_v1/contracts/wrapper/README.md:L32 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/README.md:L34 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L27 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L312 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L382 @ ens_v1@91c966f) | `covered_pipeline(wrapper::*, wrapper_registration::*, wrapper_renewal_and_transfers::*)` |
| Basenames | Registration, independent registry/registrar control movement, resolver records, subnames, renewal/re-registration, proxy-backed controller behavior, third-party controllers, and legacy reverse-registry facts run against the pinned Base contracts and project under the Basenames namespace. The pinned Base registrar exposes registration, registration-with-record, availability, and renewal, while the registry emits resolver-pointer updates. (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L230 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L252 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L289 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L299 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/Registry.sol:L126 @ basenames@1809bbc) | `covered_pipeline(basenames::*, basenames_lifecycle::*)` |
| ENSv2 | Root attachment, registry/registrar lifecycle, role changes, reserved/foreign registration, token sale/regeneration, resolver/subregistry changes, renewal, expiry, and re-registration run through the post-audit manifest mirror and schema-v2 phases. The post-audit registry interface defines the exercised registration, reservation, expiry, subregistry, resolver, regeneration, and parent events. (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L18 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L33 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L46 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L52 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L62 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L78 @ ens_v2@a971bd64) (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L84 @ ens_v2@a971bd64) | `covered_pipeline(ens_v2_lifecycle::*, ens_v2_registry_lifecycle_survives_successive_fixture_replays)` |
| Fixture/RPC equivalence | Upfront immutable facts and production RPC ingest produce equal normalized-event rows after normalizing only corpus-local [contract-instance ids](../glossary.md#contract-instance). The comparison includes transaction index, log index, raw-fact reference, before/after state, and every other deterministic selected column. Wrapper/reverse outputs are covered by the second scenario. The pure contract-instance normalization oracle is counted as a support check, not a semantic scenario. | `covered_ingest(upfront_facts_match_rpc_ingest_outputs, upfront_facts_match_rpc_ingest_wrapper_reverse_outputs)` |
| Successive execution and redo determinism | Successive synchronous fixture replays equal a single finished-chain pass. Projection-only redo and interpret-plus-project redo keep the selected schema-v2 read models stable. These replace the old worker-restart and backfill claims; they do not claim process restart or checkpoint behavior. | `covered_pipeline(rich_chain_successive_fixture_replays_match_single_pass, rich_chain_projection_and_normalized_event_replay_are_route_stable, ens_v2_registry_lifecycle_survives_successive_fixture_replays)` |
| Reorgs | RPC ingest first derives a losing fork. Rewind then proves that losing normalized rows remain physically present but are excluded by the mandatory chain-lineage join. The stamped [plain-events redo](../glossary.md#plain-events-redo) removes the superseded derivation, derives the winning fork, retains losing raw facts and orphaned lineage, and matches a clean winning projection corpus. The rich-chain scenario also pins the #305 production history loader: the canonical read excludes the losing event before redo and returns the winning event afterward through the lineage join. The composed case proves the untouched Ethereum phase head and name projection do not move during a Base-only reorg. | `covered_ingest(rich_chain_live_reorg_converges_to_winning_branch, base_reorg_leaves_ethereum_canonicality_untouched)` |
| Silent log omission | A valid but silently incomplete log array is accepted by ingest and the harness publishes the range as readable without the selected raw log. No durable marker forces a refetch. The scenario explicitly runs a clean ingest redo and then proves normalized rows and projections match a clean control. | `known_defect(silently_short_logs_are_accepted_until_explicit_refetch_matches_control; #154)` |
| Retriable provider and receipt faults | Truncated JSON, transient JSON-RPC errors, delayed responses, and a partial receipt batch pass through the production provider/ingest implementation. Failed or incomplete attempts are followed by an explicit clean redo, after which raw facts, normalized rows, and projections match a clean control. | `covered_ingest(transient_provider_faults_and_partial_receipts_recover_to_control)` |
| Cross-protocol composition | One dual-Anvil corpus retains separate ENS and Basenames resources, positions, address collections, and manifest state. The ENS generic reverse observation remains `not_found` as a declared primary name, while the admitted Base claim serves only under Basenames; neither namespace leaks into the other. | `covered_pipeline(composed_mainnet_profile_serves_both_protocols_without_leakage)` |

## Explicit retirements

The ignored tests remain executable inventory entries, so their count and
reason are visible in the gate output.

- `retired(provider_faults::transient_get_code_retries_primary_without_using_configured_fallback; runtime bytecode-hash admission and its primary retry path were deleted)`
- `retired(provider_faults::pruned_get_code_fails_closed_then_uses_configured_fallback; the archive fallback was deleted with runtime bytecode-hash admission)`
- `retired(registry_preimages::registry_only_non_eth_tree_derives_declared_state; the schema-v2 identity gate does not materialize an unknown registry-only surface from a generic resolver event)`
## Capabilities not covered here

These gaps are not hidden behind a green phase-runner scenario:

- Public route status codes, envelopes, query validation, pagination, and
  lookup/explain output are covered outside this suite by API crate tests.
- Wildcard, alias, CCIP-read, and Basenames L1 transport execution do not have
  a deployable pinned end-to-end corpus in this suite.
- ENSv2 reverse/primary intake, registry TTL changes, root/DNS registrar
  operations, wrapper upgrades, and indexed approval state have no current
  contract-backed scenario.
- A continuous service loop, worker restart, old backfill scheduler,
  completeness/frontier proofs, startup adapter checkpoints, and runtime
  bytecode-hash discovery are deleted semantics rather than current gaps.
- No scenario starts the API, so public behavior must not be inferred from
  route-shaped projection reads.

## Runtime topology

Each scenario owns an isolated PostgreSQL database cloned from a migration
template. `phase-runner init-schema` creates schema-v2. A generated
[deployment profile](../glossary.md#deployment-profile) substitutes only
scenario-local chain ids, addresses, and ranges while preserving the shipped
family/version structure. Manifest-specific phase-runner binaries are hard
links with bounded lifetimes, so concurrent scenarios can share builds without
accumulating executable copies.

Most scenarios capture a finished Anvil chain as immutable facts and run the
finite phase spine synchronously. Only intake-parity, provider-fault, and reorg
scenarios fetch through JSON-RPC. A readiness predicate is evaluated once after
the required phases; a false result is a failure, not an asynchronous retry
signal.

## CI gate

The `e2e` CI job provisions PostgreSQL, installs pinned Foundry, verifies
`.refs`, prebuilds `phase-runner`, and runs `tests/e2e/run-gate` with eight test
threads. The gate requires the exact 79/0/3 library-test summary in addition to
Cargo's exit status. The aggregate `test` job requires this e2e job, so a
filtered, compile-only, or partially executed suite cannot satisfy CI.

## Ledger discipline

- Change this ledger in the same PR that changes a scenario's evidence layer,
  retirement, or deletion.
- A green projection assertion never graduates a public API claim.
- New upstream behavior claims in tests or docs cite pinned `.refs` evidence.
- A production contradiction is a stop condition for this lane; the scenario
  reports it, and production code changes land separately.
- Scenario removal requires an explicit reason. Renaming a
  scenario must preserve its semantic row or document why that semantic no
  longer exists.
