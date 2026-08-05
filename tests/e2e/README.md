# bigname end-to-end scenario tests

This package exercises ENSv1, ENSv2, and Basenames contract emissions against
schema-v2 through the production `phase-runner` binary. It does not start the
deleted indexer or worker, and it does not start the retained v1 API. Assertions
that previously read HTTP responses now read schema-v2
[projections](../../docs/glossary.md#projection) and phase state directly
through the test-only `ProjectionReader`.

## Prerequisites

- Foundry v1.7.1 with `anvil` on `PATH`.
- Pinned upstream checkouts from `scripts/sync-refs`.
- An isolated PostgreSQL test server supplied by `scripts/test-db`.

Run the same passed-count gate used by CI:

```sh
PATH=/home/ubuntu/.foundry/bin:$PATH scripts/test-db -- tests/e2e/run-gate
```

If Foundry is already on `PATH`, omit the explicit prefix. For a plain test
run without the count assertion:

```sh
scripts/test-db -- cargo test --manifest-path tests/e2e/Cargo.toml --locked -- --test-threads=8
```

The gate requires the exact library-test summary `79 passed; 0 failed; 6
ignored`. It checks both Cargo's exit status and the passed/failed/ignored
counts, so a prematurely successful process or an accidentally filtered suite
cannot satisfy CI.

## Harness design

1. `HarnessDb::create` clones an isolated migration template and runs
   `phase-runner init-schema`. Scenario pools select only `bigname_phase`.
2. Each scenario deploys its local ENS or Basenames topology on Anvil and
   generates a temporary [deployment profile](../../docs/glossary.md#deployment-profile)
   with scenario-local addresses. The checked-in manifest tree is never edited.
3. Most scenarios materialize immutable Anvil blocks, transactions, receipts,
   and logs as [raw facts](../../docs/glossary.md#raw-fact) up front, then
   execute `phase-runner redo` for interpret and project. This is the
   deterministic fixture pattern used by the production phase-runner tests.
   Scenarios that add facts in stages synchronously replay each selected
   snapshot; they do not claim a continuously running worker or live
   checkpoint.
4. Provider-fault and intake-path parity scenarios execute the real JSON-RPC
   ingest redo path.
   Truncated JSON, transient errors, delayed responses, omitted logs, and
   partial receipts pass through the production provider implementation.
   The silent-log case explicitly pins pre-existing defect #154: a valid but
   incomplete response is accepted and published, and only the scenario's
   explicit clean redo repairs it. Its green result is evidence of that known
   limitation plus deterministic repair, not automatic containment.
5. Reorg scenarios ingest both forks through that same RPC path, invoke the
   production `phase-runner rewind` command at the stored ancestor, then
   complete the mandatory interpret/project replay stamped by the rewind.
   Completed redo removes superseded losing-fork normalized derivations and
   derives the winning fork. Losing raw facts remain anchored to retained
   orphaned lineage as the audit trail, and winning projections must match a
   clean corpus. The composed Base/ENS case also proves the untouched chain
   does not move.
6. Schema-v2 projections are queried directly. Route-shaped helper inputs are
   retained only to keep each scenario's semantic assertions recognizable;
   no network API server or legacy public-schema read occurs.
7. A scenario readiness predicate is evaluated once after its synchronous
   phase-runner commands. A false result fails the scenario instead of being
   treated as an asynchronous retry condition.
8. Deployment-profile-specific phase-runner executables are hard links, not multi-gigabyte
   copies. Direct test runs unlink each one when its last scenario lease is
   dropped. The counted gate retains links only for in-process build sharing
   and removes its run-scoped directory on exit.

The verification-only execution scenario remains deferred because its only
observable contract is the public lookup/explain API. No runnable scenario
claims v1 API integration or deleted checkpoint/completeness semantics.

## CI shape

The `e2e` job in `.github/workflows/ci.yml` keeps PostgreSQL and Foundry,
syncs and verifies `.refs`, prebuilds the shared `phase-runner` binary, and
runs `tests/e2e/run-gate` with eight test threads. The aggregate `test` job
continues to require the e2e job.

## Coverage ledger

The semantic inventory contains 61 scenario tests:

- 55 retargeted and runnable;
- 5 explicitly retired with one-line reasons;
- 1 explicitly deferred to C2/C3.

The 55 runnable scenarios include the #154 known-defect reproduction described
above; it is kept runnable so the provider path and explicit repair remain
observable rather than being hidden as an ignored test.

The crate contains 85 total tests when 24 harness/support checks are included.
The pre-retarget crate contained 88; the net change is -3: five obsolete
Cargo-artifact tests for the deleted API/indexer/worker binary bundle were
removed, while deployment-profile binary lifecycle and normalized-event
parity-completeness regression tests were added. The pure in-memory
`catchup_equivalence::primary_route_normalization_preserves_contract_instance_identity`
normalization oracle is counted as support rather than as a contract-backed
semantic scenario. No semantic scenario was silently removed.

### Retargeted and runnable (55)

- Basenames:
  `basenames::basenames_declared_state_matrix_end_to_end`;
  `basenames_lifecycle::basenames_subnames_list_preimages_placeholders_and_tombstones`;
  `basenames_lifecycle::l2_resolver_records_clear_and_contenthash_gap`;
  `basenames_lifecycle::legacy_reverse_registrar_stays_registry_and_raw_record_only`;
  `basenames_lifecycle::renew_release_and_premium_reregistration_rotate_lineage`;
  `basenames_lifecycle::third_party_controller_registration_degrades_without_label_events`;
  `basenames_lifecycle::upgradeable_controller_proxy_registers_and_renews`.
- Intake-path parity and composition:
  `catchup_equivalence::upfront_facts_match_rpc_ingest_outputs`;
  `catchup_equivalence::upfront_facts_match_rpc_ingest_wrapper_reverse_outputs`;
  `cross_protocol::base_reorg_leaves_ethereum_canonicality_untouched`;
  `cross_protocol::composed_mainnet_profile_serves_both_protocols_without_leakage`.
- ENSv2:
  `ens_v2_lifecycle::expiry_passes_then_reregistration_advances_lineage`;
  `ens_v2_lifecycle::renewal_preserves_promoted_coverage_and_registry_edges_follow`;
  `ens_v2_lifecycle::reserved_labels_foreign_registrar_and_token_sale`;
  `ens_v2_lifecycle::resolver_and_subregistry_edges_follow_set_change_zero`;
  `ens_v2_lifecycle::root_apex_attach_and_root_scope_roles`;
  `ens_v2_live_poll::ens_v2_registry_lifecycle_survives_successive_fixture_replays`.
- ENSv1 lifecycle:
  `lifecycle::expire_without_reregistration_releases_and_unlists_registration`;
  `lifecycle::expiry_grace_and_reregistration_rotate_identity`;
  `lifecycle::register_without_resolver_keeps_declared_resolver_empty`;
  `lifecycle::renew_and_transfer_keep_identity`;
  `lifecycle_divergence::transfer_without_reclaim_keeps_registry_owner_divergent`.
- Replay, reorg, and provider faults:
  `perturbations::rich_chain_live_reorg_converges_to_winning_branch`;
  `perturbations::rich_chain_projection_and_normalized_event_replay_are_route_stable`;
  `perturbations::rich_chain_rpc_ingest_normalized_events_match_upfront_facts`;
  `perturbations::rich_chain_successive_fixture_replays_match_single_pass`;
  `provider_faults::silently_short_logs_are_accepted_until_explicit_refetch_matches_control`;
  `provider_faults::transient_provider_faults_and_partial_receipts_recover_to_control`.
- Registrations and record families:
  `record_families::pubkey_write_on_admitted_resolver_stays_raw_only`;
  `record_families::remaining_record_families_derive_normalized_but_stay_unenumerated`;
  `register_eth_name::register_eth_name_end_to_end`;
  `registration_burst::registration_with_records_reverse_and_referrer_derives_single_burst`.
- Registry and migration:
  `registry_driven_reads::deep_registry_hierarchy_lists_direct_children_only`;
  `registry_driven_reads::registry_driven_reads`;
  `registry_driven_reads::same_label_under_two_parents_keeps_children_distinct`;
  `registry_driven_reads::zero_owner_subname_leaves_default_children_listing`;
  `registry_migration::registry_migration_legacy_to_current_semantics`;
  `registry_preimages::label_preimage_revealed_later_upgrades_child_listing`.
- Resolver and reverse claims:
  `resolver_authorization::operator_delegate_writes_match_owner_authorship`;
  `resolver_records::records_route_values_and_version_boundaries_follow_current_resolver`;
  `resolver_records::resolver_changes_follow_registry_and_zero_releases`;
  `resolver_records::shared_resolver_keeps_per_name_records_and_projection_marks_fan_in_unsupported`;
  `reverse_primary::reverse_claim_invalid_name_surfaces_raw_claim`;
  `reverse_primary::generic_name_record_set_changed_then_cleared_stays_unadmitted`;
  `reverse_primary_claims::authorised_third_party_generic_name_record_does_not_key_claim`;
  `reverse_primary_claims::claim_without_name_record_keeps_candidate_absent`;
  `reverse_primary_claims::forward_mismatch_keeps_generic_name_record_unadmitted`;
  `reverse_primary_claims::unadmitted_reverse_resolver_keeps_candidate_absent`.
- Authority and wrapping:
  `unadmitted_controller::unadmitted_controller_registration_derives_registry_side_only`;
  `wrapper::wrapper_wrap_fuses_subnames_and_unwrap_restore_identity`;
  `wrapper_registration::born_wrapped_registration_retains_wrapper_authority`;
  `wrapper_registration::parent_burns_pcc_then_extends_existing_child_expiry`;
  `wrapper_registration::wrap_existing_registry_subname_rotates_child_only`;
  `wrapper_renewal_and_transfers::wrapped_erc1155_single_and_batch_transfers_preserve_identity`;
  `wrapper_renewal_and_transfers::wrapped_renewal_tracks_registrar_expiry_without_wrapper_event`.

### Retired with reason (5)

- `register_eth_name::live_worker_applies_registration_and_renewal_while_api_serves` — the continuously running worker/v1 API coordination loop was deleted with the old runtime.
- `provider_faults::transient_get_code_retries_primary_without_using_configured_fallback` — runtime bytecode-hash admission and its `eth_getCode` retry path were deleted in Stage B.
- `provider_faults::pruned_get_code_fails_closed_then_uses_configured_fallback` — the code-hash archive fallback was deleted with runtime bytecode-hash admission.
- `resolver_records::byte_identical_public_resolver_copy_converges_to_admitted_profile` — observed-code-hash resolver admission was replaced by declared-list classification.
- `registry_preimages::registry_only_non_eth_tree_derives_declared_state` — generic resolver `NameChanged` no longer materializes an unknown registry-only surface under the schema-v2 identity gate.

### Deferred to C2/C3 (1)

- `verified_resolution::direct_path_verified_query_via_local_universal_resolver_persists_trace` — this is exclusively a public lookup/explain API contract over execution output; it cannot be asserted honestly before API cutover.

### Count deltas

| Measure | Historical baseline | Retargeted suite | Delta |
| --- | ---: | ---: | ---: |
| Total crate tests | 88 | 85 | -3 |
| Semantic scenario inventory | 62 at the retarget base, including one pure helper | 61 | -1 reclassified |
| Runnable passed-count gate | 65 in the historical Anvil gate | 79 | +14 |
| Anvil-backed semantic inventory | 65 historical gate reference | 61 | -4 |
| Runnable Anvil-backed semantic scenarios | 65 historical gate reference | 55 | -10 |

The two 65 comparisons are reported because that is the historical gate
reference, but the current passed-count denominator is explicit: 55 runnable
Anvil scenarios and 24 harness/support checks produce 79 passes. Six Anvil
semantic scenarios are explicitly
ignored (five retired and one deferred).

## Diagnostics

- `BIGNAME_E2E_KEEP_DB=1` keeps per-scenario databases and prints their URLs.
- `BIGNAME_E2E_READY_TIMEOUT_SECS` controls readiness waits (default 600s).
- `BIGNAME_E2E_COMMAND_TIMEOUT_SECS` controls bounded phase-runner commands
  (default 600s).
- `BIGNAME_E2E_TEST_THREADS` controls the gate's test thread count (default
  8).
- Command logs are written under the system temporary directory and removed
  after successful completion; failures include their paths and reversed
  tails.
