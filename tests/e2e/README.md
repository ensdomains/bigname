# bigname end-to-end scenario tests

This package exercises ENSv1, ENSv2, and Basenames contract emissions against
schema-v2 through the production `phase-runner` binary. It does not start the
deleted indexer or worker and does not start an API server. Assertions that
previously read HTTP responses now read schema-v2
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

The default gate requires the exact library-test summary `84 passed; 0 failed;
3 ignored; 0 filtered out`. CI shard 1 requires `43 passed; 0 failed; 2
ignored; 42 filtered out`, and shard 2 requires `41 passed; 0 failed; 1
ignored; 45 filtered out`. The gate checks both Cargo's exit status and every
summary count, so a prematurely successful process or an incorrectly filtered
suite cannot satisfy CI.

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
   snapshot; they do not claim a continuously running service or live
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
   The rich-chain case also pins the #305 production history-loader fix: its
   canonical read excludes the losing event before redo and returns the winning
   event after redo through the chain-lineage join.
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

The executor-only verified-resolution scenario was deleted with the legacy
execution plane. Public lookup behavior remains covered by API crate tests; no
runnable e2e scenario claims deleted checkpoint or completeness semantics.

## CI shape

The e2e builder in `.github/workflows/ci.yml` builds one shared
`phase-runner` artifact. Two explicit scenario shards independently provision
PostgreSQL and Foundry, verify that artifact, and run `tests/e2e/run-gate` with
eight test threads. A result-only `test (e2e)` job rejects any builder or shard
result other than success, and the aggregate `test` job continues to require
that result.

## Shard assignment and refresh

`tests/e2e/run-gate` checks in two duration-balanced lists of full test names.
Before executing scenarios, each shard discovers the complete library-test set
and ignored subset, then proves that the two lists have no duplicates or
intersection and that their union exactly equals discovery. Any added, removed,
renamed, or newly ignored test therefore fails closed before scenario execution.

To refresh the lists, provision PostgreSQL and Foundry as above, then discover
and time the runnable names from the repository root:

```sh
scripts/test-db -- bash -euo pipefail -c '
  cargo build --locked --package phase-runner --bin phase-runner
  cargo test --manifest-path tests/e2e/Cargo.toml --locked -- --list |
    sed -n "s/: test$//p" | LC_ALL=C sort > /tmp/e2e-all.names
  cargo test --manifest-path tests/e2e/Cargo.toml --locked -- --ignored --list |
    sed -n "s/: test$//p" | LC_ALL=C sort > /tmp/e2e-ignored.names
  LC_ALL=C comm -23 /tmp/e2e-all.names /tmp/e2e-ignored.names \
    > /tmp/e2e-runnable.names
  : > /tmp/e2e-durations.tsv
  while IFS= read -r name; do
    start=$(date +%s%N)
    cargo test --manifest-path tests/e2e/Cargo.toml --locked "$name" -- \
      --exact --test-threads=1
    end=$(date +%s%N)
    awk -v name="$name" -v start="$start" -v end="$end" \
      "BEGIN { printf \"%s\\t%.3f\\n\", name, (end-start)/1000000000 }" \
      >> /tmp/e2e-durations.tsv
  done < /tmp/e2e-runnable.names
'
```

Sort durations descending, seed shard 1 with
`scenarios::cross_protocol::composed_mainnet_profile_serves_both_protocols_without_leakage`
and shard 2 with the second-longest scenario by measured duration, then assign each remaining name
to the lower predicted load subject to the required final capacities. The
post-#816 runnable split is 43 on shard 1 and 41 on shard 2. Break equal-duration
or equal-load ties by full test name, keep at most five of the measured top ten
on either shard, and keep two ignored tests on shard 1 and one on shard 2.
Update the lists, expected ignored-name set, counts, and predicted totals
together in the block at the top of `run-gate`, then run its default, shard 1,
and shard 2 modes. The explicit root-workspace build above removes a one-time
canonical `phase-runner` compile from the first measured scenario while leaving
scenario-specific generated builds in the timing sample.

PR #612 added four runnable scenarios, and PR #816 adds one, changing the
inventory from 79/3 to 84/3. The set-equality gate deliberately fails closed
when the inventory changes without a matching assignment refresh.

## Coverage ledger

The semantic inventory contains 63 scenario tests:

- 60 retargeted and runnable;
- 3 explicitly retired with one-line reasons.

The 60 runnable scenarios include the #154 known-defect reproduction described
above; it is kept runnable so the provider path and explicit repair remain
observable rather than being hidden as an ignored test.

The crate contains 87 total tests when 24 harness/support checks are included.
The pre-retarget crate contained 88; the net change is -1: obsolete
Cargo-artifact tests for the old indexer, worker, v1 API, and execution plane
were removed, while deployment-profile binary lifecycle and normalized-event
parity-completeness regressions and the separate-transaction wrapper activation
scenario were added. The pure in-memory
`catchup_equivalence::primary_route_normalization_preserves_contract_instance_identity`
normalization oracle is counted as support rather than as a contract-backed
semantic scenario. The final worker-coordination stub, verified-resolution
scenario, and stale observed-code-hash admission scenario were removed
explicitly with issue #314.

### Retargeted and runnable (60)

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
  `perturbations::pre_surface_records_converge_fresh_incremental_and_restored`;
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
  `resolver_records::pre_surface_newowner_record_serves_after_late_surface`;
  `resolver_records::pre_surface_record_attribution_is_node_scoped_and_never_materializes_unknown_names`;
  `resolver_records::pre_surface_record_history_follows_current_resolver_and_version_boundary`;
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
  `unadmitted_controller::later_wrap_exposes_unadmitted_controller_registrar_owner_and_expiry`;
  `unadmitted_controller::unadmitted_controller_registration_retains_resource_keyed_registrar_lease`;
  `wrapper::wrapper_wrap_fuses_subnames_and_unwrap_restore_identity`;
  `wrapper_registration::born_wrapped_registration_retains_wrapper_authority`;
  `wrapper_registration::parent_burns_pcc_then_extends_existing_child_expiry`;
  `wrapper_registration::wrap_existing_registry_subname_rotates_child_only`;
  `wrapper_renewal_and_transfers::wrapped_erc1155_single_and_batch_transfers_preserve_identity`;
  `wrapper_renewal_and_transfers::wrapped_renewal_tracks_registrar_expiry_without_wrapper_event`.

### Retired with reason (3)

- `provider_faults::transient_get_code_retries_primary_without_using_configured_fallback` — runtime bytecode-hash admission and its `eth_getCode` retry path were deleted in Stage B.
- `provider_faults::pruned_get_code_fails_closed_then_uses_configured_fallback` — the code-hash archive fallback was deleted with runtime bytecode-hash admission.
- `registry_preimages::registry_only_non_eth_tree_derives_declared_state` — generic resolver `NameChanged` no longer materializes an unknown registry-only surface under the schema-v2 identity gate.

### Count deltas

| Measure | Historical baseline | Retargeted suite | Delta |
| --- | ---: | ---: | ---: |
| Total crate tests | 88 | 87 | -1 |
| Semantic scenario inventory | 62 at the retarget base, including one pure helper | 63 | -1 reclassified, -3 deleted, +5 added |
| Runnable passed-count gate | 65 in the historical Anvil gate | 84 | +19 |
| Anvil-backed semantic inventory | 65 historical gate reference | 63 | -2 |
| Runnable Anvil-backed semantic scenarios | 65 historical gate reference | 60 | -5 |

The two 65 comparisons are reported because that is the historical gate
reference, but the current passed-count denominator is explicit: 60 runnable
Anvil scenarios and 24 harness/support checks produce 84 passes. Three semantic
scenarios are explicitly ignored with their retired behavior recorded above.

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
