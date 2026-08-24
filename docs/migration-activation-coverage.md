# ENSv1→ENSv2 production-activation coverage

This ledger maps the canonical 65-scenario ENSv1→ENSv2 migration catalog
to production activation behavior. The source catalog is the read-only
[`worknotes/migration-catalog` commit `d110108f`](https://github.com/ensdomains/bigname/tree/d110108f2f098d1b43804c64c80d0b4588286326);
its 63 `validation/<ID>.json` files record the catalog runner's transaction
results. Those files are supporting execution evidence, not checked-in bigname tests or a
substitute for the governed `.refs/` citations below. `C-07` and `G-05` are the
two scenarios without an executed catalog result.
The folded aliases `P-01`→`U-06` and `P-10`→`U-07` are not additional catalog
scenarios and therefore do not increase the ledger from 65 to 67.

The dispositions are:

- `activated`: the scenario contains a [complete](glossary.md#complete-group) authority-boundary group and
  production emits its existing activated `MigrationApplied` and
  `MigrationAuthorityTransition`.
- `non-boundary`: the scenario may retain ordinary or correlation-dependent
  effects but emits no migration authority boundary.
- `refused`: the transaction reverts or the observed evidence fails an existing
  path gate, so no migration boundary is admitted.

No row is deferred and no row requires new schema, manifest, event, selector,
or public vocabulary. Each executed scenario links its exact immutable catalog
result rather than inferring it from another scenario. The final column separately
names the exact checked-in test when the repository imports that scenario shape
or pins the production rule it exercises; `exact catalog result only` states
plainly that the external artifact is not itself a checked-in test.

| ID | Production disposition | Pinned exact catalog result | Checked-in rule anchor |
| --- | --- | --- | --- |
| U-01 | activated — `unwrapped` | [validation/U-01.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/U-01.json) | `cross_family_registrar_transfer_emits_one_unwrapped_activated_boundary`; production DB path `checked_in_sepolia_manifests_materialize_exactly_one_transition_predecessor` |
| U-02 | activated — `unwrapped` | [validation/U-02.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/U-02.json) | exact catalog result only; `cross_family_registrar_transfer_emits_one_unwrapped_activated_boundary` pins production `unwrapped` activation |
| U-03 | activated — `unwrapped` | [validation/U-03.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/U-03.json) | exact catalog result only; `cross_family_registrar_transfer_emits_one_unwrapped_activated_boundary` pins production `unwrapped` activation |
| U-04 | activated — `unwrapped` | [validation/U-04.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/U-04.json) | `resolver_and_ttl_clears_are_optional_boundary_evidence`; `assert_activated_transition` matrix |
| U-05 | activated — `unwrapped` | [validation/U-05.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/U-05.json) | `assert_activated_transition` matrix; ordinary subregistry output stays independent |
| U-06 | activated — `unwrapped`; later ENSv1 residue stays historical | [validation/U-06.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/U-06.json) | `assert_activated_transition` matrix; ordinary post-boundary facts remain byte-for-byte independent |
| U-07 | activated — `unwrapped`; reverse claim is independent | [validation/U-07.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/U-07.json) | `assert_activated_transition` matrix; Project authority fanout suite |
| U-08 | activated — `unwrapped`; emitted [migration expiry jump](glossary.md#migration-expiry-jump) | [validation/U-08.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/U-08.json) | `cross_family_registrar_transfer_emits_one_unwrapped_activated_boundary` |
| U-09 | activated — `unwrapped`; contract owner is retained | [validation/U-09.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/U-09.json) | `assert_activated_transition` matrix |
| X-U-01 | refused — reverted transaction | [validation/X-U-01.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/X-U-01.json) | exact catalog result only; a reverted transaction supplies no production raw facts |
| X-U-02 | refused — reverted transaction | [validation/X-U-02.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/X-U-02.json) | exact catalog result only; a reverted transaction supplies no production raw facts |
| X-U-03 | refused — name/resource mismatch | [validation/X-U-03.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/X-U-03.json) | exact catalog result only; no exact checked-in scenario execution |
| X-U-04 | refused — malformed payload | [validation/X-U-04.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/X-U-04.json) | exact catalog result only; no exact checked-in scenario execution |
| X-U-05 | refused — wrong controller | [validation/X-U-05.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/X-U-05.json) | exact catalog result only; no exact checked-in scenario execution |
| X-U-06 | refused — grace-period transfer reverts | [validation/X-U-06.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/X-U-06.json) | exact catalog result only; a reverted transaction supplies no production raw facts |
| X-U-07 | refused — recipient rejection | [validation/X-U-07.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/X-U-07.json) | exact catalog result only; a reverted transaction supplies no production raw facts |
| W-01 | activated — `unlocked_wrapped` | [validation/W-01.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/W-01.json) | `unlocked_wrapped_catalog_shape_is_distinguished_per_name`; production DB path `complete_unlocked_wrapped_migration_closes_the_reactivated_registrar_at_cleanup` |
| W-02 | activated — `unlocked_wrapped` | [validation/W-02.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/W-02.json) | `unlocked_wrapped_catalog_shape_is_distinguished_per_name` |
| W-03 | activated — two exact groups | [validation/W-03.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/W-03.json) | `two_names_in_one_transaction_keep_separate_authority_boundaries` |
| X-W-01 | refused — wrong controller | [validation/X-W-01.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/X-W-01.json) | exact catalog result only; no exact checked-in scenario execution |
| X-W-02 | refused — wrong controller | [validation/X-W-02.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/X-W-02.json) | exact catalog result only; no exact checked-in scenario execution |
| X-W-03 | refused — grace-period transfer reverts | [validation/X-W-03.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/X-W-03.json) | exact catalog result only; a reverted transaction supplies no production raw facts |
| L-01 | activated — `locked_wrapped` | [validation/L-01.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/L-01.json) | `locked_migration_does_not_require_a_registrar_admission`; `assert_activated_transition` matrix |
| L-02 | activated — `locked_wrapped` | [validation/L-02.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/L-02.json) | `assert_activated_transition` matrix; optional resolver-output pins |
| L-03 | activated — `locked_wrapped` | [validation/L-03.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/L-03.json) | `assert_activated_transition` matrix; optional resolver-output pins |
| L-04 | activated — `locked_wrapped`; wrapper terminal history retained | [validation/L-04.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/L-04.json) | `assert_activated_transition` matrix; exact transition-writer tests |
| L-05 | activated — `locked_wrapped` | [validation/L-05.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/L-05.json) | `assert_activated_transition` matrix |
| L-06 | activated — `locked_wrapped` | [validation/L-06.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/L-06.json) | `assert_activated_transition` matrix |
| L-07 | activated — `locked_wrapped` | [validation/L-07.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/L-07.json) | `assert_activated_transition` matrix |
| L-08 | activated — two exact groups and registries | [validation/L-08.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/L-08.json) | `two_names_in_one_transaction_keep_separate_authority_boundaries`; `migration_registry_association_preserves_the_ordinary_announcement_edge` |
| X-L-01 | refused — transfer prohibited | [validation/X-L-01.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/X-L-01.json) | exact catalog result only; a reverted transaction supplies no production raw facts |
| X-L-02 | refused — live approval conflicts with frozen token | [validation/X-L-02.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/X-L-02.json) | exact catalog result only; a reverted transaction supplies no production raw facts |
| X-L-03 | refused — registry/name mismatch | [validation/X-L-03.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/X-L-03.json) | exact catalog result only; no exact checked-in scenario execution |
| X-L-04 | refused — wrong locked path | [validation/X-L-04.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/X-L-04.json) | exact catalog result only; no exact checked-in scenario execution |
| C-01 | activated — `locked_child` | [validation/C-01.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/C-01.json) | `locked_child_correlates_through_the_parent_migration_registry`; `the_activation_matrix_covers_the_child_catalog` |
| C-02 | activated — `emancipated_child` | [validation/C-02.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/C-02.json) | `emancipated_child_correlates_without_a_nested_registry`; `the_activation_matrix_covers_the_child_catalog` |
| C-03 | activated — child boundary plus ordinary renewal | [validation/C-03.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/C-03.json) | `self_service_child_renewal_adds_no_second_boundary`; `the_activation_matrix_covers_the_child_catalog` |
| C-04 | activated — each child has its own exact group | [validation/C-04.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/C-04.json) | `chained_child_registries_correlate_at_unbounded_depth`; `the_activation_matrix_covers_the_child_catalog` |
| C-05 | non-boundary — unmigrated child remains ENSv1-authoritative | [validation/C-05.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/C-05.json) | `unmigrated_child_proves_no_boundary`; `the_activation_matrix_covers_the_child_catalog` |
| C-06 | non-boundary — ordinary parent clobber, no child migration | [validation/C-06.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/C-06.json) | `parent_controlled_clobber_is_not_a_migration_boundary`; `the_activation_matrix_covers_the_child_catalog` |
| C-08 | non-boundary — protection/reclaim lifecycle only | [validation/C-08.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/C-08.json) | `a_self_claim_without_v1_cleanup_derives_no_boundary` |
| C-07 | refused — no direct path; folded into H-04 | non-executed [catalog.md entry](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/catalog.md) | `unmigrated_parent_leaves_no_child_evidence`; `unmigrated_parent_leaves_no_child_evidence` maps the observable refusal |
| H-01 | activated — four per-log groups in one transaction | [validation/H-01.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/H-01.json) | `mixed_helper_batch_attributes_children_per_log` |
| H-02 | refused — helper transaction reverts | [validation/H-02.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/H-02.json) | exact catalog result only; a reverted transaction supplies no production raw facts |
| H-03 | refused — mixed-owner helper group reverts | [validation/H-03.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/H-03.json) | exact catalog result only; a reverted transaction supplies no production raw facts |
| H-04 | refused — unmigrated parent | [validation/H-04.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/H-04.json) | `unmigrated_parent_leaves_no_child_evidence` |
| G-01 | non-boundary — historical cleanup only | [validation/G-01.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/G-01.json) | `cross_family_registrar_cleanup_and_historical_renewal_reject_lookalikes` |
| G-02 | non-boundary — `graveyard_cleanup`, never a lease | [validation/G-02.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/G-02.json) | `graveyard_class_expiry_with_foreign_owner_is_not_cleanup_evidence`; cleanup positive fixture |
| G-03 | refused — live-name cleanup reverts | [validation/G-03.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/G-03.json) | exact catalog result only; a reverted transaction supplies no production raw facts |
| G-04 | non-boundary — wrapper cleanup history only | [validation/G-04.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/G-04.json) | `cross_family_registrar_cleanup_and_historical_renewal_reject_lookalikes` |
| G-05 | non-boundary — prehashed cleanup has the same historical class | non-executed [catalog.md entry](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/catalog.md); pinned upstream unit evidence | `cross_family_registrar_cleanup_and_historical_renewal_reject_lookalikes` |
| R-01 | non-boundary — complete synchronized-renewal effects activate | [validation/R-01.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/R-01.json) | `bulk_renewals_with_a_shared_expiry_correlate_per_name_envelopes` |
| R-02 | activated authority group after a distinct renewal group | [validation/R-02.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/R-02.json) | `later_idempotent_expiry_update_does_not_collapse_the_renewal_envelope`; authority matrix |
| R-03 | refused — renewal transaction reverts | [validation/R-03.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/R-03.json) | exact catalog result only; a reverted transaction supplies no production raw facts |
| R-04 | refused — migrated-name renewal reverts | [validation/R-04.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/R-04.json) | exact catalog result only; a reverted transaction supplies no production raw facts |
| R-05 | non-boundary — historical synchronization effects only | [validation/R-05.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/R-05.json) | `aligned_v1_expiry_keeps_base_renewal_out_of_v2_evidence`; `later_idempotent_expiry_update_does_not_collapse_the_renewal_envelope` |
| P-02 | refused — post-boundary ENSv1 write reverts | [validation/P-02.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/P-02.json) | exact catalog result only; a reverted transaction supplies no production raw facts |
| P-03 | refused — post-boundary ENSv1 operator write reverts | [validation/P-03.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/P-03.json) | exact catalog result only; a reverted transaction supplies no production raw facts |
| P-04 | refused — post-boundary wrapper write reverts | [validation/P-04.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/P-04.json) | exact catalog result only; a reverted transaction supplies no production raw facts |
| P-05 | non-boundary — unmigrated sibling remains on ENSv1 | [validation/P-05.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/P-05.json) | `unmigrated_child_proves_no_boundary` |
| P-06 | activated boundary followed by ordinary ENSv2 renewal | [validation/P-06.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/P-06.json) | authority matrix; ordinary renewal remains independently admitted |
| P-07 | activated boundary followed by ordinary token regeneration | [validation/P-07.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/P-07.json) | `cross_family_registrar_transfer_emits_one_unwrapped_activated_boundary` retains correlated token regeneration |
| P-08 | activated boundary followed by ordinary ENSv2 transfer | [validation/P-08.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/P-08.json) | authority matrix; ordinary transfer remains independently admitted |
| P-09 | non-boundary — fresh ENSv2 registration after reservation lapse | [validation/P-09.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/P-09.json) | `reservation_without_claim_boundary`; reservation flood fixture |
| P-11 | activated boundary followed by ordinary ENSv2 release | [validation/P-11.json](https://github.com/ensdomains/bigname/blob/d110108f2f098d1b43804c64c80d0b4588286326/validation/P-11.json) | authority matrix; Project released-v2-authority tests |

The exact catalog outcomes above are pinned task evidence; the following
checked-in upstream sources pin the contract mechanisms behind each family.
The ENSv1 owner/operator and grace-period outcomes in the `U-*`, `W-*`, and
corresponding `X-*` rows follow the registrar's transfer authorization and
availability rules. (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L35-L49 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L67-L103 @ ens_v1@91c966f)
Wrapped owner/operator outcomes follow the wrapper's approval and transfer
checks. (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L105-L169 @ ens_v1@91c966f)
The `U-*`/`X-U-*` and `W-*`/`X-W-*` rows use the unwrapped and wrapper injection
branches of the unlocked controller. (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L85-L165 @ ens_v2@ccaeb58)
The `L-*`/`X-L-*`, `C-*`, and `H-*` rows use the locked controller and receiver
branches. (upstream: .refs/ens_v2/contracts/src/migration/LockedMigrationController.sol:L81-L114 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L104-L207 @ ens_v2@ccaeb58)
The helper batch owner/refusal outcomes use its explicit owner-or-approved
check. (upstream: .refs/ens_v2/contracts/src/migration/MigrationHelper.sol:L178-L200 @ ens_v2@ccaeb58)
The `G-*` rows use the Graveyard cleanup and self-claim rules, while the `R-*`
rows use the synchronized ENSv1 renewal bridge. (upstream: .refs/ens_v2/contracts/src/migration/Graveyard.sol:L99-L170 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/src/registrar/ETHRenewerV1.sol:L106-L146 @ ens_v2@ccaeb58)
The reservation and ordinary registry outcomes in the `P-*` rows follow the
batch registrar and permissioned-registry state transitions. (upstream: .refs/ens_v2/contracts/src/registrar/BatchRegistrar.sol:L43-L70 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L181-L220 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L407-L475 @ ens_v2@ccaeb58)
