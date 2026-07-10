# bigname end-to-end scenario tests

This package exercises selected end-to-end paths against local EVM chains
using contracts sourced from the pinned upstream checkouts. Where
`tests/conformance` seeds synthetic rows directly into Postgres, this harness
starts from actual contract emissions, drives on-chain state transitions, and
checks the resulting raw facts, normalized events, projections, execution
artifacts, or HTTP responses required by each scenario. Most scenarios use the
real `bigname-indexer run` loop followed by one-shot projection replay and the
real `bigname-api`; one focused smoke keeps `bigname-indexer run`,
`bigname-worker run`, and the API live together.

The two packages are complementary:

- `tests/conformance` — fast, hermetic ownership of public route contracts,
  filter/mode/meta permutations, coverage semantics, and replay determinism
  over hand-authored state.
- `tests/e2e` — selected high-value checks that upstream-shaped contract
  emissions cross the real process and storage boundaries as expected, using
  deployments loaded from pinned upstream artifacts or built from pinned
  upstream sources.

## Prerequisites

- [foundry](https://getfoundry.sh) (`anvil` on `PATH`)
- pinned upstream checkouts: `scripts/sync-refs` (this also initializes the
  recursive Basenames Forge-library submodules)
- a test Postgres: run through `scripts/test-db`

```sh
scripts/test-db -- cargo test --manifest-path tests/e2e/Cargo.toml -- --test-threads=8
```

## How a scenario runs

1. **Chain** — `harness::anvil` starts a local node with a fixed genesis
   timestamp, presented to the indexer by provider label (`ethereum-mainnet`
   for ENSv1 scenarios, `ethereum-sepolia` for ENSv2 sepolia-dev, and
   `base-mainnet` for Basenames). Chain identity is the provider label; the
   local numeric chain id is only for realistic receipts.
2. **Contracts** — `harness::ens_v1` deploys an ENSv1 topology from prebuilt
   creation bytecode in the pinned deployment artifacts
   (`.refs/ens_v1/deployments/`): the legacy
   registry, the current registry deployed with the legacy registry as its
   constructor argument
   (upstream: .refs/ens_v1/deployments/sepolia/ENSRegistry.json:L414 @ ens_v1@91c966f),
   the `.eth` base registrar, the current registrar controller with its
   commit/reveal flow
   (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L210 @ ens_v1@91c966f),
   the exponential-premium price oracle over upstream's own dummy USD oracle
   (upstream: .refs/ens_v1/contracts/ethregistrar/DummyOracle.sol:L3 @ ens_v1@91c966f),
   reverse registrars, name wrapper, and public resolver. `harness::ens_v2`
   likewise loads prebuilt creation bytecode from the pinned `sepolia-dev`
   deployment artifacts under `.refs/ens_v2`. These artifact-backed stacks
   avoid local recompilation, but scenario-local addresses, constructor
   arguments, and wiring are not a claim that the resulting deployment is
   byte-for-byte identical to production. `harness::basenames` instead deploys
   the Base Registry, BaseRegistrar, RegistrarController, helper
   ReverseRegistrar, and L2Resolver forge-built on demand from the pinned
   sources (the
   committed broadcast bytecode predates the pinned tree and its
   constructors differ; `scripts/sync-refs` checks out every pinned recursive
   Forge-library submodule, and one incremental build runs per test process)
   with the script-declared `base.eth` and `80002105.reverse` wiring
   (upstream: .refs/basenames/script/deploy/DeployReverseRegistrar.s.sol:L19 @ basenames@1809bbc).
   The declared-primary contract is ENSv1's Base L2ReverseRegistrar artifact,
   whose deployment carries coin type `2147492101`
   (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L391 @ ens_v1@91c966f).
3. **Manifests** — `harness::manifests` copies the checked-in version files
   for the selected manifest families. It preserves rollout statuses,
   capability flags, ABI declarations, and discovery-rule structure while
   substituting scenario-local contract identities and start blocks. Roles a
   scenario does not deploy get deterministic placeholder addresses with no
   code or logs. The mirror is structurally faithful to the selected checked-in
   families, but it is not production-identity-equivalent and a silent
   placeholder does not test that role's behavior. The ENSv1 mirror includes
   the active registry v3 manifest and its old-registry role. Execution-plane
   ENS scenarios also mirror
   `ens_execution` when they supply a local `universal_resolver` target; the
   base ENSv1 scenarios keep execution manifests out of the generated profile.
   Basenames scenarios mirror the shipped
   `manifests/mainnet/base/basenames` family versions with local Base
   addresses; the Phase 5 declared-state slice does not mirror
   `manifests/mainnet/ethereum/basenames` because no L1-compatibility or
   execution-plane row runs yet. ENSv2 scenarios mirror the shipped
   `manifests/sepolia/ethereum/ens` families into a generated
   `manifests-sepolia` root so the selected profile remains the sepolia-dev
   one. Cross-protocol scenarios structurally mirror the eleven currently
   checked-in mainnet families across both chains, including the two
   ethereum-chain glue families (`basenames_l1_compat`,
   `basenames_execution`), into one generated root and run a single live
   session over two anvils. Nothing under the checked-in `manifests/` tree
   changes.
4. **Pipeline** — most scenarios run an `indexer run` live-intake session
   supervised until the canonical checkpoint reaches the scenario head (the
   live loop, not `backfill`, is
   what promotes checkpoints that snapshot-selected API reads require), then
   `worker replay all-current-projections`, then `bigname-api serve` on a
   local port. The
   `register_eth_name::live_worker_applies_registration_and_renewal_while_api_serves`
   smoke instead keeps the production `worker run` loop active with the
   indexer and API, proving bootstrap handoff and continuous projection apply
   for one registration/renewal path. Execution-plane scenarios start the API
   with `--chain-rpc-url ethereum-mainnet=<anvil>` so on-demand verified
   resolution executes against the selected stored snapshot. Backfill helpers
   exercise raw-fact-to-normalized-event and projection rebuild boundaries
   where canonical checkpoints intentionally make API reads unavailable.
5. **Assertions** — each scenario asserts the validation layers material to
   its claim. Many cover persisted raw logs, canonical normalized events,
   rebuilt projections, and public HTTP output; the verified-resolution
   scenario also checks durable execution traces, steps, and cache outcomes.
   Backfill-only cases stop before HTTP rather than implying route coverage.

## Coverage ownership

| Evidence question | Owning suite |
| --- | --- |
| Public status/body schema, filters, modes, metadata levels, pagination, unsupported behavior, and route permutations across the full documented `v1` surface | `tests/conformance` |
| Selected upstream-shaped lifecycle, admission, replay, reorg, projection, execution, and cross-protocol paths through real binaries and storage | `tests/e2e` |
| High-value HTTP integration smokes, including exact-name reads, `GET /v1/status`, and `POST /v1/identity:lookup` | Selected `tests/e2e` scenarios; conformance still owns their contract permutations |
| Most scenario projection state | `tests/e2e` via one-shot `worker replay all-current-projections` |
| Continuous production projection apply while indexer and API remain live | The focused `register_eth_name::live_worker_applies_registration_and_renewal_while_api_serves` smoke |

The e2e scenario list is deliberately risk-weighted. It is not an exhaustive
route inventory or a claim that every protocol transition is covered.

## Scenarios

- `register_eth_name` — walking skeleton. Registers `alice.eth` through the
  controller's commit/reveal flow (time-warped past the minimum commitment
  age) and asserts raw-log persistence, canonical normalized event kinds,
  and the exact-name route's registration/coverage output. Verified
  resolution is out of scope: no execution RPC is configured.
- `register_eth_name::live_worker_applies_registration_and_renewal_while_api_serves`
  — starts the production indexer, worker, and API loops together, registers
  and then renews `liveworker.eth` after startup, and waits for continuous
  projection apply before checking each exact-name result. It also smokes
  `POST /v1/identity:lookup` against the live name and confirms
  `GET /v1/status` reports ready. This is the production worker-loop smoke;
  the broader scenario matrix uses deterministic one-shot projection replay.
- `registry_driven_reads` — registry-sourced declared state under the
  shipped profile: declared resolver bindings, registry owner,
  record-inventory selectors, and registry-only subnames appearing as
  bracketed labelhash placeholder children with no exact-name surface
  minted.
- `lifecycle::renew_and_transfer_keep_identity` — renewal extends expiry on
  the same backing resource; the two-transaction transfer→reclaim pair
  opens a genuine registry-owner divergence window (transient anchor) and
  converges back to the original registrar resource.
- `lifecycle_divergence::transfer_without_reclaim_keeps_registry_owner_divergent`
  — leaves the registrar token holder and registry owner split by omitting
  the separate reclaim call
  (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172 @ ens_v1@91c966f),
  then pins the registry-only exact binding and the address-collection gap.
- `lifecycle::expiry_grace_and_reregistration_rotate_identity` — ingests
  the same chain twice: once inside the grace window (registration stays
  `active` with a past expiry; no wire-level grace status) and once after a
  different account re-registers post-premium-decay (new backing resource;
  both leases' history preserved under distinct resources).
- `lifecycle::register_without_resolver_keeps_declared_resolver_empty` —
  registers through the controller with resolver `address(0)` and asserts
  active registration state with a supported null declared-resolver shape.
- `lifecycle::expire_without_reregistration_releases_and_unlists_registration`
  — registers for the upstream minimum duration and warps past expiry plus
  grace without re-registering. Pins both halves of the contract: on a
  quiet chain the release never settles (sync boundaries are driven by
  log-bearing blocks), and the first unrelated post-grace activity lets the
  next sync round derive the release, flip exact-name to `released`, and
  drop the name from the current registrant collection.
- `registration_burst::registration_with_records_reverse_and_referrer_derives_single_burst`
  — supplies controller registration data and the Ethereum reverse bit,
  deriving registrar, registry, resolver, and reverse facts from one
  transaction
  (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L307 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L319 @ ens_v1@91c966f);
  the nonzero referrer is decoded from the retained controller log
  (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L340 @ ens_v1@91c966f),
  while record authorship is pinned to that controller transaction and the
  normalized record shape's explicit lack of a writer field. Pins the
  chipped anchor-rebind review point: the burst's records derive only under
  the transient registry-only resource, so exact-name serves an empty
  selector inventory with explicit gaps and the mid-burst controller as
  registry_owner; a second ingest shows later plain writes restoring the
  inventory while the stale owner facet persists.
- `registry_driven_reads::same_label_under_two_parents_keeps_children_distinct`
  — creates `sub` under two registered parents and asserts separate child
  namehashes/owners with no cross-parent leakage.
- `registry_driven_reads::deep_registry_hierarchy_lists_direct_children_only`
  — creates a registry-only grandchild under a placeholder parent. Registry
  facts derive at any depth, but enumeration stops at unknown surfaces:
  placeholder names are rejected as `invalid_input` and the grandchild
  projects no children row.
- `registry_driven_reads::zero_owner_subname_leaves_default_children_listing`
  — creates and then zeroes a registry-only subname, asserting the tombstoned
  child leaves the default parent children listing.
- `registry_preimages::registry_only_non_eth_tree_derives_declared_state` —
  builds `leaf.xyz` entirely through registry ownership
  (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75 @ ens_v1@91c966f),
  then uses admitted reverse `NameChanged` text
  (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L18 @ ens_v1@91c966f)
  to release the already-observed forward resolver and record facts into a
  registry-only exact surface.
- `registry_preimages::label_preimage_revealed_later_upgrades_child_listing`
  — observes a label through a later controller registration
  (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L334 @ ens_v1@91c966f),
  upgrades the bracketed child display, and confirms that label proof alone
  does not mint an exact-name surface. Phase 2 pins the reveal via
  backfill + projection replay because live re-ingest of the reveal chain
  hangs the run loop before checkpoint promotion (chipped review point);
  `BIGNAME_E2E_READY_TIMEOUT_SECS` shortens the readiness deadline when
  reproducing that wedge.
- `unadmitted_controller::unadmitted_controller_registration_derives_registry_side_only`
  — adds a fresh EOA as a registrar controller and registers directly on
  the registrar
  (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L110 @ ens_v1@91c966f):
  registrar-plane facts persist raw-only (no lease events derive, not even
  `TokenControlTransferred` — fresh mints have no existing lease), exactly
  one registry-side `SubregistryChanged` derives, the child stays a
  bracketed placeholder, and no exact-name surface or registrant-collection
  entry appears.
- `registry_migration::registry_migration_legacy_to_current_admission` —
  exercises the active registry v3 old-registry role end to end: a pure
  legacy 2LD derives subregistry state without minting an exact-name surface,
  a current-registry registration suppresses later legacy resolver/owner
  writes for that node, and a different unmigrated legacy child remains
  admitted after the cutover.
- `resolver_records::resolver_changes_follow_registry_and_zero_releases` —
  registers with the admitted PublicResolver, moves the registry binding to a
  second deployed PublicResolver copy, then sets it to zero; exact-name and
  compact records resolver state follow each transition.
- `resolver_records::records_route_values_and_version_boundaries_follow_current_resolver`
  — writes a non-60 multicoin addr record and contenthash, asserts compact
  cached values and selectors, then checks resolver replacement and
  `clearRecords` move the record-version boundary without leaking prior
  values.
- `resolver_records::unadmitted_custom_resolver_observes_facts_but_keeps_profile_gated`
  — binds a name to an unpatched PublicResolver copy and writes a text
  record on it; declared reads never surface the unadmitted write (no
  record events, no inventory selectors, `not_found` on request,
  enumeration supported-but-empty).
- `resolver_records::shared_resolver_keeps_per_name_records_and_overview_fan_in_unsupported`
  — two names share one resolver while per-node records stay distinct; the
  resolver overview keeps binding fan-in explicitly unsupported.
- `resolver_authorization::operator_delegate_writes_match_owner_authorship`
  — compares owner-authored and delegated text/subname writes after separate
  registry and resolver approvals, asserting equal normalized semantics and
  the owner/operator addresses retained as the respective raw transaction
  senders (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L19 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L112 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L98 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L128 @ ens_v1@91c966f).
- `record_families::remaining_record_families_derive_normalized_but_stay_unenumerated`
  — writes ABI, interface, a DNS RRset (then deletes it), a zonehash, and a
  forward name() record: every family derives fully keyed `RecordChanged`
  at the normalized layer, `DNSRecordDeleted` derives as
  supersession-by-delete on the same key
  (upstream: .refs/ens_v1/contracts/resolvers/profiles/DNSResolver.sol:L186 @ ens_v1@91c966f),
  and the inventory enumerates selectors only for addr/text/contenthash —
  the keyed families stay out of selectors, gaps, and unsupported_families.
- `record_families::pubkey_write_on_admitted_resolver_stays_invisible` —
  setPubkey on the admitted resolver
  (upstream: .refs/ens_v1/contracts/resolvers/profiles/PubkeyResolver.sol:L25 @ ens_v1@91c966f)
  is invisible at every layer: the topic-filtered live scan persists no raw
  log, nothing derives, and no pubkey family surfaces in the inventory
  (the adapter gate rejects the family by tested design; drift-vs-narrowing
  is a doc-first question).
- `wrapper::wrapper_wrap_fuses_subnames_and_unwrap_restore_identity` —
  wraps registrar names through the pinned NameWrapper, asserts wrapper
  resource/token-lineage rotation, burns `CANNOT_UNWRAP`,
  `CANNOT_TRANSFER`, and `CANNOT_SET_RESOLVER`, and pins the emitted
  `PermissionScopeChanged` fuse bitmaps. It also exposes the current
  publication gap: wrapper-backed resources have no holder subject grants or
  published masked `effective_powers`, while a wrap-of-existing-name retains
  stale control facets. The scenario therefore does not claim effective-power
  masking is published. It creates wrapped subnames with
  `PARENT_CANNOT_CONTROL`, checks wrapper expiry vs registrar expiry, and
  unwraps a separate name before lease end to confirm the prior registrar
  resource and lineage reactivate.
- `wrapper_turn_k::born_wrapped_registration_exposes_trailing_grant_rebind`
  — deploys and authorises the manifest-admitted mainnet
  WrappedETHRegistrarController artifact, registers through its flat
  commit/reveal ABI and NameWrapper's registerAndWrapETH2LD entrypoint
  (upstream: .refs/ens_v1/deployments/mainnet/WrappedETHRegistrarController.json:L656 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289 @ ens_v1@91c966f),
  and pins the current mixed result: one transient wrapper resource, a final
  registrar binding, and registry-only exact-name authority fields.
- `wrapper_turn_k_transfers::wrapped_renewal_tracks_registrar_expiry_without_wrapper_event`
  — renews a wrapped 2LD through the current controller, proving the wrapper
  emits no expiry event and its onchain expiry stays stale while exact-name
  follows the registrar `RegistrationRenewed` value
  (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L366 @ ens_v1@91c966f).
- `wrapper_turn_k_transfers::wrapped_erc1155_single_and_batch_transfers_preserve_identity`
  — performs real single and two-id batch ERC1155 transfers, pins per-id
  `TransferBatch` fan-out, holder-following registrants, stable wrapper
  resource/lineage, zero registry/lifecycle derivation, and the existing
  stale control facets under holder rotation
  (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L154 @ ens_v1@91c966f).
- `wrapper_turn_k::parent_burns_pcc_then_extends_existing_child_expiry`
  — creates a live wrapped child without PCC, burns the exact 0→65536
  transition through parent-authorised setChildFuses, then extends the child
  to its parent's expiry cap without rotating identity
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L517 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L475 @ ens_v1@91c966f).
- `wrapper_turn_k::wrap_existing_registry_subname_rotates_child_only` —
  wraps a plain child under a registry-only parent using DNS wire bytes and
  registry operator approval; the child's registry `Transfer` (not
  `NewOwner`) rotates it to a distinct wrapper resource and publishes the
  `NameWrapped` label preimage while the parent stays registry-only
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L342 @ ens_v1@91c966f).
  Reveal-via-wrap trips the same live-intake hang as
  reveal-via-registration (chipped), so this scenario pins derivation and
  projections via backfill + replay — no API layer.
- `reverse_primary::reverse_claim_set_changed_then_cleared_tracks_declared_candidate`
  — drives `ReverseRegistrar.setName` through the admitted reverse family
  and asserts declared primary-name readback: `mode=declared` exposes only
  the claimed candidate, `mode=both` keeps verified state separate as
  `not_found`, and later claim/blank-name updates replace and then clear the
  candidate.
- `reverse_primary::reverse_claim_invalid_name_surfaces_raw_claim` — writes
  a nonblank reverse claim that fails ENSIP-15 normalization and asserts
  `claimed_primary_name.status=invalid_name` with `raw_claim_name` preserved
  and no coerced candidate name.
- `reverse_primary_turn_j::claim_without_name_record_keeps_candidate_absent`
  — calls `claim` without a name write, asserting the registry child edge and
  reverse claim derive separately, no resolver log or candidate appears, and
  the persisted/public tuple is explicitly `not_found`
  (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L64 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L84 @ ens_v1@91c966f).
- `reverse_primary_turn_j::authorised_third_party_claim_keys_candidate_to_claimed_address`
  — registry-authorises an operator to call `setNameForAddr`, then proves the
  reverse node, candidate tuple, and primary-name route key off the claimed
  address while raw transaction provenance retains the operator sender
  (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L44 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L129 @ ens_v1@91c966f).
- `reverse_primary_turn_j::unadmitted_reverse_resolver_keeps_candidate_absent`
  — claims through a fresh PublicResolver copy and writes its reverse-node
  name; the admitted reverse claim remains visible, but the unadmitted
  `NameChanged` derives nothing and the candidate stays `not_found`
  (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L93 @ ens_v1@91c966f).
- `reverse_primary_turn_j::forward_mismatch_keeps_declared_candidate_but_verified_not_found`
  — runs with chain RPC and the local Universal Resolver, writes a forward
  `addr:60` different from the reverse claimant, and pins the current honest
  gap: the declared candidate succeeds, but a tuple-present claim never
  invokes primary verification, so verified mode is `not_found` with no
  primary execution trace or cache outcome
  (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L105 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L26 @ ens_v1@91c966f).
- `basenames::basenames_declared_state_matrix_end_to_end` — deploys the
  Basenames Base stack forge-built from the pinned sources plus the ENSv1 Base
  L2ReverseRegistrar, mirrors the Base Basenames manifests, registers
  `alice.base.eth`, asserts Base-side authority split and L2Resolver
  `addr:60` record readback, exercises NFT-only, management-only, and full
  transfer control vectors, then sets and clears the Base coin-type primary
  claim. Verified execution remains out of scope for this row: `mode=both`
  keeps verified primary state as `not_found`.
- `basenames_turn_m::renew_release_and_premium_reregistration_rotate_lineage`
  — renews through the legacy controller's three-argument `NameRenewed`,
  advances beyond expiry plus grace, emits admitted post-grace activity, and
  re-registers to a different owner. It pins release synthesis, the two
  burn/re-mint transfers, lineage rotation, and distinct lease resources
  (upstream: .refs/basenames/src/L2/RegistrarController.sol:L497 @ basenames@1809bbc)
  (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L294 @ basenames@1809bbc)
  (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L443 @ basenames@1809bbc).
- `basenames_turn_m::upgradeable_controller_proxy_registers_and_renews` —
  deploys and initializes the upgradeable controller implementation and proxy,
  authorizes the proxy, and drives registration and renewal through it. The
  admitted events retain proxy-emitter provenance while contract-instance
  identity keeps the implementation distinct
  (upstream: .refs/basenames/test/Integration/SwitchToUpgradeableRegistrarController.t.sol:L45 @ basenames@1809bbc)
  (upstream: .refs/basenames/test/Integration/SwitchToUpgradeableRegistrarController.t.sol:L59 @ basenames@1809bbc)
  (upstream: .refs/basenames/test/Integration/SwitchToUpgradeableRegistrarController.t.sol:L68 @ basenames@1809bbc).
- `basenames_turn_m::basenames_subnames_list_preimages_placeholders_and_tombstones`
  — creates a revealed child and hash-only sibling under a registered Base
  parent, pins child listing and the bracketed placeholder, then removes the
  hash-only child through a zero-owner write.
- `basenames_turn_m::l2_resolver_records_clear_and_contenthash_gap` — writes
  text, non-60 multicoin address, and name records in separate transactions,
  then clears them and pins keyed state plus the version boundary. A
  contenthash write on the same watched resolver is retained as a raw log,
  but resolver-family admission rejects normalized derivation; it remains the
  explicit `not_observed_on_current_resolver` inventory gap
  (upstream: .refs/basenames/src/L2/resolver/ResolverBase.sol:L35 @ basenames@1809bbc)
  (upstream: .refs/basenames/src/L2/resolver/ContentHashResolver.sol:L32 @ basenames@1809bbc)
  (upstream: .refs/basenames/src/L2/resolver/ContentHashResolver.sol:L34 @ basenames@1809bbc).
- `basenames_turn_m::unadmitted_resolver_rotation_stays_profile_gated_then_clears`
  — rotates to an L2Resolver built against an alternate registry, pins the
  immutable-dependent code-hash mismatch as empty selectors with
  `resolver_family_unsupported` entries under `unsupported_families` (rather
  than `explicit_gaps`), keeps its records profile-gated, then rotates to zero
  and clears declared resolver state
  (upstream: .refs/basenames/src/L2/L2Resolver.sol:L113 @ basenames@1809bbc)
  (upstream: .refs/basenames/src/L2/L2Resolver.sol:L114 @ basenames@1809bbc).
- `basenames_turn_m::legacy_reverse_registrar_stays_registry_and_raw_record_only`
  — drives helper `claimForBaseAddr` and `setNameForAddr`; a claim-only ingest
  derives `NewOwner`, while the combined replay retains the latter child
  assignment and resolver discovery keeps `NewResolver` with no logical name.
  `NameChanged` remains raw-only; no normalized record, reverse child
  placeholder, or primary candidate is inferred
  (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L158 @ basenames@1809bbc)
  (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L193 @ basenames@1809bbc)
  (upstream: .refs/basenames/src/L2/resolver/NameResolver.sol:L30 @ basenames@1809bbc).
- `basenames_turn_m::third_party_controller_registration_degrades_without_label_events`
  — authorizes an EOA controller and pins direct `register` as a raw token
  mint plus one registry authority derivation without `RegistrationGranted`;
  `registerOnly` retains only the raw token mint and creates no registry node
  (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L237 @ basenames@1809bbc)
  (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L248 @ basenames@1809bbc).
- `ens_v2::ens_v2_sepolia_dev_declared_matrix_end_to_end` — deploys the
  admitted sepolia-dev ENSv2 root registry, ETH registry, registrar, rent
  oracle, and payment-token artifacts from `.refs/ens_v2`, mirrors
  `manifests/sepolia/ethereum/ens`, registers `.eth` names through the
  commit/reveal ETHRegistrar, and asserts identity/registration/control
  under `ethereum-sepolia`, role-driven token regeneration and current
  permission rows, and subregistry attach/swap behavior across two
  ingests. Fresh registration confirms the promoted exact-name profile end
  to end (`full`, `authoritative`, and `exact_name_profile`). It retains two
  review points recorded in the ledger: discovered child-registry logs are
  never scanned in-session, and unregister→re-register wedges intake when
  ingested (exercised on-chain only, post-ingest). The on-chain cycle rotates
  both resource and token lineage: `unregister` burns the token and increments
  both `eacVersionId` and `tokenVersionId`, from which the registry
  reconstructs later resource and token IDs
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L237 @ ens_v2@554c309)
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L241 @ ens_v2@554c309)
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L242 @ ens_v2@554c309)
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L542 @ ens_v2@554c309)
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L547 @ ens_v2@554c309).
- `ens_v2_turn_l::renewal_promotes_coverage_and_registry_edges_follow` —
  registrar renewal derives both fragments and preserves the promoted
  exact-name coverage end to end; a direct
  registry renew emits `ExpiryUpdated` alone on the wire but derives both
  `ExpiryChanged` and a registry-family `RegistrationRenewed`; expiry
  reduction reverts upstream
  (upstream: .refs/ens_v2/contracts/src/registrar/ETHRegistrar.sol:L196 @ ens_v2@554c309).
- `ens_v2_turn_l::resolver_and_subregistry_edges_follow_set_change_zero` —
  resolver set/change/zero and subregistry attach/detach derive NULL-edge
  detaches; pinned via backfill + replay because the composed live chain
  hangs intake (chipped; every op ingests cleanly alone), which also pinned
  the backfill/live parity gap: backfill derives zero v2
  `PermissionChanged`.
- `ens_v2_turn_l::expiry_passes_then_reregistration_advances_lineage` —
  the event-silent expiry flip serves last-known active state with a past
  expiry; re-registration advances the on-chain counters while BOTH intake
  paths refuse the cycle (live hang; backfill anchor-conflict abort,
  asserted verbatim).
- `ens_v2_turn_l::root_apex_attach_and_root_scope_roles` — the root
  family's first transitions: `eth` apex registration + attach derive,
  root-scope grant/revoke read from the resulting bitmap and clear
  `permissions_current`, and registry-level setParent derives
  `ParentChanged`
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L152 @ ens_v2@554c309).
- `ens_v2_turn_l::reserved_labels_foreign_registrar_and_token_sale` —
  labelhash-keyed token-less reservations promote in place preserving
  expiry; a non-admitted root-role registrar derives registry-only facts
  with gated coverage; an ERC1155 sale migrates roles (admin-half rendered
  as `admin_*` powers) with no token regeneration while the registrant
  facet stays at the seller (chipped).
- `ens_v2_turn_l::discovered_v2_resolver_records_stay_unscanned` — a
  VerifiableFactory-proxied writable resolver
  (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L177 @ ens_v2@554c309)
  is discovery-admitted from the registry's `ResolverUpdated`, but zero raw
  logs are scanned at the discovered address in-session and zero record
  events derive — the discovered-registry scan gap extends to resolvers.
- `verified_resolution::direct_path_verified_query_via_local_universal_resolver_persists_trace`
  — deploys the pinned ENSv1 UniversalResolver with local constructor
  dependencies (upstream: .refs/ens_v1/contracts/universalResolver/UniversalResolver.sol:L11 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/universalResolver/UniversalResolver.sol:L19 @ ens_v1@91c966f),
  installs its runtime bytecode at the address used by the execution crate,
  mirrors `ens_execution`, registers `verified.eth`, writes `addr:60`, and
  calls the public profile route with API chain RPC pointed at anvil.
  It asserts declared and verified values match, plus persisted
  `execution_traces`, `execution_steps`, and `execution_cache_outcomes`
  rows, and successful explain readback with the same execution trace. The
  harness installs the local runtime at bigname's frozen official Universal
  Resolver proxy address; this validates that admitted entrypoint class, not
  arbitrary local substitution of the execution role address.
- `perturbations::*` — one representative, moderately rich ENSv1 chain shape
  (`perturb.eth` registration, addr/text records, and a registry-only subname)
  run through the phase-3 multipliers: projection replay plus
  normalized-event replay, indexer restart after the first live checkpoint,
  backfill-from-zero normalized-event digest parity, and a live same-session
  reorg that converges to the winning branch while retaining orphaned
  losing-branch audit rows.
  Backfill parity is intentionally asserted at `normalized_events`, not API
  routes, because the backfill command does not promote canonical checkpoints
  required by snapshot-selected reads. These multipliers validate that one
  corpus; they are not applied to every scenario or protocol.
- `cross_protocol::composed_mainnet_profile_serves_both_protocols_without_leakage`
  — ingests a generated structural mirror of the currently checked-in mainnet
  families (ENSv1 ethereum + Basenames base + the ethereum-chain glue
  families) as one corpus over two anvils:
  per-chain checkpoints coexist, each protocol's exact-name body equals its
  single-protocol baseline after normalizing corpus-minted identifiers
  (`authority_key`'s third segment is the per-corpus contract-instance
  ordinal), the base.eth namespace boundary holds with zero cross-chain
  position leakage, per-namespace address collections and primary
  candidates stay scoped, and the glue families' admission syncs as stored
  manifest state (manifest bookkeeping events are backfill-only).
- `cross_protocol::base_reorg_leaves_ethereum_canonicality_untouched` — a
  live mid-session reorg on the Base chain of the composed corpus converges
  Base to the winning branch with orphaned losing rows while the ethereum
  chain keeps zero orphaned rows, an unmoved checkpoint, and a served name.

## Debugging

- `BIGNAME_E2E_KEEP_DB=1` keeps each scenario's database (the URL is
  printed) instead of dropping it.
- The supervised `indexer run` session writes its full log to
  `$TMPDIR/bigname-e2e-indexer-<pid>-<target block>.log`; failures include
  the tail.
- `BIGNAME_E2E_READY_TIMEOUT_SECS` shortens the 600s readiness deadline
  when reproducing intake wedges locally.
- Full local runs are most stable with bounded parallelism
  (`-- --test-threads=8`): most scenarios spawn an anvil plus several
  pipeline processes, and unbounded parallelism saturates the shared
  postgres into pool-acquire timeouts in unrelated tests. The harness caps
  each spawned binary's pool via `BIGNAME_DATABASE_MAX_CONNECTIONS` and
  `scripts/test-db` raises the server ceiling to 300 (recreate the
  container with `docker rm -f bigname-test-postgres` to pick that up).

## Extending

The scenario matrices, representative perturbation multipliers, harness
roadmap, and phasing live in
[`docs/internal/e2e-testing-plan.md`](../../docs/internal/e2e-testing-plan.md)
— that document is the scenario ledger, not an exhaustiveness guarantee;
update it in the same change that adds or unblocks a scenario. Scenarios are
ordered on-chain action scripts with named checkpoints; prefer one scenario
per lifecycle path over one per event.

Keep upstream behavior claims cited to pinned `.refs/` sources; uncited
claims get rejected in review (AGENTS.md § Upstream anchors).
