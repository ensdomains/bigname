# End-to-end testing plan

Status: living coverage ledger for `tests/e2e`. Update the matrices in the
same change that adds or unblocks a scenario. The harness mechanics live in
`tests/e2e/README.md`; the contractual scenario list this plan expands is
`docs/architecture.md` § Test matrix.

## What this suite is for

Every other suite in the repo starts from state we authored: unit tests and
`tests/conformance` seed the database with rows that encode our own beliefs
about what the ENS, ENSv2, and Basenames contracts emit. This suite starts
from the contracts themselves: pinned ENSv1/ENSv2 deployment artifacts and
forge-built pinned Basenames sources run on local chains, real transactions
drive name lifecycles, and the real indexer, worker, and API binaries process
the results. It answers two questions nothing else answers:

1. Are our beliefs about upstream behavior true? (decoding, event mix,
   ordering, state transitions)
2. Does the pipeline hold its guarantees across *paths* between states —
   with reorgs, restarts, backfills, and replays landing mid-path — not just
   at hand-picked end states?

Each matrix row names its deepest validated layer. `covered_e2e` rows assert
the applicable contract action, retained facts/events, projections, and public
HTTP result. `covered_backfill_only` and `covered_contract_only` deliberately
stop at the layer named by the row and are not evidence for public route
behavior. Execution rows also assert durable traces when the execution plane is
configured.

## Verified foundations (phase 1, done)

The walking skeleton (`register_eth_name`) established, on `main`-quality
evidence, that:

- All three protocols are locally deployable from pinned inputs: ENSv1
  hardhat-deploy artifacts (`.refs/ens_v1/deployments/`), ENSv2 post-audit
  Sepolia artifacts (`.refs/ens_v2/contracts/deployments/sepolia/`, creation
  bytecode present including migration controllers and UniversalResolverV2),
  and Basenames contracts compiled on demand from `.refs/basenames` plus its
  recursively pinned git submodules.
- The indexer admits a local anvil node as any chain: chain identity is the
  provider label, so no fork or fake RPC layer is needed.
- Live intake (`indexer run`, supervised until the canonical checkpoint
  reaches the scenario head) is the correct ingest path; `backfill` alone
  does not promote the checkpoints snapshot-selected API reads need.
- Manifest profiles can be generated per scenario by preserving shipped
  family/version/capability structure while substituting local contract
  identities, addresses, and start blocks; checked-in manifests never change.
- Chain time can be warped (`evm_increaseTime`), so expiry, grace, premium
  decay, and commit-age waits are testable in seconds.

Phase 1 also produced the suite's first finding — which, on challenge,
turned out to be a **harness defect, not a product one**, and is worth
recording as a lesson. The initial symptom (declared resolver state missing
after a registration that set a resolver, while the registry's
`NewResolver` log was verifiably persisted) was diagnosed as "the shipped
profile doesn't ingest the registry" because the harness mirrored only
`v1.toml` per family — and the registry family's `v1.toml` is a deprecated
seed. In reality families version their manifests in place:
`ens_v1_registry_l1/v3.toml` is `active` and admits the current registry,
the old registry (`registry_old`), the resolver/subregistry discovery
rules, and the registry event ABI. Production does ingest the registry.
The harness now mirrors every `v*.toml` per family, and all
registry-driven scenarios pass under the true shipped profile with no
divergence. Standing rule: a "faithful mirror" claim requires mirroring
every manifest version, and any future "production doesn't do X" finding
must be validated against the complete profile (and, where possible, the
live API) before it is reported.

## Scenario matrices

Legend:

- `covered_e2e(scenario)` — contract action through live intake, projections,
  and the applicable public HTTP assertion.
- `covered_backfill_only(scenario)` — contract action through backfill to the
  layer asserted by the row: normalized events and, where stated, projection
  replay; no live checkpoint or API claim.
- `covered_contract_only(scenario)` — on-chain behavior only; ingestion and
  serving remain unproved.
- `known_broken(scenario; reason)` — a test reproduces and pins behavior that
  contradicts an intended contract or pipeline invariant; green is not a
  coverage graduation.
- `blocked(reason)` — no honest scenario reaches the required evidence layer.

There are no `planned` rows. `covered_e2e` never means every route, query
mode, failure class, or protocol path is exhaustive.

### ENSv1 — .eth second-level lifecycle

| Transition | Key assertions | Status |
| --- | --- | --- |
| Register via controller commit/reveal | registration active, registrant, expiry math, coverage full/authoritative | covered_e2e(register_eth_name) |
| Register without resolver | registration active with full/authoritative coverage, registrant and registry owner set, declared resolver `address`/`chain_id` null | covered_e2e(register_without_resolver_keeps_declared_resolver_empty) |
| Renew before expiry | expiry extends, RegistrationRenewed derived, same backing resource | covered_e2e(renew_and_transfer_keep_identity) |
| Transfer the registrar token, then reclaim | registrant and registry owner follow; the two-transaction transfer→reclaim window is a real registry-owner divergence that mints a transient anchor and converges back to the original registrar resource | covered_e2e(renew_and_transfer_keep_identity) |
| Expire → grace | no wire-level grace status: registration stays `active` with `released_at` null and expiry in the past; grace is consumer-derived | covered_e2e(expiry_grace_and_reregistration_rotate_identity) |
| Grace end → premium decay → re-register (different owner) | new backing resource minted; both leases' registration events persist under distinct resources | covered_e2e(expiry_grace_and_reregistration_rotate_identity) |
| Expire with no re-registration | two pinned facts: on a chain with no activity after grace end the release never settles (authority sync rounds are driven by log-bearing blocks; empty blocks advance no boundary — registration stays last-known `active` with past expiry); once any later admitted activity lands, the next round's boundary passes expiry+grace and `RegistrationReleased` materializes anchored to the first block after grace end, flipping exact-name to `released` with `released_at` from that event and excluding the name from the current registrant collection | covered_e2e(expire_without_reregistration_releases_and_unlists_registration) |
| Transfer the registrar token, never reclaim | the non-converging branch of the documented registry-owner divergence: the registry owner moves only on reclaim (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172 @ ens_v1@91c966f), so the registry-only authority interval persists as current state — which anchor exact-name binds, whose registrant/registry_owner serve, and address-collection membership pinned per docs/upstream.md § registry-owner divergence | covered_e2e(transfer_without_reclaim_keeps_registry_owner_divergent) |
| Register via a controller outside the admitted set | any owner-added controller may register (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L79 @ ens_v1@91c966f); observed honest gap is even narrower than planned: the registrar-plane facts persist raw-only (fresh-mint `Transfer` derives no `TokenControlTransferred` — the adapter requires an existing lease) and the whole registration derives exactly one registry-side `SubregistryChanged`; the child projects as a bracketed placeholder, no exact-name surface or registrant-collection entry exists | covered_e2e(unadmitted_controller_registration_derives_registry_side_only) |
| Register with record data and reverse-record flags in one tx | controller-authored record writes land through the resolver's trusted-controller bypass (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L116 @ ens_v1@91c966f) and the ETHEREUM reverse bit claims the reverse node in the same tx (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L319 @ ens_v1@91c966f): registration, records, and reverse candidate derive from one log burst across four families; the nonzero referrer decodes from the raw controller log only (no normalized field); **REVIEW POINT**: the burst's record writes derive only under the transient registry-only anchor — the same-tx `RegistrationGranted` rebinds the surface to the registrar resource, carrying the resolver across but neither the records nor the registry-owner facet, so exact-name serves an empty selector inventory with explicit `not_observed_on_current_resolver` gaps and the mid-burst controller as registry_owner; later plain writes restore the inventory (pinned) while the stale owner facet persists; pinned, chipped | known_broken(registration_with_records_reverse_and_referrer_derives_single_burst) |
| Registrar controller-set rotation | — | blocked(`ControllerAdded`/`ControllerRemoved` (upstream: .refs/ens_v1/contracts/ethregistrar/IBaseRegistrar.sol:L8 @ ens_v1@91c966f) are absent from every active manifest ABI, so the change that would make unadmitted-controller registrations live is unwatched; admission decision first) |

### ENSv1 — subnames

| Transition | Key assertions | Status |
| --- | --- | --- |
| Parent creates registry-only subname | child listed under parent with correct owner | covered_e2e(registry_driven_reads) |
| Subname created with unrevealed label (labelhash only — the registry never carries label strings for subnames) | bracketed placeholder child row; no exact-name surface minted (404) | covered_e2e(registry_driven_reads) |
| Same label under two different parents | same labelhash under `alice.eth` and `bob.eth` produces distinct child namehashes and owners, with no cross-parent leakage in either `/children` route | covered_e2e(same_label_under_two_parents_keeps_children_distinct) |
| Deep hierarchy (three+ levels) | registry facts derive at any depth (canonical SubregistryChanged for the grandchild under the placeholder's node), but enumeration stops at unknown surfaces: bracketed placeholder names are rejected as `invalid_input` at the ENSIP-15 boundary, and children under an unrevealed-label parent project no `children_current` row | covered_e2e(deep_registry_hierarchy_lists_direct_children_only) |
| Subname owner set to zero | zero-owner tombstone removes the child from the parent's default `/children` listing | covered_e2e(zero_owner_subname_leaves_default_children_listing) |
| Label preimage revealed later | placeholder upgrades to the real name: registrar `PreimageObserved` and the `label_preimages` row derive, `children_current` re-projects the child as `later.preimage.eth` with `label_preimage` provenance and a stable node/owner, and no exact-name surface is minted — pinned at the derivation and projection layers via backfill + replay; **REVIEW POINT**: live re-ingest of a chain whose later 2LD registration reveals an existing placeholder child's label hangs the run loop before checkpoint promotion (silent async wedge; catch-up replay of the same span derives fine), so API-layer reads of the upgraded child stay untestable pending the intake fix; pinned, chipped | covered_backfill_only(label_preimage_revealed_later_upgrades_child_listing) |
| Set TTL on a node (registry, and routed via the wrapper) | — | blocked(manifest/adapter drift found while implementing: `ens_v1_registry_l1/v3.toml` declares the `NewTTL` fragment, but the adapter has no NewTTL observation/apply path — only migration-guard suppression (`crates/adapters/src/ens_v1_unwrapped_authority/migration_guard.rs`) — so the raw log is retained and nothing can derive; needs a doc-first decision or adapter change before an honest row) |
| Registry-only name tree under a non-.eth TLD | owner, resolver, and records derive for a 2LD with no registrar family at any ancestor (the shape of DNSSEC-claimed trees); exact-name coverage and registration facets pinned for the no-registration-anchor shape | covered_e2e(registry_only_non_eth_tree_derives_declared_state) |
| Root contract / DNS registrar TLD operations | — | blocked(neither deployed by the harness nor admitted by any family; their effects arrive as ordinary admitted registry events, which the non-.eth TLD row exercises) |

### ENSv1 — wrapper

| Transition | Key assertions | Status |
| --- | --- | --- |
| Wrap a registrar name | adapter layer rotates fully (surface binding follows the wrapper resource + lineage; canonical AuthorityTransferred to the NameWrapper derives), and the wrapped holder shows as registrant; **REVIEW POINT**: the exact-name projection's control section retains the pre-wrap registry owner and a registrar-anchored authority_key — projection and adapter disagree for names wrapped after registrar birth (wrapper-born children project correctly, isolating the wrap-window ordering) | known_broken(wrapper_wrap_fuses_subnames_and_unwrap_restore_identity) |
| Unwrap before lease end | prior registrar anchor and lineage reactivate | covered_e2e(wrapper_wrap_fuses_subnames_and_unwrap_restore_identity) |
| Burn CANNOT_UNWRAP / CANNOT_TRANSFER / CANNOT_SET_RESOLVER | fuse changes arrive as PermissionScopeChanged scope events with exact raw bitmaps (196608 → 196621, validating pinned fuse constants); wrapper resources publish no subject grants, and the NameWrapper contract holds the registrar-anchor resource_control grant while wrapped; **REVIEW POINT**: no published effective-powers row exists for the wrapped holder anywhere, so the public docs explicitly classify fuse masking and wrapper-holder powers as unprojected | known_broken(wrapper_wrap_fuses_subnames_and_unwrap_restore_identity) |
| Emancipate a wrapped subname (PARENT_CANNOT_CONTROL) | no parent-owner powers published over the child (trivially satisfied today because wrapper-anchored resources publish no grants at all — see the fuse-row review point) | known_broken(wrapper_wrap_fuses_subnames_and_unwrap_restore_identity) |
| Wrapped expiry/grace edge | wrapETH2LD projects wrapper expiry as registrar expiry plus grace; exact-name expiry follows the wrapper authority | covered_e2e(wrapper_wrap_fuses_subnames_and_unwrap_restore_identity) |
| Wrapped owner ≠ registrant | wrapped holder appears as registrant while the pre-wrap owner remains in the (stale — see wrap-row review point) registry_owner facet | known_broken(wrapper_wrap_fuses_subnames_and_unwrap_restore_identity) |
| Wrapper-created subname | wrapper-born child projects fully wrapper-anchored: wrapper authority_kind/key, its own resource, registry_owner = the NameWrapper contract, holder as registrant, setSubnodeRecord resolver projected | covered_e2e(wrapper_wrap_fuses_subnames_and_unwrap_restore_identity) |
| Register born-wrapped via the wrapped controller (registerAndWrapETH2LD) | the admitted mainnet wrapped-controller artifact drives a single-tx `RegistrationGranted` + `NameWrapped` + registry owner→wrapper path through registerAndWrapETH2LD (upstream: .refs/ens_v1/deployments/mainnet/WrappedETHRegistrarController.json:L656 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289 @ ens_v1@91c966f), with one wrapper resource and no registrar holder-transfer derivation from the zero-address mint; **REVIEW POINT**: the controller's trailing grant makes the adapter clear the just-created wrapper, transition wrapper→registry-only→registrar, and leave registrar as the active resource, while boundary-event ordering makes exact-name serve a registry-only authority kind/key alongside the registrar resource/expiry and NameWrapper registry owner — born-wrapped therefore disproves the hypothesis that stale control is limited to the post-registration wrap window | known_broken(born_wrapped_registration_exposes_trailing_grant_rebind) |
| Renew a wrapped 2LD via the controller | controller renew touches only the registrar (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L366 @ ens_v1@91c966f) while the wrapper's separate renewal path updates stored expiry without emitting `ExpiryExtended` (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L337 @ ens_v1@91c966f): the current-controller transaction derives `RegistrationRenewed` and no wrapper `ExpiryChanged`, onchain wrapper expiry stays stale, but exact-name expiry honestly follows the registrar renewal while the wrapper resource and lineage remain stable | covered_e2e(wrapped_renewal_tracks_registrar_expiry_without_wrapper_event) |
| Wrapped ERC1155 transfer (single and batch) | real `safeTransferFrom` and `safeBatchTransferFrom` calls derive no registry or wrapper-lifecycle events; registrant follows each holder while resource and lineage stay stable, and the one `TransferBatch` raw log fans out into collision-free per-id `TokenControlTransferred` rows; **REVIEW POINT**: holder rotation does not repair the existing stale exact-name registry owner / registrar authority-key disagreement | known_broken(wrapped_erc1155_single_and_batch_transfers_preserve_identity) |
| Parent burns PARENT_CANNOT_CONTROL on an existing child; extends child expiry | a live child created without PCC transitions through exact fuse bitmaps 0→65536 via setChildFuses (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L517 @ ens_v1@91c966f), then extendExpiry emits `ExpiryExtended` (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L475 @ ens_v1@91c966f); the derived `ExpiryChanged` moves exact-name from registrar-expiry wrap time to the parent's registrar-expiry-plus-grace cap without rotating the child wrapper resource or lineage | covered_e2e(parent_burns_pcc_then_extends_existing_child_expiry) |
| Wrap an existing registry subname via wrap() | a plain child under a live registry-only parent rotates registry-only→wrapper through registry `Transfer` from setOwner (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L372 @ ens_v1@91c966f), not `NewOwner`; `NameWrapped` proves the DNS label preimage, the child gets a distinct wrapper resource/lineage and wrapper-consistent control (the placeholder interval minted a registry-only resource but never a binding — the wrap is the child's first and only surface binding), and the parent remains on its registry-only resource with no lineage; **REVIEW POINT**: this reveal-via-wrap path triggers the same live-intake hang as reveal-via-registration (chipped) — the scenario pins derivation and projections via backfill + replay, so API-layer reads stay untestable pending the intake fix | covered_backfill_only(wrap_existing_registry_subname_rotates_child_only) |
| NameWrapper upgrade path | — | blocked(the wrapper manifest deliberately excludes upgrade history, but admission leaks: upgrade() burns the wrapped token (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L500 @ ens_v1@91c966f) emitting an admitted `TransferSingle` to zero with no `NameUnwrapped` (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L278 @ ens_v1@91c966f); what that bare burn means needs an admission decision before an honest row) |
| Delegated-authority state (registry/resolver/wrapper approvals) | — | blocked(no approval event is admitted anywhere — e.g. registry `ApprovalForAll` (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L117 @ ens_v1@91c966f); delegated-authority state is not indexed, only its resulting writes are — the operator-mediated-write row covers the consequence side) |

### ENSv1 — resolvers and records

| Transition | Key assertions | Status |
| --- | --- | --- |
| Set resolver at registration | declared resolver populated with ResolverChanged provenance | covered_e2e(registry_driven_reads) |
| Write addr(60) and text records | record inventory carries the written selectors at the current boundary | covered_e2e(registry_driven_reads) |
| Change resolver later / set to zero | exact-name and records-route resolver state follow public resolver → second PublicResolver copy → zero-address null shape | covered_e2e(resolver_changes_follow_registry_and_zero_releases) |
| Multicoin addr and contenthash records; cached values on the records route | inventory carries `addr:0` and `contenthash`; compact records route returns raw multicoin bytes and contenthash bytes at the current boundary | covered_e2e(records_route_values_and_version_boundaries_follow_current_resolver) |
| Resolver replaced by another resolver | after moving to another PublicResolver copy, current records route no longer returns resolver-A `addr:0` or `contenthash` successes; the record-version boundary moves positionally (the wire boundary object carries chain position only — its event-identity fields are null) | covered_e2e(records_route_values_and_version_boundaries_follow_current_resolver) |
| Record version bump (clear records) | `clearRecords` moves the record-version boundary to a later position and the prior cached text value no longer returns success | covered_e2e(records_route_values_and_version_boundaries_follow_current_resolver) |
| Unadmitted custom resolver emits records | writes on an unadmitted-generation resolver are invisible to declared reads end to end: no `RecordChanged` derives, inventory publishes no selectors and reports explicit `not_observed_on_current_resolver` gaps per family, the requested text returns `not_found` with no value, and known-key enumeration stays supported-but-empty (a record-free unadmitted binding instead reports families as `resolver_family_pending` — asymmetric shapes, both pinned); watch item: one saturated pre-stabilization full run derived exactly one `RecordChanged` here (2026-07-10), not reproduced since across a bounded-parallelism full run and a 5-way stress — any recurrence is a derivation-determinism finding; capture with `BIGNAME_E2E_KEEP_DB=1` and inspect which sync path inserted the row | covered_e2e(unadmitted_custom_resolver_observes_facts_but_keeps_profile_gated) |
| One shared resolver serving many names | per-name text/addr reads stay node-scoped while resolver overview keeps `nodes` fan-in unsupported with `resolver_binding_enumeration_not_projected` | covered_e2e(shared_resolver_keeps_per_name_records_and_overview_fan_in_unsupported) |
| Write and delete records across the remaining admitted families (ABI, interface, DNS RRset, zonehash, forward name()) | all five families derive fully keyed `RecordChanged` at the normalized layer (`abi:<contentType>`, `interface:<id>`, `dns:<type>:<dns-name>`, `dns:zonehash`, `name`); `DNSRecordDeleted` (upstream: .refs/ens_v1/contracts/resolvers/profiles/DNSResolver.sol:L186 @ ens_v1@91c966f) derives as supersession-by-delete (`{deleted: true}`) on the same key; forward name() stays a record with a null selector key; the projection enumerates selectors for addr/text/contenthash only — the keyed families enter neither selectors nor gaps nor unsupported_families, pinned as family-level honesty | covered_e2e(remaining_record_families_derive_normalized_but_stay_unenumerated) |
| Operator/delegate-mediated record and subname writes | operators authorised via registry setApprovalForAll (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L112 @ ens_v1@91c966f) and resolver delegation emit events identical to owner-authored writes — pins that derivation never assumes owner-authored provenance and that the raw fact carries the true sender | covered_e2e(operator_delegate_writes_match_owner_authorship) |
| setPubkey on the admitted PublicResolver | the pinned resolver composes PubkeyResolver (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L29 @ ens_v1@91c966f) and setPubkey emits `PubkeyChanged` (upstream: .refs/ens_v1/contracts/resolvers/profiles/PubkeyResolver.sol:L25 @ ens_v1@91c966f) — the only composed-profile event absent from the resolver ABI; observed shape is sharper than planned: the live scan is topic-filtered by the manifest ABI, so the write is invisible at the raw layer too (zero raw logs, nothing derives, no pubkey family anywhere in the inventory), and the adapter's gate rejects the family by tested design; drift-vs-narrowing stays a doc-first question — no divergence entry names pubkey | covered_e2e(pubkey_write_on_admitted_resolver_stays_invisible) |

### ENSv1 — reverse and primary names

| Transition | Key assertions | Status |
| --- | --- | --- |
| Reverse claim set | claimed primary name appears as candidate only | covered_e2e(reverse_claim_set_changed_then_cleared_tracks_declared_candidate) |
| Claim whose forward resolution mismatches | declared candidate and mismatching forward `addr:60` are both present under an execution-enabled API; **REVIEW POINT**: tuple-present primary claims never enter the on-demand verifier, so verified mode returns `not_found` and persists no `verified_primary_name` trace/outcome rather than reporting the observable mismatch — this is an execution-entrypoint gap, not the verified-resolution cache-key mismatch | known_broken(forward_mismatch_keeps_declared_candidate_but_verified_not_found) |
| Claim changed, then cleared | candidate follows, then empties | covered_e2e(reverse_claim_set_changed_then_cleared_tracks_declared_candidate) |
| Claim string that fails normalization | surfaces invalid_name, never silently dropped | covered_e2e(reverse_claim_invalid_name_surfaces_raw_claim) |
| Claim without a name record (claim/claimWithResolver) | the reverse node's registry owner rotates via setSubnodeRecord with no `NameChanged` (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L84 @ ens_v1@91c966f) — no candidate minted; reverse-node `NewOwner` never mistaken for a claim | covered_e2e(claim_without_name_record_keeps_candidate_absent) |
| Claim set for another address by an authorised third party | claimForAddr/setNameForAddr (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L123 @ ens_v1@91c966f) — candidate derives from the node labelhash, not the tx sender | covered_e2e(authorised_third_party_claim_keys_candidate_to_claimed_address) |
| Claim routed through an unadmitted resolver | a live claim whose `NameChanged` never derives: declared candidate honestly absent — the reverse-plane analog of the unadmitted-resolver row | covered_e2e(unadmitted_reverse_resolver_keeps_candidate_absent) |
| Mainnet default.reverse claim (DefaultReverseRegistrar) | — | blocked(the current controller writes it on the DEFAULT reverse bit (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L326 @ ens_v1@91c966f), emitting `NameForAddrChanged` (upstream: .refs/ens_v1/contracts/reverseRegistrar/StandaloneReverseRegistrar.sol:L30 @ ens_v1@91c966f), but no mainnet reverse family admits the role or event — claims the pipeline can never see; admission decision first) |

### ENSv1 — registry migration (legacy → current registry)

The skeleton already deploys both registries with the real fallback wiring.

| Transition | Key assertions | Status |
| --- | --- | --- |
| Name existing only on the legacy registry | legacy-only 2LD derives canonical `SubregistryChanged` with old-registry emitter/current-registry authority provenance; no exact-name surface is minted and `eth` has no routeable children surface; a legacy-only child under a registered parent appears as a bracketed placeholder | covered_e2e(registry_migration_legacy_to_current_admission) |
| Migrate (first write on current registry) | asserted across two ingests of one chain because subregistry observations are one-per-node current-edge state (the legacy observation is superseded, not retained): pre-migration the legacy-emitted owner state is admitted; post-migration the current-registry controller registration supersedes it, later old-registry resolver and owner writes emit no normalized resolver/owner changes, and current-registry resolver and registry owner stay visible in normalized events and exact-name reads | covered_e2e(registry_migration_legacy_to_current_admission) |
| Legacy write to an unmigrated node post-cutover | a different legacy child written after another node migrates is still admitted with migration-epoch provenance and appears as a bracketed child under its registered parent | covered_e2e(registry_migration_legacy_to_current_admission) |

### ENSv2 (post-audit Sepolia profile)

Deployment module from `.refs/ens_v2/contracts/deployments/sepolia/`
artifacts; scenarios mirror the admitted four families only.

| Transition | Key assertions | Status |
| --- | --- | --- |
| Register through the v2 registrar | registrar intent links to the registry resource; identity, registration (`authority_kind=ens_v2_registry`), control, and history serve under `ethereum-sepolia`; the retained `RegistrarNameRegistered` evidence graduates fresh exact-name coverage to `full` / `authoritative` with `enumeration_basis=exact_name_profile`, so registration no longer waits for a renewal to lift shadow coverage | covered_e2e(ens_v2_sepolia_post_audit_declared_matrix_end_to_end) |
| Token regenerated (role change burns/mints token) | resource identity and surface binding stable, current token id updates | covered_e2e(ens_v2_sepolia_post_audit_declared_matrix_end_to_end) |
| Role bitmap grant/revoke, root vs resource scope | effective powers per registry vocabulary; revoke removes current subject row | covered_e2e(ens_v2_sepolia_post_audit_declared_matrix_end_to_end) |
| Subregistry attached, then swapped | SubregistryChanged derives for attach and swap at the parent registry, the parent resource survives the swap, and the old child stays out of the parent's children; **REVIEW POINT**: a discovered child registry's own logs are never scanned within the discovering session (discovery admits the edge, but zero raw logs are fetched from the discovered address in-session), so registrations INSIDE discovered subregistries derive nothing live — they need a later backfill/ops-catchup; child-name reads under discovered registries are pinned absent | covered_e2e(ens_v2_sepolia_post_audit_declared_matrix_end_to_end) |
| Alias-derived surface with no direct registry entry | alias path visible in topology, surface exists | blocked(the admitted resolver family decodes `AliasChanged`, but the post-audit Sepolia profile does not admit a contract-backed path that creates a `resolver_alias_path` surface binding; emitting alias topology alone would not mint an exact-name surface) |
| Shared subregistry → multiple surfaces, one resource | grouping by resource works, identities distinct | blocked(current discovery maps one registry address to one active suffix, so the same subregistry attached under multiple parents is superseded rather than represented as simultaneous surfaces; needs a doc/admission decision before an honest e2e row can assert it) |
| Unregister → re-register | resource and token lineage both advance across the cycle; live intake reaches the post-cycle checkpoint, keeps the stable name surface, closes the released resource binding before publishing the successor, and exact-name readback serves the new owner/resource | covered_e2e(ens_v2_sepolia_post_audit_declared_matrix_end_to_end) |
| ENSv1→ENSv2 migration flow | — | blocked(migration controllers outside admission; doc-first change required) |
| Renew via the v2 registrar; direct registry renew | after registry expiry but within the post-audit grace period, registrar renew forwards to the registry (upstream: .refs/ens_v2/contracts/src/registrar/AbstractETHRegistrar.sol:L84 @ ens_v2@48b3e2d) deriving `RegistrationRenewed` + `ExpiryChanged` from one action; direct renew emits `ExpiryUpdated` only and rejects reduction (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L214 @ ens_v2@48b3e2d); promoted exact-name coverage remains `full` / `authoritative`. Observed nuance: the wire emits `ExpiryUpdated` alone for a direct renew but the adapter derives both `ExpiryChanged` and `RegistrationRenewed` from that one log with registry-family provenance | covered_e2e(renewal_promotes_coverage_and_registry_edges_follow) |
| Registry token transferred (ERC1155 sale) | transfer migrates all roles as a paired `EACRolesChanged` revoke/grant with callbacks suppressed, so no `TokenRegenerated` (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L479 @ ens_v2@48b3e2d) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L494 @ ens_v2@48b3e2d) (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L226 @ ens_v2@48b3e2d) — the inverse coupling of the regeneration row, pinned exactly (paired `PermissionChanged`, zero `TokenRegenerated`); **REVIEW POINT**: the declared registrant facet stays at the SELLER after the sale (no registration event fires) while the migrated roles — including distinctly rendered `admin_*` powers — follow the buyer in `permissions_current`; pinned, chipped | known_broken(reserved_labels_foreign_registrar_and_token_sale) |
| Resolver set, changed, and zeroed at the watched registry | registration emits `ResolverUpdated` only for a nonzero resolver (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L474 @ ens_v2@48b3e2d); the edge follows set/change/zero with the zero-set deriving a NULL-resolver `ResolverChanged` (proper detach) and `name_current` clearing the facet. Persistent live polling now proves registry state survives separate registration, resolver, subregistry, token-regeneration, and unregister polls without false expiry history (`ens_v2_registry_state_survives_distinct_live_polls`). The richer renewal + three resolver changes + attach/detach row remains backfill-only until it is rerun live; its backfill still derives zero v2 `PermissionChanged` while live registration derives one (parity gap, chipped) | covered_backfill_only(resolver_and_subregistry_edges_follow_set_change_zero) |
| Record write, clearRecords, and name-scoped permission grant on a discovered v2 resolver | the admitted resolver family has zero exercised transitions: `RecordChanged`, `RecordVersionChanged` via clearRecords (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L249 @ ens_v2@48b3e2d), and the manifest-declared one-event→two-kinds fan-out derive — or the discovered-address scan gap is pinned for resolvers exactly as for registries; observed: the scan gap holds — discovery admits the resolver edge from the watched registry's `ResolverUpdated`, but zero raw logs are fetched from the discovered VerifiableFactory-proxied resolver in-session and zero record events derive (the registry review point's gap extends to resolvers verbatim) | known_broken(discovered_v2_resolver_records_stay_unscanned) |
| Expiry passes with no re-registration; re-register after expiry and grace | registry status changes by clock without a release event (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L625 @ ens_v2@48b3e2d), while registrar availability waits until the post-audit grace period ends (upstream: .refs/ens_v2/contracts/src/registrar/ETHRegistrar.sol:L291 @ ens_v2@48b3e2d): the event-silent phase serves the retained registration with its past expiry and no release-like event; re-registering then burns and bumps both version counters inside register with no `LabelUnregistered` and no `TokenRegenerated` (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L452 @ ens_v2@48b3e2d). Live intake reaches the successor checkpoint, retains both resource epochs as adjacent binding intervals, and exact-name readback serves the new owner/resource with `full` / `authoritative` coverage | covered_e2e(expiry_passes_then_reregistration_advances_lineage) |
| TLD attach at the RootRegistry apex; root-scope grant and revoke | the root family's first exercised transitions: the `eth` apex registration and subregistry attach derive at `ens_v2_root_l1`; `RootPermissionChanged` carries no action field — grant/revoke read from the resulting root-scope `role_bitmap` (set → zero), and the revoke clears the subject from `permissions_current`; `ParentChanged` derives from the watched registry's registry-level child declaration (setParent is root-scoped and registry-wide, not per-name) | covered_e2e(root_apex_attach_and_root_scope_roles) |
| Label reserved, then registered from reserved | owner=0 reserves token-less, deriving `RegistrationReserved` (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L462 @ ens_v2@48b3e2d); promotion requires ROLE_REGISTER_RESERVED (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L442 @ ens_v2@48b3e2d) and preserves the reservation expiry when expiry=0 (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L444 @ ens_v2@48b3e2d); observed shape: `RegistrationReserved` is labelhash-keyed with no label string in its state, status `reserved`, token-less; the promotion's `RegistrationGranted` carries the preserved expiry byte-for-byte | covered_e2e(reserved_labels_foreign_registrar_and_token_sale) |
| Registration by a non-admitted registrar | direct registry registration requires only root ROLE_REGISTRAR (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L430 @ ens_v2@48b3e2d), so a root-role holder outside the admitted registrar address can produce `RegistrationGranted` with registry-only provenance, no registrar intent, and permanently gated coverage — the adversarial complement of the registrar-intent row; pinned: the transaction's derived families are exactly `ens_v2_registry_l1` and exact-name coverage stays `unsupported` | covered_e2e(reserved_labels_foreign_registrar_and_token_sale) |
| Subregistry detached (set to zero); admin-half role rendering | setSubregistry stores the new value without a zero check (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L145 @ ens_v2@48b3e2d) — edge removal as distinct from the covered swap: the detach derives a NULL-subregistry `SubregistryChanged` through backfill + replay; admin-half bits render as distinct `admin_*` powers in `permissions_current` on the separate live token-sale corpus, while post-registration admin grants stay upstream-impossible for registered tokens (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L568 @ ens_v2@48b3e2d) | covered_backfill_only(resolver_and_subregistry_edges_follow_set_change_zero; live admin rendering covered by reserved_labels_foreign_registrar_and_token_sale) |
| ENSv2 reverse/primary plane | — | blocked(post-audit Sepolia deploys reverse-registrar adapters (upstream: .refs/ens_v2/contracts/deployments/sepolia/DefaultReverseRegistrarAdapter.json:L2 @ ens_v2@48b3e2d) (upstream: .refs/ens_v2/contracts/deployments/sepolia/ReverseRegistrarAdapter.json:L2 @ ens_v2@48b3e2d), but no Sepolia family admits a reverse or primary source; a doc-first admission decision is required) |
| ERC1155 ownership/approval surface | — | blocked(`TransferSingle`/`TransferBatch`/`ApprovalForAll` are in no manifest ABI; the role-migration half is the planned token-sale row — the ownership-motion half needs an explicit admitted-or-unsupported statement) |
| UUPS implementation rotation on discovered registries/resolvers | — | blocked(UserRegistry is UUPS-upgradeable (upstream: .refs/ens_v2/contracts/src/registry/UserRegistry.sol:L20 @ ens_v2@48b3e2d) and ROLE_UPGRADE exists, but no `Upgraded` event is admitted — a discovered authority can silently change behavior; admission decision) |

### Basenames (second chain instance)

Deployment module forge-builds the pinned `.refs/basenames` sources (the
committed broadcast bytecode predates the pinned tree — its constructor
layouts differ and cannot be cited); runs on a
second anvil presented as `base-mainnet`.
The Phase 5 primary-name rows use ENSv1's Base L2ReverseRegistrar
`NameForAddrChanged` event and constructor coin type 2147492101
(upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L98 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L391 @ ens_v1@91c966f).

| Transition | Key assertions | Status |
| --- | --- | --- |
| Register a *.base.eth name | Base-side authority split (registry/registrar/resolver families) | covered_e2e(basenames_declared_state_matrix_end_to_end) |
| NFT-only transfer vs management-only transfer vs full transfer | registrar token and registry-owner control facets move independently, then converge on reclaim | covered_e2e(basenames_declared_state_matrix_end_to_end) |
| Address-resolution change on the L2 resolver | declared `addr:60` record updates through the admitted Base L2Resolver | covered_e2e(basenames_declared_state_matrix_end_to_end) |
| Primary name set/unset (Base reverse registrar event) | declared candidate tracks `NameForAddrChanged` at Base coin type 2147492101 and clears on blank | covered_e2e(basenames_declared_state_matrix_end_to_end) |
| L1 compatibility resolution | transport path, verified only through the execution plane | blocked(needs the Basenames L1Resolver transport with a CCIP gateway pair the pins cannot deploy standalone — same gateway/verifier limitation as the CCIP execution row) |
| Renew; expire → grace → premium re-registration (different owner) | Base's 3-arg `NameRenewed` fragment decodes from the legacy controller (upstream: .refs/basenames/src/L2/RegistrarController.sol:L497 @ basenames@1809bbc); availability returns only after expiry plus the grace window (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L294 @ basenames@1809bbc); re-registration burns then re-mints in one tx (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L443 @ basenames@1809bbc) — release synthesis settles at the sync boundary the first post-grace admitted activity advances (observed at the boundary block immediately before the activity transaction, never later than it), rotates token lineage, and preserves distinct resources for both leases | covered_e2e(renew_release_and_premium_reregistration_rotate_lineage) |
| Register and renew via the UpgradeableRegistrarController (erc1967 proxy) | the production-current Base registration path (upstream: .refs/basenames/README.md:L37 @ basenames@1809bbc) decodes admitted registration and renewal fragments from the proxy emitter (upstream: .refs/basenames/src/L2/UpgradeableRegistrarController.sol:L515 @ basenames@1809bbc); `contract_instances` and event provenance keep proxy and implementation identities distinct | covered_e2e(upgradeable_controller_proxy_registers_and_renews) |
| Subname created under a registered *.base.eth name | the Base registry's `declared_children = supported` surface lists a revealed child under its Base parent, brackets a hash-only sibling, and removes the zero-owned child under Base namespace/suffix rules | covered_e2e(basenames_subnames_list_preimages_placeholders_and_tombstones) |
| Text, multicoin, and name records plus clearRecords on the L2Resolver | all four writes derive through the admitted resolver family with keyed record state; inventory carries the selectors at the boundary and the `VersionChanged` bump prevents the prior successful values from serving on the records route | covered_e2e(l2_resolver_records_clear_and_contenthash_gap) |
| Resolver rotated to a second (unadmitted-profile) instance, then to zero | binding follows registry `NewResolver`, record consumption stays profile-gated (zero `RecordChanged` ever derives for the name), and the zero rotation clears the declared resolver facet; the on-chain generation mismatch is pinned via RPC code hashes; **REVIEW POINT**: live intake hangs on this rotation-to-discovered-instance chain (the Base sibling of the chipped hang family — the ENSv1 twin ingests live cleanly), so the scenario pins via backfill + replay, where the transport excludes the unadmitted instance entirely (no raw logs, no stored code hash) and the stored inventory is a stub — the wire-level unadmitted-binding inventory rendering stays untestable on Base pending the intake fix (the ENSv1 twin pins that shape) | covered_backfill_only(unadmitted_resolver_rotation_stays_profile_gated_then_clears) |
| Legacy ReverseRegistrar claim/setName (registry-driven reverse hierarchy) | claims write the registry under `80002105.reverse` via `setSubnodeRecord` (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L158 @ basenames@1809bbc). A claim-only ingest derives `NewOwner` as `SubregistryChanged`, and resolver discovery preserves `NewResolver` as a null-logical-name `ResolverChanged`. **REVIEW POINT:** a one-shot claim-plus-setName replay collapses the repeated reverse-child assignment to the latter `SubregistryChanged`; the reverse-node `NameChanged` survives only as a raw fact, and without an admitted primary-claim source or known reverse parent no normalized `RecordChanged` or `children_current` reverse placeholder appears and the primary candidate stays absent | covered_e2e(legacy_reverse_registrar_stays_registry_and_raw_record_only) |
| Registration through an owner-added third-party controller (incl. registerOnly) | registrar-level label events are out of admission: direct `register` retains the raw token mint plus one registry-side authority derivation with no `RegistrationGranted` or normalized token-control event; `registerOnly` retains only the raw token mint and creates no registry node or normalized event (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L248 @ basenames@1809bbc) | covered_e2e(third_party_controller_registration_degrades_without_label_events) |
| Contenthash (and other unadmitted-family) writes on the admitted L2Resolver | the pinned L2Resolver composes `ContentHashResolver` (upstream: .refs/basenames/src/L2/L2Resolver.sol:L29 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/L2Resolver.sol:L33 @ basenames@1809bbc), whose setter emits `ContenthashChanged` (upstream: .refs/basenames/src/L2/resolver/ContentHashResolver.sol:L32 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/resolver/ContentHashResolver.sol:L34 @ basenames@1809bbc); observed: the raw contenthash log IS retained at the watched instance (asymmetric with the mainnet pubkey pin, where the topic-filtered scan persists no raw log at all) — the profile gate rejects it before derivation, zero normalized events derive, and the inventory reports the explicit `not_observed_on_current_resolver` gap; the admission narrowing is documented in docs/upstream.md § Known divergences | covered_e2e(l2_resolver_records_clear_and_contenthash_gap) |
| setTTL / setRecord → NewTTL decode | — | blocked(same manifest/adapter drift as the ENSv1 TTL row and the same chip: `basenames_base_registry/v2.toml` declares the `NewTTL` fragment, but the shared unwrapped-authority adapter has no NewTTL observation path (`crates/adapters/src/ens_v1_unwrapped_authority/observation.rs` proceeds from NewOwner/Transfer/NewResolver straight to resolver events) — doc-first or adapter change required before an honest row) |

### Verified resolution and offchain (execution plane)

| Scenario | Key assertions | Status |
| --- | --- | --- |
| Direct-path verified query via locally deployed UniversalResolver | verified section agrees with declared; execution trace, steps, and cache outcome persist and are asserted at layer 3; explain immediately reads the persisted outcome at the route-selected head and returns the identical trace id even though `name_current` predates that head. Harness boundary: the locally deployed runtime is installed at bigname's frozen official Universal Resolver proxy address, so this covers that admitted entrypoint class rather than arbitrary manifest-address substitution | covered_e2e(verified_resolution::direct_path_verified_query_via_local_universal_resolver_persists_trace) |
| Wildcard-derived answer | wildcard topology populated, supported class honored | blocked(no e2e-deployed pinned contract path currently produces the `observed_wildcard_path` topology required by the public support class; API tests seed that topology, and faking it here would bypass the contract-first harness) |
| Alias-path answer | alias hops recorded | blocked(no ENSv1 pinned deployment helper or local manifest admission path currently produces `resolver_alias_path`; alias coverage is seeded below e2e and needs a contract-backed source before this row can flip) |
| CCIP-Read success / failure / proof mismatch (local mock gateway) | statuses distinguish; failures never fabricate values | blocked(local mock gateway coverage needs a deployable offchain resolver/gateway pair from pins; the simple ENSv1 mock resolver is test source without a harness deployment artifact (upstream: .refs/ens_v1/contracts/test/mocks/MockOffchainResolver.sol:L15 @ ens_v1@91c966f), while shipped offchain resolver artifacts require DNSSEC/gateway-verifier topology not deployed by the ENSv1 e2e stack) |
| Cache invalidation on record change, topology change, reorg | stale verified answers do not survive | blocked(record-change refresh requires a supported same-DB live ingest plus projection replay to move the selected snapshot and record boundary after the on-chain mutation; worker invalidation commands can delete exact stale boundaries but do not refresh the snapshot or re-execute against the post-change block by themselves) |

### Cross-protocol composition (mainnet profile)

The shipped `mainnet` profile is one corpus spanning ENSv1 on `ethereum`,
Basenames on `base-mainnet`, and two glue families on the ethereum chain
(`basenames_l1_compat` v1 and `basenames_execution` v2, both active).
The protocol-specific sections above use single-protocol, single-chain corpora.
The scenarios below close that composition gap with both chains and the glue
families in one mainnet-profile corpus; they do not cover the blocked CCIP
transport path.
Non-goal: ENSv1↔ENSv2 same-corpus composition — the post-audit Sepolia profile is
ENSv2-only, so that crossing is fenced by profile separation on top of the
blocked migration row; revisit only if a profile ever admits both.

| Scenario | Key assertions | Status |
| --- | --- | --- |
| Composed-corpus equivalence | one corpus, both anvils, full mainnet-profile mirror (ENSv1 ethereum + Basenames base-mainnet + the ethereum-chain glue families): per-chain canonical checkpoints coexist, and each protocol's exact-name body equals its single-protocol baseline after normalizing corpus-minted identifiers — including the found nuance that `authority_key`'s third segment is the per-corpus contract-instance ordinal (9 vs 1 across corpora), so the key is corpus-relative rather than purely chain-derived | covered_e2e(composed_mainnet_profile_serves_both_protocols_without_leakage) |
| base.eth namespace handoff | pinned: `base.eth` has no ENSv1-side registration in the corpus (404 under the ens namespace), `*.base.eth` names serve under the basenames namespace with base-mainnet positions only, and neither name ever carries the other chain's position | covered_e2e(composed_mainnet_profile_serves_both_protocols_without_leakage) |
| One address, two protocols | one EOA registers on both chains in one corpus: per-namespace address collections each list exactly their own name with distinct backing resources — no identity bleed | covered_e2e(composed_mainnet_profile_serves_both_protocols_without_leakage) |
| Primary-name coexistence | the same address holds a mainnet `addr.reverse` claim (coin 60) and a Base `80002105.reverse` claim (coin 2147492101) in one corpus; each namespace's declared candidate serves its own name with no leak | covered_e2e(composed_mainnet_profile_serves_both_protocols_without_leakage) |
| Cross-chain perturbations | a LIVE mid-session reorg on the Base chain converges Base to the winning branch (losing rows orphaned by block hash) while the ethereum chain keeps zero orphaned rows, an unmoved canonical checkpoint, and a still-served name | covered_e2e(base_reorg_leaves_ethereum_canonicality_untouched) |
| L1-compat declared side | both glue families (`basenames_l1_compat` v1, `basenames_execution` v1+v2) sync their admission into the composed corpus as stored manifest state on `ethereum-mainnet` (live runs derive no manifest bookkeeping events — those are backfill-only extras per the phase-3 parity pin) and the undeployed placeholder role stays silent; the CCIP transport hop stays blocked | covered_e2e(composed_mainnet_profile_serves_both_protocols_without_leakage) |

## Representative perturbation corpus (phase 3)

These four tests exercise one moderately rich ENSv1 corpus (`perturb.eth`
registration, resolver records, and a registry-only child). They establish
that the harness can drive each failure mode and that this representative
shape converges; they do not multiply every protocol scenario or checkpoint.

| Perturbation | Mechanism | Convergence requirement | Status |
| --- | --- | --- | --- |
| Mid-session reorg at one rich-corpus checkpoint | anvil snapshot/revert via `harness::rpc::{evm_snapshot, evm_revert}`, mine a divergent longer branch under one live `pipeline::IndexerRunSession` | winning-branch route snapshots equal a fresh winning-chain control; losing-branch `raw_logs` and `normalized_events` remain present with orphaned canonicality by losing block hash | covered_e2e(`perturbations::rich_chain_live_reorg_converges_to_winning_branch`, `harness::pipeline::IndexerRunSession`, `harness::perturb::route_snapshots`) |
| Indexer killed and relaunched mid-scenario | `pipeline::indexer_run_restart_after_first_checkpoint` kills the first `indexer run` after the first canonical checkpoint row, then restarts to scenario readiness | final route snapshots equal an unperturbed live ingest of the same finished chain | covered_e2e(`perturbations::rich_chain_indexer_restart_mid_scenario_matches_control`, `harness::pipeline::indexer_run_restart_after_first_checkpoint`, `harness::perturb::assert_snapshots_equal`) |
| Backfill-from-zero after the fact | the harness's `backfill` runner over the finished chain, block `0..head` | every deterministic normalized-event field is compared after mapping only corpus-minted contract-instance UUIDs to stable chain/address identities; multiplicity is retained; live ⊆ backfill exactly, with backfill-only extras bounded to bookkeeping/late-round kinds (`SourceManifestUpdated`/`CapabilityChanged`/`PreimageObserved`); no API-route parity claim because backfill does not promote snapshot checkpoints | covered_backfill_only(`perturbations::rich_chain_backfill_normalized_events_match_live_ingest`, `harness::perturb::assert_backfill_normalized_event_parity`) |
| Projection replay | snapshot fixed route set, run `replay all-current-projections`, then run full-range `replay normalized-events` plus projection replay and re-snapshot | route snapshots remain byte-equal after projection replay and after normalized-event replay plus projection replay | covered_e2e(`perturbations::rich_chain_projection_and_normalized_event_replay_are_route_stable`, `harness::perturb::route_snapshots`) |

Runtime verification of these four surfaced three wire/derivation facts now
encoded in the harness: `last_updated` on empty collections is read-time
wall clock and is normalized only for those empty envelopes; non-empty route
timestamps remain part of equality. Contract-instance ids are minted per
corpus, so cross-database comparison replaces each with its stable
chain/address identity without dropping the surrounding field. Control runs
ingest to the identical head (`ingest_at_current_head`) because route bodies
embed `chain_positions`.

## Runtime topology coverage

Most matrix scenarios run the production `indexer run` loop to readiness,
stop it, execute the worker's deterministic one-shot
`replay all-current-projections`, and then start the API. That is full evidence
for intake, rebuild, and serving, but not for the worker's continuous apply
loop. `live_worker_applies_registration_and_renewal_while_api_serves` is the
bounded production-loop smoke: indexer, `worker run`, and API stay live while
a registration and later renewal land, and the API observes both projection
updates. Continuous worker/restart coverage is not implied for every row.

## Harness roadmap

| Capability | Needed by | Notes |
| --- | --- | --- |
| Checkpoint abstraction (named on-chain step + per-checkpoint route snapshots) | 2 | snapshots as checked-in JSON; diff-reviewable; becomes the documented state machine |
| Route snapshot walker (canonical route set per name/address under test) | 2 | normalize away run-varying fields (timestamps, UUIDs) explicitly, never blindly |
| Perturbation runner wrapping any scenario | 3 | one implementation, N scenarios × M variants |
| Second anvil instance + `base-mainnet` manifest generation | 5 | covered: `Anvil::spawn_base_mainnet`, multi-provider pipeline runners, and Base Basenames manifest mirroring |
| Basenames artifact source | 5 | covered: forge-builds the pinned sources on demand after recursively initializing their pinned git submodules — the committed broadcast bytecode predates the pinned tree and was rejected as uncitable |
| ENSv2 post-audit Sepolia deployment module + profile generation | 7 | covered: `harness::ens_v2`, `manifests-sepolia` mirror, and `ethereum-sepolia` checkpoint target |
| Mock CCIP gateway (local HTTP server) | 6 | request/response digests must land in execution traces |
| Execution RPC wiring (`--chain-rpc-url` on API/worker pointed at anvil) | 6 | UniversalResolver artifact bytecode is pinned for both v1 and v2 |
| Merged mainnet-profile mirror + dual-anvil pipeline run | 9 | covered: `manifests::generate_local_mainnet_composed_profile` unions all eleven mainnet families (five ENSv1, four Base, two ethereum-chain glue) into one root, and `pipeline::indexer_run_until_chain_checkpoints` waits each chain's canonical checkpoint under one live session |

## Phasing

The original order put the determinism harness before breadth so later
scenarios could reuse it. The shipped phase-3 tests apply that harness to one
representative rich corpus; reuse across the full matrix remains future work.

All seven original phases executed (2026-07-09, commits
`f13fe50`..`1bdf886`; 24 scenarios green). A 2026-07-10 adversarial
completeness audit (three independent reviewers, one per protocol,
enumerating from the pinned contracts toward the matrix) found the
matrices helper-shaped rather than contract-shaped and opened phase 8
(the missing transitions) and phase 9 (cross-protocol composition).
The implementation pass closed the planned queue the same day (commits
`08fe850`..`448704e`), but green tests exposed several shallower or broken
paths. The ledger now records those as `covered_backfill_only`,
`covered_contract_only`, or `known_broken` rather than treating them as full
e2e graduation. No planned rows remain; open work is visible in those states
and the blocked rows.

| Phase | Scope | Status |
| --- | --- | --- |
| 1 | Walking skeleton | done — registration scenario green in CI |
| 2 | ENSv1 lifecycle + subnames + registry migration | done — live rows are classified; label-preimage reveal remains backfill-only because live intake wedges |
| 3 | Representative perturbation corpus: reorg, restart, backfill parity, replay equality | done — all four checks run over one rich ENSv1 shape, not the whole matrix |
| 4 | Wrapper + reverse/primary | done as a matrix pass — wrapper control/effective-power and tuple-present primary verification rows remain `known_broken` |
| 5 | Basenames dual-chain | done — declared-state matrix covered; L1-compat blocked with the CCIP-gateway reason |
| 6 | Execution plane | done as a first slice — direct path and persisted explain are covered; wildcard/alias/CCIP/cache invalidation remain blocked |
| 7 | ENSv2 declared-state matrix | done as a matrix pass — fresh registration coverage is repaired; discovered-source scanning and re-registration intake remain broken or shallower than e2e |
| 8 | Audit-driven matrix extensions: missing transitions and admission-blocked rows from the 2026-07-10 completeness audit | done as classification — every row has evidence or an explicit blocker, but several are intentionally not `covered_e2e` |
| 9 | Cross-protocol composition (mainnet profile): merged-profile mirror, dual-anvil corpus, composition rows | done — all six rows covered over the composed corpus, including a live one-chain reorg; findings: `authority_key` is corpus-relative (instance ordinal), and manifest bookkeeping events are backfill-only |

## CI gate

The current `test (e2e)` job runs the full suite, including the representative
perturbation tests, on every pull request and push with
`--test-threads=8`. It also runs fmt/check/clippy against the standalone e2e
workspace. There is no scheduled nightly workflow and no env-gated slow tier;
all compiled scenarios are expected to run, so an omitted scenario cannot be
mistaken for a green nightly result.

## Ledger discipline

- A matrix row changes status only in the PR that adds the scenario or the
  blocker resolution.
- New upstream behavior claims in scenarios cite pinned `.refs/` sources,
  same as everywhere else (AGENTS.md § Upstream anchors).
- When a scenario contradicts shipped semantics, the finding goes to a
  doc-first task before the assertion is changed — this suite reports on
  the contract, it does not quietly redefine it.
- If `.refs` pins rotate, the suite re-verifies decoding against the new
  artifacts by construction; a pin rotation PR that breaks e2e is evidence
  of a real upstream-facing change, not test flakiness.
