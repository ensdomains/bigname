# End-to-end testing plan

Status: living coverage ledger for `tests/e2e`. Update the matrices in the
same change that adds or unblocks a scenario. The harness mechanics live in
`tests/e2e/README.md`; the contractual scenario list this plan expands is
`docs/architecture.md` § Test matrix.

## What this suite is for

Every other suite in the repo starts from state we authored: unit tests and
`tests/conformance` seed the database with rows that encode our own beliefs
about what the ENS, ENSv2, and Basenames contracts emit. This suite starts
from the contracts themselves: the pinned upstream bytecode runs on a local
chain, real transactions drive name lifecycles, and the real indexer, worker,
and API binaries process the results. It answers two questions nothing else
answers:

1. Are our beliefs about upstream behavior true? (decoding, event mix,
   ordering, state transitions)
2. Does the pipeline hold its guarantees across *paths* between states —
   with reorgs, restarts, backfills, and replays landing mid-path — not just
   at hand-picked end states?

Each scenario asserts at the validation layers from the architecture doc:
persisted raw logs, canonical normalized events, execution traces (once the
execution plane is configured), and public API output over HTTP.

## Verified foundations (phase 1, done)

The walking skeleton (`register_eth_name`) established, on `main`-quality
evidence, that:

- All three protocols are locally deployable from pinned artifacts with no
  re-compilation: ENSv1 hardhat-deploy artifacts
  (`.refs/ens_v1/deployments/`), ENSv2 sepolia-dev artifacts
  (`.refs/ens_v2/contracts/deployments/sepolia-dev/`, creation bytecode
  present including migration controllers and UniversalResolverV2), and
  Basenames forge broadcast logs (`.refs/basenames/broadcast/`).
- The indexer admits a local anvil node as any chain: chain identity is the
  provider label, so no fork or fake RPC layer is needed.
- Live intake (`indexer run`, supervised until the canonical checkpoint
  reaches the scenario head) is the correct ingest path; `backfill` alone
  does not promote the checkpoints snapshot-selected API reads need.
- Manifest profiles can be generated per scenario by re-pointing copies of
  the shipped family manifests at local addresses; checked-in manifests
  never change.
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

Legend: `covered(scenario)` / `planned(N)` = target phase / `blocked(reason)`.

### ENSv1 — .eth second-level lifecycle

| Transition | Key assertions | Status |
| --- | --- | --- |
| Register via controller commit/reveal | registration active, registrant, expiry math, coverage full/authoritative | covered(register_eth_name) |
| Register without resolver | registration active with full/authoritative coverage, registrant and registry owner set, declared resolver `address`/`chain_id` null | covered(register_without_resolver_keeps_declared_resolver_empty) |
| Renew before expiry | expiry extends, RegistrationRenewed derived, same backing resource | covered(renew_and_transfer_keep_identity) |
| Transfer the registrar token, then reclaim | registrant and registry owner follow; the two-transaction transfer→reclaim window is a real registry-owner divergence that mints a transient anchor and converges back to the original registrar resource | covered(renew_and_transfer_keep_identity) |
| Expire → grace | no wire-level grace status: registration stays `active` with `released_at` null and expiry in the past; grace is consumer-derived | covered(expiry_grace_and_reregistration_rotate_identity) |
| Grace end → premium decay → re-register (different owner) | new backing resource minted; both leases' registration events persist under distinct resources | covered(expiry_grace_and_reregistration_rotate_identity) |
| Expire with no re-registration | two pinned facts: on a chain with no activity after grace end the release never settles (authority sync rounds are driven by log-bearing blocks; empty blocks advance no boundary — registration stays last-known `active` with past expiry); once any later admitted activity lands, the next round's boundary passes expiry+grace and `RegistrationReleased` materializes anchored to the first block after grace end, flipping exact-name to `released` with `released_at` from that event and excluding the name from the current registrant collection | covered(expire_without_reregistration_releases_and_unlists_registration) |
| Transfer the registrar token, never reclaim | the non-converging branch of the documented registry-owner divergence: the registry owner moves only on reclaim (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172 @ ens_v1@91c966f), so the registry-only authority interval persists as current state — which anchor exact-name binds, whose registrant/registry_owner serve, and address-collection membership pinned per docs/upstream.md § registry-owner divergence | covered(transfer_without_reclaim_keeps_registry_owner_divergent) |
| Register via a controller outside the admitted set | any owner-added controller may register (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L79 @ ens_v1@91c966f); observed honest gap is even narrower than planned: the registrar-plane facts persist raw-only (fresh-mint `Transfer` derives no `TokenControlTransferred` — the adapter requires an existing lease) and the whole registration derives exactly one registry-side `SubregistryChanged`; the child projects as a bracketed placeholder, no exact-name surface or registrant-collection entry exists | covered(unadmitted_controller_registration_derives_registry_side_only) |
| Register with record data and reverse-record flags in one tx | controller-authored record writes land through the resolver's trusted-controller bypass (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L116 @ ens_v1@91c966f) and the ETHEREUM reverse bit claims the reverse node in the same tx (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L319 @ ens_v1@91c966f): registration, records, and reverse candidate derive from one log burst across four families; the nonzero referrer decodes from the raw controller log only (no normalized field); **REVIEW POINT**: the burst's record writes derive only under the transient registry-only anchor — the same-tx `RegistrationGranted` rebinds the surface to the registrar resource, carrying the resolver across but neither the records nor the registry-owner facet, so exact-name serves an empty selector inventory with explicit `not_observed_on_current_resolver` gaps and the mid-burst controller as registry_owner; later plain writes restore the inventory (pinned) while the stale owner facet persists; pinned, chipped | covered(registration_with_records_reverse_and_referrer_derives_single_burst) |
| Registrar controller-set rotation | — | blocked(`ControllerAdded`/`ControllerRemoved` (upstream: .refs/ens_v1/contracts/ethregistrar/IBaseRegistrar.sol:L8 @ ens_v1@91c966f) are absent from every active manifest ABI, so the change that would make unadmitted-controller registrations live is unwatched; admission decision first) |

### ENSv1 — subnames

| Transition | Key assertions | Status |
| --- | --- | --- |
| Parent creates registry-only subname | child listed under parent with correct owner | covered(registry_driven_reads) |
| Subname created with unrevealed label (labelhash only — the registry never carries label strings for subnames) | bracketed placeholder child row; no exact-name surface minted (404) | covered(registry_driven_reads) |
| Same label under two different parents | same labelhash under `alice.eth` and `bob.eth` produces distinct child namehashes and owners, with no cross-parent leakage in either `/children` route | covered(same_label_under_two_parents_keeps_children_distinct) |
| Deep hierarchy (three+ levels) | registry facts derive at any depth (canonical SubregistryChanged for the grandchild under the placeholder's node), but enumeration stops at unknown surfaces: bracketed placeholder names are rejected as `invalid_input` at the ENSIP-15 boundary, and children under an unrevealed-label parent project no `children_current` row | covered(deep_registry_hierarchy_lists_direct_children_only) |
| Subname owner set to zero | zero-owner tombstone removes the child from the parent's default `/children` listing | covered(zero_owner_subname_leaves_default_children_listing) |
| Label preimage revealed later | placeholder upgrades to the real name: registrar `PreimageObserved` and the `label_preimages` row derive, `children_current` re-projects the child as `later.preimage.eth` with `label_preimage` provenance and a stable node/owner, and no exact-name surface is minted — pinned at the derivation and projection layers via backfill + replay; **REVIEW POINT**: live re-ingest of a chain whose later 2LD registration reveals an existing placeholder child's label hangs the run loop before checkpoint promotion (silent async wedge; catch-up replay of the same span derives fine), so API-layer reads of the upgraded child stay untestable pending the intake fix; pinned, chipped | covered(label_preimage_revealed_later_upgrades_child_listing) |
| Set TTL on a node (registry, and routed via the wrapper) | — | blocked(manifest/adapter drift found while implementing: `ens_v1_registry_l1/v3.toml` declares the `NewTTL` fragment, but the adapter has no NewTTL observation/apply path — only migration-guard suppression (`crates/adapters/src/ens_v1_unwrapped_authority/migration_guard.rs`) — so the raw log is retained and nothing can derive; needs a doc-first decision or adapter change before an honest row) |
| Registry-only name tree under a non-.eth TLD | owner, resolver, and records derive for a 2LD with no registrar family at any ancestor (the shape of DNSSEC-claimed trees); exact-name coverage and registration facets pinned for the no-registration-anchor shape | covered(registry_only_non_eth_tree_derives_declared_state) |
| Root contract / DNS registrar TLD operations | — | blocked(neither deployed by the harness nor admitted by any family; their effects arrive as ordinary admitted registry events, which the non-.eth TLD row exercises) |

### ENSv1 — wrapper

| Transition | Key assertions | Status |
| --- | --- | --- |
| Wrap a registrar name | adapter layer rotates fully (surface binding follows the wrapper resource + lineage; canonical AuthorityTransferred to the NameWrapper derives), and the wrapped holder shows as registrant; **REVIEW POINT**: the exact-name projection's control section retains the pre-wrap registry owner and a registrar-anchored authority_key — projection and adapter disagree for names wrapped after registrar birth (wrapper-born children project correctly, isolating the wrap-window ordering) | covered(wrapper_wrap_fuses_subnames_and_unwrap_restore_identity) |
| Unwrap before lease end | prior registrar anchor and lineage reactivate | covered(wrapper_wrap_fuses_subnames_and_unwrap_restore_identity) |
| Burn CANNOT_UNWRAP / CANNOT_TRANSFER / CANNOT_SET_RESOLVER | fuse changes arrive as PermissionScopeChanged scope events with exact raw bitmaps (196608 → 196621, validating pinned fuse constants); wrapper resources publish no subject grants, and the NameWrapper contract holds the registrar-anchor resource_control grant while wrapped; **REVIEW POINT**: no published effective-powers row exists for the wrapped holder anywhere, while the docs describe wrapper powers as "masked before publication" — a published-then-masked shape the pipeline never produces | covered(wrapper_wrap_fuses_subnames_and_unwrap_restore_identity) |
| Emancipate a wrapped subname (PARENT_CANNOT_CONTROL) | no parent-owner powers published over the child (trivially satisfied today because wrapper-anchored resources publish no grants at all — see the fuse-row review point) | covered(wrapper_wrap_fuses_subnames_and_unwrap_restore_identity) |
| Wrapped expiry/grace edge | wrapETH2LD projects wrapper expiry as registrar expiry plus grace; exact-name expiry follows the wrapper authority | covered(wrapper_wrap_fuses_subnames_and_unwrap_restore_identity) |
| Wrapped owner ≠ registrant | wrapped holder appears as registrant while the pre-wrap owner remains in the (stale — see wrap-row review point) registry_owner facet | covered(wrapper_wrap_fuses_subnames_and_unwrap_restore_identity) |
| Wrapper-created subname | wrapper-born child projects fully wrapper-anchored: wrapper authority_kind/key, its own resource, registry_owner = the NameWrapper contract, holder as registrant, setSubnodeRecord resolver projected | covered(wrapper_wrap_fuses_subnames_and_unwrap_restore_identity) |
| Register born-wrapped via the wrapped controller (registerAndWrapETH2LD) | the dominant modern mainnet registration path (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289 @ ens_v1@91c966f): single-tx `RegistrationGranted` + `NameWrapped` + registry owner→wrapper with no pre-wrap registrar-holder anchor — pins whether the wrap-row projection disagreement is wrap-window ordering only (a born-wrapped control section should be wrapper-consistent from birth) | planned(8) |
| Renew a wrapped 2LD via the controller | controller renew touches only the registrar (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L366 @ ens_v1@91c966f) and the wrapper's stored expiry syncs only through an event-less path (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L337 @ ens_v1@91c966f), so `RegistrationRenewed` derives with no wrapper `ExpiryChanged` possible — pin whether projected exact-name expiry tracks the renewal or serves the stale wrapper authority | planned(8) |
| Wrapped ERC1155 transfer (single and batch) | ownership motion with zero registry and zero wrapper lifecycle events: registrant follows the holder while resource and lineage stay put; `TransferBatch` fan-out is admitted but never decoded anywhere; probes the stale-control review point under holder rotation | planned(8) |
| Parent burns PARENT_CANNOT_CONTROL on an existing child; extends child expiry | emancipation as a transition on a live child via setChildFuses (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L517 @ ens_v1@91c966f) and extendExpiry emitting `ExpiryExtended` (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L475 @ ens_v1@91c966f) — the covered emancipation row only creates children already emancipated; child `ExpiryChanged` moves projected expiry off wrap-time math | planned(8) |
| Wrap an existing registry subname via wrap() | the child rotates to wrapper authority through registry `Transfer` (setOwner (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L372 @ ens_v1@91c966f)) rather than `NewOwner`, under a parent that stays registry-anchored — every covered wrapped child is born under an already-wrapped parent | planned(8) |
| NameWrapper upgrade path | — | blocked(the wrapper manifest deliberately excludes upgrade history, but admission leaks: upgrade() burns the wrapped token (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L500 @ ens_v1@91c966f) emitting an admitted `TransferSingle` to zero with no `NameUnwrapped` (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L278 @ ens_v1@91c966f); what that bare burn means needs an admission decision before an honest row) |
| Delegated-authority state (registry/resolver/wrapper approvals) | — | blocked(no approval event is admitted anywhere — e.g. registry `ApprovalForAll` (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L117 @ ens_v1@91c966f); delegated-authority state is not indexed, only its resulting writes are — the operator-mediated-write row covers the consequence side) |

### ENSv1 — resolvers and records

| Transition | Key assertions | Status |
| --- | --- | --- |
| Set resolver at registration | declared resolver populated with ResolverChanged provenance | covered(registry_driven_reads) |
| Write addr(60) and text records | record inventory carries the written selectors at the current boundary | covered(registry_driven_reads) |
| Change resolver later / set to zero | exact-name and records-route resolver state follow public resolver → second PublicResolver copy → zero-address null shape | covered(resolver_changes_follow_registry_and_zero_releases) |
| Multicoin addr and contenthash records; cached values on the records route | inventory carries `addr:0` and `contenthash`; compact records route returns raw multicoin bytes and contenthash bytes at the current boundary | covered(records_route_values_and_version_boundaries_follow_current_resolver) |
| Resolver replaced by another resolver | after moving to another PublicResolver copy, current records route no longer returns resolver-A `addr:0` or `contenthash` successes; the record-version boundary moves positionally (the wire boundary object carries chain position only — its event-identity fields are null) | covered(records_route_values_and_version_boundaries_follow_current_resolver) |
| Record version bump (clear records) | `clearRecords` moves the record-version boundary to a later position and the prior cached text value no longer returns success | covered(records_route_values_and_version_boundaries_follow_current_resolver) |
| Unadmitted custom resolver emits records | writes on an unadmitted-generation resolver are invisible to declared reads end to end: no `RecordChanged` derives, inventory publishes no selectors and reports explicit `not_observed_on_current_resolver` gaps per family, the requested text returns `not_found` with no value, and known-key enumeration stays supported-but-empty (a record-free unadmitted binding instead reports families as `resolver_family_pending` — asymmetric shapes, both pinned); watch item: one saturated pre-stabilization full run derived exactly one `RecordChanged` here (2026-07-10), not reproduced since across a bounded-parallelism full run and a 5-way stress — any recurrence is a derivation-determinism finding; capture with `BIGNAME_E2E_KEEP_DB=1` and inspect which sync path inserted the row | covered(unadmitted_custom_resolver_observes_facts_but_keeps_profile_gated) |
| One shared resolver serving many names | per-name text/addr reads stay node-scoped while resolver overview keeps `nodes` fan-in unsupported with `resolver_binding_enumeration_not_projected` | covered(shared_resolver_keeps_per_name_records_and_overview_fan_in_unsupported) |
| Write and delete records across the remaining admitted families (ABI, interface, DNS RRset, zonehash, forward name()) | all five families derive fully keyed `RecordChanged` at the normalized layer (`abi:<contentType>`, `interface:<id>`, `dns:<type>:<dns-name>`, `dns:zonehash`, `name`); `DNSRecordDeleted` (upstream: .refs/ens_v1/contracts/resolvers/profiles/DNSResolver.sol:L186 @ ens_v1@91c966f) derives as supersession-by-delete (`{deleted: true}`) on the same key; forward name() stays a record with a null selector key; the projection enumerates selectors for addr/text/contenthash only — the keyed families enter neither selectors nor gaps nor unsupported_families, pinned as family-level honesty | covered(remaining_record_families_derive_normalized_but_stay_unenumerated) |
| Operator/delegate-mediated record and subname writes | operators authorised via registry setApprovalForAll (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L112 @ ens_v1@91c966f) and resolver delegation emit events identical to owner-authored writes — pins that derivation never assumes owner-authored provenance and that the raw fact carries the true sender | covered(operator_delegate_writes_match_owner_authorship) |
| setPubkey on the admitted PublicResolver | the pinned resolver composes PubkeyResolver (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L29 @ ens_v1@91c966f) and setPubkey emits `PubkeyChanged` (upstream: .refs/ens_v1/contracts/resolvers/profiles/PubkeyResolver.sol:L25 @ ens_v1@91c966f) — the only composed-profile event absent from the resolver ABI; observed shape is sharper than planned: the live scan is topic-filtered by the manifest ABI, so the write is invisible at the raw layer too (zero raw logs, nothing derives, no pubkey family anywhere in the inventory), and the adapter's gate rejects the family by tested design; drift-vs-narrowing stays a doc-first question — no divergence entry names pubkey | covered(pubkey_write_on_admitted_resolver_stays_invisible) |

### ENSv1 — reverse and primary names

| Transition | Key assertions | Status |
| --- | --- | --- |
| Reverse claim set | claimed primary name appears as candidate only | covered(reverse_claim_set_changed_then_cleared_tracks_declared_candidate) |
| Claim whose forward resolution mismatches | declared candidate and mismatching forward `addr:60` are both present under an execution-enabled API; **REVIEW POINT**: tuple-present primary claims never enter the on-demand verifier, so verified mode returns `not_found` and persists no `verified_primary_name` trace/outcome rather than reporting the observable mismatch — this is an execution-entrypoint gap, not the verified-resolution cache-key mismatch | covered(forward_mismatch_keeps_declared_candidate_but_verified_not_found) |
| Claim changed, then cleared | candidate follows, then empties | covered(reverse_claim_set_changed_then_cleared_tracks_declared_candidate) |
| Claim string that fails normalization | surfaces invalid_name, never silently dropped | covered(reverse_claim_invalid_name_surfaces_raw_claim) |
| Claim without a name record (claim/claimWithResolver) | the reverse node's registry owner rotates via setSubnodeRecord with no `NameChanged` (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L84 @ ens_v1@91c966f) — no candidate minted; reverse-node `NewOwner` never mistaken for a claim | covered(claim_without_name_record_keeps_candidate_absent) |
| Claim set for another address by an authorised third party | claimForAddr/setNameForAddr (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L123 @ ens_v1@91c966f) — candidate derives from the node labelhash, not the tx sender | covered(authorised_third_party_claim_keys_candidate_to_claimed_address) |
| Claim routed through an unadmitted resolver | a live claim whose `NameChanged` never derives: declared candidate honestly absent — the reverse-plane analog of the unadmitted-resolver row | covered(unadmitted_reverse_resolver_keeps_candidate_absent) |
| Mainnet default.reverse claim (DefaultReverseRegistrar) | — | blocked(the current controller writes it on the DEFAULT reverse bit (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L326 @ ens_v1@91c966f), emitting `NameForAddrChanged` (upstream: .refs/ens_v1/contracts/reverseRegistrar/StandaloneReverseRegistrar.sol:L30 @ ens_v1@91c966f), but no mainnet reverse family admits the role or event — claims the pipeline can never see; admission decision first) |

### ENSv1 — registry migration (legacy → current registry)

The skeleton already deploys both registries with the real fallback wiring.

| Transition | Key assertions | Status |
| --- | --- | --- |
| Name existing only on the legacy registry | legacy-only 2LD derives canonical `SubregistryChanged` with old-registry emitter/current-registry authority provenance; no exact-name surface is minted and `eth` has no routeable children surface; a legacy-only child under a registered parent appears as a bracketed placeholder | covered(registry_migration_legacy_to_current_admission) |
| Migrate (first write on current registry) | asserted across two ingests of one chain because subregistry observations are one-per-node current-edge state (the legacy observation is superseded, not retained): pre-migration the legacy-emitted owner state is admitted; post-migration the current-registry controller registration supersedes it, later old-registry resolver and owner writes emit no normalized resolver/owner changes, and current-registry resolver and registry owner stay visible in normalized events and exact-name reads | covered(registry_migration_legacy_to_current_admission) |
| Legacy write to an unmigrated node post-cutover | a different legacy child written after another node migrates is still admitted with migration-epoch provenance and appears as a bracketed child under its registered parent | covered(registry_migration_legacy_to_current_admission) |

### ENSv2 (sepolia-dev profile)

Deployment module from `.refs/ens_v2/contracts/deployments/sepolia-dev/`
artifacts; scenarios mirror the admitted four families only.

| Transition | Key assertions | Status |
| --- | --- | --- |
| Register through the v2 registrar | registrar intent linked to registry resource; identity, registration (authority_kind `ens_v2_registry`), control, and history all serve under `ethereum-sepolia`; **REVIEW POINT**: exact-name coverage stays `unsupported`/`ensv2_exact_name_profile_shadow` for freshly registered names, contradicting docs/api-v1-routes.md's promised full coverage — the coverage gate needs a supported-flagged registrar event but the projection loader's RELEVANT_EVENT_KINDS excludes RegistrarNameRegistered (renewed names would pass); pinned, chipped | covered(ens_v2_sepolia_dev_declared_matrix_end_to_end) |
| Token regenerated (role change burns/mints token) | resource identity and surface binding stable, current token id updates | covered(ens_v2_sepolia_dev_declared_matrix_end_to_end) |
| Role bitmap grant/revoke, root vs resource scope | effective powers per registry vocabulary; revoke removes current subject row | covered(ens_v2_sepolia_dev_declared_matrix_end_to_end) |
| Subregistry attached, then swapped | SubregistryChanged derives for attach and swap at the parent registry, the parent resource survives the swap, and the old child stays out of the parent's children; **REVIEW POINT**: a discovered child registry's own logs are never scanned within the discovering session (discovery admits the edge, but zero raw logs are fetched from the discovered address in-session), so registrations INSIDE discovered subregistries derive nothing live — they need a later backfill/ops-catchup; child-name reads under discovered registries are pinned absent | covered(ens_v2_sepolia_dev_declared_matrix_end_to_end) |
| Alias-derived surface with no direct registry entry | alias path visible in topology, surface exists | blocked(the admitted resolver family decodes `AliasChanged`, but the sepolia profile does not admit a contract-backed path that creates a `resolver_alias_path` surface binding; emitting alias topology alone would not mint an exact-name surface) |
| Shared subregistry → multiple surfaces, one resource | grouping by resource works, identities distinct | blocked(current discovery maps one registry address to one active suffix, so the same subregistry attached under multiple parents is superseded rather than represented as simultaneous surfaces; needs a doc/admission decision before an honest e2e row can assert it) |
| Unregister → re-register | ON-CHAIN identity contract covered: resource and token lineage both advance across the cycle (pinned registry increments both); **the ingestion half is blocked by a wedge bug (chipped)**: with these events in an ingested chain, the run loop's full-closure catch-up fails permanently on a stable-identity anchor conflict refreshing the re-registered surface, aborting every poll iteration and halting checkpoint advancement — the scenario exercises the flow post-ingest only | covered(ens_v2_sepolia_dev_declared_matrix_end_to_end) |
| ENSv1→ENSv2 migration flow | — | blocked(migration controllers outside admission; doc-first change required) |
| Renew via the v2 registrar; direct registry renew | registrar renew forwards to the registry (upstream: .refs/ens_v2/contracts/src/registrar/ETHRegistrar.sol:L196 @ ens_v2@554c309) deriving `RegistrationRenewed` + `ExpiryChanged` from one action; direct renew emits `ExpiryUpdated` only and rejects reduction (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L249 @ ens_v2@554c309); the renewed name's coverage promotes to supported — the sole promotion path in the shipped profile and the closing move for the register-row review point | planned(8) |
| Registry token transferred (ERC1155 sale) | transfer migrates all roles as a paired `EACRolesChanged` revoke/grant with callbacks suppressed, so no `TokenRegenerated` (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L403 @ ens_v2@554c309) (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L220 @ ens_v2@554c309) — the inverse coupling of the regeneration row; pin whether declared registrant follows the buyer, goes stale, or reports explicitly; `TransferSingle` itself stays out-of-admission | planned(8) |
| Resolver set, changed, and zeroed at the watched registry | `ResolverChanged` has never been derived on this profile — registration emits `ResolverUpdated` only for a nonzero resolver (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L225 @ ens_v2@554c309) and every scenario registration passes zero; declared resolver edge follows set/change/zero; prerequisite for the resolver-family row | planned(8) |
| Record write, clearRecords, and name-scoped permission grant on a discovered v2 resolver | the admitted resolver family has zero exercised transitions: `RecordChanged`, `RecordVersionChanged` via clearRecords (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L223 @ ens_v2@554c309), and the manifest-declared one-event→two-kinds fan-out derive — or the discovered-address scan gap is pinned for resolvers exactly as for registries | planned(8) |
| Expiry passes with no re-registration; re-register after expiry | v2's release-equivalent is event-silent — names flip available purely by clock (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L531 @ ens_v2@554c309): reads pinned to flip or last-known-state; re-registering an expired name burns and bumps both version counters inside register with no `LabelUnregistered` and no `TokenRegenerated` (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L204 @ ens_v2@554c309) — lineage advances on two `LabelRegistered`s alone, distinct from the covered unregister cycle | planned(8) |
| TLD attach at the RootRegistry apex; root-scope grant and revoke | the root family has zero exercised transitions beyond deploy-time grants: register `eth` in the RootRegistry, attach ETHRegistry as its subregistry, rotate a root role in both directions — `RootPermissionChanged` produced and asserted, revoke removes the subject row, `ParentChanged` derives from a watched registry | planned(8) |
| Label reserved, then registered from reserved | owner=0 reserves token-less, deriving `RegistrationReserved` (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L214 @ ens_v2@554c309); promotion requires ROLE_REGISTER_RESERVED (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L194 @ ens_v2@554c309) and preserves the reservation expiry when expiry=0 (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L197 @ ens_v2@554c309) | planned(8) |
| Registration by a non-admitted registrar | register requires only root ROLE_REGISTRAR (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L182 @ ens_v2@554c309), and sepolia-dev ships further ETHRegistrar builds (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/FastETHRegistrar.json:L1146 @ ens_v2@554c309): `RegistrationGranted` derives with registry-only provenance, no registrar intent, and permanently gated coverage — the adversarial complement of the registrar-intent row | planned(8) |
| Subregistry detached (set to zero); admin-half role rendering | setSubregistry stores the new value without a zero check (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L137 @ ens_v2@554c309) — edge removal as distinct from the covered swap; admin-half bits ride every registration bitmap and must render distinctly in the permissions vocabulary, while post-registration admin grants are upstream-impossible for registered tokens (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L480 @ ens_v2@554c309) | planned(8) |
| ENSv2 reverse/primary plane | — | blocked(sepolia-dev deploys a full reverse stack — ReverseRegistry is itself a PermissionedRegistry (upstream: .refs/ens_v2/contracts/deployments/sepolia-dev/ReverseRegistry.json:L1586 @ ens_v2@554c309) — but no sepolia family admits any of it, and registry-ABI-shaped events at an unadmitted reverse address are a discovery/admission trap; the only matrix with no reverse row — doc-first admission decision) |
| ERC1155 ownership/approval surface | — | blocked(`TransferSingle`/`TransferBatch`/`ApprovalForAll` are in no manifest ABI; the role-migration half is the planned token-sale row — the ownership-motion half needs an explicit admitted-or-unsupported statement) |
| UUPS implementation rotation on discovered registries/resolvers | — | blocked(UserRegistry is UUPS-upgradeable (upstream: .refs/ens_v2/contracts/src/registry/UserRegistry.sol:L20 @ ens_v2@554c309) and ROLE_UPGRADE exists, but no `Upgraded` event is admitted — a discovered authority can silently change behavior; admission decision) |

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
| Register a *.base.eth name | Base-side authority split (registry/registrar/resolver families) | covered(basenames_declared_state_matrix_end_to_end) |
| NFT-only transfer vs management-only transfer vs full transfer | registrar token and registry-owner control facets move independently, then converge on reclaim | covered(basenames_declared_state_matrix_end_to_end) |
| Address-resolution change on the L2 resolver | declared `addr:60` record updates through the admitted Base L2Resolver | covered(basenames_declared_state_matrix_end_to_end) |
| Primary name set/unset (Base reverse registrar event) | declared candidate tracks `NameForAddrChanged` at Base coin type 2147492101 and clears on blank | covered(basenames_declared_state_matrix_end_to_end) |
| L1 compatibility resolution | transport path, verified only through the execution plane | blocked(needs the Basenames L1Resolver transport with a CCIP gateway pair the pins cannot deploy standalone — same gateway/verifier limitation as the CCIP execution row) |
| Renew; expire → grace → premium re-registration (different owner) | Base's 3-arg `NameRenewed` fragment (upstream: .refs/basenames/src/L2/RegistrarController.sol:L497 @ basenames@1809bbc) is decoded nowhere in e2e (mainnet exercises the 4/5-arg fragments); availability returns only after expiry plus the grace window (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L294 @ basenames@1809bbc); re-registration burns then re-mints in one tx (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L443 @ basenames@1809bbc) — release synthesis, token-lineage rotation, and both leases' resources on `base-mainnet` | planned(8) |
| Register and renew via the UpgradeableRegistrarController (erc1967 proxy) | the production-current Base registration path (upstream: .refs/basenames/README.md:L37 @ basenames@1809bbc) and the only erc1967 proxy role in the manifest tree, currently mirrored to a placeholder address with no code — proxy-role admission is validated nowhere; admitted fragments decode from the proxy emitter (upstream: .refs/basenames/src/L2/UpgradeableRegistrarController.sol:L515 @ basenames@1809bbc); proxy vs implementation instance identity holds | planned(8) |
| Subname created under a registered *.base.eth name | the Base registry manifest flags `declared_children = supported`, asserted nowhere on Base — child listing, bracketed placeholders, and zero-owner removal per the mainnet rows, under Base namespace/suffix rules | planned(8) |
| Text, multicoin, and name records plus clearRecords on the L2Resolver | four of the five admitted resolver events are never emitted in any e2e chain; inventory carries the selectors at the boundary and the `VersionChanged` bump invalidates prior cached values on `base-mainnet` | planned(8) |
| Resolver rotated to a second (unadmitted-profile) instance, then to zero | binding follows registry `NewResolver` while record consumption stays profile-gated with explicit gaps — the documented divergence's Base shape exists in live data via the UpgradeableL2Resolver proxy (upstream: .refs/basenames/README.md:L39 @ basenames@1809bbc); post-registration resolver motion is otherwise never driven on Base | planned(8) |
| Legacy ReverseRegistrar claim/setName (registry-driven reverse hierarchy) | claims write the registry under `80002105.reverse` via setSubnodeRecord (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L158 @ basenames@1809bbc) with an in-admission `NameChanged` at the reverse node — validates the documented narrowing that this path is not primary-name value authority (candidate untouched, record-only) plus placeholder handling for hex-address labels; deploy-time constructor claims already sit unasserted in every ingested Base chain | planned(8) |
| Registration through an owner-added third-party controller (incl. registerOnly) | registrar-level events are out-of-admission, so a third-party controller registration degrades to admitted facts only — ERC721 `Transfer` plus registry `NewOwner`, no `RegistrationGranted`; registerOnly mints a token with no registry node at all (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L248 @ basenames@1809bbc); the controller set has already rotated once upstream, so this is the admission-drift shape a controller v3 would produce | planned(8) |
| Contenthash (and other unadmitted-family) writes on the admitted L2Resolver | the pinned resolver composes the profile (upstream: .refs/basenames/src/L2/resolver/ContentHashResolver.sol:L32 @ basenames@1809bbc) but the Base family admits five events where mainnet admits contenthash/ABI/DNS/interface — the write derives nothing and inventory reports an explicit family gap; the record-family narrowing needs a docs/upstream.md § Known divergences entry (doc-first) | planned(8) |
| setTTL / setRecord → NewTTL decode | the family's only wire-driven `AuthorityEpochChanged` — every current Base assertion of that kind is a pipeline-epoch terminator, not a `NewTTL`; registrations pass ttl=0 so the emit is suppressed (upstream: .refs/basenames/src/L2/Registry.sol:L229 @ basenames@1809bbc) | planned(8) |

### Verified resolution and offchain (execution plane)

| Scenario | Key assertions | Status |
| --- | --- | --- |
| Direct-path verified query via locally deployed UniversalResolver | verified section agrees with declared; execution trace, steps, and cache outcome persist and are asserted at layer 3; **REVIEW POINTS**: (1) execution calls a hardcoded Universal Resolver address, ignoring the manifest-declared ens_execution role (the harness installs the local UR runtime at that address via anvil_setCode); (2) the persisted on-demand outcome is not explain-readable — persist-side and explain-side cache keys derive from different inputs, so explain-after-verify 404s even with head, row position, and record set aligned (pinned as 404; conformance never exercises this composition because it seeds outcomes to match the read side) | covered(verified_resolution::direct_path_verified_query_via_local_universal_resolver_persists_trace) |
| Wildcard-derived answer | wildcard topology populated, supported class honored | blocked(no e2e-deployed pinned contract path currently produces the `observed_wildcard_path` topology required by the public support class; API tests seed that topology, and faking it here would bypass the contract-first harness) |
| Alias-path answer | alias hops recorded | blocked(no ENSv1 pinned deployment helper or local manifest admission path currently produces `resolver_alias_path`; alias coverage is seeded below e2e and needs a contract-backed source before this row can flip) |
| CCIP-Read success / failure / proof mismatch (local mock gateway) | statuses distinguish; failures never fabricate values | blocked(local mock gateway coverage needs a deployable offchain resolver/gateway pair from pins; the simple ENSv1 mock resolver is test source without a harness deployment artifact (upstream: .refs/ens_v1/contracts/test/mocks/MockOffchainResolver.sol:L15 @ ens_v1@91c966f), while shipped offchain resolver artifacts require DNSSEC/gateway-verifier topology not deployed by the ENSv1 e2e stack) |
| Cache invalidation on record change, topology change, reorg | stale verified answers do not survive | blocked(record-change refresh requires a supported same-DB live ingest plus projection replay to move the selected snapshot and record boundary after the on-chain mutation; worker invalidation commands can delete exact stale boundaries but do not refresh the snapshot or re-execute against the post-change block by themselves) |

### Cross-protocol composition (mainnet profile)

The shipped `mainnet` profile is one corpus spanning ENSv1 on `ethereum`,
Basenames on `base-mainnet`, and two glue families on the ethereum chain
(`basenames_l1_compat` v1 and `basenames_execution` v2, both active).
Every scenario above runs a single-protocol, single-chain corpus, so the
composed shape production actually ships has no e2e validation, and the
glue families fell outside both per-protocol audits by construction.
Non-goal: ENSv1↔ENSv2 same-corpus composition — the sepolia profile is
ENSv2-only, so that crossing is fenced by profile separation on top of the
blocked migration row; revisit only if a profile ever admits both.

| Scenario | Key assertions | Status |
| --- | --- | --- |
| Composed-corpus equivalence | one corpus, both anvils, full mainnet-profile mirror (ENSv1 ethereum + Basenames base-mainnet + the ethereum-chain glue families): each protocol's route snapshots equal its single-protocol baseline, and per-chain checkpoints advance independently | planned(9) |
| base.eth namespace handoff | L1 `base.eth` declared state (ENSv1 registry) coexists with Base-authoritative `*.base.eth` children; the exact-name/children boundary sits where the manifests say, with no cross-chain leakage in either direction | planned(9) |
| One address, two protocols | names registered on both chains by one address in one corpus: address-scoped collections union with chain-scoped surfaces and no identity bleed — the ADR-0002 scoping claim, currently only ever tested one chain at a time | planned(9) |
| Primary-name coexistence | the same address holds a mainnet `addr.reverse` claim and a Base `80002105.reverse` claim in one corpus; each chain's declared candidate stays correct | planned(9) |
| Cross-chain perturbations | reorg one chain of a two-chain corpus — the other chain's canonicality untouched; backfill parity holds per chain | planned(9) |
| L1-compat declared side | the active `basenames_l1_compat` family's watch/admission exercised (what the corpus derives at the L1Resolver address) while the CCIP transport hop stays blocked | planned(9) |

## Perturbation multipliers (phase 3, cross-cutting)

These wrap *existing* scenarios rather than adding new ones — each scenario
gains hostile variants once, via the harness:

| Perturbation | Mechanism | Convergence requirement | Status |
| --- | --- | --- | --- |
| Reorg at each checkpoint | anvil snapshot/revert via `harness::rpc::{evm_snapshot, evm_revert}`, mine a divergent longer branch under one live `pipeline::IndexerRunSession` | winning-branch route snapshots equal a fresh winning-chain control; losing-branch `raw_logs` and `normalized_events` remain present with orphaned canonicality by losing block hash | covered(`perturbations::rich_chain_live_reorg_converges_to_winning_branch`, `harness::pipeline::IndexerRunSession`, `harness::perturb::route_snapshots`) |
| Indexer killed and relaunched mid-scenario | `pipeline::indexer_run_restart_after_first_checkpoint` kills the first `indexer run` after the first canonical checkpoint row, then restarts to scenario readiness | final route snapshots equal an unperturbed live ingest of the same finished chain | covered(`perturbations::rich_chain_indexer_restart_mid_scenario_matches_control`, `harness::pipeline::indexer_run_restart_after_first_checkpoint`, `harness::perturb::assert_snapshots_equal`) |
| Backfill-from-zero after the fact | the harness's `backfill` runner over the finished chain, block `0..head` | scenario-scoped (per touched surface) normalized-event digests match exactly; after normalizing per-corpus contract-instance ids, live ⊆ backfill exactly, with backfill-only extras bounded to bookkeeping/late-round kinds (`SourceManifestUpdated`/`CapabilityChanged`/`PreimageObserved`); no API-route parity claim because backfill does not promote snapshot checkpoints | covered(`perturbations::rich_chain_backfill_normalized_events_match_live_ingest`, `harness::perturb::assert_backfill_normalized_event_parity`) |
| Projection replay | snapshot fixed route set, run `replay all-current-projections`, then run full-range `replay normalized-events` plus projection replay and re-snapshot | route snapshots remain byte-equal after projection replay and after normalized-event replay plus projection replay | covered(`perturbations::rich_chain_projection_and_normalized_event_replay_are_route_stable`, `harness::perturb::route_snapshots`) |

Wall-clock cost is the constraint: perturbed variants belong to the nightly
tier, not the PR gate.

Runtime verification of these four surfaced three wire/derivation facts now
encoded in the harness: `last_updated` on empty collections is read-time
wall clock (the only run-varying route field found — normalized in
snapshots); contract-instance ids are minted per corpus, so cross-database
event comparison must strip exactly those fields; and control runs must
ingest to the identical head (`ingest_at_current_head`) because route
bodies embed `chain_positions`.

## Harness roadmap

| Capability | Needed by | Notes |
| --- | --- | --- |
| Checkpoint abstraction (named on-chain step + per-checkpoint route snapshots) | 2 | snapshots as checked-in JSON; diff-reviewable; becomes the documented state machine |
| Route snapshot walker (canonical route set per name/address under test) | 2 | normalize away run-varying fields (timestamps, UUIDs) explicitly, never blindly |
| Perturbation runner wrapping any scenario | 3 | one implementation, N scenarios × M variants |
| Second anvil instance + `base-mainnet` manifest generation | 5 | covered: `Anvil::spawn_base_mainnet`, multi-provider pipeline runners, and Base Basenames manifest mirroring |
| Basenames artifact source | 5 | covered: forge-builds the pinned sources on demand (offline; libs vendored in the pin) — the committed broadcast bytecode predates the pinned tree and was rejected as uncitable |
| ENSv2 sepolia-dev deployment module + profile generation | 7 | covered: `harness::ens_v2`, `manifests-sepolia` mirror, and `ethereum-sepolia` checkpoint target |
| Mock CCIP gateway (local HTTP server) | 6 | request/response digests must land in execution traces |
| Execution RPC wiring (`--chain-rpc-url` on API/worker pointed at anvil) | 6 | UniversalResolver artifact bytecode is pinned for both v1 and v2 |
| Merged mainnet-profile mirror + dual-anvil pipeline run | 9 | union of the ENSv1 and Basenames target sets including the ethereum-chain glue families; the pipeline runner already accepts a `chain_rpc_urls` list |

## Phasing

Order optimizes for information per unit of work: the determinism
multipliers (3) come before breadth because they multiply every scenario
that exists by then.

All seven phases executed (2026-07-09, commits `f13fe50`..`1bdf886`; 24
scenarios green). A 2026-07-10 adversarial completeness audit (three
independent reviewers, one per protocol, enumerating from the pinned
contracts toward the matrix) found the matrices helper-shaped rather than
contract-shaped: upstream transitions outside the harness helpers were
missing as a class. Its missing transitions enter the matrices as
`planned(8)` rows and its admission findings as new blocked rows; phase 8
tracks closing them. Open rows are now the `planned(8)` set, the blocked
rows carrying their pin-level reasons, and the review points recorded per
matrix.

| Phase | Scope | Status |
| --- | --- | --- |
| 1 | Walking skeleton | done — registration scenario green in CI |
| 2 | ENSv1 lifecycle + subnames + registry migration | done — matrices covered except the deferred label-preimage-reveal row; route snapshots live in the perturbation harness rather than checked-in files |
| 3 | Perturbation runner: reorg, restart, backfill parity, replay equality | done — all four multipliers green over the rich chain; nightly tiering remains a CI follow-up |
| 4 | Wrapper + reverse/primary | done — covered with two wrapper review points |
| 5 | Basenames dual-chain | done — declared-state matrix covered; L1-compat blocked with the CCIP-gateway reason |
| 6 | Execution plane | done — direct path covered with layer-3 trace assertions and two review points; wildcard/alias/CCIP/cache-invalidation blocked with pin-level reasons |
| 7 | ENSv2 declared-state matrix | done — covered except alias/shared-subregistry/migration blocked rows, with three review points (coverage promotion, discovered-registry scan, re-registration intake wedge) |
| 8 | Audit-driven matrix extensions: the missing transitions and admission blocked rows from the 2026-07-10 completeness audit | open — highest-risk first moves: ENSv2 renewal (closes the coverage-promotion review point), born-wrapped registration, and the Basenames UpgradeableRegistrarController path |
| 9 | Cross-protocol composition (mainnet profile): merged-profile mirror, dual-anvil corpus, composition rows | open — gated on the merged mainnet-profile harness capability |

## CI tiers

- **PR gate (`test (e2e)` job, required)** — the fast subset: walking
  skeleton plus at most a handful of single-lifecycle scenarios; target
  under ~10 minutes wall on a warm cache.
- **Nightly (scheduled workflow, phase 3+)** — full matrix including all
  perturbation variants; failures open an issue rather than blocking merges.
- Slow scenarios are opted into the nightly tier explicitly (env-gated),
  never silently skipped: a scenario that doesn't run must show up as
  `not run`, not as green.

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
