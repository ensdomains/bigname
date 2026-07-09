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

### ENSv1 — subnames

| Transition | Key assertions | Status |
| --- | --- | --- |
| Parent creates registry-only subname | child listed under parent with correct owner | covered(registry_driven_reads) |
| Subname created with unrevealed label (labelhash only — the registry never carries label strings for subnames) | bracketed placeholder child row; no exact-name surface minted (404) | covered(registry_driven_reads) |
| Same label under two different parents | same labelhash under `alice.eth` and `bob.eth` produces distinct child namehashes and owners, with no cross-parent leakage in either `/children` route | covered(same_label_under_two_parents_keeps_children_distinct) |
| Deep hierarchy (three+ levels) | registry facts derive at any depth (canonical SubregistryChanged for the grandchild under the placeholder's node), but enumeration stops at unknown surfaces: bracketed placeholder names are rejected as `invalid_input` at the ENSIP-15 boundary, and children under an unrevealed-label parent project no `children_current` row | covered(deep_registry_hierarchy_lists_direct_children_only) |
| Subname owner set to zero | zero-owner tombstone removes the child from the parent's default `/children` listing | covered(zero_owner_subname_leaves_default_children_listing) |
| Label preimage revealed later | placeholder upgrades to the real name | planned(2) |

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

### ENSv1 — resolvers and records

| Transition | Key assertions | Status |
| --- | --- | --- |
| Set resolver at registration | declared resolver populated with ResolverChanged provenance | covered(registry_driven_reads) |
| Write addr(60) and text records | record inventory carries the written selectors at the current boundary | covered(registry_driven_reads) |
| Change resolver later / set to zero | exact-name and records-route resolver state follow public resolver → second PublicResolver copy → zero-address null shape | covered(resolver_changes_follow_registry_and_zero_releases) |
| Multicoin addr and contenthash records; cached values on the records route | inventory carries `addr:0` and `contenthash`; compact records route returns raw multicoin bytes and contenthash bytes at the current boundary | covered(records_route_values_and_version_boundaries_follow_current_resolver) |
| Resolver replaced by another resolver | after moving to another PublicResolver copy, current records route no longer returns resolver-A `addr:0` or `contenthash` successes; the record-version boundary moves positionally (the wire boundary object carries chain position only — its event-identity fields are null) | covered(records_route_values_and_version_boundaries_follow_current_resolver) |
| Record version bump (clear records) | `clearRecords` moves the record-version boundary to a later position and the prior cached text value no longer returns success | covered(records_route_values_and_version_boundaries_follow_current_resolver) |
| Unadmitted custom resolver emits records | writes on an unadmitted-generation resolver are invisible to declared reads end to end: no `RecordChanged` derives, inventory publishes no selectors and reports explicit `not_observed_on_current_resolver` gaps per family, the requested text returns `not_found` with no value, and known-key enumeration stays supported-but-empty (a record-free unadmitted binding instead reports families as `resolver_family_pending` — asymmetric shapes, both pinned) | covered(unadmitted_custom_resolver_observes_facts_but_keeps_profile_gated) |
| One shared resolver serving many names | per-name text/addr reads stay node-scoped while resolver overview keeps `nodes` fan-in unsupported with `resolver_binding_enumeration_not_projected` | covered(shared_resolver_keeps_per_name_records_and_overview_fan_in_unsupported) |

### ENSv1 — reverse and primary names

| Transition | Key assertions | Status |
| --- | --- | --- |
| Reverse claim set | claimed primary name appears as candidate only | covered(reverse_claim_set_changed_then_cleared_tracks_declared_candidate) |
| Claim whose forward resolution mismatches | claimed present, verified reports mismatch (needs execution plane) | planned(6) |
| Claim changed, then cleared | candidate follows, then empties | covered(reverse_claim_set_changed_then_cleared_tracks_declared_candidate) |
| Claim string that fails normalization | surfaces invalid_name, never silently dropped | covered(reverse_claim_invalid_name_surfaces_raw_claim) |

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
| Register through the v2 registrar | registrar intent linked to registry resource | planned(7) |
| Token regenerated (role change burns/mints token) | resource identity and surface binding stable, current token id updates | planned(7) |
| Role bitmap grant/revoke, root vs resource scope | effective powers per vocabulary, unknown bits omitted | planned(7) |
| Subregistry attached, then swapped | subtree surfaces rebind; old subtree partitioned | planned(7) |
| Alias-derived surface with no direct registry entry | alias path visible in topology, surface exists | planned(7) |
| Shared subregistry → multiple surfaces, one resource | grouping by resource works, identities distinct | planned(7) |
| Unregister → re-register | resource version increments, prior history preserved | planned(7) |
| ENSv1→ENSv2 migration flow | — | blocked(migration controllers outside admission; doc-first change required) |

### Basenames (second chain instance)

Deployment module from `.refs/basenames/broadcast/` transactions; runs on a
second anvil presented as `base-mainnet`.

| Transition | Key assertions | Status |
| --- | --- | --- |
| Register a *.base.eth name | Base-side authority split (registry/registrar/resolver families) | planned(5) |
| NFT-only transfer vs management-only transfer vs full transfer | the three control facets move independently | planned(5) |
| Address-resolution change on the L2 resolver | declared record updates | planned(5) |
| Primary name set/unset (Base reverse registrar event) | claimed primary at the Base coin type | planned(5) |
| L1 compatibility resolution | transport path, verified only through the execution plane | planned(6) |

### Verified resolution and offchain (execution plane)

| Scenario | Key assertions | Status |
| --- | --- | --- |
| Direct-path verified query via locally deployed UniversalResolver | verified section agrees with declared; execution trace persisted (layer 3) | planned(6) |
| Wildcard-derived answer | wildcard topology populated, supported class honored | planned(6) |
| Alias-path answer | alias hops recorded | planned(6) |
| CCIP-Read success / failure / proof mismatch (local mock gateway) | statuses distinguish; failures never fabricate values | planned(6) |
| Cache invalidation on record change, topology change, reorg | stale verified answers do not survive | planned(6) |

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
| Second anvil instance + `base-mainnet` manifest generation | 5 | dual-chain profile in the generated root |
| Broadcast-log artifact loader (forge `run-latest.json`) | 5 | Basenames deploys |
| ENSv2 sepolia-dev deployment module + profile generation | 7 | artifacts verified present; scenario set gated on the shipped sepolia profile semantics |
| Mock CCIP gateway (local HTTP server) | 6 | request/response digests must land in execution traces |
| Execution RPC wiring (`--chain-rpc-url` on API/worker pointed at anvil) | 6 | UniversalResolver artifact bytecode is pinned for both v1 and v2 |

## Phasing

Order optimizes for information per unit of work: the determinism
multipliers (3) come before breadth because they multiply every scenario
that exists by then.

| Phase | Scope | Entry criteria | Exit criteria |
| --- | --- | --- | --- |
| 1 | Walking skeleton | — | done: registration scenario green in CI |
| 2 | ENSv1 lifecycle + subnames + registry migration; checkpoint/snapshot abstraction | resolver finding triaged (its outcome shapes assertions) | lifecycle and subname matrices fully `covered`; snapshots checked in |
| 3 | Perturbation runner: reorg, restart, backfill parity, replay equality | ≥ 5 scenarios exist | every scenario runs perturbed nightly; PR tier unchanged |
| 4 | Wrapper + reverse/primary | 2 | wrapper and reverse matrices `covered` |
| 5 | Basenames dual-chain | 3 (perturbations apply from day one) | Basenames declared-state matrix `covered` |
| 6 | Execution plane: verified resolution + CCIP + layer-3 assertions | 5 | verified matrix `covered`; execution traces asserted |
| 7 | ENSv2 declared-state matrix | sepolia-dev profile semantics confirmed against shipped manifests | ENSv2 matrix `covered` except admission-blocked rows |

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
