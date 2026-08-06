# ENSv1 → ENSv2 migration scenario catalog

Scenario set for bigname's e2e suite, validated against the pinned contracts
(`ens_v2@ccaeb58b`, `ens_v1@91c966f`). Mechanism citations live in
`mechanism.md`; the reachability pruning behind this set is `dimensions.md`.

**Validation** — `validation/<ID>.json` holds, per scenario, the exact
executed transaction list (contract, function, args, sender), captured
on-chain event logs (decoded, with emitting contract), observed reverts, and
programmatic checks. Runs execute on anvil against the pinned devnet stack
(upstream `script/setup.ts` deployment of both the synthetic v1 stack and the
v2 stack, all from the pinned tree), driven by `catalog.e2e.test.ts` (copy
kept in `validation/` as `_driver.ts`). `VALIDATED` = executed end-to-end
with all checks passing. `SPEC-ONLY` = not executed, reason given.

**Harness gap (read first):** bigname's e2e harness
(`tests/e2e/src/harness/ens_v2.rs`) deploys only LabelStore + RootRegistry +
ETHRegistry + ETHRegistrar + oracle + mock tokens. None of the migration
surface (UnlockedMigrationController, LockedMigrationController,
WrapperRegistryImpl, VerifiableFactory, PublicResolverSet, Graveyard,
MigrationHelper, ETHRenewerV1, ENSV1Resolver) is deployed, and no manifest
admits those emitters (`docs/manifests.md` §ENSv2 keeps migration surfaces
outside admission). Implementing this catalog in-repo therefore needs, in
order: (1) doc-first admission of the migration source family(ies), (2)
harness deployment of the migration contracts — pinned deployment artifacts
exist for all of them under
`.refs/ens_v2/contracts/deployments/sepolia/*.json` (bytecode route, same as
the existing v2 contracts; constructor args wired to local addresses), (3)
the scenarios below. Every contract needed was deployable from pinned
artifacts; no forge-build fallback was required (the only forge-built item in
validation was the MigrationHelper *ABI* for the driver, from the pinned
sources).

**Assertion conventions** (bigname semantics, current repo state):

- *identity*: `logical_name_id = ens:<namehash>` must be preserved across
  migration (same namehash, same surface); `resource_id` must rotate at the
  migration boundary (authority moves from v1 registrar/wrapper anchor to the
  ENSv2 EAC resource); `token_lineage_id` mints a v2 lineage and must then
  survive `TokenRegenerated`.
- *registration_status*: post-migration = `registered`
  (`authority_kind = ens_v2_registry` + owner present, per
  `apps/api/src/v2/name_record.rs::classify_registration_status`);
  pre-migration unwrapped = `active` (registrar lease), wrapped = `wrapped`.
- *authority precedence*: once the v2 claim is canonical, v1-side facts about
  the same node are superseded; later v1 events under the name (subnode
  residue, graveyard clears) must be observable as events but must not
  overwrite v2-derived current state. The assertion shape generalizes
  `tests/e2e/src/scenarios/registry_migration.rs` ("a later write to the
  superseded registry must not stand") — with the twist that for the migrated
  node itself, later v1 writes are impossible by construction (P-02..P-04
  prove the reverts), so precedence assertions concentrate on residue,
  graveyard activity, and dual-expiry divergence.
- *event kinds*: v2 registry events map into the existing normalized kinds
  (`RegistrationGranted`, `ExpiryChanged`, `AuthorityTransferred`,
  `TokenControlTransferred`, `ResolverChanged`, `SubregistryChanged`);
  `docs/architecture.md` additionally reserves `MigrationApplied` with no
  producer — see `findings.md` F-07.

Sender/actor names below (`deployer`, `owner`, `user`, `user2`) are the
devnet named accounts; in bigname's harness they map to anvil accounts.
"launch" = the `activateV2()` sequence (mechanism.md §2) executed after v1
setup and premigration, before migration transactions.

Common setup shorthand:

- `REG(label, who[, dur])` = v1 `BaseRegistrar.register(labelhash, who, dur)`
  (controller path), default duration 365d.
- `PREMIG(label)` = v2 `ETHRegistry.register(label, 0x0, 0x0, ENSV1Resolver,
  0, v1expiry+bonus)` as the DAO/deployer (`LabelReserved`).
- `WRAP(label, who, fuses)` = `BaseRegistrar.safeTransferFrom(who,
  NameWrapper, labelhash, abi(label, who, uint16 fuses, 0x0))`.
- `CHILD(parent, label, who, fuses[, expiry])` = `NameWrapper.setSubnodeOwner`.
- `RECORDS(name, who)` = set v1 PublicResolver + addr/text records.
- `MIG721(name, data)` = `BaseRegistrar.safeTransferFrom(owner,
  UnlockedMigrationController, labelhash, abi(Data))`.
- `MIG1155(name, target, data)` = `NameWrapper.safeTransferFrom(owner,
  target, namehash, 1, abi(Data))`.

Expected-event notation lists only migration-relevant logs in emission order;
`validation/<ID>.json` has the complete captured streams with args.

---

## U — E1: unwrapped .eth 2LD via ERC-721 transfer

### U-01 — unwrapped happy path with records — VALIDATED
- Dimensions: unwrapped / records set / no subnames / far expiry / EOA / direct.
- Txs: REG(u01,user); PREMIG(u01); RECORDS(u01.eth,user); launch;
  MIG721(u01.eth, {label:"u01", owner:user, subregistry:0, resolver:v1
  PublicResolver}).
- Expected events (migration tx): v1 `NewOwner(eth, h(u01), controller)`
  [reclaim] → v1 `Transfer(node→Graveyard)`, `NewResolver(node, 0)`,
  `NewTTL(node,0)` [setRecord] → v1 registrar ERC-721
  `Transfer(controller→Graveyard)` → v2 `Label` (LabelStore), v2
  `LabelRegistered(tokenId, h(u01), "u01", user, reservedExpiry, controller)`,
  ERC-1155 `TransferSingle(mint→user)`, `TokenResource`, EAC roles event,
  `ResolverUpdated(tokenId, PublicResolver, controller)`.
- Indexer invariants: same `logical_name_id` before/after; status
  `active` → `registered`; expires_at = v1expiry+bonus (62d+1s later than the
  v1 lease the consumer saw — see findings F-05); resolver surface carries
  the v1 resolver address (records keep resolving; verified via v2
  UniversalResolver in-run); v1 registry owner fact for the node ends as
  Graveyard and must not surface as user-facing ownership.

### U-02 — E1 by approved operator — VALIDATED
- Txs: REG(u02,user); PREMIG; launch; `setApprovalForAll(user2)` by user;
  MIG721 sent by user2, `Data.owner = user`.
- Invariants: v2 owner = user (payload, not transferrer); operator identity
  appears only as tx sender; `LabelRegistered.sender` is the controller —
  provenance of "who migrated" is only the outer tx from-address.

### U-03 — E1 with owner override — VALIDATED
- `Data.owner = user2`. Invariants: v2 owner = user2 while the v1-side
  final owner facts (pre-graveyard) referenced user: registrant continuity
  breaks by design; token_lineage must mint fresh (different holder), while
  logical_name_id is unchanged.

### U-04 — E1 with resolver override — VALIDATED
- `Data.resolver = per-user custom resolver` while v1 had PublicResolver +
  records. Invariants: v2 `ResolverUpdated` = override; indexed records for
  the name must now come from the override resolver (old records are
  v1-resolver-scoped and no longer reachable through the name); v1 resolver
  cleared (`NewResolver(node,0)`).

### U-05 — E1 with custom subregistry — VALIDATED
- `Data.subregistry = arbitrary address`. Expected extra event:
  `SubregistryUpdated(tokenId, subregistry, controller)` in the claim.
- Invariants: discovery must record a v2 `subregistry` edge from .eth
  registry to that address WITHOUT admitting it as a registry until it
  behaves like one (mirrors the "owner must remain a leaf" assertion of
  registry_migration.rs); children surfaces must not be invented from it.

### U-06 — E1 with pre-existing v1 registry subnode + late residue write — VALIDATED
- Txs: REG; v1 `setSubnodeOwner(u06.eth, sub, user2)`; PREMIG; launch;
  MIG721; then user2 `setResolver(sub.u06.eth, X)` on v1 (lands, post-parent-
  migration).
- Invariants (authority precedence, residue class): the parent's current
  state is v2-authoritative; the v1 subnode remains an observable v1 fact
  (SubregistryChanged-kind event with the old registry as emitter) but must
  not create an exact-name surface, and the post-migration v1 write must not
  mutate any v2-derived parent state. The subname surface under a migrated
  parent follows current v1 placeholder-child semantics until/unless the
  child gets a v2 existence.

### U-07 — E1 with v1 primary name set — VALIDATED
- Txs: REG; RECORDS; v1 `ReverseRegistrar.setName("u07.eth")` by user;
  launch; MIG721.
- Invariants: the reverse claim (v1 reverse registrar, `addr.reverse`
  subnode) is untouched by migration; bigname treats reverse records as
  claims + forward verification — post-migration the forward check must
  resolve u07.eth through its v2 resolver surface and still match, keeping
  primary-name continuity. If the migration had changed the resolver (as in
  U-04) with unmigrated records, the claim must degrade to unverified, not
  to a different name.

### U-08 — E1 near expiry (still active) — VALIDATED
- REG with 30d duration; warp to v1expiry−1d; migrate.
- Invariants: v2 expiry = v1expiry + 62d+1s (checked on-chain in-run);
  consumer-visible expires_at jumps at migration; grace semantics after
  migration are v2's 28d, not v1's 90d (see findings F-05).

### U-09 — E1 with contract as v2 owner — VALIDATED
- `Data.owner` = a contract implementing `onERC1155Received` (run uses the
  Graveyard as a convenient known ERC1155Holder). Invariants: mint succeeds;
  owner surfaces as a contract address (no EOA assumptions downstream).

### X-U-01 — not premigrated → EACUnauthorizedAccountRoles — VALIDATED (revert)
Proves: migration is impossible for un-reserved names; the whole tx reverts
atomically (no partial v1 destruction — nothing to index).

### X-U-02 — Data.owner=0 → InvalidOwner — VALIDATED (revert)
### X-U-03 — label/tokenId mismatch → NameDataMismatch — VALIDATED (revert)
### X-U-04 — short/garbage payload → InvalidData — VALIDATED (revert)
### X-U-05 — unwrapped token to LockedMigrationController → ERC-721 receiver revert — VALIDATED (revert)
### X-U-06 — E1 during v1 grace → transfer impossible; v2 stays RESERVED — VALIDATED (revert)
Proves the D4 wall: in-grace names cannot migrate; the reservation is intact
(status re-checked in-run) so indexers must not infer lapse from v1 grace.
### X-U-07 — Data.owner = non-receiver contract → ERC1155InvalidReceiver — VALIDATED (revert)

## W — E2: wrapped-unlocked .eth 2LD via ERC-1155 transfer

### W-01 — wrapped-unlocked happy path with records — VALIDATED
- Txs: REG; PREMIG; WRAP(fuses=0); RECORDS; launch;
  MIG1155(w01.eth, UnlockedMigrationController, Data).
- Expected events (migration tx): ERC-1155 `TransferSingle(user→controller)`
  → v1 `NewResolver(node,0)` [wrapper setResolver] → `NameUnwrapped(node,
  Graveyard)` + ERC-1155 `TransferSingle(controller→0)` [burn] + v1
  `NewOwner(eth,h,Graveyard)` + registrar `Transfer(wrapper→Graveyard)` →
  v2 claim events as U-01.
- Invariants: status `wrapped` → `registered`; wrapper-position resource
  ends; v2 resource begins (resource rotation across the boundary);
  wrapper-token burn must not read as name death (surface persists).

### W-02 — E2 with owner override + custom subregistry — VALIDATED
Same as U-03+U-05 through the wrapped path.

### W-03 — E2 batch (`safeBatchTransferFrom`, 2 names) — VALIDATED
- Expected: ERC-1155 `TransferBatch`, then per-name unwrap+claim streams in
  one tx. Invariants: two independent name migrations from one tx; event
  attribution per name must key on ids/labels, not tx granularity.

### X-W-01 — wrapped-unlocked to LockedMigrationController → NameNotLocked — VALIDATED (revert)
### X-W-02 — wrapped-locked to UnlockedMigrationController → NameIsLocked — VALIDATED (revert)
### X-W-03 — wrapped 2LD in grace → transfer reverts (PCC + grace-start rule) — VALIDATED (revert)

## L — E3: wrapped-locked .eth 2LD via LockedMigrationController

### L-01 — locked plain (CANNOT_UNWRAP) happy path — VALIDATED
- Txs: REG; PREMIG; WRAP; `setFuses(CANNOT_UNWRAP)` implicit via wrap fuse
  arg; RECORDS; launch; MIG1155(l01.eth, LockedMigrationController, Data).
- Expected events (migration tx): ERC-1155 `TransferSingle(user→controller)`
  → v1 `NewResolver(node,0)` → ERC-1155
  `TransferSingle(controller→Graveyard)` [still wrapped!] → VerifiableFactory
  `ProxyDeployed` + WrapperRegistry `RegistryCreated` + `ParentUpdated(
  ETHRegistry, "l01", virtualOwner)` + EAC root-role grant [initialize] →
  v2 .eth `LabelRegistered` + mint + `TokenResource` + roles +
  `SubregistryUpdated(tokenId, WrapperRegistry(l01.eth), controller)` +
  `ResolverUpdated`.
- Invariants: status → `registered`; a **new registry contract instance**
  (the WrapperRegistry proxy) must be discovered from `SubregistryUpdated`
  and admitted as the name's subregistry via the discovery `subregistry`
  edge (registry-of-record for children of l01.eth); token roles reflect
  fuse translation (in-run check: SET_RESOLVER yes, RENEW no); the still-
  wrapped ERC-1155 parked in the Graveyard must not be read as a live
  wrapper position (wrapper-authority anchor is dead even though the token
  exists — see findings F-06).

### L-02 — locked + CANNOT_SET_RESOLVER, recognized PublicResolver — VALIDATED
- Extra: `setFuses(CANNOT_SET_RESOLVER)`; payload resolver deliberately
  garbage. Expected: v1 resolver NOT cleared; v2 `ResolverUpdated` =
  **PublicResolverV2** (the replacement, from `PUBLIC_RESOLVER_SET`
  membership), payload ignored.
- Invariants: the v2 resolver differs from every address the caller supplied
  — resolver provenance is "migration-substituted", and the v1 resolver
  stays set on the (superseded) v1 node: a resolver-address equality check
  across v1/v2 must not be used as a migration-consistency invariant.
  Records must be re-read from PublicResolverV2 (wrapper-aware fallback).

### L-03 — locked + CANNOT_SET_RESOLVER, custom resolver — VALIDATED
- Expected: custom v1 resolver address carried into v2 verbatim; payload
  ignored; v1 resolver not cleared.

### L-04 — locked + CANNOT_BURN_FUSES (frozen) — VALIDATED
- Expected: token roles granted WITHOUT admin counterparts; in-run check:
  owner's later `grantRoles(SET_RESOLVER→user2)` reverts.
- Invariants: permissions projection must show non-delegatable roles;
  no `TokenRegenerated` can ever follow from owner-initiated grants.

### L-05 — locked + CANNOT_CREATE_SUBDOMAIN — VALIDATED
- Expected: WrapperRegistry root roles lack REGISTRAR; in-run check: owner
  `register()` of a fresh child in the WrapperRegistry reverts.
- Invariants: children surface of l05.eth is frozen at migration content
  (only migratable v1 children can ever appear); no revival after child
  expiry (`_canRevive` gate).

### L-06 — locked + CANNOT_APPROVE (no live approval) — VALIDATED
### L-07 — locked with records: continuity via v2 UniversalResolver — VALIDATED
### L-08 — E3 batch (two locked names, one owner, `TransferBatch`) — VALIDATED
Two WrapperRegistry deployments in one tx; discovery must admit both.

### X-L-01 — locked + CANNOT_TRANSFER → OperationProhibited — VALIDATED (revert)
The permanent wall: such names can never migrate (mechanism §13). Indexer
consequence: `wrapped` names with CANNOT_TRANSFER stay v1-authoritative
forever (or until expiry); any "migration complete" coverage claim must
exclude them.
### X-L-02 — locked + CANNOT_APPROVE with live approval → FrozenTokenApproval — VALIDATED (revert)
### X-L-03 — locked 2LD into a foreign WrapperRegistry → NameDataMismatch — VALIDATED (revert)
### X-L-04 — locked 3LD to LockedMigrationController → NameDataMismatch — VALIDATED (revert)

## C — E4: children into WrapperRegistry

### C-01 — locked child into migrated parent's WrapperRegistry — VALIDATED
- Txs: parent locked migrate (L-01 flow); then MIG1155(sub.c01.eth,
  WrapperRegistry(c01.eth), Data) by child owner (user2).
- Expected: same locked-branch stream as L-01 but the claim lands in the
  parent's WrapperRegistry (`LabelRegistered` emitted by the proxy) at
  **wrapper expiry** (not a reservation), and a nested
  WrapperRegistry(sub.c01.eth) is deployed and bound.
- Invariants: child's logical_name_id continuity; child registration rows
  come from a *dynamically discovered* registry contract (the parent's
  WrapperRegistry) — the registry set is open-ended post-migration, one new
  emitter per locked name (findings F-03); parent-child discovery edge
  chain root→eth→l01→sub must resolve the child exactly.

### C-02 — detached (emancipated, unlocked) child — VALIDATED
- Expected: unwrap branch — v1 `NewResolver(0)`, `NameUnwrapped(node,
  Graveyard)`, registry `NewOwner(→Graveyard)`; claim in parent
  WrapperRegistry with REGISTRATION_ROLE_BITMAP at wrapper expiry; NO nested
  registry (payload subregistry used, zero in run).
- Invariants: unlike C-01 the child has no own registry; children of the
  child have no v2 home (registry edge absent) — subname surface below it
  must stay empty.

### C-03 — child with CAN_EXTEND_EXPIRY — VALIDATED
- Expected extra roles: ROLE_RENEW(+admin); in-run check: child owner renews
  itself in the WrapperRegistry (`ExpiryUpdated` by non-registrar sender).
- Invariants: expiry changes for such children are self-service — renewal
  provenance is the owner, not a registrar contract.

### C-04 — deep chain 2LD→3LD→4LD all locked — VALIDATED
- Sequential migrations; `findExactRegistry` resolves the 4LD through three
  chained WrapperRegistries (in-run check).
- Invariants: discovery must walk arbitrary-depth subregistry chains;
  depth is unbounded in principle.

### C-05 — unmigrated emancipated child under migrated parent — VALIDATED
- In-run checks: `WrapperRegistry.getResolver(sub)` = ENSV1Resolver
  fallback; `register(sub,...)` by parent owner reverts
  `NameRequiresMigration`.
- Invariants: **bridge state**: the child has no v2 registration events at
  all, yet v2-side resolution answers through the fallback. An indexer
  reading only v2 registry events sees nothing for the child; one reading
  resolution sees the fallback resolver. bigname must keep the child's
  authority v1-side (wrapper) while the parent is v2-side — the
  mixed-authority tree is a persistent, legitimate state (findings F-04).

### C-06 — parent-controlled child: unmigratable + clobberable — VALIDATED
- In-run: child transfer reverts `NameNotLocked`; `getResolver` = 0 (no
  fallback!); parent owner `register(sub, ...)` in the WrapperRegistry
  succeeds ("clobber").
- Invariants: after the clobber there are TWO live "sub.c06.eth" facts: the
  v1 wrapped child (still owned by user2 on v1) and the v2 registration
  (user). v2 must win current-state; the v1 child keeps existing as
  superseded residue. This is the *within-name-tree* analogue of the 2020
  registry migration precedence assertion.

### C-08 — abandoned child: unwrap → registry owner=0 → label reclaimable — VALIDATED
- Sequence proves the `_isMigratableChild` boundary: while unwrapped-but-
  owned, register still reverts (protected, yet unmigratable — permanent v1
  residence, findings F-04); after `setOwner(0)` the label registers freely.
- Invariants: protection hinges on the *v1 registry owner* being nonzero —
  an indexer modeling "migratable" must read fuse memory (PCC survives
  unwrap) plus live v1 owner, exactly as the contract does.

### C-07 (folded into H-04) — child migration before parent has no direct
path (no receiver exists); the helper's `ParentNotMigrated` is the only
observable form. SPEC-ONLY as a distinct direct-path scenario (nonexistent
by construction).

## H — E5: MigrationHelper batches

### H-01 — mixed batch: unwrapped + unlocked group + locked group + locked child of the just-migrated parent, one tx — VALIDATED
- Proves intra-tx ordering (unwrapped → unlocked → locked → children) makes
  parent-then-child single-tx migration legal.
- Invariants: four names' full event streams interleave in one transaction —
  normalized-event attribution must be per-log, never per-tx; the child's
  registry (parent's WrapperRegistry) is discovered from a log earlier in
  the SAME tx (discovery must be intra-block/intra-tx ordered).

### H-02 — helper without operator approval → NotApprovedOperator — VALIDATED (revert)
### H-03 — wrapped group with mixed owners → WrappedOwnerMismatch — VALIDATED (revert)
Execution surfaced an ordering nuance: the per-token caller-approval check
precedes the same-owner rule, so `WrappedOwnerMismatch` is only reachable
when every group owner has approved the *caller*; third-party batches need
owner→caller AND owner→helper approvals (mechanism.md §8).
### H-04 — locked child group with unmigrated parent → ParentNotMigrated — VALIDATED (revert)

## G — E6: Graveyard

### G-01 — clear v1 registry subnode residue under E1-migrated 2LD — VALIDATED
- Tx: `Graveyard.clear([dns("sub.g01.eth")])` by an unrelated EOA.
- Expected: v1 `NewOwner(g01.eth, h(sub), Graveyard)` + `NewResolver(sub,0)`
  + `NewTTL` [setSubnodeRecord] — emitted long after migration, by a
  permissionless call.
- Invariants: these late v1 registry writes must be ingested as superseded-
  side facts and must not resurrect or mutate any name surface (the classic
  precedence assertion, now with the Graveyard as writer).

### G-02 — clear fully-expired unmigrated 2LD: graveyard self-claims — VALIDATED
- After v1 grace: `clear` makes the Graveyard a **registrar controller
  registering the name to itself** with ≈max duration: v1 `NameRegistered
  (id, Graveyard, huge expiry)` + `NewOwner` + resolver clear.
- Invariants: a post-launch v1 `NameRegistered` exists that is *not* a
  user registration; bigname's v1 registrar adapter must not mint a fresh
  active lease/lineage for the Graveyard claim (status must not flip to
  `active` with a year-2100+ expiry). Needs an explicit suppression/
  classification rule — findings F-02.

### G-03 — clear a live unmigrated name → NameNotClearable — VALIDATED (revert)
### G-04 — clear wrapped parent-controlled child residue under E3-migrated parent — VALIDATED
- Expected: wrapper `setSubnodeRecord` (NewOwner+fuse reset) then
  `NameUnwrapped(child, Graveyard)` — a post-migration wrapper write + burn
  for a name that never migrated.
- Invariants: same residue rule; the child's v1 wrapped token dies without
  any v2 counterpart ever existing.

### G-05 — clear by labelhash (`\x00`+hash modified DNS encoding) — SPEC-ONLY
Not executed: exercised by pinned unit tests
(Graveyard.t.sol `test_clear_prehashedLabel_*`); adds no new indexer-visible
event shape beyond G-01/G-04 (the registry writes look identical). The
mechanism accepts hash-only names; bigname needs no special handling beyond
preimage-less labels it already models as placeholder children.

## R — E7: ETHRenewerV1

### R-01 — renew unmigrated premigrated name (dual-write) — VALIDATED
- Expected: v2 `ExpiryUpdated(tokenId, newExpiry, renewer)` (on the RESERVED
  entry) + v1 `NameRenewed(id, expires)` + renewer `NameRenewed(tokenId,
  label, duration, newExpiry, MockUSDC, referrer, amount)`; both sides move
  in lockstep (checked in-run).
- Invariants: v1 lease extension post-launch comes only from this path; the
  v2 `ExpiryUpdated` on a *reserved* (ownerless) entry must not create a
  registration row.

### R-02 — renew during v1 grace restores, then migrate — VALIDATED
- Proves the only escape from the in-grace migration wall: renew (v1 owner
  restored, since the grace-period token was never burned), then E1.
- Invariants: v1 status sequence active→(grace)→active→migrated with no
  release event; bigname must not emit RegistrationReleased for the grace
  dip (release is only observable via v1 events it never got).

### R-03 — renew after combined grace → NameNotRenewable — VALIDATED (revert)
### R-04 — renewer refuses migrated names → NameNotRenewable — VALIDATED (revert)
Migrated names renew only via v2 ETHRegistrar (P-06).
### R-05 — syncWrapper after renewer renewal — VALIDATED
- Expected: v1 `ControllerAdded(NameWrapper)` + wrapped-controller renew(0)
  → wrapper `ExpiryExtended(node, expiry)` (+ v1 `NameRenewed(id, dur 0)`)
  → `ControllerRemoved(NameWrapper)` — transient controller churn.
- Invariants: manifest/watch-plan treats BaseRegistrar controller set as
  DYNAMIC post-launch (add/remove per syncWrapper call) — a static
  admitted-controller assumption breaks (findings F-02).

## P — post-migration perturbations & authority precedence

### P-01 (folded into U-06) — late v1 subnode write lands — VALIDATED
### P-02 — old owner cannot write the migrated node on v1 — VALIDATED (reverts)
### P-03 — pre-migration v1 registry operator approval is dead post-migration — VALIDATED (revert)
Operator approvals are per-owner; owner is now the Graveyard.
### P-04 — old owner cannot write E3-migrated node via wrapper — VALIDATED (reverts)
P-02..P-04 together prove: unlike the 2020 registry migration, the
superseded side is *unwritable at the node itself* — the late-write
suppression assertions of registry_migration.rs port to residue + graveyard
classes only.
### P-05 — emancipated sibling stays v1-live under migrated parent — VALIDATED
Wrapper `NewResolver` + record writes for the child land normally after the
parent moved to v2. Indexer: child records remain v1-sourced; parent
registration v2-sourced; both under one tree (findings F-04).
### P-06 — v2 renew of migrated name; v1 husk expiry frozen — VALIDATED
v2 `ExpiryUpdated` + registrar `NameRenewed`; v1 `nameExpires` unchanged
(checked). expires_at must come from the v2 side alone; monitoring that
compares v1/v2 expiry for "drift" must whitelist migrated names.
### P-07 — role grant regenerates token id — VALIDATED
`TokenRegenerated(old,new)` + burn/mint `TransferSingle` pair; resource id
unchanged (checked). token_lineage_id must survive; any cache keyed by
ERC-1155 id must rebind.
### P-08 — v2 transfer of migrated name — VALIDATED
`TransferSingle` + role transfer; owner flips v2-side only.
### P-09 — reservation lapse → fresh v2 registration by third party — VALIDATED
- Full commit/reveal by user2 after everything expired; in-run check: fresh
  token has no `ROLE_WAS_RESERVED`.
- Invariants: logical_name_id persists (same namehash); resource_id,
  token_lineage, registration row all NEW; prior v1 facts (old owner in
  expired registry entry, old records) are dead history; primary-name claims
  pointing at the name must fail forward verification once records change.
### P-10 (folded into U-07) — reverse claim continuity — VALIDATED
### P-11 — v2 unregister of a migrated name — VALIDATED
- Run result: the devnet deployer (holding root UNREGISTER via root-role
  grant) unregisters; `LabelUnregistered(tokenId, sender)` + burn; status
  back to AVAILABLE while the v1 husk still shows Graveyard ownership.
- Invariants: governance-only path (neither migration role bitmap includes
  UNREGISTER); if it ever fires, the name dies v2-side with no v1
  counterpart event — surface must show unregistered, not fall back to v1.

---

## Coverage summary

Mechanism paths executed at least once: E1 (U-01..U-09), E2 single+batch
(W-01..W-03), E3 single+batch (L-01..L-08), E4 locked child + detached child
+ nested chain (C-01..C-04), E5 mixed batch + all three revert classes
(H-01..H-04), E6 clear residue/expired/live-revert/wrapped-child
(G-01..G-04), E7 renew/renew-in-grace/refusals/syncWrapper (R-01..R-05),
plus every documented revert wall (X-*), and the post-migration perturbation
classes (P-*). Scenario statuses: see `validation/SUMMARY.json` (counts in
the final report); the only deliberately unexecuted scenarios are G-05 and
the C-07 placeholder, each with reason above.
