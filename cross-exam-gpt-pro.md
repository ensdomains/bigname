# Cross-examination vs the GPT Pro catalog (2026-08-05)

The user commissioned GPT Pro to produce the same migration scenario
catalog independently. An evidence agent cross-examined both sets
against .refs/ens_v2 @ ccaeb58b and .refs/ens_v1 @ 91c966f. Full
report below. Corrections adopted into THIS catalog's routing:

1. **C3 — our mechanism §4 / U-01 over-constrain the v1 destruction
   event shape.** `setRecord` emits `NewResolver`/`NewTTL` only when
   the value CHANGES (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L33-L41,L174-L188 @ ens_v1@91c966f).
   Migration txs on names with TTL already 0 (virtually all) emit no
   `NewTTL`; names with no v1 resolver emit no `NewResolver`. Any
   fixture or adapter expectation requiring their presence would fail
   on mainnet. Fix fixtures/expectations when the migration-family
   slice is built; do NOT copy the unconditional triple from
   mechanism.md §4.
2. **NEW FINDING F-13 (from GPT Pro F-01, verified at current main):
   the mixed v1+v2 corpus coverage collision.**
   `build_exact_name_coverage` (apps/worker/src/name_current/coverage.rs:10-29)
   returns `unsupported`/`mixed_ensv1_ensv2_exact_name_corpus` for any
   name with both ENSv1- and ENSv2-family events — which is EVERY
   migrated name, since v1 destruction and v2 claim land in one tx
   (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L111-L119 @ ens_v2@ccaeb58b).
   Post-launch this marks essentially the entire migrated .eth corpus
   unsupported. Routes with F-04 into the migration-family admission
   slice: the mixed-corpus ownership rule must REPLACE this blanket
   marker.
3. **F-03 reframed (from GPT Pro F-03 nuance): WrapperRegistry
   proxies are already mechanically admitted** via the match-all
   `RegistryCreated` rule + `registry_announcement` edge
   (manifests/sepolia/ethereum/ens/ens_v2_registry_l1/v2.toml:45-61;
   crates/adapters/src/schema_v2/discovery.rs:24-67). The F-03 task is
   naming/ratifying that class and testing same-tx ordering — not
   building admission.
4. **Harness-fixture warning (C1 aside):** upstream's fork-mode
   rehearsal re-adds the deployer as a v1 controller
   (upstream: .refs/ens_v2/contracts/script/setup.ts:L883-L888 @ ens_v2@ccaeb58b).
   Fixtures copied from rehearsal topology can fabricate "fresh v1
   re-registration after migration" streams that are unreachable in
   the deployed launch topology (all public controllers removed).
   Comment this in the future migration e2e harness.

Adjudications that STAND for our set: C1 (no fresh v1 re-registration
post-launch; B's "critical" was a harness artifact — only the
Graveyard's own ~uint64.max self-claim is reachable, already our
F-02 rule 2), C2 (B fabricated the MigrationHelper entrypoint matrix
row; real shape is four arrays + live-tree findExactRegistry).
Premigration (F-12), launch freeze, Graveyard depth, v2-side
perturbations, and F-01/F-07/F-10/F-11 remain ours alone. Mutual
independent confirmation on the shared core (lock classification,
fuse→role translation, resolver preservation, reservation-expiry
inheritance, renewability predicate, CANNOT_TRANSFER unreachability).

---

[Full evidence-agent report follows]

Provenance note on Set B: its upstream citations resolve in the
pinned checkout (spot-checked ~20). Its bigname citations pin
bigname@311e192 (stale); load-bearing repo claims re-verified at
current main. Its validation artifacts were not uploaded, so its
execution evidence (26 scenarios) is UNVERIFIED as artifacts. Set B
executed 26 scenarios; ours executed 63.

## 1. Coverage delta — B beyond A (ranked)

1. B F-01 mixed-corpus coverage collision — adopted as F-13 above.
2. B F-03 nuance: proxies already admitted — adopted as F-03 reframe.
3. B F-05 precision: registrar manifest restricts NameRenewed to
   emitter_roles=["registrar"] (ens_v2_registrar_l1/v3.toml:49-53), so
   only the renewer's own event (payment token, referrer, amount —
   upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRenewer.sol:L21-L29 @ ens_v2@ccaeb58b)
   is lost; registry-family ExpiryUpdated still lands. Finer than our
   F-02; no contradiction.
4. Executed late-write to a still-writable custom v1 resolver
   (B AUTH-V1-CUSTOM-RESOLVER): migration clears only the registry
   pointer, never resolver storage
   (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L86-L95 @ ens_v1@91c966f).
   We noted it in dimensions D7; B's executed stream is the better
   conformance fixture. Moderate value.
5. Atomicity-failure shapes (whole-batch revert; helper unwind
   SPEC-HELPER-003): real by construction
   (upstream: .refs/ens_v2/contracts/src/migration/AbstractWrapperReceiver.sol:L149-L154,L164-L174 @ ens_v2@ccaeb58b);
   indexer consequence low (reverted tx ⇒ nothing indexed), but
   SPEC-HELPER-003 is a good "no partial WrapperRegistry discovery"
   guard.
6. B F-07 WrapperRegistryImpl sepolia artifact source-hash mismatch:
   UNVERIFIED (validation files not uploaded); one check before
   deploying from those artifacts.

## 2. Coverage delta — A beyond B

Premigration entirely absent from B (BatchRegistrar, bonus arithmetic
1s+90d−28d per deploy-constants.ts:L216-L219, LabelReserved flood
F-12, D4 window table). Launch freeze absent (activateV2, setup.ts
L860-L879) — the omission that produced B's C1 error. Graveyard depth
(G-01/G-04 executed; NameRequiresPreimage), v2-side perturbations
(TokenRegenerated P-07, unregister P-11, ROLE_WAS_RESERVED lapse P-09
per PermissionedRegistry.sol:L444-L447), wrong-receiver
cross-products, C-06 parent-clobber dual-fact case, and findings
F-01/F-07/F-10/F-11.

## 3. Contradictions (adjudicated)

C1 — fresh v1 re-registration post-migration: A WINS on mechanism
(BaseRegistrarImplementation._register is live onlyController per
ens_v1 BaseRegistrarImplementation.sol:L130-L136; launch leaves only
Graveyard + ETHRenewerV1 as controllers per setup.ts:L860-L893;
renewer only renews, ETHRenewerV1.sol:L133-L135; Graveyard registers
only to itself, Graveyard.sol:L161-L165). B's executed "proof" ran no
launch freeze. B's narrowed adapter observation is confirmed at
current main (identity.rs:150-176 closes prior bindings with no
authority-epoch guard) but the only reachable trigger is the
Graveyard self-claim = our F-02 rule 2.

C2 — MigrationHelper shape: A WINS. Real entrypoint is four arrays
(MigrationHelper.sol:L94-L99) and parent resolution walks the live
tree via LibRegistry.findExactRegistry, reverting ParentNotMigrated
(MigrationHelper.sol:L122-L127). B's matrix row fabricated (its own
prose disagrees with its matrix).

C3 — v1 destruction event shape: B WINS. Adopted above.

C4 — stale-pin citation counts drift with tree state (B: 357@311e192;
main now: 301 across 21 files). Same finding (our F-08 = B F-06).

## 4. Verdict on our 12 findings

F-01 not covered · F-02 confirmed + nuance (renewer sync churn
observed; NameRenewed attribution precision) · F-03 confirmed, task
shape corrected · F-04 confirmed + strengthened (F-13 collision) ·
F-05 confirmed weakly (B silent on 62d+1s) · F-06 confirmed ·
F-07 not covered · F-08 confirmed quantified · F-09 confirmed ·
F-10 not covered · F-11 not covered · F-12 not covered (B's largest
blind spot).

Net: no B finding overturns a routed rule. Absorb C3 + F-03 reframe +
new F-13; adopt B's monotonic-epoch idea only as defense-in-depth.
