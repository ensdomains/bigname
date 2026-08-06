# Findings: locked-in migration mechanism vs current bigname repo

Each finding: what the pinned mechanism does (cited), what the repo currently
says or assumes (path:line), and the doc-first action it implies. Ordered by
severity for the indexer. Repo state as of `main@04e096d4` (2026-08-05); pins
`ens_v2@ccaeb58b` (post-audit-2 HEAD) and `ens_v1@91c966f`.

## F-01 — Live L2/Namechain-era remnants contradict the L1-only mechanism

The mechanism is entirely L1 (mechanism.md). Namechain is cancelled
(maintainer statement recorded in `simplification-audit-20260730.md:81-83`).
Remnants that still assert or model an L2-shaped ENSv2:

- `docs/partners/partner-1-indexing-requirements.md:92,307,340` — "ENSv2 L2
  destination chains" / "ENSv2 onchain records on ... partner-1-requested L2
  destinations". Partner wording is disclaimed at :87 but the coverage asks
  build on a nonexistent destination class.
- `docs/api-v1.md:165` — "Production ENSv2/L2 manifest admission remains a
  separate workstream" — the pairing implies production ENSv2 arrives on an
  L2; production ENSv2 is L1 mainnet.
- `apps/api/src/tests/namespaces.rs:28-41` (+ :163..:658),
  `apps/api/src/tests/v2_envelope_conformance.rs:1547-1559` — fixtures invent
  `ens_v2_registry_l2` on `base-mainnet` with an `ens_v2_base` epoch. No such
  source family can exist under the locked mechanism.
- `apps/api/src/tests/v2_permissions.rs:631-640`,
  `apps/api/src/tests/v2_address_names.rs:611,956` — fixtures seed
  `PermissionScope::TransportDerived { transport: "l1_to_l2" }` on ENS
  resources; `transport_derived` itself
  (`schema-v2/baseline/06_projections.sql:186-187`,
  `crates/storage/src/permissions/types.rs:315-320`,
  `docs/architecture.md:398`, `docs/api-v2-routes.md:337`) is reserved
  schema/API surface with **no producer** — a bridge/ejection-era design
  remnant whose only exemplars are these fixtures.

Action: doc-first sweep deleting/retiring the L2-destination framing and the
`transport_derived`/`l1_to_l2` exemplars, or an explicit divergence note
stating they are dead reserved surface. (Basenames' L2 is unaffected.)

## F-02 — Migration surface is locked-in but wholly outside admission, and two of its behaviors break current adapter assumptions

Repo: `docs/internal/e2e-testing-plan.md:227` marks the migration flow
"blocked (migration controllers outside admission; doc-first change
required)"; `docs/manifests.md:209-211` keeps migration, renewer, factory,
batch-registrar surfaces un-admitted. That was correct while the mechanism
was in flux; it is now final on post-audit-2. Two concrete adapter traps the
catalog validated:

1. **Post-launch v1 registrar controller set is dynamic.** Launch removes
   all public controllers and adds Graveyard + ETHRenewerV1
   (upstream: .refs/ens_v2/contracts/script/setup.ts:L844-L893 @ ens_v2@ccaeb58b);
   `syncWrapper` transiently re-adds/removes NameWrapper per call
   (upstream: .refs/ens_v2/contracts/src/registrar/ETHRenewerV1.sol:L106-L112 @ ens_v2@ccaeb58b;
   validated R-05). Any manifest/watch-plan admission that assumes a static
   v1 controller set, or treats `ControllerAdded` as a rare governance
   event, must be revisited.
2. **The Graveyard emits v1 `NameRegistered` self-claims with ~uint64.max
   expiry** for fully-expired names
   (upstream: .refs/ens_v2/contracts/src/migration/Graveyard.sol:L158-L170 @ ens_v2@ccaeb58b;
   validated G-02). A v1 registrar adapter that mints an active lease +
   fresh token lineage from `NameRegistered` will show the Graveyard as a
   registrant with an astronomically distant expiry. Needs an explicit
   classification rule (suppress or mark as burn-claim) in the same change
   that admits the family.

Action: the doc-first admission change for the migration source family
should carry both rules explicitly.

## F-03 — Locked migration mints one new registry contract per name; discovery must admit unbounded, dynamically-deployed registries — including intra-tx

Mechanism: every locked-name migration deploys a `WrapperRegistry` proxy via
`VerifiableFactory.deployProxy(salt = namehash)` and binds it with
`SubregistryUpdated`
(upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L146-L175 @ ens_v2@ccaeb58b);
chains nest to arbitrary depth (validated C-01/C-04/L-08), and a parent's
registry can be created and *receive its child's migration in the same
transaction* via MigrationHelper ordering (validated H-01). On mainnet this
is an open-ended set (order of magnitude: all locked wrapped names).
bigname's ENSv2 discovery currently reasons about a fixed manifest-declared
registry set plus discovery edges (stage B2 semantics, PR #291); the
catalog's C/H scenarios are the conformance target for: subregistry-edge
admission of a factory-deployed proxy, per-log (not per-tx) ordering, and
registry identity for proxies sharing one implementation
(`WrapperRegistryImpl`).

## F-04 — Mixed-authority name trees are a permanent, legitimate steady state (not a transition artifact)

Mechanism: an unmigrated emancipated child under a migrated parent stays
v1-resident indefinitely — protected from v2 clobber
(`NameRequiresMigration`), resolved through the per-registry `V1_RESOLVER`
fallback, invisible in v2 registry events
(upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L155-L184,L290-L304 @ ens_v2@ccaeb58b;
validated C-05, P-05). A child that unwraps after parent migration is
*protected but permanently unmigratable* until abandoned (validated C-08).
No bigname doc describes a tree whose parent registration is
v2-authoritative while a child's control and records remain v1-authoritative
without any terminating event. Identity/coverage semantics (which source
family "owns" the child) need a written rule before the migration family is
admitted. `docs/glossary.md` has no terms for any of this (see F-10).

## F-05 — Expiry and grace semantics shift at migration; v1/v2 expiries then diverge permanently

- Premigration reserves at `v1 expiry + 62d + 1s`
  (upstream: .refs/ens_v2/contracts/script/deploy-constants.ts:L216-L219 @ ens_v2@ccaeb58b);
  the migrated registration inherits that padded expiry (`expiry=0 =>
  reserved expiry`,
  upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L444-L446 @ ens_v2@ccaeb58b;
  validated U-08: consumer-visible `expires_at` jumps by 62d+1s at the
  migration boundary).
- Post-migration renewals move only the v2 expiry; the v1 husk's
  `nameExpires` freezes forever (validated P-06). Expiry-drift monitors and
  any "ENS .eth grace = 90 days" assumption break: v2 grace is 28d
  (upstream: .refs/ens_v2/contracts/script/deploy-constants.ts:L216-L217 @ ens_v2@ccaeb58b).
- Unmigrated names renew through ETHRenewerV1 with a renewability window
  that maps exactly onto the old v1 90d grace (validated R-01/R-02/R-03).

Action: the registrar-semantics doc for ENSv2 admission must state the
padded-expiry inheritance and the divergence rule (v2 side wins for
migrated names).

## F-06 — After locked migration, a live-looking NameWrapper position is dead; `wrapped` status must yield to `registered`

Mechanism: locked names stay wrapped; the ERC-1155 (fuses intact) is parked
in the Graveyard and the v1 registry owner remains the NameWrapper
(upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L144 @ ens_v2@ccaeb58b;
validated L-01 terminal-state checks — note this catalog's own driver first
asserted the wrong terminal state here, which is exactly the mistake an
adapter would make). A v1 wrapper adapter sees: token transfer to an
address, no unwrap, no burn — and would keep classifying the name `wrapped`
(`classify_registration_status`, `apps/api/src/v2/name_record.rs:476-491`:
`authority_kind = "wrapper"` => `Wrapped`). Post-migration the correct
status is `registered` (`ens_v2_registry`). The admission change needs an
authority-precedence rule: wrapper position held by the Graveyard =
superseded anchor, and the v2 claim event is the rebinding boundary.

## F-07 — Reserved machinery (`MigrationApplied`, `migration_rebind`, `migration_derived`) now has a concrete mechanism to bind to — none of it is produced or specified

- `docs/architecture.md:305` reserves normalized kind `MigrationApplied`
  (absent from `schema-v2/baseline/05_normalized_events.sql`).
- `migration_rebind` binding kind (`docs/architecture.md:88`,
  `docs/adrs/0002-surface-resource-identity.md:58-69`,
  `schema-v2/baseline/03_identity.sql:278`).
- `PermissionScope::MigrationDerived { predecessor_resource_id }`
  (`crates/storage/src/permissions/types.rs:315-317`,
  `docs/api-v2-routes.md:336-337`) — no producer.

The real boundary event is now known precisely: the controller-sent
`LabelRegistered` claim on the v2 .eth registry (plus `SubregistryUpdated`
for locked names), in the same tx as the v1 destruction writes
(mechanism.md §§4-6, validated U-01/W-01/L-01 event streams). Doc-first
task: either specify `MigrationApplied`/`migration_rebind`/
`migration_derived` in terms of that tx shape, or delete the reserved
surface. The catalog's expected-event streams are the specification input.

## F-08 — Stale citation short-hashes against the moved pin

Repo cites `@ ens_v2@48b3e2d` in `docs/upstream.md:146`,
`docs/manifests.md:424`,
`manifests/sepolia/ethereum/ens/ens_v2_resolver_l1/v2.toml:27-33`,
`docs/glossary.md` (token-lineage entries), and
`tests/e2e/src/harness/ens_v2.rs` comments, while the pin is `ccaeb58b`.
Spot-checks: the cited LockedWrapperReceiver lines still match at the
current pin (resolver substitution now at L139-L141, Graveyard transfer
L144). Already logged as a TODO in `simplification-audit-20260730.md:132-141`;
reaffirmed here because this catalog supersedes some of those claims' line
numbers.

## F-09 — Upstream's own WrapperRegistry doc-comment still says "namechain"

(upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L25-L26 @ ens_v2@ccaeb58b)
reads "supporting migration of wrapped names into the namechain registry
system". Cosmetic upstream staleness — the code is L1-only. Flagged so
nobody cites that comment as evidence of an L2 design.

## F-10 — Glossary has no vocabulary for the migration mechanism

`docs/glossary.md:161-167` lists `migration` and `transport` discovery-edge
kinds without defining them; there are no entries for: premigration
reservation (`RESERVED` .eth entries with the padded expiry), migration
controllers, WrapperRegistry (per-name bridge registry), Graveyard,
ETHRenewerV1, "migratable child", or the v1-fallback resolver
(ENSV1Resolver / `V1_RESOLVER`). Most existing "migration-era" glossary
entries mean bigname's own schema migration — the overload is exactly what
the Communication guardrail warns about. Doc-first: add mechanism terms in
the same change that admits the family, and qualify "migration-era".

## F-11 — `MigrationHelper` is a name collision between ENSv1 and ENSv2 artifacts

ENSv1 ens-contracts ships a `MigrationHelper` (wrapper-migration helper:
`migrateNames`/`migrateWrappedNames`); ENSv2 ships an unrelated
`MigrationHelper` (batch migration:
`migrate(unwrapped, unlockedGroups, lockedGroups, lockedChildrenGroups)`,
upstream: .refs/ens_v2/contracts/src/migration/MigrationHelper.sol:L94-L99 @ ens_v2@ccaeb58b).
Observed concretely: the upstream devnet's own deployment registry resolved
the name to the v1 contract (validation deployed the v2 helper from the
pinned forge build instead; see `validation/H-01.json`). The pinned sepolia
artifact `.refs/ens_v2/contracts/deployments/sepolia/MigrationHelper.json`
IS the v2 one (ABI carries `migrate`). Trap for any harness/manifest tooling
that keys contracts by artifact basename across both pins.

## F-12 — Premigration itself is an indexable event flood with unusual shapes

- Reserving emits `LabelReserved` + `ResolverUpdated(ENSV1Resolver)` for
  every existing .eth 2LD — on mainnet, millions of events from
  `BatchRegistrar`, with **no token mint and no owner**
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L461-L462,L474-L476 @ ens_v2@ccaeb58b).
  Adapters must accept resolver records and expiry (`ExpiryUpdated` via
  renew-extension of reservations, validated R-01) on ownerless RESERVED
  entries without creating registrations; `registration_status` for such
  names must remain derived from the v1 side until the claim.
- During the RESERVED window, v2-side resolution of the name already works
  through the ENSV1Resolver fallback while v1 remains authoritative —
  resolution-source attribution is time-dependent (also true per-child for
  WrapperRegistry `V1_RESOLVER` fallback, validated C-05).

## Verification notes

- All repo line references checked against the working tree at
  `main@04e096d4`; upstream citations against the pins named above.
- The alignment claims found in the sweep (`docs/upstream.md:146`,
  `docs/manifests.md:424`, resolver manifest comment) were verified still
  true at the current pin — they are correct, only their hashes are stale
  (F-08).
- Not a finding: `isMigrated`/`v1_is_migrated` in
  `crates/storage/src/name_current/list.rs:80-83` and
  `crates/adapters/src/schema_v2/protocol/v1/registry.rs:80-88` concern the
  2020 within-v1 registry migration and the v2-membership filter
  respectively; both compose correctly with this mechanism as long as
  F-06's precedence rule lands.
