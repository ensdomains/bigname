# Glossary

Plain-language definitions for bigname-specific terms. Standard ENS, Ethereum,
and indexing vocabulary is used unmodified and is not defined here. Wire field
names and enum values are contract, not prose; this glossary explains concepts,
it does not rename fields. Docs should link here on a term's first use instead
of re-defining or assuming it.

Three terms are overloaded enough that bare use is discouraged: **promotion**
(always qualify: checkpoint promotion vs. capability promotion), **profile**
(always qualify: deployment profile, resolver profile, exact-name profile, or
the `/v2/lookup` `profile=` parameter), and **migration** (always qualify:
bigname's own schema migration, written here as *schema-migration*, vs. the
on-chain [ENSv1→ENSv2 migration](#ensv1ensv2-migration)). Entries below that
describe retired bigname database state say *schema-migration-era*; they have
nothing to do with ENS's protocol migration.

---

**Absence-aware replay** — a replay that is allowed to treat "not re-derived
this pass" as "no longer true" and deactivate stale state. The license is
scope-relative: a replay may infer absence only where it saw complete retained
history for the scope it covers — the whole source, or a bounded target such as
one resolver's addresses. Destructive retention rotation revokes the license
until gap-free, generation-current backfill coverage re-establishes
completeness over the scope. Without completeness — a block-limited pass, or a
pass over rotated history before that recovery — a replay updates only what it
touches and never infers deletion from omission.

**Admission** — the act of authorizing an input. A contract, event, or data
source is *admitted* when a manifest declares it or a discovery rule reaches it
from a declared root; only admitted inputs can produce normalized events or
public coverage. Cross-reference: allowlisting.

**Admission epoch** (discovery-admission epoch) — a schema-migration-era per-chain
counter used to fence old-runtime watch-plan reconciliation. The table remains
in immutable schema-migration history, but Stage B manifest synchronization and
interpretation have no Rust writer or consumer for this counter; phase locks,
content hashes, and explicit redo state now fence derived work.

**Anchor** — the concrete object a stable identity is pinned to. An *authority
anchor* is the registry entry, registrar lease
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L10 @ ens_v1@91c966f),
wrapper position
(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L27 @ ens_v1@91c966f),
or ENSv2 resource
(upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L38 @ ens_v2@ccaeb58)
behind a `resource_id`; the id survives changes within one anchor and rotates
when authority moves to a different anchor. An *observation anchor* is the
exact chain/block identity a stored row was observed at.

**Authority epoch** (`authority_epoch`) — which protocol authority regime
backs a name at a given time, scoped per namespace: for `ens` names the value
is `ens_v1` or `ens_v2` per name and time, while `basenames` authority lives in
the registry/registrar/resolver system on Base
(upstream: .refs/basenames/README.md:L70 @ basenames@1809bbc)
and has no ENSv1/ENSv2 era split. The related `AuthorityEpochChanged`
normalized event is broader than an era flip: it records every move of a
name's authority anchor (registry-, registrar-, or wrapper-held), so most such
rows — millions on Basenames alone — mark within-era anchor transitions.

**Backfill coverage fact** — a schema-migration-era record that asserted one completed
old-runtime backfill job fetched all matching logs over one block interval.
The tables remain in schema-migration history, but the Stage B runtime has no writer,
repair path, or checkpoint-promotion consumer for these records.

**Batch grid** — the partition of one interpret walk into consecutive physical
batches (today 500-block ranges). Grids never split a block: the block is the
atomic unit every grid loads. Where the boundaries fall is an execution
detail, not an input to interpretation — surviving identity rows, discovery
edges, and normalized events must be identical across grids over identical
input. That identity is verified for the ENSv1 divergence classes
[#336](https://github.com/ensdomains/bigname/issues/336) catalogued, with two
known open exceptions of distinct kinds:
[#348](https://github.com/ensdomains/bigname/issues/348) is a true grid
divergence, on the ENSv2 resolver path — a late resolver `RecordChanged` on a
lapsed registration keeps an attribution only in a grid that never splits the
walk;
[#347](https://github.com/ensdomains/bigname/issues/347) diverges
whole-walk versus split-walk output but is not a boundary-carry artifact —
the uninterrupted walk under-derives a wrapper authority lapse that a split
walk derives.

**Stored-history verification** — the read-only phase that compares canonical
selected raw logs with the chain's configured reference source through a
finalized block. Base uses dRPC to cross-check the Coinbase-loaded history and
cannot extend that independent comparison past the Coinbase-to-dRPC ingest seam.
Ethereum uses local reth for a node check. The phase records only its block
extent, trust level, and any fatal mismatch in phase state. It does not write
coverage attestations or repair raw data.

**Verification level** — the source-bounded trust label for a chain's stored
history through its reported verification extent: `quick_synced` is
provider-trusted bootstrap data, `cross_checked` matched an independent
provider, and `node_checked` matched a local node. The live-follow block is a
separate position rather than a verification level. A partial verification
redo rechecks its requested range but retains the level that applies to the
whole recorded extent; only a full-extent redo can change that level. A normal
scan starts from the durable ingest extent. If it resumes through a reference
with a different level, the completed extent retains the weaker level rather
than upgrading the already-checked prefix.

**Canonicality** — whether a stored fact belongs to the chain branch currently
accepted as real, and how final that acceptance is. States: `observed` (seen,
unproven), `canonical`, `safe`, `finalized` (standard Ethereum finality tags),
and `orphaned` (on a losing branch; kept for audit, excluded from reads).

**Capability promotion** ("graduation") — the deliberate, doc-first act of
moving a capability from `shadow`/`unsupported` to publicly `supported`.
Nothing else promotes a capability: backfill completion, conformance passes,
and manifest presence are necessary evidence, never the promotion itself.

**Checkpoint promotion** — advancing a chain's stored canonical/safe/finalized
markers after proving block lineage and fetch coverage. Distinct from
capability promotion above; avoid bare "promotion" where the two could be
confused.

**Claim anchor** — the `primary_names_current` row for an exact
(address, coin type, namespace) tuple. It is the only lookup and invalidation
key for persisted primary-name claims; presence of the row never widens what
claim sources are trusted.

**Closure** — everything an interpreter's state depends on. A *closure
boundary* is the earliest block from which replaying that state is
deterministic. The deleted old runtime called its cross-family operation a
*full-closure replay*; Stage B uses explicit interpret redo state instead.

**Companion rows** — the same-transaction raw context rows demanded for a
family-selected log (emitter watched under a source family, block inside that
entry's active window, topic0 in the family's manifest ABI): the transaction,
its receipt, and the emitter's code observation. Replay must see the same
context live intake saw, so checkpoint promotion verifies exactly those
companions for family-selected logs ("companion checks"). Same-transaction
sibling logs are retained as replay context too, but they are never required to
produce companions of their own.

**Consumer-replacement claim** — the assertion that bigname can replace a
specific consumer's existing indexer for a capability. It requires documented
routes, fixtures, and conformance evidence, and is never implied by coverage,
backfill, or manifest state.

**Contract instance** (`contract_instance_id`) — the stable identity of a
watched contract. Addresses are time-ranged attributes of an instance, a proxy
keeps its instance across implementation changes, and re-admitting an old
address reuses its prior instance with a new active range.

**Coverage frontier** (stored-lineage coverage frontier) — a schema-migration-era,
revision-checked old-runtime proof of which watched block intervals had
complete log-fetch coverage. Its tables remain in schema-migration history, but its
Rust writers, readers, and checkpoint-promotion path were deleted in Stage B.

**Declared vs verified** — *declared* state is what protocol-side observation
says: indexed onchain events, plus the documented hydration of event-silent
contracts from pinned calls (see Hydration, Event-silent). *Verified* state is
what actually executing resolution (e.g. through the ENS Universal Resolver)
returns. The v2 API returns schema-v2 live verified results without caching
them and persists only a direct-answer disagreement in the
[resolution divergence ledger](#resolution-divergence-ledger). The two are
never merged; `mode`/`source` selects which a route returns.

**Deployment epoch** (`deployment_epoch`) — the manifest label naming which
protocol deployment generation a source family belongs to (for example
`ens_v2_sepolia_post_audit`), so facts from different deployments of the same
protocol never mix silently.

**Deployment profile** — the single manifest tree a runtime loads
(`manifests/mainnet/` or `manifests/sepolia/`), which fixes its chains and
admitted contracts. One runtime, one profile. "Profile" also means resolver
profile, exact-name profile, or the `/v2/lookup` `profile=` parameter; always
qualify which one is meant.

**Derivation kind** — the persisted string naming which adapter pipeline
produced a normalized event (for example `ens_v1_unwrapped_authority`,
`ens_v2_registry_resource_surface`, `raw_log_preimage_observation`). These are
stored identifiers: define, never rename. "Unwrapped authority" is a historical
name kept because it is a stored identifier: that pipeline derives ownership
and control for ENSv1 and Basenames names alike, whether the name is registry-,
registrar-, or NameWrapper-held.

**Discovery graph / discovery edge** — the time-versioned indexability and
relationship graph that extends the manifest-declared contract graph. The
schema-v2 baseline constrains an edge's kind to five values: `resolver`,
`subregistry`, `proxy_implementation`, `registry_announcement`, and
[`migration`](#migration-edge-migration). An edge's kind decides whether it
admits an emitter or only records topology. In particular, a registry
announcement admits an ENSv2 registry independently of parent reachability,
while a subregistry edge records parent-child reachability without admitting
its target.

**Migration edge** (`migration`) — the fifth discovery edge kind. It is
[reserved surface](#reserved-surface): the schema-v2 baseline accepts the
value, and three manifest views explicitly exclude it from watch-plan,
resolver-profile, and code-hash-drift reasoning, but no adapter writes one. Why
it was allocated is not recorded anywhere; only the absence of a producer is
provable. Do not read its presence in the enum as evidence that migration
topology is tracked. Note that the older `public` schema built from
`migrations/` puts no constraint on the column at all, so this five-value list
describes the schema-v2 baseline rather than every row that has ever existed.

**Registry announcement edge** (`registry_announcement`) — an ENSv2 discovery
edge created when a contract emits `RegistryCreated()`. It makes the emitting
registry indexable from that event position. It does not assert parent-child
reachability or attach the registry to a name. `SubregistryUpdated` supplies
that separate relationship. (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L9 @ ens_v2@ccaeb58)

**Event-silent** — a contract that changes relevant state without emitting a
usable event (for example a legacy reverse resolver whose `name` value changes
with no log
(upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L311 @ ensnode@2017ae6)
(upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L316 @ ensnode@2017ae6)).
Event-silent state must be observed by pinned calls; it cannot be replayed
from logs. Retained direct-call observations do not carry the changed state —
the stored transaction shape does not decode which node was touched — they
only trigger hydration to recheck.

**Emancipated NameWrapper state** — the ENSv1 NameWrapper lifecycle state in
which `PARENT_CANNOT_CONTROL` is burned and `CANNOT_UNWRAP` is not. The parent
can no longer replace or modify the wrapped child, while the wrapped owner can
still unwrap it. (upstream: .refs/ens_v1/contracts/wrapper/README.md:L73 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/README.md:L75 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/README.md:L85 @ ens_v1@91c966f)
The API exposes this as `wrapper_state="emancipated"` only while the wrapper
expiry is not earlier than the served block timestamp. After that boundary,
NameWrapper reads the fuses and owner as zero, so `wrapper_state` is omitted.
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L848 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L849 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L852 @ ens_v1@91c966f)
For a `.eth` second-level name, the 90-day registrar grace period starts before
the stored wrapper expiry. Entering grace does not change this lifecycle value,
but the owner can no longer modify or transfer the name; per-token approval
remains separately governed by `CANNOT_APPROVE` until wrapper expiry.
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L48 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L218 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L221 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L820 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L825 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L127 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L132 @ ens_v1@91c966f)

**ENSv1→ENSv2 migration** — the on-chain move of an existing ENS name from the
ENSv1 contracts to the ENSv2 registries. It happens entirely on one chain, it
is per-name rather than a global state copy, and it is driven by a token
transfer: the holder sends the name's ENSv1 token (the registrar ERC-721, or
the NameWrapper ERC-1155) to a receiver contract, which retires the name's ENSv1
presence into the [Graveyard](#graveyard) and creates its ENSv2 entry in the same
transaction. Which receiver depends on the name's depth. A `.eth` second-level
name goes to a [migration controller](#migration-controller), and its ENSv2 entry
is the [premigrated](#premigration-reservation) reservation being claimed. A
subname goes to its already-migrated parent's
[migration registry](#migration-registry-wrapperregistry), which registers the
label outright — premigration reserved only second-level names, so there is
nothing to claim
(upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L212 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L224 @ ens_v2@ccaeb58).
Nothing about it is cross-chain; see [`upstream.md`](upstream.md#known-divergences)
for the stale upstream comment that says otherwise. **No bigname source family
is admitted for any of this yet** — the terms below describe the upstream
mechanism an adapter would have to handle, not implemented behavior. Distinct
from bigname's own *schema-migration* history; see the note at the top of this
file.

**Premigration reservation** — the pre-launch step that writes every existing
`.eth` second-level name into the new ENSv2 `.eth` registry as a placeholder
before anyone migrates. A batch tool registers each label with owner
`address(0)`
(upstream: .refs/ens_v2/contracts/src/registrar/BatchRegistrar.sol:L65 @ ens_v2@ccaeb58),
which makes the entry `RESERVED`: it has an expiry, a subregistry, and a
resolver, but no ERC-1155 token and no roles, and it emits `LabelReserved`
rather than `LabelRegistered`
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L462 @ ens_v2@ccaeb58).
The reserved expiry is the ENSv1 registrar expiry plus a bonus period of 62 days
and 1 second — the difference between ENSv1's 90-day grace and ENSv2's 28-day
grace, plus a second
(upstream: .refs/ens_v2/contracts/script/deploy-constants.ts:L216 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/script/deploy-constants.ts:L217 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/script/deploy-constants.ts:L218 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/script/deploy-constants.ts:L219 @ ens_v2@ccaeb58);
`ETHRenewerV1` recovers the ENSv1 expiry by subtracting that bonus back off the
reservation
(upstream: .refs/ens_v2/contracts/src/registrar/ETHRenewerV1.sol:L119 @ ens_v2@ccaeb58),
so the ENSv2 reservation outlives the whole ENSv1 lifetime including grace. The
consequence for an indexer: a name can carry ENSv2 expiry and resolver facts,
and no owner, while ENSv1 is still the authority for it. Reserved entries are
not registrations.

**Migration controller** — an ENSv2 contract that accepts a transferred ENSv1
token and performs that name's migration. There are two, split by whether the
name can still be unwrapped: "locked" means the `CANNOT_UNWRAP` fuse is burned
(upstream: .refs/ens_v2/contracts/src/migration/libraries/LibMigration.sol:L76 @ ens_v2@ccaeb58),
which is exactly what makes an unwrap revert
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1023 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1024 @ ens_v1@91c966f).
`UnlockedMigrationController` takes unwrapped registrar ERC-721s
(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L92 @ ens_v2@ccaeb58)
and unlocked wrapped `.eth` 2LDs — it rejects a locked name, and it also
rejects anything whose token id is not the namehash of a `.eth` second-level
label, so subnames never reach it
(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L143 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L144 @ ens_v2@ccaeb58).
`LockedMigrationController` takes locked wrapped `.eth` 2LDs. Both inherit their
ERC-1155 receiver from a shared base
(upstream: .refs/ens_v2/contracts/src/migration/AbstractWrapperReceiver.sol:L101 @ ens_v2@ccaeb58).

Neither controller receives subnames. Both are bound to `.eth`: the locked
controller's wrapped node is fixed to `ETH_NODE`
(upstream: .refs/ens_v2/contracts/src/migration/LockedMigrationController.sol:L81 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/src/migration/LockedMigrationController.sol:L82 @ ens_v2@ccaeb58),
and the shared receiver requires each incoming name to be an immediate child of
that node
(upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L117 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L118 @ ens_v2@ccaeb58).
A child migrates into its already-migrated parent's
[migration registry](#migration-registry-wrapperregistry) instead, which is the
same receiver bound to the parent's node
(upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L197 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L287 @ ens_v2@ccaeb58).
The batch helper routes accordingly: locked 2LD groups go to the locked
controller
(upstream: .refs/ens_v2/contracts/src/migration/MigrationHelper.sol:L121 @ ens_v2@ccaeb58),
while child groups are looked up by parent name and sent to that parent's
registry, reverting if the parent has not migrated
(upstream: .refs/ens_v2/contracts/src/migration/MigrationHelper.sol:L124 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/src/migration/MigrationHelper.sol:L126 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/src/migration/MigrationHelper.sol:L131 @ ens_v2@ccaeb58).
Attributing a child migration to the locked controller would name the wrong
receiver and the wrong registry.
A separate `MigrationHelper` batches many names through operator approval
(upstream: .refs/ens_v2/contracts/src/migration/MigrationHelper.sol:L94 @ ens_v2@ccaeb58).
Name collision worth guarding against: ENSv1 also ships a `MigrationHelper`, a
controller-gated wrapper-migration tool with an entirely different interface
(upstream: .refs/ens_v1/contracts/utils/MigrationHelper.sol:L32 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/utils/MigrationHelper.sol:L51 @ ens_v1@91c966f),
and it has its own mainnet deployment artifact
(upstream: .refs/ens_v1/deployments/mainnet/MigrationHelper.json:L2 @ ens_v1@91c966f),
so harness or manifest tooling that keys contracts by artifact basename across
both pins will resolve the wrong one.

**Migration registry** (`WrapperRegistry`) — the ENSv2 registry contract
deployed for one specific migrated **locked** name, to hold that name's
children. Only the locked branch of migration creates one: whichever contract
receives the transfer deploys a proxy through `VerifiableFactory.deployProxy`
(upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L149 @ ens_v2@ccaeb58)
using the name's namehash as the salt
(upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L151 @ ens_v2@ccaeb58),
then binds it as the name's subregistry, which emits `SubregistryUpdated`
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L472 @ ens_v2@ccaeb58).
The receiver is the [migration controller](#migration-controller) for a locked
`.eth` 2LD, but for a locked child it is the parent's own migration registry:
`WrapperRegistry` inherits the same receiver
(upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L30 @ ens_v2@ccaeb58),
so the deployment above runs with the parent registry as the caller. Naming the
controller as the deployer is wrong for every level below the second. A
*non-locked* [helper-positive child](#migratable-child) gets no registry at
all — a locked child does get one, per the split in that entry — because that
branch
unwraps the name and registers it with whatever subregistry the caller supplied
(upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L186 @ ens_v2@ccaeb58).

Each registry is a token receiver in its own right, so it is where that name's
children migrate to, and it deploys its children's registries from its own
*current* implementation
(upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L88 @ ens_v2@ccaeb58) —
not from a fixed one, because the implementation address it forwards is baked
into whichever implementation the proxy points at today
(upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L53 @ ens_v2@ccaeb58)
and these registries are upgradeable in place: migration grants `ROLE_UPGRADE`
into every migrated registry's root bitmap
(upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L220 @ ens_v2@ccaeb58),
and an upgrade is bounded only by membership in an allowlist of approved
implementations
(upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L280 @ ens_v2@ccaeb58).
For discovery this means the set of ENSv2 registries is open-ended and grows by
contract deployment — one per migrated locked name, at any depth, all sharing a
single implementation *family* rather than one implementation — instead of being
fixed by manifest declaration. Code that identifies these registries by a single
expected implementation address will miss upgraded ones.

**Migratable child** — a child of an already-migrated name whose label its
parent's [migration registry](#migration-registry-wrapperregistry) will not let
anyone register
(upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L170 @ ens_v2@ccaeb58),
so the child cannot be taken on the ENSv2 side and keeps resolving through
ENSv1 for as long as it stays unmigrated. Three conditions must hold at once,
and failing each one means something different:

1. *Never registered on ENSv2* — the label has never had an entry, live or
   lapsed, in the parent's ENSv2 registry
   (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L293 @ ens_v2@ccaeb58).
   Failing this is permanent: once the label has had an entry, ENSv2 is its
   authority and the protection never comes back. What becomes of the label is
   then decided on the ENSv2 side alone — a registered entry blocks
   registration with `LabelAlreadyRegistered`
   (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L437 @ ens_v2@ccaeb58),
   a reserved one is claimable only by a holder of `ROLE_REGISTER_RESERVED`
   (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L442 @ ens_v2@ccaeb58) —
   the premigration-claim path migration itself takes
   (upstream: .refs/ens_v2/contracts/src/migration/LockedMigrationController.sol:L110 @ ens_v2@ccaeb58) —
   while re-reserving it reverts `LabelAlreadyReserved`
   (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L439 @ ens_v2@ccaeb58),
   and only an expired ENSv2 entry can be registered again
   (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L428 @ ens_v2@ccaeb58).
2. *`PARENT_CANNOT_CONTROL` burned* — the child is *helper-positive* under
   `LibMigration.isEmancipatedChild`, the superset the three-way split below is
   built on
   (upstream: .refs/ens_v2/contracts/src/migration/libraries/LibMigration.sol:L88 @ ens_v2@ccaeb58).
   Failing this means the child was never emancipated: the register test is
   false, so the label can be taken out from under a name that is still live on
   ENSv1
   (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L303 @ ens_v2@ccaeb58),
   and migration reverts as well
   (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L188 @ ens_v2@ccaeb58).
3. *Nonzero ENSv1 registry owner* — the second half of that same test
   (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L303 @ ens_v2@ccaeb58).
   Failing this means the name was abandoned, and releasing the label is the
   designed end state rather than a loss. Such a child cannot migrate either: a
   zero registry owner means the name is not wrapped, because wrapping puts the
   NameWrapper itself in that slot
   (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L580 @ ens_v1@91c966f),
   so no ERC-1155 is left to transfer.

The `PARENT_CANNOT_CONTROL` condition is nonetheless broader than the
[emancipated NameWrapper state](#emancipated-namewrapper-state) defined above,
and this is the one place in the migration cluster where "emancipated" must not
be read as that state. Beyond that fuse, `isEmancipatedChild` requires only that
the name is not a `.eth` 2LD
(upstream: .refs/ens_v2/contracts/src/migration/libraries/LibMigration.sol:L88 @ ens_v2@ccaeb58);
it never consults `CANNOT_UNWRAP`, so it is *also* true of a
[locked](#locked-namewrapper-state) child. Three states therefore have to be
kept apart, because two of them migrate by different paths:

- *Helper-positive child* — any child satisfying `isEmancipatedChild`. This is
  the superset that "migratable child" is defined over, and it is not a
  migration path in itself.
- *Locked child* — helper-positive and `CANNOT_UNWRAP` burned
  (upstream: .refs/ens_v2/contracts/src/migration/libraries/LibMigration.sol:L76 @ ens_v2@ccaeb58).
  The receiver tests locked **first**
  (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L127 @ ens_v2@ccaeb58),
  so such a child never reaches the emancipated branch: its ERC-1155 moves to
  the [Graveyard](#graveyard) with no unwrap
  (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L144 @ ens_v2@ccaeb58)
  and it gets a [migration registry](#migration-registry-wrapperregistry) of its
  own
  (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L149 @ ens_v2@ccaeb58).
- *Non-locked helper-positive child* — the true emancipated state, and the only
  one that reaches the second branch
  (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L176 @ ens_v2@ccaeb58).
  It is unwrapped into the Graveyard
  (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L178 @ ens_v2@ccaeb58)
  and injected into the parent's existing registry with no registry deployed for
  it
  (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L186 @ ens_v2@ccaeb58).

Anything else reverts
(upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L188 @ ens_v2@ccaeb58).
Reading the no-deploy rule as covering every helper-positive child is the
mistake this three-way split exists to prevent.

The unmigrated state is a legitimate steady state,
not a transient one: a name tree can have a parent whose registration is
ENSv2-authoritative and a child whose control and records stay
ENSv1-authoritative, and nothing terminates that split automatically — it ends
only when the child itself migrates, or when it is abandoned. Note the test is
on fuses, not on being currently wrapped, and NameWrapper deliberately keeps
fuses and expiry when it burns a token
(upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L276 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L277 @ ens_v1@91c966f),
so a child that unwraps after its parent migrated still counts
as migratable — and is stuck, because migration needs a NameWrapper token to
transfer and it no longer has one. It stays blocked until its ENSv1 registry
owner is zeroed — condition 3 above failing — which is what releases the
label
(upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L303 @ ens_v2@ccaeb58).

**Graveyard** — the ENSv2 contract that ends up holding the dead ENSv1 side of
every migrated name. **What it receives, and what is left behind, differs by
migration path**, and an ENSv1 adapter that assumes one shape will expect the
wrong events and the wrong terminal state:

- *Unwrapped `.eth` 2LD.* The controller reclaims the name, rewrites the ENSv1
  registry record so the Graveyard owns it and the resolver is cleared, then
  transfers the registrar ERC-721 to the Graveyard
  (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L111 @ ens_v2@ccaeb58)
  (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L112 @ ens_v2@ccaeb58)
  (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L114 @ ens_v2@ccaeb58)
  (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L115 @ ens_v2@ccaeb58)
  (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L118 @ ens_v2@ccaeb58).
- *Unlocked wrapped `.eth` 2LD.* Resolver cleared, then unwrapped straight out
  of NameWrapper into the Graveyard
  (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L146 @ ens_v2@ccaeb58)
  (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L147 @ ens_v2@ccaeb58).
- *Locked wrapped name.* ENSv1 ownership does not move: the NameWrapper stays the
  registry owner and the name stays wrapped. Only the ERC-1155 moves to the
  Graveyard, fuses intact, with no unwrap and no burn
  (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L144 @ ens_v2@ccaeb58),
  so a wrapper adapter watching only transfers would keep calling the name
  `wrapped` after it has migrated. The resolver is conditional, and the two
  branches differ in whether ENSv1 is written at all. With `CANNOT_SET_RESOLVER`
  unburned, migration clears the name's **ENSv1** resolver through the wrapper
  (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L135 @ ens_v2@ccaeb58)
  (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L136 @ ens_v2@ccaeb58),
  which reaches the ENSv1 registry and emits `NewResolver(node, 0)`
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L670 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L93 @ ens_v1@91c966f)
  — the one ENSv1 registry write a locked migration makes, and one an adapter
  could easily mistake for an unrelated user action. With the fuse burned, ENSv1
  is left untouched and the name's existing ENSv1 resolver is carried over as its
  **ENSv2** resolver, swapped for the new `PublicResolver` when the carried-over
  one is a recognized old `PublicResolver`
  (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L138 @ ens_v2@ccaeb58)
  (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L140 @ ens_v2@ccaeb58).
- *Emancipated child, in the narrow non-locked sense
  ([three-way split](#migratable-child)) — a locked child takes the bullet
  above instead.* Resolver cleared, then unwrapped into the Graveyard
  (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L177 @ ens_v2@ccaeb58)
  (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L178 @ ens_v2@ccaeb58).

The deployment script's activation sequence also adds the Graveyard as an ENSv1
registrar controller alongside `ETHRenewerV1`
(upstream: .refs/ens_v2/contracts/script/setup.ts:L873 @ ens_v2@ccaeb58),
which is what lets its permissionless `clear` entrypoint
(upstream: .refs/ens_v2/contracts/src/migration/Graveyard.sol:L99 @ ens_v2@ccaeb58)
tidy leftover ENSv1 registry state. One more trap there: for a fully expired
name `clear` self-claims it from the registrar with a duration chosen to pin
expiry at `uint64` max minus the ENSv1 grace period
(upstream: .refs/ens_v2/contracts/src/migration/Graveyard.sol:L164 @ ens_v2@ccaeb58),
which surfaces as an ENSv1 `NameRegistered` naming the Graveyard as registrant
with an absurdly distant expiry.

**ETHRenewerV1** — the only renewal path left for a name that was
[premigrated](#premigration-reservation) but has not migrated. The deployment
script's activation sequence revokes every ENSv1 registration path as a
registrar controller
(upstream: .refs/ens_v2/contracts/script/setup.ts:L860 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/script/setup.ts:L866 @ ens_v2@ccaeb58)
and adds the Graveyard and this contract in their place
(upstream: .refs/ens_v2/contracts/script/setup.ts:L873 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/script/setup.ts:L877 @ ens_v2@ccaeb58).
Renewal writes both sides: the shared registrar base extends the ENSv2 entry
(upstream: .refs/ens_v2/contracts/src/registrar/AbstractETHRegistrar.sol:L91 @ ens_v2@ccaeb58)
and this contract's hook extends the ENSv1 registrar expiry by the same
duration
(upstream: .refs/ens_v2/contracts/src/registrar/ETHRenewerV1.sol:L134 @ ens_v2@ccaeb58),
so the two stay in step until the name migrates — after which only the ENSv2
expiry moves and the ENSv1 value freezes forever. Its `syncWrapper` entrypoint
(upstream: .refs/ens_v2/contracts/src/registrar/ETHRenewerV1.sol:L106 @ ens_v2@ccaeb58)
pushes registrar expiry into NameWrapper by adding NameWrapper as an ENSv1
registrar controller and removing it again within the same call
(upstream: .refs/ens_v2/contracts/src/registrar/ETHRenewerV1.sol:L107 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/src/registrar/ETHRenewerV1.sol:L111 @ ens_v2@ccaeb58).
Any code that treats the ENSv1 controller set as static, or `ControllerAdded`
as a rare governance event, is wrong once this is live.

**v1 fallback resolver** (`ENSV1Resolver`, exposed as `V1_RESOLVER`) — the
ENSv2-side resolver that answers by reading ENSv1. It looks the name up in the
ENSv1 registry
(upstream: .refs/ens_v2/contracts/src/resolver/ENSV1Resolver.sol:L40 @ ens_v2@ccaeb58)
and forwards the resolve call to whatever resolver it finds there
(upstream: .refs/ens_v2/contracts/src/resolver/AbstractMirrorResolver.sol:L69 @ ens_v2@ccaeb58).
It is the resolver the premigration tooling writes onto every reservation
(upstream: .refs/ens_v2/contracts/script/preMigrationUtils.ts:L52 @ ens_v2@ccaeb58),
and a
[migration registry](#migration-registry-wrapperregistry) returns it for any
child that has not migrated
(upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L183 @ ens_v2@ccaeb58),
where it is held as an immutable set at deploy time
(upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L40 @ ens_v2@ccaeb58).
Its effect is that a name resolves correctly through the ENSv2 tree while ENSv1
is still its authority, so attributing which version served a resolution is
time-dependent and cannot be read off the resolver pointer alone.

**Exact-name profile** (`exact_name_profile`) — the per-manifest capability
flag that, when `supported`, makes declared exact-name reads authoritative for
that deployment profile. Today the only family whose active manifest carries
`supported` is the ENSv2 Sepolia registrar; the flag also exists in `shadow`
elsewhere (for example the mainnet ENSv1 registrar). It promotes nothing else.

**Generation** (raw-log retention generation) — a schema-migration-era per-chain
counter used by old-runtime destructive raw-log repair and backfill coverage.
The schema remains in schema-migration history, but Stage B has no Rust writer or
coverage consumer for this counter.

**Hash-pinned** — anchored to an exact block hash rather than a block number or
`latest` tag, so a chain reorganization cannot silently change what was read.

**Hydration** — a projection-owned repair pass that fills current-state values
by making hash-pinned RPC calls (for example legacy reverse-resolver names or
missing text values). Hydration writes only projection rows: no normalized
events, verified output, reusable outcomes, or execution traces. Verified
lookup always reads the newly selected projection state and executes for that
request.

**Input revision** (raw-log input revision) — a schema-migration-era per-chain counter
used by the old runtime's raw-log mutation fence and replay caches. Its Rust
writer and consumers were deleted before the legacy schema was dropped.

**Block-revision evidence floor** — the schema-migration-era lower bound used by the
old runtime's raw-log revision evidence. Its tables remain historical; the
Stage B runtime no longer computes or consumes this floor.

**Latest-only** — semantics where only the current value is observable and
history cannot be reconstructed reliably (for example event-silent reverse
resolver state).

**Lease** — (1) an ENS registrar registration with an expiry (standard ENS
usage)
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L10 @ ens_v1@91c966f).

**Lineage mutation revision** — a schema-migration-era per-chain counter and evidence
trail used by old-runtime stored-lineage coverage and adapter checkpoint reuse.
The migration-owned database objects remain, but their Rust consumers were
deleted in Stage B.

**Lineage orphaning epoch** — the per-chain counter on the current head row
that increases whenever head publication moves previously readable blocks to
`orphaned`. Interpretation uses an unchanged value to reuse prior-state
lineage checks; a changed value requires every retained prior-state block
anchor to be checked again.

**Locked NameWrapper state** — the ENSv1 NameWrapper lifecycle state selected
by the `CANNOT_UNWRAP` fuse. Locking requires emancipation and allows further
owner-controlled permissions to be revoked. (upstream: .refs/ens_v1/contracts/wrapper/README.md:L87 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/README.md:L91 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/README.md:L93 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/README.md:L95 @ ens_v1@91c966f)
The API exposes this as `wrapper_state="locked"` only while the wrapper expiry
is not earlier than the served block timestamp; after that boundary the
NameWrapper reads both owner and fuses as zero and `wrapper_state` is omitted.
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L848 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L849 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L852 @ ens_v1@91c966f)
For a `.eth` second-level name, entering the registrar grace period does not
change `wrapper_state`, but it removes the remaining owner modification and
transfer powers before the later wrapper expiry.
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L48 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L218 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L221 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L820 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L825 @ ens_v1@91c966f)

**Non-name form** — a string a route puts in a name-typed field for a label
bigname cannot state as a name. Registry events prove a child node and its
labelhash without proving the label, so some children have no name to serve;
rather than omit the row or return null, the read composes a readable stand-in.
Two exist today, both on `GET /v2/names/{name}/subnames`: the placeholder
`[<labelhash-without-0x>].<parent-name>` for a label never observed, and, for a
label observed as bytes that are not valid UTF-8 or that contain a NUL, the
PostgreSQL `escape` encoding of the whole stored child name — the parent portion
included, since the encoding runs over the whole byte string. Neither is
reserved syntax — a label really spelled that way produces the same string — so
a caller distinguishing rows should use `namehash` and `labelhash`, not the
served text. A non-name form is not addressable and may not be fed back into a
name-shaped route.

**Normalized event** — the current-interpretation-epoch record of one semantic
protocol transition, carrying identity, provenance, optional chain position,
and before/after state. Normal forward interpretation appends events. For a
block-anchored, chain-derived event, canonical readability comes from its
block-anchor join to chain lineage, never from row-local canonicality alone. A
reorg orphans the losing lineage and thereby excludes its retained event rows
until the required bounded interpret redo deletes and re-derives the range.
Unanchored manifest-control events such as `SourceManifestUpdated` instead use
their row-local finalized state because they have no chain observation anchor
and are not reorg-addressable. Durable raw facts and competing chain lineage,
not superseded normalized events, are the permanent audit trail for chain
events. The current event stream is what projections consume. See the [reorg
and redo boundary](storage.md#reorg-and-redo-boundary).

**Path class / support class** — the classification of a resolution's shape
that decides which verified answers are publicly supported. Direct, alias-only,
wildcard-derived, and transport-assisted are the classes most relevant to
refusal semantics, not a closed list: the docs also classify shapes such as
ancestor-selected, linked-subregistry, CCIP-participating, transport-free, and
offchain-gateway. A class is "frozen": fixed at admission and re-derived from
stored inputs before any outcome persists as supported.

**Plain-events redo** — the bounded interpret-redo model in which normalized
events carry no revision or supersession history. A reorg leaves losing event
rows physically present but unreadable through their orphaned lineage until
required redo starts; redo then deletes and re-derives its selected range from
readable raw facts. Durable raw facts and competing chain lineage preserve the
audit trail instead of accumulating stale normalized derivations across
interpreter versions.

**Preimage observation / label preimage** — learning the human-readable string
behind a name or label hash, from an event, a retained name surface, or a
rainbow-table import. Every preimage is proof-checked: the stored labelhash is
the keccak256 of the raw label bytes, and a candidate that does not re-hash to
its claimed labelhash is rejected. The normalization verdict is stored as a
flag, not used as an admission gate. A preimage improves display only; it never
creates ownership, resolver, record, or primary-name truth.

**Projection** — a disposable read-model table rebuilt deterministically from
canonical facts and normalized events (standard event-sourcing usage);
resource-keyed rows additionally require the event's resource to resolve to a
canonical identity row at rebuild time. The schema-v2 Project phase is the only
projection writer.

**Raw facts** — the stored record of what was observed on chain: selected
logs and the transaction/receipt fields needed to decode them. Their content is
append-only, edited only by explicit, documented corrections;
`canonicality_state` is mutable operational state — ordinary reorg repair
reclassifies a losing branch's rows as `orphaned` without touching content.
Provider responses used by hydration or request-scoped lookup are not raw
facts.

**Readable / read-safe** — a row whose canonicality is `canonical`, `safe`, or
`finalized`. `observed` and `orphaned` rows are excluded from public reads and
kept as audit input; internal invalidation and reorg-repair machinery still
consumes them. Readability is a statement about block canonicality only, not
about support: a readable row may still carry an unsupported support status,
and routes that additionally require supported rows say so. `POST /v2/lookup`
reverse address results are one such route
([api-v2.md](api-v2.md#cursors-and-pagination)).

**Reserved surface** — a schema value, enum variant, or documented field that
the system accepts and can render but that no code path ever produces. It
exists because some earlier design allocated it, and removing it would cost a
schema migration or an API-contract change for no behavioral gain. Reserved
surface is not deferred work and not a partially built feature: absent a named
producer, it will never appear in a response. Current examples are the
[`migration` discovery edge](#migration-edge-migration) and the
`migration_derived` and `transport_derived` permission scopes. Anything
reserved should say so where it is documented, so a reader does not mistake it
for coverage. Don't add new fixtures or exemplars that make reserved surface
look produced; existing `migration_derived` exemplars are retained deliberately
because that scope has a real mechanism it may yet bind to, while
`transport_derived` does not. A test that pins the *read* path is not an
exemplar in that sense: proving a stored row carrying a reserved value still
decodes asserts that the retained surface keeps working, not that anything
produces it. Guarding the absence of a producer, and guarding that the retained
reader still accepts the value, are both fair game — publishing the value as
expected output is not.

**Resolver profile** — a declared resolver classification. ENSv1 and
Basenames use an exact resolver-address declaration; ENSv2 requires the
proxy's latest canonical ERC-1967 `Upgraded` event to name a declared
implementation. Classification permits supported projection of retained
canonical normalized observations, but does not assert exhaustive history or
event-to-call parity. Unknown or mismatched resolvers are explicitly
unsupported. See [source manifests](manifests.md#required-fields).

**Resolution divergence ledger** — the schema-v2 audit table whose active rows
record only when a direct, hash-pinned resolution answer disagrees with the
exact indexed record entry used for comparison. It is not a result cache:
agreement creates no divergence but may clear a matching active row, wildcard
resolution without an exact comparison row writes nothing, and any answer that
used CCIP-read never writes or clears a row. A mutation succeeds only while
the compared projection row and its canonical block lineage remain unchanged.

**Resource** (backing resource, `resource_id`) — the authority object behind a
name: a registry entry, registrar lease, wrapper position, or ENSv2 EAC
resource
(upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L38 @ ens_v2@ccaeb58).
Permissions and control history key to the resource, never to the name string
or token id.

**Retained-history proof** — a schema-migration-era ENSv2 tuple (retention generation,
discovery-admission epoch, proven-through block) used by the deleted
full-closure replay. Its SQL history remains, but Stage B has no Rust writer or
consumer for this proof.

**Rewind horizon** — the earliest chain position reorg repair might need to
rewind to. Compaction and pruning must never delete data needed at or behind
it.

**Run shape** — how one interpret walk executes over its input: fresh (from
the start of the chain), incremental (continuing from retained prior events),
or resumed (continuing from a persisted progress marker after an interruption,
including an interrupted redo's persisted intermediate state).
Batch-independence rules require identical surviving rows in every run shape
over identical input. That identity is verified for the ENSv1 divergence
classes [#336](https://github.com/ensdomains/bigname/issues/336) catalogued,
with two known open exceptions of distinct kinds:
[#348](https://github.com/ensdomains/bigname/issues/348) is a batch-boundary
divergence, on the ENSv2 resolver path — an attribution an uninterrupted walk
keeps and a walk continued across a batch boundary does not reproduce;
[#347](https://github.com/ensdomains/bigname/issues/347) is not a
boundary-carry artifact — the uninterrupted walk under-derives a wrapper
authority lapse that a walk split into two shapes derives.

**Shadow** — (1) manifest rollout/capability value: facts may be interpreted
but general public reads are not enabled; (2) *shadow comparison*:
running a new read surface in parallel with an existing one and diffing
responses during a migration (the identity route's `profile=shadow`).

**Sidecar** — a retired legacy companion-table pattern that precomputed
reverse-identity counts and feed rows with database triggers. Those tables were
operational summaries, never protocol truth, and were removed with the old
schema. See the superseded ADR 0005.

**Source family** — a named group of contracts on one chain that owns one slice
of protocol authority (for example `ens_v1_registrar_l1`). The unit of manifest
admission, capability ownership, replay coverage, and provenance attribution.

**Surface (name surface)** — an on-chain name identity
(`logical_name_id = namespace:namehash`), distinct from whatever authority
currently backs it. Raw labels and their normalization flags are observations,
not identity; display names are derived when read, following the audit's
[normalization-as-a-gate decision](../simplification-audit-20260730.md#normalization-as-a-gate-not-stored-identity).
A **surface binding** is the time-ranged record of which resource backed a
surface when. Surfaces survive re-registration; resources rotate.


**Token lineage** (`token_lineage_id`) — the continuity of tokenized ownership
across token-id changes (for example ENSv2 token regeneration, where a role
change burns and mints a replacement token while leaving the resource unchanged
(upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L82 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L528 @ ens_v2@ccaeb58)).
It rotates only when authority moves to a different tokenized anchor. A return
to the exact prior tokenized anchor — for example unwrapping back to a
still-live lease — resumes that anchor's prior lineage, but not after release
or across mismatched holder/controller authority: a name that fully lapses and
is re-registered mints a new lineage.

**Transport** — in `/v2/lookup` topology, the field describing a resolution
that was served across a chain boundary: `{source_chain_id, target_chain_id,
contract_address, latest_event_kind}`, all `null` when no chain boundary was
crossed. The only path that populates it is Basenames, whose names live on Base
while an L1 Resolver on Ethereum Mainnet answers for them
(upstream: .refs/basenames/README.md:L69 @ basenames@1809bbc),
answering `base.eth` directly
(upstream: .refs/basenames/src/L1/L1Resolver.sol:L166 @ basenames@1809bbc)
(upstream: .refs/basenames/src/L1/L1Resolver.sol:L167 @ basenames@1809bbc)
and reverting `OffchainLookup` for anything below it
(upstream: .refs/basenames/src/L1/L1Resolver.sol:L173 @ basenames@1809bbc).
Do not confuse this with two unrelated uses of the same word: network transport
failures (a provider's DNS, TLS, or connection error, which abort a request
before persistence), and the `transport_derived` permission scope, which is
[reserved surface](#reserved-surface) from an abandoned cross-chain ENSv2
design and has no producer. There is no `transport` discovery edge kind.

**Verified lookup** — request-scoped resolution or primary-name verification
that calls admitted contracts at the selected block identity. It creates no
durable execution trace or reusable outcome. Its only possible durable output
is the guarded resolution divergence ledger for an eligible direct
live/indexed comparison.

**Walking skeleton** — the standard XP term for a minimal end-to-end path
proving all layers connect. In this repo it names the first e2e scenario
(`register_eth_name`); prefer "the first end-to-end scenario" in prose.

**Watch plan / watched tuple** — the materialized set of
(source family, address, active block range) targets derived from manifest
declarations plus indexability-producing discovery edges. Topology-only edges,
including ENSv2 subregistry edges, do not add targets. A *watched tuple* is one
such entry; its *watched window* is the active block range. Addresses are
derived watch targets, never the durable identity.

**Wrapped NameWrapper state** — the ENSv1 NameWrapper lifecycle state in which
the wrapper manages the name and issues its ERC-1155 token, but neither
`PARENT_CANNOT_CONTROL` nor `CANNOT_UNWRAP` provides protection. The parent can
still modify or reclaim the name, and the wrapped owner can unwrap it.
(upstream: .refs/ens_v1/contracts/wrapper/README.md:L65 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/README.md:L67 @ ens_v1@91c966f)
The API exposes this as `wrapper_state="wrapped"`. Passing the stored wrapper
expiry clears effective fuses but does not remove a plain wrapped name or this
state. (upstream: .refs/ens_v1/contracts/wrapper/README.md:L99 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/README.md:L101 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/README.md:L103 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/README.md:L109 @ ens_v1@91c966f)
