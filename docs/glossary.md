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
that separate relationship. For a registry created through the
[ENSv1→ENSv2 migration source family](manifests.md#ensv2-migration-family-admission-plan), the
edge remains ordinary and the watch plan traverses it while a separate
`migration_registry_creation` candidate association carries consumer-visibility
provenance. The association never turns indexability itself into name authority.
(upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L9 @ ens_v2@ccaeb58)

**Event-silent** — a contract that changes relevant state without emitting a
usable event (for example a legacy reverse resolver whose `name` value changes
with no log
(upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L311 @ ensnode@2017ae6)
(upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L316 @ ensnode@2017ae6)).
Event-silent state must be observed by pinned calls; it cannot be replayed
from logs. Retained direct-call observations do not carry the changed state —
the stored transaction shape does not decode which node was touched — they
only trigger hydration to recheck.

**Emitter-role-independent event** — a manifest ABI event whose adapter behavior and output do
not depend on which admitted contract role selected the event. Selection may clear that role only
for the finite `(source_family, event)` list documented in
[Manifest authoring](manifests.md#admission-selection-for-addresses-with-multiple-declared-roles);
events outside that list must declare `emitter_roles`, except for the documented ENSv2 registry
announcement case.

**Emancipated NameWrapper state** — bigname's ENSv1 NameWrapper lifecycle label
for a currently wrapped name where `PARENT_CANNOT_CONTROL` is burned and
`CANNOT_UNWRAP` is not. The parent can no longer replace or modify the wrapped
child, while the wrapped owner can still unwrap it. NameWrapper rejects parent
replacement while `PARENT_CANNOT_CONTROL` is effective and rejects later
parent fuse changes after that bit is burned.
(upstream: .refs/ens_v1/contracts/wrapper/README.md:L75 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/README.md:L81 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L726 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L730 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L547 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L553 @ ens_v1@91c966f)
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

**Expiry-effective NameWrapper fuse word** — the uint32 fuse value that
NameWrapper's `getData` read returns at the served block timestamp, rather than
the expiry-unadjusted value retained in normalized events. That normalized value
is interpreted state: on an unexpired rewrap, it includes the retained
parent-controlled bits as well as the emitted fuse argument.
(upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L235 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L248 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L901 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L902 @ ens_v1@91c966f)
When wrapper expiry is earlier than the served timestamp, NameWrapper clears
every effective fuse. It also clears the owner when `PARENT_CANNOT_CONTROL` was
burned, which removes an expired
[emancipated](#emancipated-namewrapper-state) or
[locked](#locked-namewrapper-state) lifecycle value; a plain
[wrapped](#wrapped-namewrapper-state) value remains because its owner is not
cleared. The [projection](#projection) rebuild applies this serving convention
to current summaries at its target timestamp and does not rewrite the
normalized value.
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L143 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L153 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L856 @ ens_v1@91c966f)

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
for the stale upstream comment that says otherwise. The
`ens_v2_migration_l1` [source family](#source-family) admits
the fixed contracts and event shapes described in
[`manifests.md`](manifests.md#ensv2-migration-family-admission-plan); public
mixed-history ownership remains capability-gated separately. Distinct from
bigname's own *schema-migration* history; see the note at the top of this file.

**Migration boundary** (ENSv1→ENSv2 authority boundary) — the candidate
`MigrationApplied` normalized event records the position at which one logical
name can stop taking current registration and control from ENSv1 and start
taking them from its ENSv2 resource. Slice 1 records that position as a
candidate with `consumer_visibility=candidate`; it does not close the current
ENSv1 `SurfaceBinding` or make the ENSv2 binding eligible for current
selection. Slice 2 re-derives the candidate with
`consumer_visibility=activated` and performs that deferred binding transition
at the recorded position without deleting ENSv1 history. A candidate or
activated boundary is derived only from the complete admitted successful
ENSv1→ENSv2 migration shape for that name, never from family coexistence or a
transaction hash alone. Descendants keep their own authority until they reach
their own activated boundary or obtain a current registration in the admitted
migration registry below that migrated parent; the latter does not invent a
child boundary. (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L169 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L172 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L290 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L303 @ ens_v2@ccaeb58) The unlocked path transfers the
ENSv1 position to the
Graveyard before registering the reserved ENSv2 label, while the locked
receiver moves the wrapper token to the Graveyard and injects the ENSv2
registration.
(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L111 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L118 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L144 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L168 @ ens_v2@ccaeb58).

**Migration correlation group** — a deterministic set of raw evidence and
derived effects for one operation admitted through the ENSv1→ENSv2 migration
family. Per-name `correlation_kind` values are `authority_transition`,
`synchronized_renewal`, `graveyard_cleanup`, and
`migration_registry_creation`; the name-independent
`controller_configuration` kind covers a launch-bounded registrar controller
change that cannot be assigned to a name. Only an `authority_transition` group
with the complete ENSv1→ENSv2 migration shape can produce a candidate
[migration boundary](#migration-boundary); a renewal or cleanup group never
does. The stable correlation ID is derived from `correlation_kind`, logical
name when applicable, anchor position, and complete evidence set, never from the
transaction hash alone. A `controller_configuration` ID instead uses the
registrar emitter, controller account, and event kind in place of a logical
name. A later operation has a different ID.

Every dependent effect whose existence relies on the correlation carries a
sorted, duplicate-free `migration_correlation_ids` set and
`consumer_visibility`, including effects emitted under an existing source
family after ENSv1→ENSv2 migration-created registry discovery. A per-name effect
has one ID. One shared correlation-dependent event, such as a newly admitted
controller change surrounding several bridge labels, is stored once with every
participating synchronized-renewal ID and stays `candidate` until every
referenced group is activated. A name-independent controller event has one
`controller_configuration` ID. No event is duplicated or arbitrarily assigned
to one name. Unrelated facts in the same transaction do not join the set.

Independent admission has precedence: an ordinary normalized event that the
existing manifest and discovery rules produce without this correlation remains
byte-for-byte `activated` and product-visible. Slice 1 records its candidate
relationship separately in `migration_event_associations`; correlation never
duplicates, suppresses, or reclassifies the ordinary event. The same precedence
keeps an independently admitted `registry_announcement` edge ordinary and makes
it a watch-plan input; `migration_discovery_associations` attaches the candidate
relationship without changing that edge. Correlation-dependent downstream
normalized, identity, topology, permission, registration, and renewal effects
remain candidate and are invisible to Project and product history but available
to diagnostics. The association alone cannot reclassify output that the existing
registry family derives from the ordinary edge and raw event without migration
correlation; that output remains ordinary. Slice 2 re-derives the same groups with
`consumer_visibility=activated`; independently admitted event and announcement
rows remain unchanged. Replay under a fixed manifest set and [interpreter
content hash](#interpreter-content-hash) produces the same group IDs, event
identities, and payloads.

The separately reviewed slice-1 and slice-2 implementations deploy together
with [PR #391](https://github.com/ensdomains/bigname/pull/391) at one planned
[re-derivation boundary](#re-derivation-boundary). That boundary adopts one
interpreter content hash,
performs one full source re-walk, and makes one Project publication decision for
`ethereum-sepolia`. Candidate and activated states remain distinct replay and
acceptance-test inputs, but production makes only that activated Project
publication. Other chains retain independent
publication decisions.

**Premigration reservation** — the pre-launch step that writes every existing
`.eth` second-level name into the new ENSv2 `.eth` registry as a placeholder
before anyone migrates. In ENSv1→ENSv2 migration discussions, an **ENSv2
reservation** means this placeholder. A batch tool registers each label with owner
`address(0)`
(upstream: .refs/ens_v2/contracts/src/registrar/BatchRegistrar.sol:L65 @ ens_v2@ccaeb58),
which makes the entry `RESERVED`: it has an expiry, a subregistry, and a
resolver, but no ERC-1155 token and no roles, and it emits `LabelReserved`
rather than `LabelRegistered`
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L462 @ ens_v2@ccaeb58).
The reserved expiry is an explicit BatchRegistrar input, not a value the
registry derives. (upstream: .refs/ens_v2/contracts/src/registrar/BatchRegistrar.sol:L52 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registrar/BatchRegistrar.sol:L65 @ ens_v2@ccaeb58) The pinned premigration tool converts a configurable whole-day
bonus to seconds, defaults it to 62 days, and writes the ENSv1 registrar expiry
plus that value. (upstream: .refs/ens_v2/contracts/script/preMigration.ts:L973 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/script/preMigration.ts:L1035 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/script/preMigration.ts:L1265 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/script/preMigration.ts:L1267 @ ens_v2@ccaeb58) Separately, the deployment passes `ETHRenewerV1` a
62-day-and-1-second bonus computed from the two grace periods.
(upstream: .refs/ens_v2/contracts/script/deploy-constants.ts:L216 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/script/deploy-constants.ts:L217 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/script/deploy-constants.ts:L218 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/script/deploy-constants.ts:L219 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/deploy/03_ETHRenewerV1.ts:L38 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/deploy/03_ETHRenewerV1.ts:L39 @ ens_v2@ccaeb58).
Those defaults differ by one second. Bigname therefore preserves the emitted
reservation expiry and never reconstructs it from the renewal-bridge constant;
any deployment or test reservation with another explicit expiry retains that
value.
At its stored expiry the entry becomes `AVAILABLE`, and registry resolver reads
return zero
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L259 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L626 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L653 @ ens_v2@ccaeb58).
During the remaining ENSv2 28-day grace window, the status is `AVAILABLE` but
the registrar still treats the entry as in grace, and `ETHRenewerV1` can still
renew it
(upstream: .refs/ens_v2/contracts/src/registrar/ETHRegistrar.sol:L290 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/src/registrar/ETHRenewerV1.sol:L145 @ ens_v2@ccaeb58).
The consequence for an indexer: before that expiry, a name can carry ENSv2
expiry and resolver facts, and no owner, while ENSv1 is still the authority for
it. Reserved entries are not registrations.

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

The unmigrated state is a legitimate [mixed-authority
tree](#mixed-authority-tree), not a transient one. Note the test is on fuses,
not on being currently wrapped, and NameWrapper deliberately keeps
fuses and expiry when it burns a token
(upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L276 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L277 @ ens_v1@91c966f),
so a child that unwraps after its parent migrated still counts
as migratable — and is stuck, because migration needs a NameWrapper token to
transfer and it no longer has one. It stays blocked until its ENSv1 registry
owner is zeroed — condition 3 above failing — which is what releases the
label
(upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L303 @ ens_v2@ccaeb58).

**Mixed-authority tree** — a name tree in which an ancestor registration is
authoritative in ENSv2 while an unmigrated child's control and records remain
authoritative in ENSv1. This is a durable state, not an incomplete transaction:
the parent's migration registry blocks ordinary ENSv2 registration for the
child and returns the v1 fallback resolver instead
(upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L169 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L183 @ ens_v2@ccaeb58).
An expired ENSv2 ancestor returns no subregistry, so the fallback path is no
longer reachable
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L253 @ ens_v2@ccaeb58).
While the ENSv2 ancestor remains active and reachable, the split ends only if
the child migrates or its ENSv1 registry owner becomes zero, which releases the
protection
(upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L293 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L303 @ ens_v2@ccaeb58).

**ENSv1 husk** — the residual ENSv1 registry and token state left after the
corresponding ENSv2 registration becomes authoritative. It is not a second live
registration. An unwrapped `.eth` name leaves the ENSv1 registry and registrar
token with the [Graveyard](#graveyard)
(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L111 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L118 @ ens_v2@ccaeb58),
while a locked wrapped name keeps NameWrapper as the ENSv1 registry owner and
moves only its ERC-1155 token to the Graveyard
(upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L135 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L144 @ ens_v2@ccaeb58).
See [Graveyard state](#graveyard-state) for those terminal shapes.

**Graveyard state** — an [ENSv1 husk](#ensv1-husk) whose terminal token or
registry position is held by the [Graveyard](#graveyard). This bigname term is
not the `Graveyard.State` implementation enum
(upstream: .refs/ens_v2/contracts/src/migration/Graveyard.sol:L31 @ ens_v2@ccaeb58).
An unwrapped or unlocked-wrapped `.eth` 2LD path leaves both ENSv1 registry
ownership and the registrar ERC-721 with the Graveyard
(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L111 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L118 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L147 @ ens_v2@ccaeb58).
An emancipated non-locked child instead transfers its ENSv1 registry position
to the Graveyard during unwrap, then enters ENSv2 without a registrar ERC-721
(upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L178 @ ens_v2@ccaeb58)
(upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L186 @ ens_v2@ccaeb58).
The locked path transfers the wrapper ERC-1155 while NameWrapper remains the
registry owner
(upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L144 @ ens_v2@ccaeb58).

**Graveyard** — the ENSv2 contract that holds the [ENSv1 husk](#ensv1-husk) of
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

**Interpreter content hash** — the build-time identifier for checked-in inputs
that can change Interpret or Project output. Derived phase state records this
hash, and a binary with a different value must re-walk the complete retained
range before normal derived writes continue. It covers interpreter, projection,
manifest, ABI, normalization, provider-response decoding, and selected
dependency inputs as detailed under [interpretation
replay](storage.md#interpretation-replay). It is not an Ethereum contract
bytecode hash.

**Interpreter session** — the in-process state carried between consecutive
physical Interpret batches for one chain (`AdapterSession` in code). It holds
protocol topology and current authority needed by the next batch, plus a
bounded cache of persisted event values. It is disposable: a cold restore
rebuilds it from readable `normalized_events` rows.

**Interpreter state key** — the opaque string an adapter derives for one
before/after state stream. It combines the ENS namespace, source family, name
or resource identity, [state facet](#state-facet), and source-specific scope.
The exact key is stored in each current normalized event so a cache miss can
retrieve the latest readable `after_state` for that stream without replaying an
event range.

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
`orphaned`. Interpretation uses an unchanged value to reuse an interpreter
session. A changed value discards that session and rebuilds it from readable
rows. The value loaded with Interpret inputs is checked again in the write
transaction, fencing lineage changes between the input snapshot, any bounded
state-cache reload, and the final write even if the same block hashes become
readable again.

**State facet** — the part of an interpreter state key that groups normalized
event kinds which update one logical value stream. For example, registrar
grants, renewals, releases, and reservations share the `registration` facet;
permission grants and revocations share the `permission` facet.

**Logical discovery-edge identity** (`logical_edge_identity`) — the
rebuild-stable Keccak-256 identity of one fact-derived discovery-edge epoch. It
uses semantic manifest and edge fields plus the observation position, never a
sequence-assigned database ID. The exact tuple, encoding, domain separator, and
rendering are defined in [ADR 0002](adrs/0002-surface-resource-identity.md#discovery-edge-observation-identity).

**Locked NameWrapper state** — bigname's ENSv1 NameWrapper lifecycle label for
a currently wrapped name where `CANNOT_UNWRAP` is burned. NameWrapper rejects
unwrap when that fuse is effective. Burning an owner-controlled fuse requires
`PARENT_CANNOT_CONTROL` and `CANNOT_UNWRAP` together, so locked implies
[emancipated](#emancipated-namewrapper-state); locking then allows further
owner-controlled permissions to be revoked.
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1025 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1058 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1067 @ ens_v1@91c966f)
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

**Manifest-authority marker** — a
`manifest-authority:<authority-fingerprint>:<invalidation-token>` value that
manifest synchronization records in a derived phase's input-hash field when
the active manifest authority changes. The fingerprint identifies the desired
manifest set. The database mints a new invalidation token for every transition,
including a later return to the same desired set. The marker poisons ordinary
hash adoption until the required full redo begins. It proves that authority
changed; it does not prove that facts required by a widened watch plan were
fetched. When Interpret would discharge the marker, the operator must complete
the manifest widening procedure or confirm that nothing widened, then attest
with the current token. Finite cursors and readable lineage both prove only the
watch plan active when facts were loaded.

**Non-name form** — a string a route puts in a name-typed field for a label
bigname cannot state as a name. Registry events prove a child node and its
labelhash without proving the label, so some children have no name to serve;
rather than omit the row or return null, the read composes a readable stand-in.
Two exist today, both on `GET /v2/names/{name}/subnames`: the placeholder
`[<labelhash-without-0x>].<parent-name>` for a label never observed or whose
observed text fails ENSIP-15 normalization, and, for a
label observed as bytes that are not valid UTF-8 or that contain a NUL, the
PostgreSQL `escape` encoding of the whole stored child name — the parent portion
included, since the encoding runs over the whole byte string. A
normalization-failing label takes the placeholder rather than the escape form:
its decoded text is a valid string but not a name for the proven node, and
escaping it would serve the same misleading text. Neither is
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
flag, not used as an admission gate; it instead gates whether the decoded text
may serve as a name — see [non-name form](#non-name-form) for what serves when
it may not. A preimage improves display only; it never
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

**Re-derivation boundary** — a coordinated deployment point for changes that
alter interpreted or projected content. The boundary adopts one interpreter
content hash, performs the mandatory historical Ingest fetch for every range
added by the generated watch plan, runs Interpret from the earliest affected
source position through the fixed readable head, runs Project through that same
head, and makes one publication decision for that target chain. The boundary is
not a cross-chain transaction: a multi-chain deployment retains one independent
decision and readiness result per chain. A *full source re-walk* in this contract
means that complete Ingest, Interpret, and Project sequence; it is not an
Interpret-only replay. Production Verify follows Project publication and gates
readiness and traffic, not the already committed Project rows.

**Reserved surface** — a schema value, enum variant, or documented field that
the system accepts and can render but that no code path ever produces. It
exists because some earlier design allocated it, and removing it would cost a
schema-migration or an API-contract change for no behavioral gain. Reserved
surface is not deferred work and not a partially built feature: absent a named
producer, it will never appear in a response. Current examples are the
[`migration` discovery edge](#migration-edge-migration). Anything reserved
should say so where it is documented, so a reader does not mistake it for
coverage. Don't add fixtures or exemplars that make reserved surface look
produced.

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
Do not confuse this with network transport failures: a provider's DNS, TLS, or
connection error aborts a request before persistence. There is no `transport`
discovery edge kind.

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

**Wrapped NameWrapper state** — bigname's ENSv1 NameWrapper lifecycle label for
a name whose wrapper token has a nonzero owner and whose registry owner is the
NameWrapper, while neither `PARENT_CANNOT_CONTROL` nor `CANNOT_UNWRAP` provides
protection. The parent can still modify or reclaim the name, and the wrapped
owner can unwrap it. NameWrapper uses the same token-owner and registry-owner
conditions for its internal wrapped guard.
(upstream: .refs/ens_v1/contracts/wrapper/README.md:L65 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/README.md:L67 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1076 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1080 @ ens_v1@91c966f)
The API exposes this as `wrapper_state="wrapped"`. Passing the stored wrapper
expiry clears effective fuses but does not remove a plain wrapped name or this
state. (upstream: .refs/ens_v1/contracts/wrapper/README.md:L99 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/README.md:L101 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/README.md:L103 @ ens_v1@91c966f)
(upstream: .refs/ens_v1/contracts/wrapper/README.md:L109 @ ens_v1@91c966f)
