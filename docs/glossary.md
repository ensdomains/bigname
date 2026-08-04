# Glossary

Plain-language definitions for bigname-specific terms. Standard ENS, Ethereum,
and indexing vocabulary is used unmodified and is not defined here. Wire field
names and enum values are contract, not prose; this glossary explains concepts,
it does not rename fields. Docs should link here on a term's first use instead
of re-defining or assuming it.

Two terms are overloaded enough that bare use is discouraged: **promotion**
(always qualify: checkpoint promotion vs. capability promotion) and **profile**
(always qualify: deployment profile, resolver profile, exact-name profile, the
identity route's `profile=` parameter, or the `/v1/profiles/...` route).

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

**Admission epoch** (discovery-admission epoch) — a migration-era per-chain
counter used to fence old-runtime watch-plan reconciliation. The table remains
in immutable migration history, but Stage B manifest synchronization and
interpretation have no Rust writer or consumer for this counter; phase locks,
content hashes, and explicit redo state now fence derived work.

**Anchor** — the concrete object a stable identity is pinned to. An *authority
anchor* is the registry entry, registrar lease
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L10 @ ens_v1@91c966f),
wrapper position
(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L27 @ ens_v1@91c966f),
or ENSv2 resource
(upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L38 @ ens_v2@48b3e2d)
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

**Backfill coverage fact** — a migration-era record that asserted one completed
old-runtime backfill job fetched all matching logs over one block interval.
The tables remain in migration history, but the Stage B runtime has no writer,
repair path, or checkpoint-promotion consumer for these records.

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

**Coverage frontier** (stored-lineage coverage frontier) — a migration-era,
revision-checked old-runtime proof of which watched block intervals had
complete log-fetch coverage. Its tables remain in migration history, but its
Rust writers, readers, and checkpoint-promotion path were deleted in Stage B.

**Declared vs verified** — *declared* state is what protocol-side observation
says: indexed onchain events, plus the documented hydration of event-silent
contracts from pinned calls (see Hydration, Event-silent). *Verified* state is
what actually executing resolution (e.g. through the ENS Universal Resolver)
returns. The retained v1 API persists verified outcomes with full execution
traces until Stage C; its schema-v2 successor returns the live result without
caching it and persists only a direct-answer disagreement in the
[resolution divergence ledger](#resolution-divergence-ledger). The two are
never merged; `mode`/`source` selects which a route returns.

**Deployment epoch** (`deployment_epoch`) — the manifest label naming which
protocol deployment generation a source family belongs to (for example
`ens_v2_sepolia_post_audit`), so facts from different deployments of the same
protocol never mix silently.

**Deployment profile** — the single manifest tree a runtime loads
(`manifests/mainnet/` or `manifests/sepolia/`), which fixes its chains and
admitted contracts. One runtime, one profile. "Profile" has four other meanings
in this repo — see Resolver profile, Exact-name profile, the identity route's
`profile=` parameter, and the `/v1/profiles/names/{name}` route; always qualify
which one is meant.

**Derivation kind** — the persisted string naming which adapter pipeline
produced a normalized event (for example `ens_v1_unwrapped_authority`,
`ens_v2_registry_resource_surface`, `raw_log_preimage_observation`). These are
stored identifiers: define, never rename. "Unwrapped authority" is a historical
name kept because it is a stored identifier: that pipeline derives ownership
and control for ENSv1 and Basenames names alike, whether the name is registry-,
registrar-, or NameWrapper-held.

**Discovery graph / discovery edge** — the time-versioned indexability and
relationship graph (resolver, registry announcement, subregistry, parent,
alias, metadata, proxy/implementation, migration, transport edges) that extends
the manifest-declared contract graph. An edge's kind decides whether it admits
an emitter or only records topology. In particular, a registry announcement
admits an ENSv2 registry independently of parent reachability, while a
subregistry edge records parent-child reachability without admitting its target.

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

**Exact-name profile** (`exact_name_profile`) — the per-manifest capability
flag that, when `supported`, makes declared exact-name reads authoritative for
that deployment profile. Today the only family whose active manifest carries
`supported` is the ENSv2 Sepolia registrar; the flag also exists in `shadow`
elsewhere (for example the mainnet ENSv1 registrar). It promotes nothing else.

**Generation** (raw-log retention generation) — a migration-era per-chain
counter used by old-runtime destructive raw-log repair and backfill coverage.
The schema remains in migration history, but Stage B has no Rust writer or
coverage consumer for this counter.

**Hash-pinned** — anchored to an exact block hash rather than a block number or
`latest` tag, so a chain reorganization cannot silently change what was read.

**Hydration** — a projection-owned repair pass that fills current-state values
by making hash-pinned RPC calls (for example legacy reverse-resolver names or
missing text values). Hydration writes only projection rows: no normalized
events, no verified output, no execution traces. A hydration write or delete
that changes a primary-name claim also invalidates the matching persisted
verified answer, so verified readback re-verifies instead of serving a stale
outcome.

**Input revision** (raw-log input revision) — a migration-era per-chain counter
used by the old runtime's raw-log mutation fence and replay caches. Its Rust
writer and consumers were deleted in Stage B. This is distinct from projection
input revisions that surviving worker replay still uses.

**Block-revision evidence floor** — the migration-era lower bound used by the
old runtime's raw-log revision evidence. Its tables remain historical; the
Stage B runtime no longer computes or consumes this floor.

**Latest-only** — semantics where only the current value is observable and
history cannot be reconstructed reliably (for example event-silent reverse
resolver state).

**Lease** — (1) an ENS registrar registration with an expiry (standard ENS
usage)
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L10 @ ens_v1@91c966f);
(2) a worker's time-limited, reclaimable claim on a unit of work such as a
backfill range or projection invalidation (standard distributed-systems
usage). Context disambiguates; both senses are intentional.

**Lineage mutation revision** — a migration-era per-chain counter and evidence
trail used by old-runtime stored-lineage coverage and adapter checkpoint reuse.
The migration-owned database objects remain, but their Rust consumers were
deleted in Stage B.

**Normalized event** — the append-only, interpret-phase record of one semantic
protocol transition, carrying identity, provenance, chain position, and
before/after state. The event stream, not raw logs, is what projections
consume. Cross-reference: event sourcing.

**Path class / support class** — the classification of a resolution's shape
that decides which verified answers are publicly supported. Direct, alias-only,
wildcard-derived, and transport-assisted are the classes most relevant to
refusal semantics, not a closed list: the docs also classify shapes such as
ancestor-selected, linked-subregistry, CCIP-participating, transport-free, and
offchain-gateway. A class is "frozen": fixed at admission and re-derived from
stored inputs before any outcome persists as supported.

**Preimage observation / label preimage** — learning the human-readable string
behind a name or label hash, from an event, a retained name surface, or a
rainbow-table import. Every preimage is proof-checked (normalize, re-hash,
compare) and improves display only; it never creates ownership, resolver,
record, or primary-name truth.

**Projection** — a disposable read-model table rebuilt deterministically from
canonical facts and normalized events (standard event-sourcing usage);
resource-keyed rows additionally require the event's resource to resolve to a
canonical identity row at rebuild time. Only projection workers write
projections, with the documented sidecar exception.

**Projection replay-version fence** — the database-enforced minimum compiled
replay version for transactions that claim or write current projection work.
Each process stamps its database connections with its compiled version.
Projection writers hold the shared side of the singleton lock; a replay
transaction holds the exclusive side while it activates or raises the minimum.
This exclusive step is called *replay admission*, and a process that implements
the connection stamp and transaction checks is *fence-aware*. A stamp below the
committed minimum or a missing stamp cannot commit protected writes. Stamped
invalidation-queue DML running at `READ COMMITTED` is the narrow lock-free
exception: it checks the committed minimum without waiting for concurrent
replay admission because its durable row and generation journal make it
post-replay apply work. The database rejects this exception at transaction
isolation levels with a longer-lived snapshot. A fence-aware worker exits on an
outdated-version refusal or any other fatal fence failure, including missing
singleton state and an invalid stamp. Only a current, validly stamped writer
that loses the non-waiting admission race retries. A binary from before the
fence existed cannot acquire a stamp and remains blocked, but may keep retrying
until its operator or supervisor replaces it.

**Raw-code baseline** — the capped per-chain sweep that records at least one
non-orphaned code observation for each address in the active watch plan. Each
chain receives the configured address budget per poll tick. Observations are
durable; the process-local cursor may safely restart by rechecking the stored
prefix without repeating provider calls for observations already present.

**Raw facts** — the stored record of what was observed on chain: selected
logs, the minimal transaction/receipt fields needed to decode them, code-hash
observations, and pinned call snapshots. Their content is append-only, edited
only by explicit, documented corrections; `canonicality_state` is mutable
operational state — ordinary reorg repair reclassifies a losing branch's rows
as `orphaned` without touching content.

**Readable / read-safe** — a row whose canonicality is `canonical`, `safe`, or
`finalized`. `observed` and `orphaned` rows are excluded from public reads and
kept as audit input; internal invalidation and reorg-repair machinery still
consumes them.

**Resolver profile** — a declared resolver classification. ENSv1 and
Basenames use an exact resolver-address declaration; ENSv2 requires the
proxy's latest canonical ERC-1967 `Upgraded` event to name a declared
implementation. Classification permits supported projection of retained
canonical normalized observations, but does not assert exhaustive history or
event-to-call parity. Unknown or mismatched resolvers are explicitly
unsupported. See [source manifests](manifests.md#required-fields).

**Resolution divergence ledger** — the schema-v2 audit table that records only
when a direct, hash-pinned resolution answer disagrees with the exact indexed
record entry used for comparison. It is not a result cache: agreement writes
nothing, wildcard resolution without an exact comparison row writes nothing,
and any answer that used CCIP-read is never stored. A write succeeds only while
the compared projection row and its canonical block lineage remain unchanged.

**Resource** (backing resource, `resource_id`) — the authority object behind a
name: a registry entry, registrar lease, wrapper position, or ENSv2 EAC
resource
(upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L38 @ ens_v2@48b3e2d).
Permissions and control history key to the resource, never to the name string
or token id.

**Retained-history proof** — a migration-era ENSv2 tuple (retention generation,
discovery-admission epoch, proven-through block) used by the deleted
full-closure replay. Its SQL history remains, but Stage B has no Rust writer or
consumer for this proof.

**Rewind horizon** — the earliest chain position reorg repair might need to
rewind to. Compaction and pruning must never delete data needed at or behind
it.

**Shadow** — (1) manifest rollout/capability value: facts and traces are
written but general public reads are not enabled; (2) *shadow comparison*:
running a new read surface in parallel with an existing one and diffing
responses during a migration (the identity route's `profile=shadow`).

**Sidecar** — a small companion table maintained by database triggers (the
reverse-identity count and feed rows) that precomputes hot-path answers. A
bounded, documented exception to the projection-worker-only write rule; never
protocol truth. See ADR 0005.

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
(upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L82 @ ens_v2@48b3e2d)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L528 @ ens_v2@48b3e2d)).
It rotates only when authority moves to a different tokenized anchor. A return
to the exact prior tokenized anchor — for example unwrapping back to a
still-live lease — resumes that anchor's prior lineage, but not after release
or across mismatched holder/controller authority: a name that fully lapses and
is re-registered mints a new lineage.

**Verified execution / execution trace** — verified execution runs actual
resolution calls. An execution trace is the retained v1 subsystem's durable
step-by-step audit record (entrypoint, calls, CCIP steps, proofs, result).
Those traces are permanent except for the bounded ENS/60 missing-tuple
retention rule in [`storage.md`](storage.md#execution-storage); otherwise only
legacy cache reusability expires. A retained v1 outcome is reused only while
its request tuple, selected chain positions, manifest versions, topology
boundary, and record boundary still match; reorgs and manifest, resolver,
topology, record, or primary-claim changes evict affected entries. A
verified-primary route-local result for an absent projection tuple has no
projected topology or record identity; its two execution-cache boundary fields
explicitly carry the selected checkpoint instead. The schema-v2 successor
creates neither traces nor reusable outcomes; its only durable execution-side
output is the resolution divergence ledger.

**Walking skeleton** — the standard XP term for a minimal end-to-end path
proving all layers connect. In this repo it names the first e2e scenario
(`register_eth_name`); prefer "the first end-to-end scenario" in prose.

**Watch plan / watched tuple** — the materialized set of
(source family, address, active block range) targets derived from manifest
declarations plus indexability-producing discovery edges. Topology-only edges,
including ENSv2 subregistry edges, do not add targets. A *watched tuple* is one
such entry; its *watched window* is the active block range. Addresses are
derived watch targets, never the durable identity.
