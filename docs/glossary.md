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

**Admission epoch** (discovery-admission epoch) — a per-chain counter that any
transaction changing what is watched must bump in that same transaction:
discovery-edge changes (insert, reactivation, window update, deactivation) and
manifest-declared changes (manifest entries, seeded addresses, declared start
blocks, rollout status). Long-running work records the epoch it started under
and fails closed if the epoch moved, instead of acting on stale authority.
Cross-reference: optimistic concurrency, fencing token.

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

**Backfill coverage fact** — a durable record that one completed backfill job
fetched all matching logs over one block interval, at one of two scopes: one
(source family, address) pair, or the whole family — a family-scoped row means
every address of the source family is covered by a topics-complete fetch.
Checkpoint promotion composes these facts into gap-free proof instead of
re-deriving coverage from job definitions.

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

**Closure** — everything an adapter's internal state depends on. A *closure
boundary* is the earliest block from which replaying a stateful adapter is
deterministic; *full-closure replay* replays all participating source families
together from that boundary. Batching and paging are physical I/O details and
never create closure boundaries.

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

**Coverage frontier** (stored-lineage coverage frontier) — a saved,
revision-checked proof of which watched block intervals already have complete
log-fetch coverage, so checkpoint promotion re-verifies only new or changed
intervals instead of all history. It proves fetch coverage only; lineage and
fork checks still run per promotion.

**Declared vs verified** — *declared* state is what protocol-side observation
says: indexed onchain events, plus the documented hydration of event-silent
contracts from pinned calls (see Hydration, Event-silent). *Verified* state is
what actually executing resolution (e.g. through the ENS Universal Resolver)
returns, persisted with a full execution trace. The two are never merged;
`mode`/`source` selects which a route returns.

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

**Discovery graph / discovery edge** — the time-versioned reachability graph
(resolver, subregistry, parent, alias, metadata, proxy/implementation,
migration, transport edges) that extends authority beyond directly declared
contracts. A discovered contract is authoritative only while reachable from an
active root.

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

**Generation** (raw-log retention generation) — a per-chain counter incremented
whenever raw-log history is destroyed: rows deleted, truncated, or updated in a
way that rewrites their identity or payload. Canonicality-only changes never
bump it. *Generation zero* means history never destroyed: the only state in
which "no stored row" proves "never happened". After any destructive change,
absence claims require fresh generation-scoped backfill coverage.

**Hash-pinned** — anchored to an exact block hash rather than a block number or
`latest` tag, so a chain reorganization cannot silently change what was read.

**Hydration** — a projection-owned repair pass that fills current-state values
by making hash-pinned RPC calls (for example legacy reverse-resolver names or
missing text values). Hydration writes only projection rows: no normalized
events, no verified output, no execution traces. A hydration write or delete
that changes a primary-name claim also invalidates the matching persisted
verified answer, so verified readback re-verifies instead of serving a stale
outcome.

**Input revision** — a per-chain counter advanced by every semantic raw-log
change (insert, payload/identity change, canonicality change, delete,
truncate). Caches record the revision they saw; a later revision touching
consumed history invalidates them. Commit order, not timestamps or row ids, is
the authority.

**Latest-only** — semantics where only the current value is observable and
history cannot be reconstructed reliably (for example event-silent reverse
resolver state).

**Lease** — (1) an ENS registrar registration with an expiry (standard ENS
usage)
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L10 @ ens_v1@91c966f);
(2) a worker's time-limited, reclaimable claim on a unit of work such as a
backfill range or projection invalidation (standard distributed-systems
usage). Context disambiguates; both senses are intentional.

**Normalized event** — the append-only, adapter-produced record of one semantic
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

**Resolver profile** — an explicit admission that a resolver implementation
behaves like a known family (an ENS Labs PublicResolver generation, or the
Basenames `L2Resolver`). Profile admission gates complete record coverage and
event-to-call parity; unknown resolvers stay `pending` and expose only observed
facts.

**Resource** (backing resource, `resource_id`) — the authority object behind a
name: a registry entry, registrar lease, wrapper position, or ENSv2 EAC
resource
(upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L38 @ ens_v2@48b3e2d).
Permissions and control history key to the resource, never to the name string
or token id.

**Retained-history proof** — the ENSv2 tuple (retention generation,
discovery-admission epoch, proven-through block) that authorizes treating
stored root/registry history as complete for closure replay. Destroying raw
logs clears it; recovery requires generation-scoped backfill coverage.

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

**Surface (name surface)** — the public name string as an identity
(`logical_name_id = namespace:normalized_name`), distinct from whatever
authority currently backs it. A **surface binding** is the time-ranged record
of which resource backed a surface when. Surfaces survive re-registration;
resources rotate.

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

**Verified execution / execution trace** — running actual resolution calls and
persisting a durable step-by-step audit record (entrypoint, calls, CCIP steps,
proofs, result). Traces are permanent; only cache reusability expires. A
persisted outcome is reused only while its request tuple, selected chain
positions, manifest versions, topology boundary, and record boundary still
match; reorgs and manifest, resolver, topology, record, or primary-claim
changes evict affected entries.

**Walking skeleton** — the standard XP term for a minimal end-to-end path
proving all layers connect. In this repo it names the first e2e scenario
(`register_eth_name`); prefer "the first end-to-end scenario" in prose.

**Watch plan / watched tuple** — the materialized set of
(source family, address, active block range) targets derived from manifest
roots plus active discovery edges. A *watched tuple* is one such entry; its
*watched window* is the active block range. Addresses are derived watch
targets, never the durable identity.
