# Chain Intake

Chain intake is split into explicit per-chain phases. The checked-in runtime
implements `ingest`, `interpret`, `project`, read-only `verify`, and continuous
`live` follow. The old monolithic indexer, its provider reconciliation loop,
its persisted backfill scheduler, and its normalized-event replay driver are no
longer part of the source tree.

The architecture model lives in [`architecture.md`](architecture.md), storage
ownership in [`storage.md`](storage.md), projection behavior in
[`projections.md`](projections.md), and manifest and discovery authority in
[`manifests.md`](manifests.md).

## Implemented phase boundary

For each configured chain, the path is:

1. `ingest` resolves provider heads and finite source ranges, fetches selected
   chain data, and writes lineage plus immutable [raw facts](glossary.md#raw-fact).
2. `interpret` waits on the ingested range and writes schema-v2 identity rows,
   discovery edges, and [normalized events](glossary.md#normalized-event).
3. `project` publishes the retained current projections from canonical identity
   and normalized events. When configured, it then applies
   [canonical-head hydration](glossary.md#hydration) to the two documented
   current projection surfaces.
4. `verify` scans canonical selected raw logs through a frozen finalized
   boundary and compares them with the chain's configured reference. Base dRPC
   records `cross_checked` only through the Coinbase-to-dRPC ingest seam;
   Ethereum reth records `node_checked`.
5. `live` follows a provider snapshot from the completed ingest handoff, walks
   backward to a stored readable ancestor, loads at most one bounded winning
   suffix batch, and publishes the resulting head through the shared head path.

The runner persists phase and per-source cursors. A phase advances only through
the exact block-number/hash markers returned by its implementation. `interpret`
never fetches missing provider data or calls an old adapter; its input is the
raw-fact range already admitted by `ingest`. The project phase likewise reads
only canonical identity and normalized-event input.

After the initial spine completes, the live loop takes one provider snapshot,
fills its bounded gap, then advances or redoes `interpret` and `project` through
the published head before polling again. Base uses the RPC member of its
Coinbase-SQL/RPC source pair for head follow; Ethereum uses the same local Reth
database provider as ingest. The live code reuses ingest's provider cache,
source validation, watch plan, fetch, and persistence path.
Before publishing a loaded suffix, live verifies that its stored parent path
reaches the common ancestor selected from the snapshot. A provider reorg between
the ancestry read and suffix read leaves the immutable observation stored but
returns a retryable result, so the next attempt starts from a fresh snapshot.
Those `observed` rows are intake staging and never become a normal spine target.
Successful head publication atomically marks conflicting observed suffix rows
orphaned through the proposed latest height without widening downstream redo for
rows that were never readable. Higher observations remain staging until a
provider snapshot proves or displaces them, so a crash before publication
restarts without manual database repair. If a
provider snapshot is lower than the published latest marker but its latest hash
still matches the published readable path at that height, live treats the batch
as provider lag and performs no publication, orphaning, or stamping. A hash
mismatch remains a genuine lower-head reorg and follows the normal publication
path.

The `verify` reader may overlap the live loop. It freezes its target at the
finalized marker while live continues toward the latest head. A chain
configured with `verify-before-live` instead completes that finite scan before
entering live follow. A mismatch is non-retryable and stops only that chain.

Manifest synchronization uses the schema-v2 repository and checks the selected
[deployment profile](glossary.md#deployment-profile) fingerprint against the
interpreter content hash before a phase runs. Manifest declarations and current
discovery edges determine admission and the watch filter. Discovery does not
infer missing historical facts: a newly admitted source must return to `ingest`
for its required range before `interpret` can derive it.

## Sources and range progress

`phase-runner run` accepts a comma-delimited chain list and source descriptors.
Each source descriptor has the form:

```text
CHAIN:KEY:KIND:SEED_BASIS:START_BLOCK=URL_ENV
```

The endpoint itself is read from `URL_ENV`. Source cursors are independent, so
one source cannot claim another source's range. The runner records the resolved
target and last processed block hash for each source; restart resumes from that
stored boundary.

Production source shape is exact: `ethereum-mainnet` has one local Reth DB
source, while `base-mainnet` has one Coinbase SQL historical source and one
dRPC source meeting at block `48,428,000`. Live follow uses only the chain block
provider from that already-validated set. Verification uses local reth for
Ethereum and dRPC as the independent reference for Base facts loaded from
Coinbase. The dRPC source kind is capped at `cross_checked`, and its independent
extent cannot pass the `48,428,000` seam because dRPC supplies intake after that
block; only a local reth source can report `node_checked`. Unsupported
combinations fail as configuration errors rather than falling back to another
provider or range.

## Download range planning

The ingest [watch plan](glossary.md#watch-plan--watched-tuple) gives each
contract address an inclusive block window. When a manifest version or
discovery update retires an address, that is local indexing bookkeeping, not an
onchain event. A retired manifest-declared address with a known end block
remains in download planning for the part of its bounded history that
intersects the requested range; without an end block, it is removed. For a
discovered address, a non-orphaned discovery edge with a known end block can
supply that bound: a retired address row without its own end remains in
download planning for the overlap with the edge's bounded history. It is
removed when no end block is available from either side, and likewise when
the edge itself was retired without one (a retired end-less edge excludes
its address regardless of the address row's own bound). These retirement rules apply when an explicit historical download
range overlaps the closed interval. Later live intake does not fetch that
interval after its end block.

The planner validates stored windows before clipping them to the requested
range. Orphaned discovery edges do not participate in fetching or validation.
A manifest declaration is a configuration error only when none of the stored
address ranges considered by planning has an effective start at or before its
end. When one contract instance has multiple non-overlapping address ranges,
one valid range is enough for the declaration to pass validation; any range
whose effective start remains after its end is omitted from fetching. A
non-orphaned discovery edge whose window does not overlap the discovered
address window is also a configuration error. Planning stops and names the
governing manifest and inconsistent bounds for either error.

Planning also refuses a range the source cannot serve. Before any window is
fetched, a local reth source reports its earliest available block — reth's
expired-history floor, raised to the lowest block its receipt static files still
cover — and planning fails as a configuration error naming that floor and the
requested range instead of reading a pruned window as empty coverage. Pruning
receipts deletes whole static-file ranges while leaving their headers readable
(upstream: .refs/reth/crates/prune/prune/src/segments/receipts.rs:L34 @ reth@88505c7f)
(upstream: .refs/reth/crates/prune/prune/src/segments/mod.rs:L41 @ reth@88505c7f),
and a deleted range reads back as no rows and no error
(upstream: .refs/reth/crates/storage/provider/src/providers/static_file/manager.rs:L1996 @ reth@88505c7f)
(upstream: .refs/reth/crates/storage/provider/src/providers/static_file/manager.rs:L1998 @ reth@88505c7f).

The rule is deliberately stricter than the reference client's own guard. reth
refuses an `eth_getLogs` range below its expired-history floor with
`PrunedHistoryUnavailable`
(upstream: .refs/reth/crates/rpc/rpc/src/eth/filter.rs:L584 @ reth@88505c7f)
(upstream: .refs/reth/crates/rpc/rpc/src/eth/filter.rs:L586 @ reth@88505c7f),
but that floor tracks the lowest transaction static file
(upstream: .refs/reth/crates/storage/provider/src/providers/static_file/manager.rs:L1221 @ reth@88505c7f)
(upstream: .refs/reth/crates/storage/provider/src/providers/static_file/manager.rs:L1224 @ reth@88505c7f),
so a node whose receipts were pruned while its transactions were kept passes the
guard, and each of its receipt-less blocks then contributes no logs rather than
an error
(upstream: .refs/reth/crates/rpc/rpc/src/eth/filter.rs:L1265 @ reth@88505c7f)
(upstream: .refs/reth/crates/rpc/rpc/src/eth/filter.rs:L1272 @ reth@88505c7f).
Intake reads receipts directly, so it refuses there too. That widening is
recorded in `docs/upstream.md` § Known divergences. The receipt floor is read
from the static files on disk, so it bounds nothing on a node that keeps
receipts in database tables
(upstream: .refs/reth/crates/storage/provider/src/either_writer.rs:L188 @ reth@88505c7f)
(upstream: .refs/reth/crates/storage/provider/src/either_writer.rs:L190 @ reth@88505c7f),
whose row-wise prune checkpoints are not read. On that configuration only the
expired-history floor applies and a pruned receipt window can still be recorded
as covered.

Historical ingest is judged on the source's declared start block, not on how far
its cursor has advanced, so a resumed run whose cursor already stands above the
floor is refused too: planning cannot tell coverage recorded before the node
pruned from coverage recorded through a pruned window, and refuses both until
the node holds the declared range again or the declared start block moves. A
redo is judged on its own range instead — clipped to the source's declared start
— so a redo entirely above the floor still runs on a pruned node, and a redo
ending below a source's declared start plans nothing for that source and is
unaffected. Work already recorded is not re-examined: a completed ingest phase
is not planned again, so a chain that recorded a pruned window before this rule
existed keeps that stored coverage; a resync or a redo over the same range is
refused rather than silently repeating the empty read, so the node has to hold
the range before it can be re-indexed. Live follow extends the published head rather than planning a
declared range and does not consult the floor. Sources that do not read a node's
database report no floor: an RPC endpoint owns its retention behind the wire,
and the Coinbase SQL warehouse is not a block provider at all.

## Reorgs and required downstream redo

Head publication marks a displaced readable suffix orphaned and invalidates
affected execution-cache eligibility atomically. If that suffix starts at or
below the recorded `interpret` or `project` cursor, the same transaction stamps
the affected phase's existing redo state from the first orphaned block through
that cursor. The next live cycle runs the stamped `interpret` range and then the
stamped `project` range before either phase advances normally. If a provider's
latest marker temporarily falls below the old downstream cursor, live keeps
polling and fills the winning path through the stamped upper bound before redo
starts. On process restart, the live advisory lock also fences recovery of a
`running` live row left between atomic head publication and phase completion.

A successful interpret redo also stamps project for the same actual replayed
suffix. This includes an operator-requested data repair where the canonical
block hash did not change. The stamp is a persisted range in
`chain_phase_state`, not a repair queue or scheduler. Existing interrupted or
operator-requested redo state is extended rather than silently replaced.
System-owned state distinguishes a pending stamp from an active replay. Pending
stamps may coexist only while live fills the required winning range or the
runner selects the next dependency; once replay starts, that phase occupies the
normal writer slot and excludes every other non-verify writer.
Because interpret replaces normalized events before project runs, project redo
also retains keys from current-row provenance when a cited normalized event is
no longer readable. That deletion scope includes both the reverse event and
claim event cited by a primary-name tuple, so an event-only losing-fork update
cannot survive merely because its resource or name identity predates the redo
range.

## Redo and rewind

`phase-runner redo` runs a finite historical range through the existing phase
implementation and its per-chain writer lock:

```sh
cargo phase -- redo \
  --chain <chain> \
  --phase <ingest|interpret|project|verify|recompute-flags|all> \
  --from-block <inclusive-start> \
  --to-block <inclusive-end>
```

Ingest and `all` require a `--source` descriptor for every selected chain.
Verify requires exactly one `drpc` or `reth_db` source per selected chain;
`all` must satisfy that same verify-source rule. Verify and `all` also require
the SELECT-only verification database URL. More than one `--chain` may be
supplied. `--all-chains` is separate sugar that discovers every chain with an
active synchronized manifest and applies the same phase selection and range
through the ordinary per-chain path.

`--phase all` means all four finite phases: ingest, interpret, project, and
verify in dependency order. If Interpret widens a
partial request through its recorded head, its downstream redo stamp carries
that widened range into Project. Project still owns canonical-head hydration;
there is no standalone hydrate phase. Any already-pending redo must be
completed before `--phase all`, so the all-phases shorthand cannot consume or
clear unrelated operator work. A phase failure leaves its normal durable redo
marker, reports the exact phase-specific recovery command, and stops the
remaining phases for that chain. Complete that phase-specific redo, then rerun
`--phase all`. Historical live redo remains invalid because live is a head
follower. A multi-chain command continues with later chains and exits nonzero
with the collected chain failures; cancellation stops further chain dispatch.

Redo state is persisted and range-bound. An interpret redo prepares the
schema-v2 derived range, replays it from retained raw facts, and resumes after
interruption. Project redo uses the same state machinery to replace the
affected current projection scope. Neither path uses the deleted
normalized-event upsert, repair, supersession, adapter-checkpoint, or
coverage-authority machinery. Historical live redo is rejected because live
is a head follower. Verify redo uses the same scanner as normal verification,
rechecks the requested finalized range, and persists the level reported by the
phase. A partial redo retains the level for the full recorded extent; a
full-extent redo can report the level fixed by the reference source. Its source
and Base seam preflight happens before redo state is created. A mismatch retains
the resumable redo marker and its diagnosis;
rerunning the same command after wipe-and-resync repair resumes the attempt.
The range end must already be `canonical`, `safe`, or `finalized`; an
`observed` staging row is rejected before a redo session is claimed.
Flag recomputation is supported through `--phase recompute-flags`. Among
otherwise configured redo requests, only historical `live` redo and an
unreadable range end are rejected before a redo marker is written. These
preflight refusals and terminal verification failures cannot strand
unresumable redo state.

The thin rewind command moves only the published latest head:

```sh
cargo phase -- rewind \
  --chain <chain> \
  --ancestor-block <block> \
  --ancestor-hash <hash>
```

It takes the ingest, interpret, project, and live advisory locks so no head
publisher or downstream writer can overlap it, requires the exact ancestor to
be stored and readable, refuses to cross the safe head, and invokes normal head
publication. It does not write raw facts or normalized events. The resulting
orphaning stamps downstream redo; the next supervised run fills the winning
path before consuming those stamps.

The retained schema-v2 inspection windows are read-only `phase-runner`
subcommands alongside redo and rewind:

```sh
cargo phase -- inspect --database-url "$BIGNAME_DATABASE_URL" \
  block-canonicality --chain <chain> --from-block <n> --to-block <n>

cargo phase -- inspect --database-url "$BIGNAME_DATABASE_URL" \
  stored-lineage --chain <chain> --from-block <n> --to-block <n>

cargo phase -- inspect --database-url "$BIGNAME_DATABASE_URL" \
  raw-events --chain <chain> --from-block <n> --to-block <n>
```

Each command reads one bounded repeatable-read snapshot and emits JSON.
Block-canonicality labels every stored fork and reports raw-fact and
normalized-event counts. Stored-lineage includes optional
`chain_header_audit` fields. Raw-events joins retained logs to their raw
transaction, receipt, header-presence, lineage canonicality, and any matching
current-epoch normalized events. Orphaned lineage and retained raw facts remain
visible and explicitly labeled. Before required redo, matching losing-fork
normalized rows remain physically present but inherit unreadability from their
orphaned lineage; after redo, those superseded derivations are absent by design.
There are no API routes for these windows. Drift, cache, execution-trace, and
watch-plan inspection were cut and have no phase-runner replacements.

## Canonical-head hydration

Hydration runs as the final step of the project phase, after the event-derived
projection publication for the selected canonical head. It batches eligible
Ethereum calls through Multicall3 with an EIP-1898 block selector containing
the exact stored number and hash, then revalidates that `chain_heads` marker in
the publication transaction. If a bounded project redo publishes an older
cursor while `chain_heads` is already newer, hydration is deferred until
project reaches that exact current head; newer execution state is never layered
over an older event-derived projection target.

The candidate set is current-only:

- existing ENS/60 `primary_names_current` tuples whose latest canonical reverse
  claim and resolver edge select a configured legacy event-silent resolver (upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L311 @ ensnode@2017ae6) (upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L316 @ ensnode@2017ae6);
- supported ENSv1 text entries in `record_inventory_current` whose event did
  not retain a value, plus previously hydrated entries that need refresh.

Successful, absent, and invalid reverse-name results update only
`primary_names_current`. Successful or absent text results update only
`record_inventory_current.entries`. Provenance stores both the exact hydration
head and the event-derived fields replaced by hydration. A failed call or
whole Multicall/RPC batch restores every affected baseline in the same
head-revalidated publication transaction, removes the prior head's hydration
metadata, and keeps project retryable at the same head. If a previously
hydrated reverse tuple no longer selects a configured legacy resolver, the
same publication restores its baseline without issuing an ineligible call. No
hydration value is written to raw facts, identity rows, or normalized events,
and there is no
historical hydration pass. Advancing or replacing the canonical head causes
project to rebuild the affected event-derived scope and refresh the current
values at the new exact hash.

## Canonicality and replay facts

Block hash is identity and block number is position. [Canonicality](glossary.md#canonicality)
is stored explicitly in lineage and raw fact rows; consumers do not infer it
from row insertion order. Raw facts remain the durable input boundary.
Schema-v2 identity and normalized-event rows are derived output and can be
recreated by an explicit interpret redo over a complete ingested range.
Current projections can be rebuilt from those canonical inputs without
hydration; hydration is a separately reproducible head-only enrichment.

Execution may persist exact block-anchored call snapshots through the admitted
raw-fact boundary. That surviving path does not make execution a general chain
intake owner.

## Current Stage B limitation

The phase runner is now a continuously supervised ingest-through-live writer
with finalized
[stored-history verification](glossary.md#stored-history-verification), but it
is not yet the complete
replacement deployment. The API continues to read legacy public-schema
projections until the Stage C cutover. The surviving worker therefore
continues to serve its documented public-schema duties; it does not write the
schema-v2 project tables.

The historical `backfill_*`, `normalized_replay_*`, resolver-profile
reconciliation, raw-log revision/proof, and startup-checkpoint SQL tables remain
in migration history. This source tree has no old-runtime writer for them.
Storage exposes only the read paths still used by the worker or API, including
historical backfill-job inspection and the normalized replay cursor reads used
by the surviving public-schema projection and compaction boundary.
