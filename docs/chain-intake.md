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
4. `verify` freezes a finalized boundary. Only a [`verification-only`](glossary.md#source-role) source is independent. Base can compare with dRPC and record
   `cross_checked` only through the Coinbase-to-dRPC ingest seam, Ethereum
   Mainnet can compare with reth and record `node_checked`, and Ethereum
   Sepolia can record `cross_checked` with a distinct verification-only dRPC or
   `quick_synced` from its target-covering intake cursor without one.
   That cursor must cover the target. Its binding and coverage are checked when
   verification completes. The final returned block-number/hash marker must
   equal the frozen target before Verify records completion or Live can run.
   Every runner start checks the current source configuration and cursor again
   against that completion-time target; later Live finality does not extend the
   completed Verify extent.
5. `live` normally follows a provider snapshot from the completed ingest
   handoff, walks backward to a stored readable ancestor, loads at most one
   bounded winning suffix batch, and publishes the resulting head through the
   shared head path. Only recovery of an unreadable required Ingest end may use
   the published readable head when an interrupted finite Ingest recorded no
   handoff.

The runner persists phase and per-source cursors. A phase advances only through
the exact block-number/hash markers returned by its implementation. `interpret`
never fetches missing provider data or calls an old adapter; its input is the
raw-fact range already admitted by `ingest`. The project phase likewise reads
only canonical identity and normalized-event input.

Starting Ingest redo stamps overlapping Verify phase state with a recorded cursor
as [required redo](glossary.md#redo-marker-scope). Readiness degrades; any prior
level remains historical until the ordinary or continuous runner re-verifies.

When a non-retryable check of an already-completed Ingest or Verify phase
fails, the runner changes that phase from `completed` to `failed` and keeps its
completed range, source provenance, and verification evidence for diagnosis.
A Verify phase can reach the same failed state without ever becoming
`completed` when an ordinary, non-validation failure is recorded after its
final progress write but before phase completion. Only Ingest rows marked as a
completed-phase validation failure that still hold equal current, target, and
live-handoff block numbers and hashes, and Verify rows that still hold equal
current and target block numbers and hashes plus a retained verification level,
may be restored without replay. On the next start, the runner repeats the checks
for the retained completion and records the phase `completed` without replaying
its completed range. Rows without that structural proof resume their ordinary
phase work; error text alone never authorizes restoration. A stamped Verify row is not eligible for this shortcut.
At startup, the runner also probes the advisory locks for Interpret, Project,
and Verify. A `running` or `paused` row with no explicit redo is resolved only
while its advisory lock remains held. For such a row, a saved Interpret or
Project final checkpoint is recorded as `completed`; an earlier checkpoint is
marked `failed` and resumes through the ordinary phase path. A saved Verify
final checkpoint remains `failed` until the current verification configuration
and retained evidence pass the same checks as an already-completed Verify row.
A held lock still stops a second runner. The state update uses the same database
connection that holds the lock. If that connection is lost, the runner stops
and the next start reads the durable phase state again; this covers the case
where the client cannot tell whether PostgreSQL committed the update before the
connection failed. An unlock or connection-close error after an acknowledged
update is also reported.
After an explicit redo of a demoted Ingest or Verify row succeeds, the row
still shows `failed` until the next ordinary runner start repeats the retained
checks and records it `completed`.

Per-source cursors remain bounded by the finite ingest snapshot. Live reuses
the intake write path but does not claim that it extended every historical
source, so it does not advance those cursors. For later replay, a source cursor
proves only the part of the requested range through that source's persisted
target; complete readable lineage proves that the Live-loaded suffix contains
the facts selected by the [watch plan](glossary.md#watch-plan--watched-tuple)
active when each block was loaded. It does not prove facts required by a later
watch plan.

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
finalized marker while live continues toward the latest head. Every [provider-trusted verification](glossary.md#verification-level) plan completes that finite scan before entering Live, including reference-less Base, Ethereum Mainnet, and Sepolia. A Compared Base plan remains paired unless the chain is configured with `verify-before-live`. Ethereum-head intake derives that setting, so Mainnet and Sepolia remain serial even with a distinct verification-only reference. A mismatch is non-retryable and stops
only that chain.

Manifest synchronization uses the schema-v2 repository and checks the selected
[deployment profile](glossary.md#deployment-profile) fingerprint against the
[interpreter content hash](glossary.md#interpreter-content-hash) before a phase
runs. Manifest declarations and current discovery edges determine admission and
the watch filter. Discovery does not infer missing historical facts: a newly
admitted source must return to `ingest` for its required range before
`interpret` can derive it.

## Sources and range progress

`phase-runner run` accepts a comma-delimited chain list and source descriptors in this form:

```text
CHAIN:KEY:KIND:SEED_BASIS:START_BLOCK[:ROLE]=URL_ENV
```

The endpoint is read from `URL_ENV`; `ROLE` is `intake`, `verification-only`, or `both`, and omission defaults to `both`. Role tokens are exact: use `verification-only`, not `verification_only`; source-kind normalization does not apply to roles. Ingest, Live, cursor identity, and ingest progress receive only `intake` and `both` sources. A verification-only source receives no cursor and alone can earn independent `cross_checked` (dRPC) or `node_checked` (local reth); `both` earns only provider-trusted `quick_synced`. The runner rejects dRPC endpoints with the same parsed URL identity—including host-case, default-port, trailing-slash, and unreserved percent-encoding aliases—without logging them. It resolves relative reth paths from the process working directory and compares filesystem device and inode for both the configured datadir and each storage root the provider opens (`db`, `static_files`, and `rocksdb`); any shared object rejects independence, including symlink and bind-mount aliases. Missing or inaccessible roots fall back individually to canonical or lexical spelling identity; configured paths are not logged. No new verification level is introduced. A stronger level is downgraded when current roles support only `quick_synced`; retained `quick_synced` is not automatically upgraded. Changing intake membership requires the reset and [full source
re-walk](glossary.md#re-derivation-boundary) below. Source cursors are independent, so
one source cannot claim another source's range. The runner records the resolved
target and last processed block hash for each source; restart resumes from that
stored boundary. Before Ingest can make its first provider write, the runner
persists a cursor row containing the source kind, seed basis, and start block,
with no progress recorded. On every chain, that row makes the normalized source
kind immutable even while its progress fields are empty. Case-only changes,
surrounding whitespace, and hyphen/underscore spelling changes are equivalent;
any other kind change fails before Ingest runs or changes phase progress. Before
a runnable Ingest phase contacts a provider, the row's seed basis and start block
must also match the runtime source. A restart that skips an already-completed
Ingest phase applies the same check to every configured intake-capable source: its persisted
key, kind, seed basis, and start block must all still match, and no persisted
source key may be omitted from configuration. Interpret redo applies that exact
source-key set check before it rewrites derived data. A change to a persisted
identity field—source key, normalized kind, seed basis, or start block—requires
an explicitly reviewed reset that removes the cursor and every durable Ingest
output that may have come from that source, followed by a [full source
re-walk](glossary.md#re-derivation-boundary); it is never an in-place cursor
update. Changing only the provider endpoint is allowed because endpoints are
not persisted source identity and therefore do not trip the runtime identity
guard. An independent level attests only that the current verification-only
endpoint was excluded from intake for all facts retained since the last full
source re-walk under that endpoint-and-role configuration; it does not prove
independence across earlier endpoint values. Never assign an endpoint that
served intake during the retained walk to `verification-only`, even under a new
key. Doing so voids the `cross_checked` or `node_checked` claim despite the
current descriptors being distinct. First apply the same reviewed reset and
full source re-walk under the new configuration. The operator must preserve and
check that endpoint-rotation history because the runner cannot reconstruct it.
Retained raw facts, chain lineage, or
header-audit rows block creation
of any missing configured source row. The lineage and header rows remain even
when a loaded range contains no watched transactions, receipts, or logs. The
runner cannot distinguish a safe source addition from replacement of the
provider that supplied this retained output, so it requires the same reset
before Ingest runs. Because retained output does not identify its provider, an
explicitly reviewed reset and re-walk is required. For the Issue #411
transition, only the
[owner-ratified Sepolia source-role rollout](deployment.md#owner-ratified-sepolia-source-role-rollout),
its applicable reviewed reset and preservation procedure, and the owner-approved
rollback and restoration plan authorize that reset; the generic
verification-mismatch prose does not, and an ordinary redo is not a substitute.

Production intake shape is exact:
`ethereum-mainnet` has one local Reth DB
source, while `base-mainnet` has one Coinbase SQL historical source and one
dRPC source meeting at block `48,428,000`; either may add one distinct verification-only source of its supported kind. `ethereum-sepolia` has exactly one
dRPC intake source with `ethereum_head` seed basis and start block zero, plus zero or one verification-only dRPC with the same seed basis and start. The
runner will validate the Sepolia rule before Ingest creates a source cursor,
contacts the provider, or writes raw facts. Live follow uses only the chain
block provider from that
already-validated set. Verification uses a distinct local reth for Ethereum Mainnet and a second, distinct dRPC—the third Base source overall—as the independent reference for Base facts loaded from Coinbase. Without one, each chain records `quick_synced` from
its target-covering intake cursor. The dRPC
source kind is capped at `cross_checked`, and chain policy caps its independent extent at the `48,428,000` seam. A Base
`reth_db` reference is unsupported because the pinned reader uses reth's
Ethereum node type, whose signed transaction and receipt types are the Ethereum
primitives (upstream: .refs/reth/crates/ethereum/node/src/node.rs:L121 @ reth@88505c7f)
(upstream: .refs/reth/crates/ethereum/primitives/src/lib.rs:L27 @ reth@88505c7f)
(upstream: .refs/reth/crates/ethereum/primitives/src/lib.rs:L51 @ reth@88505c7f). Bigname does not
implement a separate OP Stack transaction and receipt reader.
Base-aware local database verification is tracked by
[issue #433](https://github.com/ensdomains/bigname/issues/433). Under the Issue #411
enforcement, a distinct verification-only Sepolia dRPC records
`cross_checked`; without one, Verify records `quick_synced` and never compares
intake with itself.
The target-covering Sepolia intake cursor must match the configured intake
source key, kind, seed basis, and start block and must cover the finalized
verification target. Verify checks
the binding and coverage when it completes, and the returned final marker must
exactly match the frozen target marker. The cursor's retained tip may later be
orphaned by a reorg above that target, but its stored parent chain must still
reach the exact frozen target hash. A fork at or below that block fails the
check. On every runner start, Verify repeats this check once against the
completion-time target and leaves the recorded `quick_synced` extent unchanged
when finality has moved.
Unsupported combinations fail as configuration errors rather than falling back to another provider or range.

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
requested range instead of reading a pruned window as empty coverage. The floor
is read a second time after a window is fetched and before it is stored, because
a node can prune while a batch is in flight; a window whose floor rose under it
fails the same way rather than being recorded. The live suffix is checked the same
way once its common ancestor is known, because extending the published head does
not imply starting above a floor that moved during downtime or a deep reorg.

Those refusals are an early, cheap answer, not the guarantee. The reported floor
is optimistic even on an idle node: reth advances a receipt static file's block
position before deciding whether to write that block's receipts
(upstream: .refs/reth/crates/storage/provider/src/providers/database/provider.rs:L2504 @ reth@88505c7f)
(upstream: .refs/reth/crates/storage/provider/src/providers/database/provider.rs:L2512 @ reth@88505c7f),
so the lowest retained range can begin with receipt-less blocks, and a receipt log
filter can drop individual receipts inside a block that was written
(upstream: .refs/reth/crates/storage/provider/src/providers/database/provider.rs:L2528 @ reth@88505c7f).
The guarantee is at the read: every fetched block whose receipt count does not
match the transaction count in its retained body indices fails the log read. That
covers what no floor can express — receipt-less blocks inside a retained range,
receipts pruned out of database tables, and a partial receipt list, which would
otherwise attribute logs to the wrong transaction. Pruning
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
whose row-wise prune checkpoints are not read. On that configuration the floor
falls back to expired history alone, and the receipt-count check is what stops a
pruned window from being recorded: any block whose bloom admits a watched event
fails the read rather than contributing nothing. A node pruning receipts by log
filter is upstream-healthy but cannot serve historical intake at all, because the
receipts it dropped are exactly the ones our reader would have to account for.

Historical ingest is judged on the source's declared start block, not on how far
its cursor has advanced, so a resumed run whose cursor already stands above the
floor is refused too: planning cannot tell coverage recorded before the node
pruned from coverage recorded through a pruned window, and refuses both until
the node holds the declared range again or the declared start block moves. A
redo is judged on what it has left to read instead: its range start, the source's
declared start, and the block after its own durable progress, whichever is
highest. A redo that has not reached the floor yet is refused, one already past
it keeps running, and one with nothing left to read plans nothing for that
source. Progress outside the range is not judged at all: a resume marker below
the range start or past its end is refused as a misconfigured request. Work
already recorded is not re-examined: an Ingest phase completed through the
normal completion checks is not planned again, so a chain that recorded a
pruned window before this rule existed keeps that stored coverage. A row changed
to `completed` only because its chain was absent from runtime configuration is
replanned if its current block or live handoff does not match its target.
Re-indexing validated stored coverage needs the node to hold the range again —
a fresh resync or redo across the pruned window is refused rather than silently
repeating the empty read. Live follow plans no declared range, so it is judged
on the suffix it is about to load. Sources that do not read a node's database
report no floor: an RPC endpoint owns its retention behind the wire, and the
Coinbase SQL warehouse is not a block provider at all.

## Reorgs and required downstream redo

Head publication marks a displaced readable suffix orphaned. If that suffix
starts at or below the recorded `interpret`, `project`, or `verify` cursor, the same
transaction stamps the affected phase's existing redo state from the first
orphaned block through that cursor and clears affected resolution-divergence
rows. The next live cycle runs the stamped `interpret` range and then the
stamped `project` and `verify` ranges before those phases advance normally.
Verify has no independent catch-up wait: its recorded cursor cannot exceed
Interpret's, so the Interpret wait fills the winning path through Verify's
stamped upper bound. If a provider's latest marker temporarily falls below the
old downstream cursor, live keeps polling and fills the winning path through
the stamped upper bound before redo starts. On process restart, the live
advisory lock also fences recovery of a
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
cargo phase redo \
  --chain <chain> \
  --phase <ingest|interpret|project|verify|recompute-flags|all> \
  --from-block <inclusive-start> \
  --to-block <inclusive-end>
```

Interpret, Project, and Verify join Ingest and `all` in requiring the complete
intake-capable `--source` descriptor set at startup for every selected chain.
Before loading each Base Ingest redo batch whose requested range spans the
Coinbase/RPC seam, the runner independently queries the seam-block identity
from Coinbase SQL `base.blocks` and from RPC. A mismatch is a terminal data
integrity error for that attempt and prevents redo completion; retry only after
the sources agree. This applies to required and ordinary redos. The
[manifest widening workflow](manifests.md#mandatory-historical-fetch-after-watch-plan-widening)
documents the source-schema evidence and why the check runs for every batch.
For `--phase verify`, Base may add one distinct verification-only `drpc`, Ethereum Mainnet one distinct verification-only `reth_db`, and Sepolia one distinct verification-only `drpc`. Without that optional reference, each chain records `quick_synced` from its target-covering intake cursor. Base with `reth_db` is rejected during configuration validation rather than starting
a database walk. More than one `--chain` may
be supplied. `--all-chains` is separate sugar that discovers every chain with
an active synchronized manifest and applies the same phase selection and range
through the ordinary per-chain path.

`--phase all` means all four finite phases: ingest, interpret, project, and
verify in dependency order. If Interpret widens a
partial request through its recorded head, its downstream redo stamp carries
that widened range into Project. Project still owns canonical-head hydration;
there is no standalone hydrate phase. Any already-pending redo must be
completed before `--phase all`, so the all-phases shorthand cannot consume or
clear unrelated operator work. Before any selected phase starts, the runner
also refuses `--phase all` when Verify has no recorded extent or the requested
end exceeds it. Complete Verify first, or run the needed finite phases
individually and then complete Verify through the normal runner. A Verify stamp
created by Ingest must match the all-phase range; a clipped overlap is reported
instead of shrinking Verify. A phase failure leaves its normal durable redo
marker, reports the phase-specific recovery command prefix, and stops the
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
is a head follower. When manifest synchronization invalidates Interpret, it
records a [manifest-authority marker](glossary.md#manifest-authority-marker). A
redo that would discharge that marker fails closed. If the manifest widened the
watch plan, run the [mandatory historical fetch for the affected
range](manifests.md#mandatory-historical-fetch-after-watch-plan-widening);
otherwise confirm that it widened nothing. Re-run the redo with
`--attest-watch-set-coverage <token>`, using the invalidation token printed by
the fence error. For a multi-chain redo, repeat
`--attest-watch-set-coverage <chain>=<token>` for each affected chain. The
locked redo begin rejects stale tokens, including a token from an earlier
transition to the same authority. The flag is the operator's attestation. The
same transaction that begins the redo appends one immutable audit row with the
chain, Interpret phase, range, authority fingerprint, token, runner instance
ID, and attestation time. The runner emits error-level telemetry from that row
after commit and re-emits it on restart only after the locked begin matches and
commits the same interrupted redo.
The same token-valued command may resume that exact active, audited redo; the
token remains invalid everywhere else. If the interpreter content hash changes
while the redo is interrupted, the same token preserves the audit association,
but the new binary clears the redo cursor written under the prior interpreter
content hash and walks the exact audited range again from its beginning.
Manifest synchronization compares the previous and desired [compiled watch
plans](glossary.md#compiled-watch-plan). A widening over retained Ingest
coverage stamps a required Ingest redo
from the earliest newly watched block through the latest published head. The
ordinary runner reports the exact chain, phase, and range command prefix and
instructs the operator to append configured sources. It refuses to run that
potentially expensive fetch automatically; successful explicit completion clears the
obligation after its start stamps any overlapping Verify phase state with a
recorded cursor.
Narrowing, a same-set sync, and a chain with no retained Ingest
coverage stamp nothing. The attestation remains required for every
manifest-authority change, including one with no Ingest stamp. Cursors and
readable lineage prove only the facts selected by the watch plan active when
each block was loaded. Interpreter content hash rotations remain flagless only
when neither a current manifest-authority marker nor an active audited redo
exists. Verify redo uses the same scanner as normal
verification, rechecks the requested finalized range, and persists the level
reported by the phase. A partial redo keeps the weaker of the retained full-extent level and the level available from the current source roles; a full-extent redo can establish the current plan's level.
Its source and Base seam preflight happens before redo state is created. A
mismatch retains the resumable redo marker and its diagnosis;
rerunning the same command after wipe-and-resync repair resumes the attempt.
The range end must already be `canonical`, `safe`, or `finalized`; an
`observed` staging row is rejected before a redo session is claimed.
Flag recomputation is supported through `--phase recompute-flags`. Among
otherwise configured redo requests, historical `live` redo, an unreadable
range end, and an Interpret, Project, or recompute-flags redo requested while
a required Ingest redo is still stamped for that chain are rejected before a
redo marker is written. These preflight refusals and terminal verification
failures cannot strand unresumable redo state.

The thin rewind command moves only the published latest head:

```sh
cargo phase rewind \
  --chain <chain> \
  --ancestor-block <block> \
  --ancestor-hash <hash>
```

It takes the ingest, interpret, project, and live advisory locks so no head
publisher or downstream writer can overlap it, requires the exact ancestor to
be stored and readable, refuses to cross the safe head, and invokes normal head
publication. It does not write raw facts or normalized events. An uncompleted
required Ingest redo remains stamped if its end moves above the readable head.
The next supervised run uses Live intake to publish the winning suffix under
the current watch plan, then repeats the required Ingest command prefix and
source instruction; it does not perform that operator-owned historical fetch
automatically. If finite
Ingest was interrupted before recording its handoff, this recovery-only Live
pass anchors at the published readable ancestor. The resulting orphaning also
stamps downstream redo, which remains fenced until the suffix is readable
again.

The retained schema-v2 inspection windows are read-only `phase-runner`
subcommands alongside redo and rewind:

```sh
cargo phase inspect --database-url "$BIGNAME_DATABASE_URL" \
  block-canonicality --chain <chain> --from-block <n> --to-block <n>

cargo phase inspect --database-url "$BIGNAME_DATABASE_URL" \
  stored-lineage --chain <chain> --from-block <n> --to-block <n>

cargo phase inspect --database-url "$BIGNAME_DATABASE_URL" \
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

Hydration and verified lookup call providers only at their documented block
position. Those request-scoped responses are not raw facts and do not make the
API a general chain-intake owner.

## Runtime boundary

The phase runner is the only continuously supervised ingest-through-live
writer. The API reads `bigname_phase` projections and lookup state; no serving
path reads the retired `public` lineage, projection, replay, or execution
tables. Historical migrations remain append-only evidence, while the legacy
tables themselves are removed by a schema-qualified versioned migration.
