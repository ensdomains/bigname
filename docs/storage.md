# Storage

Persistence boundaries for [raw facts](glossary.md), identity, [normalized events](glossary.md), [projections](glossary.md), and execution. Project-specific terms are defined in the [glossary](glossary.md). Architecture model in [`architecture.md`](architecture.md); intake detail in [`chain-intake.md`](chain-intake.md); manifest schema in [`manifests.md`](manifests.md); read model in [`projections.md`](projections.md); execution layout in [`execution.md`](execution.md).

## Invariants

- Durable raw facts are immutable. Evictable payload-cache entries and non-audit raw-log staging rows lose their system-of-record status once the replay contract that retained them is satisfied.
- Projections are disposable and rebuildable from canonical raw facts plus normalized events.
- Canonicality is explicit, never inferred from "latest row wins."
- Execution traces and steps are durable audit artifacts; cache outcomes are reusable only while their dependencies remain canonical.
- One write owner per storage family.

## Corrections

Raw-fact corrections are explicit, auditable events. They are not normal replay,
do not weaken the default immutability rule, and must name the corrupted field
set, cause, proof source, rewrite owner, acceptance checks, and ratification
record in this section before or with the tool that applies them. A correction
tool must be idempotent, resumable, fail closed on verification disagreement,
and update only the ratified fields. Any wider rewrite requirement is a new
doc-first storage task.

### 2026-07-03 raw code-hash padding correction

The maintainer ratified the re-derive-and-rewrite correction (recorded as
"option (a)" in that decision) on 2026-07-03 for
the padded `raw_code_hashes` corpus written by the pre-#21 Reth DB code reader.
The audited bigname bug used the padded/analyzed bytecode view (`bytes_ref()`)
instead of the original deployed bytecode path, while the pinned Reth RPC
account-code path reads `.original_bytes()` for account bytecode
`(upstream: .refs/reth/crates/rpc/rpc-eth-api/src/helpers/state.rs:L237 @ reth@88505c7)`
`(upstream: .refs/reth/crates/rpc/rpc-eth-api/src/helpers/state.rs:L244 @ reth@88505c7)`.
The corrupted rows therefore carried deterministic `code_hash` and
`code_byte_length` values that were 1 to 19 bytes longer than the consensus
bytecode for affected observations.

The live audit measured 3,736,298 padded-corrupt rows, 94.7% of the audited
corpus, and 208,320 already-correct rows. The corrupt writes span
2026-05-01 through 2026-07-03 by `raw_code_hashes.observed_at`. The state-root
consensus anchor for the audit was a live `eth_getProof` sample on the immutable
registry contracts: the EIP-1186 `codeHash` agreed with the value derived from
the original-bytecode reader, not with the padded stored value. Pinned Reth
serves `eth_getProof` and populates the EIP-1186 response `code_hash` from the
account bytecode hash
`(upstream: .refs/reth/crates/rpc/rpc-eth-api/src/core.rs:L900 @ reth@88505c7)`
`(upstream: .refs/reth/crates/rpc/rpc-eth-api/src/core.rs:L908 @ reth@88505c7)`
`(upstream: .refs/reth/crates/trie/common/src/proofs.rs:L733 @ reth@88505c7)`
`(upstream: .refs/reth/crates/trie/common/src/proofs.rs:L736 @ reth@88505c7)`.

The ratified correction selection excludes 432 rows whose `block_hash` is
orphaned or absent from retained `chain_lineage` for the window. Those rows
retain their padded values as unverifiable historical observations of orphaned
blocks. No future canonical upsert touches those `(chain_id, block_hash,
contract_address)` keys, and deletion of those rows was not ratified.

The guarded raw-fact rewrite scope is limited to `raw_code_hashes.code_hash` and
`raw_code_hashes.code_byte_length`. It does not alter `raw_code_hash_id`,
`chain_id`, `block_hash`, `block_number`, `contract_address`,
`canonicality_state`, `observed_at`, any other raw-fact table, manifests,
discovery rows, execution artifacts, or service configuration. Each effective
code-hash transition does, however, enter the normal durable resolver-profile
convergence path after the raw rewrite commits. That downstream pass may
reactivate or orphan resolver-local normalized events and enqueue keyed
`resolver_current` and `record_inventory_current` rebuilds; it does not write
projection rows directly. The implementation owner is the indexer repair
tooling invoking storage-owned guarded update helpers for this raw-fact family.

The approved method is:

1. Select the ratified observed-at window, excluding rows whose block hash has
   no non-orphaned `chain_lineage` row. The tool reports the excluded rows in
   the `orphaned_skipped` bucket instead of attempting to prove them against a
   node state that no longer exists.
2. Re-derive each selected `(chain_id, block_hash, contract_address)` from the
   v2.2.0 Reth DB reader that uses `original_bytes()`.
3. Classify correction candidates by direct comparison between the stored value
   and the re-derived `(code_hash, code_byte_length)`, not by padding-length
   heuristics.
4. Refuse the run if a re-derived hash falls outside the stored variant family
   for an address that already has multiple stored variants.
5. Verify a substantive JSON-RPC sample before any write: at least 1% of
   selected rows, every distinct address at least once, and all mandatory
   out-of-family findings if any exist. The mandatory per-run sample uses
   `eth_getCode` by block hash and compares both bytecode hash and byte length
   to the Reth-derived value. The tool also attempts a small best-effort
   `eth_getProof` spot-check for state-root anchoring on the most recent
   correctable row per address; node timeout or provider-serving failure is
   logged as non-fatal after the mandatory `eth_getCode` sample, while a
   completed proof disagreement remains a verification disagreement.
6. Rewrite only `code_hash` and `code_byte_length` in guarded batched
   transactions. Each batch logs a correction-event line with row counts and
   block range, and enforces that corrected, already-correct, conflicting, and
   orphaned-skipped rows account for the requested batch. A rerun skips
   already-correct rows.

The post-run acceptance checks are node-dependent and are not CI gates. The
supervised operations run must finish with zero RPC verification disagreements,
zero unexpected variant rows, a dry-run census of zero remaining correctable
non-orphan rows for the ratified window, the audited 432 `orphaned_skipped`
rows reported, recorded `eth_getProof` spot-check status, and the env-widened
live verification test green table-wide, including
`reth_db_provider_latest_rows_match_consensus`.

### 2026-07-03 Base normalized-event drop-and-rederive correction

The maintainer ratified a supervised corpus correction on 2026-07-03 for Base
Basenames normalized events and adapter-owned identity rows that accumulated
conflicting payloads across multiple derivation and manifest changes during the
outage window. The approved method is drop plus full-closure re-derive from
retained canonical raw facts. This is an exception to normal replay behavior:
the ordinary raw-fact normalized-event replay path remains upsert-only and does
not delete stale rows.

The implementation owner is the indexer command
`bigname-indexer drop-and-rederive-base-normalized-events`. Its dry run is the
maintainer review gate: it prints the exact live census by table,
derivation-kind/source-family delete/keep partition, block range, active replay
target and manifest snapshot digests, and replay reset target without writing.
Every `delete_census` field is an exact execute gate: execute requires the
corresponding `--expected-*` value and refuses review-to-write drift. The
derivation-kind/source-family partition, ratified dropped-emitter section,
cursor-census breakdown, estimated batch counts, and deferred raw-fact safety
line are review visibility only. `resolver_current` and
`primary_names_current` have no identity foreign key in this correction; they
are represented by the exact gated `current_projection_replay_status` reset
count instead of by per-row projection delete counts.
The heavyweight raw-fact completeness anti-join and retained raw-log byte proof
are intentionally deferred from dry-run and recomputed by execute-start under
the advisory lock before any delete. The execute mode
requires the explicit `--execute --confirm-ratified-2026-07-03` flags, the
reviewed `--replay-target-block`, records a structured correction-event log
line, takes a PostgreSQL exclusive advisory session lock for the full batched
run, refuses
concurrent `bigname-indexer` or `bigname-worker` sessions that are visible in
`pg_stat_activity`, and fails closed unless the reviewed expected counts still
match. Indexer and worker runtime processes and write-capable one-shot commands
also hold the corresponding shared advisory lock while they run, so the
correction command cannot execute concurrently with updated bigname writers.
Execute records durable progress in `base_normalized_rederive_runs` and
`base_normalized_rederive_run_batches`, keyed by a reviewed `--run-id`. A
re-invocation with the same run id, target block, batch size, and expected
census resumes incomplete work; if the live census plus recorded deleted counts
does not equal the reviewed census, it refuses to continue. Resume also reruns
the re-runnable replay-coverage and raw-fact completeness guards before any
additional batch is deleted. The reviewed plan stored in the run row includes
the active Base replay target/range digest, active manifest digest, reviewed
census, target, progress, and compact raw-fact range proof; it does not store
the full active target rows. Resume rebuilds the active target snapshot and
active manifest snapshot and requires their digests to match the stored
reviewed digests, so the check remains non-vacuous even after the scoped
`normalized_events` rows have already been deleted. Execute also requires the
dry-run's active target snapshot digest and active manifest snapshot digest as
expected values, so review-to-write replay-target or manifest drift cannot
become the stored run snapshot. The run row's retained raw-fact range proof
covers canonical raw-log identity, payload fields, and lineage rows at run
creation. In-progress resume validates that stored proof target, the active
target and manifest digests, raw-fact completeness, and live-plus-deleted
census, but it does not recompute the full retained raw-log byte checksum on
every resume. The session advisory lock plus guarded-writer exclusion make the
raw-fact corpus immutable for the run except for this command's scoped deletes,
which never touch raw-fact tables. A long-paused run cannot continue after the
active replay targets, active manifests, or raw-fact completeness guards have
drifted out of the reviewed safe state.

The normalized-event scope is:

- `chain_id = 'base-mainnet'`
- `block_number BETWEEN 17571485 AND <validated replay target>`
- `block_hash IS NOT NULL`
- a re-derivable derivation/source-family pair emitted by the selected Base
  closure replay adapters:
  - `ens_v1_reverse_claim`: `source_family IN ('ens_v1_reverse_l1',
    'basenames_base_primary')`
  - `ens_v1_registry_resolver_changed` or `ens_v1_subregistry_changed`:
    `source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')`
  - `ens_v1_unwrapped_authority` (the ENSv1 pipeline deriving ownership and
    control for names — registry-, registrar-, and NameWrapper-held alike —
    see [glossary](glossary.md)): `source_family IN
    ('ens_v1_registrar_l1',
    'ens_v1_registry_l1', 'ens_v1_resolver_l1', 'ens_v1_wrapper_l1',
    'basenames_base_registrar', 'basenames_base_registry',
    'basenames_base_resolver')`

The scope does not use `source_manifest_id`; rows with a NULL manifest id are
included when their derivation/source-family pair is re-derivable. The dry-run
also enumerates every other `base-mainnet` derivation/source-family pair present
in the same block-backed range and reports it as kept. In particular,
`raw_log_preimage_observation` rows and re-derivable-looking derivation kinds on
non-replay source families are not in the delete scope because this supervised
Base closure replay does not re-derive them.

A second ratification (2026-07-05, recorded as "option A" in that decision)
adds one deliberate-drop class
inside that delete scope: 3,939,502 `ens_v1_reverse_claim` /
`basenames_base_primary` log-derived rows emitted by the legacy Basenames
`ReverseRegistrar` `0x79ea96012eea67a83431f1701b3dff7e37f9e282`, with event
and source-event shape `ReverseChanged` / `BaseReverseClaimed`, coin type `60`,
and block range `17575714..46903158`. The Basenames `ReverseRegistrar` can establish primary
records upstream, but bigname's declared Base primary-name value authority for
`basenames_base_primary` is the ENSv1 Base `L2ReverseRegistrar`
`0x0000000000D8e504002cC26E3Ec46D81971C1664`, keyed by
`NameForAddrChanged(address,string)` and Base coin type `2147492101`.[^bn-revreg-l12][^bn-revreg-l150][^bn-revreg-l193][^v1-l2rev-base-deploy][^v1-l2rev-base-args][^v1-l2rev-event]
The reviewed live manifest state also had the legacy
`0x79ea96012eea67a83431f1701b3dff7e37f9e282` path deprecated behind a
deactivated `manifest_successor` edge to the ENS
`0x0000000000D8e504002cC26E3Ec46D81971C1664` authority. These 3,939,502 rows
remain in the delete census and are not re-created by the full-closure replay;
after replay and projection rebuild, Base primary names sourced only from the
legacy Basenames reverse registrar are removed and `primary_names_current`
reflects the ENS Base L2 reverse-registrar authority. Dry-run prints the
ratified dropped-emitter count separately from the ordinary delete/keep
partition so maintainers can confirm the deliberately dropped class before
execute.

The identity-row scope is `resources`, `token_lineages`, `name_surfaces`, and
`surface_bindings` where `chain_id = 'base-mainnet'` and
`provenance->>'adapter' = 'ens_v1_unwrapped_authority'`. The command also
removes dependent current-projection rows and `projection_normalized_event_changes`
rows only to satisfy foreign keys and to force the later projection rebuild to
publish from the re-derived event stream. The final reset transaction clears
`current_projection_replay_status` markers for `name_current`,
`address_names_current`, `children_current`, `permissions_current`, and
`record_inventory_current`, plus `resolver_current` and `primary_names_current`
because those families consume normalized events that this correction deletes
and re-derives. That prevents automatic all-current replay from skipping a
family with a stale completion marker after the delete finishes. The global
`projection_apply_cursors` watermark is not reset because it is not scoped to
these affected families. It does not rebuild projections, so the API must be
drained or stopped from execute through the replay, projection rebuild, and
verification window.

The delete is batched and resumable. The order is FK-safe at every commit:
current projections keyed by scoped identity rows, then
`projection_normalized_event_changes`, then scoped `normalized_events`, then
`surface_bindings`, `resources`, `name_surfaces`, and `token_lineages`.
Each execution session materializes the reviewed event and identity scopes into
temporary tables, then materializes one candidate-key table for the current
delete step by driving from those scope tables into the projection, event, or
identity lookup indexes. Batches delete from that candidate table in
deterministic key/block order. On crash or operator resume, the session
rebuilds temporary scope and candidate tables from the remaining live rows and
continues under the same reviewed run state. Identity-row batches do not begin
until all dependent current projections, projection change rows, and normalized
events are gone, so a crash leaves a partially deleted but referentially valid
database. During the destructive delete, the API must remain drained:
reverse-identity sidecar triggers are disabled only inside the affected
projection and identity-anchor delete transactions, and the final reset
transaction rebuilds `address_names_current_identity_counts` and
`address_names_current_identity_feed` from the remaining current projections
before marking the run completed.
After all delete batches have completed, one final small transaction clears
affected `current_projection_replay_status` rows,
`normalized_replay_adapter_checkpoint_items` and
`normalized_replay_adapter_checkpoints` for
`ens_v1_reverse_claim`, `ens_v1_subregistry_discovery`, and
`ens_v1_unwrapped_authority`, clears any sibling
`mainnet/base-mainnet/post_replay_live_adapter_backlog` cursor, then resets the
`normalized_replay_cursors` row for
`mainnet/base-mainnet/raw_fact_normalized_events` to
`range_start_block_number = next_block_number = 17571485` and
`target_block_number = <validated replay target>`. The final reset revalidates
that the retained canonical Base raw-log floor is exactly block `17571485`, and
the catch-up path repeats that floor check while the completed run's reset cursor
is still pending replay. Because the catch-up cursor's replay bounds are derived
from the canonical raw-log floor, replay refuses before cursor refresh if a later
retention change would widen this correction below the delete boundary. The
generic catch-up cursor refresh and older-log rewind paths otherwise retain their
normal ability to widen or rewind when older retained raw logs appear. If the
process dies before that final reset, replay cursors and projection markers
remain untouched and the same `--run-id` must be resumed before replay starts.
Guarded writer processes also refuse to start while a Base rederive run remains
incomplete (`status` other than `completed` or `aborted`), so a released session
lock after a crash is not enough for normal writers to proceed against a
partially deleted corpus.

The command must not delete `chain_lineage`, `raw_logs`, `raw_transactions`,
`raw_receipts`, `raw_code_hashes`, `payload_cache`, or any other raw-fact source.
Before execution it proves that the scoped log-derived normalized events still
join retained non-orphaned `raw_logs`, scoped boundary events still join retained
non-orphaned `chain_lineage`, and the canonical raw-log range inside the
ratified replay window spans the closure boundary and validated replay target.
It also proves that the retained canonical Base raw-log floor itself equals the
ratified closure boundary, block `17571485`.
It also refuses if any row in the delete scope is above the retained canonical
raw-log head, or if any present delete-scope `(derivation_kind, source_family)`
pair lacks currently active Base replay adapter/source-family target ranges
whose union covers the ratified closure boundary through the validated replay
target with no block gap, or if any in-scope log-derived row was emitted by an
address outside the current active replay target set for that row's source
family at the event block. The only exception is the 2026-07-05 ratified
deliberate-drop allowlist class ("option A" above)
`(ens_v1_reverse_claim, basenames_base_primary, 0x79ea96012eea67a83431f1701b3dff7e37f9e282,
ReverseChanged, BaseReverseClaimed, coin_type=60, blocks=17575714..46903158)`;
that exact class is deliberately dropped and not re-derived, and any other
orphaned emitter remains a hard stop. `ens_v1_unwrapped_authority` raw-block boundary rows
(`transaction_hash` and `log_index` are null and `raw_fact_ref.kind` is
explicitly `raw_block`) are checked against coverage for the source family that
will rederive the boundary row rather than blindly against the stored source
family. For Basenames registry boundary rows whose stored family is the legacy
`ens_v1_registry_l1` drift, that rederive family is `basenames_base_registry`.
Rows missing the explicit `raw_block` marker remain subject to strict
same-source-family coverage. Apart from the explicit 2026-07-05 deliberate-drop
class, these are hard stops because the correction may only delete rows that
current full-closure replay can recreate from retained raw facts.
The completed run records the reviewed active replay target/range digest and
the reviewed active Base manifest digest. The active manifest digest is computed
from manifest payloads plus deterministic compact row-summary digests for
manifest-linked capability flags, discovery rules, contract instances, active
addresses, and active discovery edges, so the review pin detects
manifest-linked row additions, removals, and modifications without storing the
live discovery graph in the run row. While the reset cursor is still pending
replay and the reviewed replay has not yet begun, the catch-up replay path
rebuilds the current active snapshots, compares their digests with those
reviewed digests, and refuses to replay if a different manifest image was
synced after review, even when the replay target addresses and ranges would
otherwise be unchanged. Once the reviewed replay itself has begun — detected by
a Base `full_closure` replay adapter checkpoint pinned to the reviewed replay
target (closure adapters insert their checkpoint row before any mutation, and
final replay reset deletes those rows inside the execute transaction, so within
tool-reachable states a matching row was written by the reviewed replay) — both
digest comparisons are skipped: the replay's own closure adapters legitimately
correct discovery edges and discovered contract-instance addresses, which both
reviewed digests cover, so re-comparing live state against the pre-replay pins
would fail every session resume after the first discovery commit. A leftover
checkpoint pinned to a different replay target fails closed and re-engages the
strict pre-replay comparison. Repository manifest sync is
skipped while the reviewed completed run's reset cursor is still pending, so
the active manifest tables cannot be rotated by normal repository sync between
the replay guard and the full-closure adapter reads; that sync gate is what
protects manifest-owned state during the checkpoint-skip window. A skipped repository
refresh remains marked for retry, so the same long-running indexer syncs the
repository normally once the pending reset replay cursor completes.
Because the delete scope is global for `base-mainnet` while replay reset is
profile-scoped, dry-run and execute also require the requested deployment
profile to own an existing `base-mainnet/raw_fact_normalized_events` replay
cursor before they report or run the correction.
Dry-run defaults the target to the live canonical Base raw-log head and reports
the maximum affected normalized-event block plus the effective replay target
floor. The floor is the greater of the maximum affected block and any pending
closure-boundary raw-fact replay cursor target from an earlier drop, so neither
idempotent nor partial-replay reruns can shrink the intended replay range while
replay is still pending. Execute requires an explicitly provided
`--replay-target-block`; that reviewed value is accepted when it is not above the
current canonical raw-log head and not below the reported replay target floor.
Raw-fact completeness is recomputed for the requested target. The command also
refuses if any normalized event outside the delete scope still references an
identity row that the correction would drop. If any proof fails, no write is
allowed.
The default batch size is `100000` rows; operators may lower it with
`--batch-size` to keep per-commit WAL and lock duration within the deployment's
headroom. The correction leaves row-level sidecar delete triggers enabled. That
keeps sidecar invalidation semantics normal while batching bounds WAL per
commit; sidecars are then reconciled by the required all-current projection
rebuild.

## Storage layers

The system of record splits into six layers.

1. `chain_lineage` — block ancestry, fork points, hash-first reconciliation, head promotion, one durable header-anchor row per observed block hash.
2. `raw_facts` — hot indexed replay facts: selected/admitted target logs, the minimum transaction/receipt fields needed to decode them, code-hash observations, fetched call snapshots, optional header/log audit extensions, compact payload-cache metadata. Code-hash observations are activity-scoped, not a per-block grid: a block admits `raw_code_hashes` rows only for that block's selected log emitters, plus the live tailer's one-time baseline for a watched address with no stored observation. Consumers read the latest non-orphaned observation per target; the code at an intervening block is a read-side answer, never a materialized raw fact. See [`chain-intake.md`](chain-intake.md).
3. `manifests_and_discovery` — source manifests, discovered edges, rollout flags.
4. `identity_and_events` — `NameSurface`, `SurfaceBinding`, `resources`, `token_lineages`, and append-only `normalized_events`.
5. `projections` — current-state and collection read models.
6. `execution` — durable traces and steps, `execution_cache_outcomes`, invalidation records.

Layers 1–5 rebuild current declared state. Layer 6 replays verified answers and explains them.

Worker-owned manifest/proxy alert observations live alongside these layers as an operational audit family. They record drift findings; they are not manifest truth, discovery admission, projection state, or adapter-owned events.

## Storage substrates

Postgres is the hot indexed and replay-focused store. It retains:

- lineage and header anchors needed to reconcile forks, prove ancestry, promote checkpoints, audit canonicality
- selected/admitted target logs and the minimal transaction and receipt fields while they are needed to decode those logs, route them through adapters, and append normalized events
- block-scoped call snapshots and enrichments retained by an explicit replay contract for normalized events, projections, or execution artifacts
- durable event-silent resolver call observations used as projection-invalidation inputs after selected transaction and receipt staging rows are compacted
- code-hash observations and discovery/proxy evidence used by manifests, adapter routing, and audit tooling
- compact metadata and optional digests for full payloads fetched as cache

There is no deployed object-storage layer in the current schema or compose stack. When the system retains fetched payload metadata, Postgres stores the metadata and optional digests needed to validate later cache use; fetched bytes outside durable replay facts are cache-owned and may be absent.

## Raw-log retention modes

`raw_logs`, selected `raw_transactions`, and selected `raw_receipts` have two retention modes selected operationally:

- **minimal** — these rows are adapter-replay staging. They may be compacted after the normalized replay cursor advances past the retained block range and the corresponding `normalized_events`, identity rows, lineage rows, and projection rebuild inputs are durable.
- **log-audit** — the same rows remain durable audit facts and may keep heavier indexes for historical raw-fact replay.

Switching modes is operational policy. It does not change route coverage, projection truth, canonicality semantics, manifest rollout, or consumer-replacement meaning.

Live polling may retain selected `raw_transactions` and `raw_receipts` for successful direct transactions to configured event-silent resolver addresses even when those transactions do not emit selected logs. Intake copies the chain id, resolver address, block number/hash, transaction hash/index, and canonicality into `event_silent_resolver_call_observations` before those staging rows become compactable. The durable observation row is the projection-invalidation trigger for explicitly documented hydration repairs, such as legacy ENSv1 reverse-resolver primary-name hydration. It does not authorize adapters to synthesize normalized events from calldata or receipts, does not make raw facts an API fallback, and does not change minimal/log-audit compaction boundaries once downstream normalized replay and projection inputs are durable.

`bigname-worker raw-facts compact-log-staging` is the manual compaction boundary for minimal mode. It refuses to compact unless the `raw_fact_normalized_events` replay cursor is caught up and failure-free, and only operates on raw-log staging families. Log-audit deployments do not run it for retained ranges.

Raw-log inserts and semantic updates advance a commit-ordered per-chain [input revision](glossary.md) and record the revision that last touched each old or new block hash. Semantic updates are changes to `raw_log_id`, chain, block, transaction, log position, emitting address, topics, payload bytes, or canonicality. An `observed_at`-only refresh is metadata and does not advance either revision. This is compact synchronization metadata, not a raw fact or an adapter snapshot. Stateful live adapters may use it to prove that all raw-log mutations since a cached block anchor leave the cached ancestor path unchanged; sequence-allocated `raw_log_id` values are not a commit-order proof.

[Retained-history](glossary.md) authority is a separate proof tuple: raw-log retention [generation](glossary.md), discovery-[admission epoch](glossary.md), and inclusive proven-through block. A fresh chain starts incomplete; inserting its first raw log does not claim that earlier selected history is complete. Deleting or truncating raw-log staging increments the retention generation, clears the proof, and invalidates every process-local cache built under the previous generation. Updating a raw log's identity or payload has the same destructive effect for both the old and new chain identities because the retained corpus no longer proves what was fetched; a canonicality-only update retains the generation and proof, while an `observed_at`-only update changes neither revision nor proof. Per-block input-revision metadata cannot carry a cache across a generation change.

ENSv2 full-source reconciliation is allowed only when the stored proof generation and discovery epoch still match and the requested target is not above the proven-through block. Recovery requires gap-free coverage facts from completed backfill jobs that captured the exact current retention generation for every authoritative `ens_v2_root_l1` and `ens_v2_registry_l1` address interval, including closed historical discovery intervals. Retained ENSv2 normalized-event and discovery provenance on `canonical`, `safe`, or `finalized` lineage is checked for a matching readable raw-log witness within that source-and-block boundary. Orphaned events and discovery anchors from losing branches remain audit truth, but they neither require bytes that canonical backfill cannot recreate nor define canonical closure authority. This anti-join is only a consistency check; generation-bound fetch coverage is the absence proof. Recovery reads the watched requirements while holding a shared lock on the concrete discovery-admission epoch row and persists the new proof tuple in the same short transaction. Full-source replay then uses one long transaction for the chain's registry-sync serializer, a chain-scoped raw-log semantic-mutation advisory fence, and an `ACCESS SHARE` table lock that blocks global truncation but permits ordinary row writes. Raw-log insert, semantic-update, and delete triggers take the same advisory key for every affected old or new chain in sorted order before advancing revisions; an `observed_at`-only update takes no semantic fence. Therefore another chain's intake remains writable while same-chain semantic mutation and global truncation wait through raw-log loading, discovery reconciliation, and adapter persistence. The destructive discovery writer compares the expected epoch after acquiring its writer fence before it may deactivate anything.

Before a complete live-path extension, the adapter captures the union of authoritative ENSv2 root and registry address intervals through the new target. At proof advance it reloads that union under the post-sync discovery epoch, requires current-generation backfill coverage for every newly admitted, reopened, or earlier interval portion, and rechecks raw-log witnesses against the complete post-sync union. An unchanged interval extended through the target was part of the complete fetch and needs no second historical job; resolver-only admission does not enter this root/registry closure proof. Missing current-generation coverage for one exact required interval is a distinct typed recovery condition whether the requirement was admitted during the current reconciliation or was already durable when a later pass or process restart began. ENSv2 automatic startup and normalized-event catch-up can therefore backfill the named root, registry, or resolver tuple and retry a bounded fixed point without treating unrelated adapter failures as recoverable. Before phase 1, normalized-event catch-up rebuilds a stale [retained-history proof](glossary.md) from already-durable current-generation coverage; an uncovered interval remains the same typed recovery condition and is fetched before any stateless work. Recovery at that preflight keeps the original full replay span pending, so phase 1 still covers the complete saved range once validation reaches a fixed point. When validation instead exposes a gap after phase 1 completed, catch-up retains and coalesces every exact recovered block span across further attempts and reruns the stateless producers only over those spans before restarting the full stateful adapter pass. Before it accepts the recovery's newer raw-log input revision, it checks for other committed mutations: another changed block inside the saved replay span widens the next phase 1 to that full span, while a change below the span fails the attempt so the durable cursor can replan. Newly fetched and concurrent in-span logs therefore receive their stateless rows before their revision is acknowledged. The fixed point remains bounded to 32 recovery attempts. Successful complete extensions advance the proven-through block, so a later process restart can hydrate through the advanced live target without rerunning historical bootstrap. A chain with no authoritative ENSv2 root or registry closure is an ENSv2 no-op and does not create retained-history state. If any generation, epoch, coverage, lineage, or witness check fails, reconciliation fails closed.

After compaction, `chain_lineage` and any retained compact block-anchor metadata remain the block-hash path for losing-branch repair, and `normalized_events` carry the block identity, source identity, event identity, and provenance needed by projection rebuilds and history reads. If raw-log staging rows are already gone, reorg repair marks normalized events and identity rows orphaned from lineage and updates zero raw-log rows for that range. Historical adapter replay from compacted ranges is an explicit backfill/refetch operation against the configured provider/cache substrate or requires log-audit retention; it is not an implicit API fallback.

Compaction and pruning must stay behind the rewind horizon they serve. Minimal mode may drop staging rows after replay is durable, but it must not drop lineage, normalized-event provenance, identity intervals, projection change records, or retained replay facts needed to orphan a losing branch and rebuild the canonical snapshot. If a compacted range later needs adapter-level byte replay and no retained digest-checked payload or provider/cache fill can satisfy it, that repair fails closed rather than inventing state from current projections.

## Evictable payload cache

Large/full block payloads, non-indexed transaction/receipt/block bodies, and non-audit raw-log staging rows are evictable cache by default once the selected replay contract has been satisfied. They may live inline during a hot window, in local/provider cache, or not be retained at all.

Retained cache metadata describes what was fetched: payload kind, chain id, block hash/number where block-scoped, optional digest, size, content type or encoding, source observation metadata, observed time, canonicality state. A retained digest authorizes later byte use; metadata without one cannot.

Provider re-fetch is an explicit, fallible cache-fill path. For block-scoped payloads it is block-hash-scoped, verifies the retained digest before any bytes are used, and fails closed when the digest is absent, the digest mismatches, or the provider cannot serve the exact historical payload. It is not a substitute for retained lineage, normalized events, execution artifacts, or orphaned-branch audit truth.

Local execution-client storage (e.g. a same-host Reth database) is provider/cache substrate, not a new storage family. Client table keys, row cursors, static-file offsets, and data-directory paths appear only in operational source metadata or evictable cache metadata — never as durable `raw_fact_ref` identities, normalized-event provenance, or projection inputs.

Historical backfill does not turn empty blocks into hot payload archives. A block with no selected target logs and no replay-required enrichment retains lineage/header anchors and any compact audit metadata required by the selected retention policy. Full block bodies, receipt bundles, transaction bundles, and payload-cache bytes for those empty historical blocks remain evictable or absent.

## Identity strategy

### Deterministic text IDs

`logical_name_id = "<namespace>:<normalized_name>"` — stable, human-auditable, derivable without database lookup.

`normalized_name` is the output of the single ENSIP-15 normalizer declared as `ensip15@ens-normalize-0.1.1`; storage validation and projection inputs must not substitute IDNA/UTS-46 conversion, ASCII lowercasing, or trimming. Name-surface DNS wire names, namehashes, and labelhash paths are derived from the same normalized labels. `primary_names_current` treats blank or whitespace-only reverse-claim source values as absent claims; nonblank claim-name sources either normalize through ENSIP-15 or remain verbatim as `raw_claim_name` for `invalid_name`. A successful row stores `normalized_claim_name` plus `claim_name_is_normalized`, which is true only when the untrimmed raw source byte-equals that normalized value. The database permits successful false rows because they preserve declared claim state while gating verified output.

ENSv1 name-surface materialization does not admit embedded NUL bytes in registrar labels, wrapper DNS labels, or resolver `name` record preimages. Those observations may still remain as raw facts and, where applicable, resolver-record normalized events, but they do not mint or update `NameSurface` rows or label preimage state. This keeps displayable name identity distinct from literal onchain strings that reference indexers also treat as invalid or unnormalizable for label/name interpretation.[^ens-subgraph-label-null][^ens-subgraph-name-null][^ensnode-null-label]

`label_preimages` is a hash-to-label fact table, not a name-surface table. Rows may be learned from retained `PreimageObserved` normalized events, retained `name_surfaces` that already carry normalized labels and labelhashes, or imported operationally from a rainbow-table source such as Graph Protocol's `ens-rainbow` dump. The pinned generator emits a prepared `ens_names(hash, name)` table and computes `hash` as `keccak256(name)`.[^graph-ens-rainbow-table][^graph-ens-rainbow-hash] Each imported or observed label must normalize as a single ENS label and must hash back to the stored labelhash before it can be retained. The worker-owned migration path runs a one-time retained-fact backfill after SQL migrations so upgraded databases learn already-retained `PreimageObserved` and `name_surfaces` preimages through the same Rust verification path as live upserts. Label preimages are allowed to improve projection readability, including historical ENSv1 and Basenames registry child rows, but they do not by themselves create exact-name `name_surfaces`, ownership, resolver topology, record facts, or primary-name truth.

`bigname-worker label-preimages import-ens-rainbow` is worker-owned operational tooling. Operators must first load the pinned `ens-rainbow`-shape source table `ens_names(hash, name)` into the bigname database. The command reads that table in hash order, validates and stores only labels that normalize as one ENS label and hash back to the supplied `hash`, and enqueues `children_current` invalidations for known parents that have matching canonical ENSv1 or Basenames registry child edges.

### Opaque UUIDs

- `resource_id`
- `token_lineage_id`
- `contract_instance_id`
- `surface_binding_id`
- `execution_trace_id`

UUID values are internal identities, not user-generated strings. `resource_id` and `token_lineage_id` survive projection rebuilds. Token IDs, node hashes, and resolver addresses are attributes, not identity anchors.

### Append-only event IDs

`bigint generated always as identity` for raw fact rows, normalized event rows, and projection job rows.

### Continuity rules

`logical_name_id`, `resource_id`, `token_lineage_id`, and `contract_instance_id` continuity is shared with [`architecture.md`](architecture.md) — see the identity model section there for the rules adapters must follow when minting and reusing IDs across ENSv1 wrap/unwrap/re-registration, ENSv2 token regeneration, and proxy implementation churn.

The storage-side guarantees those rules depend on:

- One admitted contract address on one chain maps to one stable `contract_instance_id` across all admission epochs. Re-admission after an inactive gap reuses the prior id and records a new non-overlapping active range.
- Proxy contracts and their implementations are separate `contract_instance_id`s. Implementation churn updates the proxy/implementation discovery edge, not the proxy id.
- Contract addresses are time-ranged attributes for raw-fact lookup, log routing, and watch-plan materialization. Addresses are never the primary key of the source graph.
- Stable adapter identity rows for `token_lineages`, `resources`, and `name_surfaces` are idempotent across retained replay anchors. Replaying a compatible readable row with the same stable identity and identity-defining fields from a later raw-log anchor may be accepted as an existing identity without rewriting the original anchor, anchor provenance, or `observed_at`; incompatible identity fields remain hard conflicts. Once the stored row is explicitly `orphaned`, re-observing the same stable identity on the winning branch replaces its chain/block anchor and anchor provenance with the winning observation while preserving all immutable identity fields. For `name_surfaces`, the compatibility key is the stable logical id plus namespace, normalized name, DNS wire name, namehash, labelhash path, and normalization errors; input spelling, display spelling, normalizer version, and warnings are retained observation metadata and may differ across compatible replay observations. Retained ENSv1 unwrapped-authority name surfaces with empty normalization errors may repair a stale normalized surface path when the stable logical id, namespace, normalized name, and normalization errors still match the replayed normalized-label surface. This repair covers stale raw-cased hash paths and stale dot-containing registrar-label surfaces whose retained DNS/namehash/labelhash path collides with the normalized multi-label name; it updates only the stored DNS wire name, hash path, and ordinary canonicality/observation metadata allowed by the stable-row merge. For token/resource identities, provenance describes the retained observation anchor and is not itself a later-anchor compatibility key. ENSv1 registrar resources materialized only from a closed surface-binding segment after the lease has been released intentionally carry binding-derived provenance: `released_at` is the binding close time, `expiry` is that time minus the ENS grace period, and the prior registrant is not reconstructed into the resource row unless an unreleased current or superseded registrar lease survives finalization.[^v1-registrar-grace]
- Normalizer-version repair follows the same split. The indexer repair command may update retained `name_surfaces` observation metadata when the current normalizer produces the same logical id, normalized name, DNS wire name, namehash, labelhash path, and empty normalization errors; retained chain/block/provenance/`observed_at` anchors are preserved. Rows that reject or remap under the current normalizer are not silently rewritten; they are recorded in `name_surface_normalization_repair_findings` for semantic review before any future orphan/remap repair.
- For interval identity rows like `surface_bindings`, `active_from`, identity-defining fields, and the observation anchor of every readable row are immutable; `active_to` is replay-derived. The only anchor exception is reorg replacement: an already `orphaned` row may adopt the winning branch's chain/block observation anchor when the stable identity, `active_from`, kind, and provenance still match, while a readable row rejects the same change. That replacement also discards the orphaned row's replay-derived close and uses the winning observation's `active_to`; a winning registration with no unregister evidence therefore restores the stable binding as open. Canonical historical replay may tighten an existing non-null `active_to` to an earlier close point when older or more complete facts reveal an earlier end. Normal replay and identity upsert paths do not extend or reopen a readable closed interval. Explicit adapter repairs are governed by the adapter-repair policy below: any future interval widening or reopen must be named there with its proof, overlap guard, and invalidation behavior. Replay batches that both close an existing interval and open a replacement at the same boundary apply the existing interval update before inserting the replacement, so the non-overlap invariant is enforced without relying on implicit snapshots.

For ENSv2, `resource_id` keys by `(chain_id, registry_contract_instance_id, upstream_eac_resource)` after observing the upstream EAC resource — not by the current ERC-1155 token id. Upstream exposes both `getResource(anyId)` and `getTokenId(anyId)`, emits `TokenResource(tokenId, resource)` when a token links to a resource, and emits `TokenRegenerated(oldTokenId, newTokenId)` when role changes burn and mint a replacement token while leaving the resource unchanged.[^v2-iperm-l34][^v2-iperm-l67][^v2-iperm-l72][^v2-events-l69][^v2-pr-l216][^v2-pr-l451] Unregister/re-register increments both `eacVersionId` and `tokenVersionId` and mints fresh `resource_id` and `token_lineage_id`.[^v2-pr-l28][^v2-pr-l203][^v2-pr-l237][^v2-pr-l536]

## Table families and write ownership

| Family | Write owner | Purpose |
| --- | --- | --- |
| `chain_*` | intake | lineage and canonical block graph |
| `raw_*` | intake; storage triggers own raw-log revision and retention-generation metadata | immutable hot replay facts, payload-cache metadata, compact per-chain/per-block-hash mutation revisions, and generation/epoch/through-bound retained-history proofs |
| `backfill_*` | worker/backfill substrate through storage-owned lifecycle helpers | persisted backfill jobs, bounded range leases, resumable range checkpoints, and completion-scoped coverage facts |
| `normalized_replay_*` | indexer/replay orchestration | operational replay cursors and adapter-private replay checkpoints |
| `resolver_profile_input_changes` | storage triggers enqueue; indexer convergence acknowledges | coalesced generation-fenced work for effective resolver code-hash, manifest, or discovery admission changes |
| `resolver_profile_authority_journal`, `resolver_profile_authority_journal_entries` | storage API persists; indexer manifest/discovery orchestration advances | revision/epoch compare-and-set header plus canonical-keyed authority entries whose forced resolver-profile work was durably queued |
| `resolver_profile_reconciliation_runs`, `resolver_profile_reconciliation_targets`, `resolver_profile_reconciliation_state_items` | indexer resolver-profile replay orchestration; adapters persist private staging state | transient run metadata, exact resolver-emitter targets, and page-evicted private adapter state plus staged events for one absence-aware resolver-profile reconciliation; not replay cursors, checkpoints, projection input, or API state |
| `resolver_profile_reconciliation_invalidation_keys` | indexer resolver-profile convergence | crash-safe chain-keyed projection keys streamed from the exact staged targets before adapter publication, then published and removed in bounded statements atomically with the matching chain-context reconciliation |
| `manifest_*` | manifests/discovery | source manifests, declared contract admission, capability versions |
| `discovery_*` | manifests/discovery | canonical reachable contract graph, watch-plan expansion keyed by `contract_instance_id` |
| `manifest_alert_*` | worker/audit | persisted manifest-drift and proxy-alert observations |
| `name_surfaces`, `surface_bindings`, `resources`, `token_lineages` | adapters | stable identity anchors |
| `name_surface_normalization_repair_findings` | indexer repair | audit table for retained name-surface rows that reject or remap under the active normalizer and therefore need explicit semantic follow-up rather than silent metadata rewrite |
| `label_preimages` | storage from verified retained name-bearing facts; worker/operator rainbow imports | retained labelhash-to-label facts used to resolve child labels and other display preimages without minting exact-name identity |
| `normalized_events` | adapters | append-only normalized protocol events |
| `event_silent_resolver_call_observations` | intake | durable block-scoped direct-call observations for documented projection hydration invalidation where the watched resolver emits no usable event |
| `projection_*` | projection workers | disposable read models |
| `address_names_current_identity_counts`, `address_names_current_identity_feed` | storage triggers on `address_names_current`, `primary_names_current`, and supporting identity-anchor and `name_current` readability changes | exact reverse identity total counts and compact feed display rows by address, role filter, and primary-name coin type for the partner-compatible identity façade, using the same canonical/read-safe and reachable-`name_current` row eligibility as reverse identity pages; this is the bounded exception in [`adrs/0005-identity-count-sidecar.md`](adrs/0005-identity-count-sidecar.md) |
| `permissions_current_publication` | permission projection worker, keyed and full publication transactions | singleton projection-owned compatibility version and monotonic data revision; API-readable schema/version and request-coherence guard, not replay progress or freshness |
| `current_projection_replay_status` | projection workers; ratified storage correction tooling may clear affected markers when it deletes projection rows | durable operational completion markers for bootstrap/full all-current projection replay |
| `projection_normalized_event_changes` | normalized-event storage trigger; projection workers consume | append-only downstream change log for normalized-event inserts and canonicality-state updates, consumed through finite, bounded-wait complete-prefix captures |
| `projection_apply_cursors`, `projection_invalidations`, `projection_invalidation_dead_letters` | projection workers; storage trigger for projection-relevant `surface_bindings` repairs; bounded normalized-event adapter repair invalidations | durable projection apply watermarks, live key-scoped projection invalidation queue, and terminal operator-visible dead-letter records |
| `service_loop_heartbeats` | indexer and worker main loops | durable per-process loop liveness, per-chain indexer loop liveness, and named worker long-operation phases; operational health evidence only, not chain progress or API read-model data |
| `execution_*` | execution workers; API on-demand verified-resolution cache misses for documented product routes; synchronous indexer/reorg repair for orphan-block cache outcome deletes only | durable traces and steps, normal `execution_cache_outcomes` writes, invalidation records |

The API process is otherwise read-only against storage.

`service_loop_heartbeats` identifies a service instance by `service_name` and
`instance_id`. Registering the process-scoped row retires every same-service
non-process row before it resets `started_at`; stale chain scopes and a prior
worker's unfinished phase therefore cannot survive a single-writer service
handoff. Process rows remain available to rank instances during that handoff.
The supported deployment has one active writer for each service. Each main-loop tick
advances `heartbeat_at` for its process row. The indexer registers this row
immediately after opening its database pool, before startup bootstrap, and
advances the process plus deduplicated chain rows after completed hash-pinned
bootstrap progress units of at most 32 blocks and after completed startup
adapter checkpoint stream pages and bounded discovery, identity, binding, and
normalized-event finalization batches. Live manifest and discovery refreshes
reuse those checkpoint-page progress callbacks and family-boundary beats. This
does not change the configured 1,024-block default checkpoint boundary for
non-startup backfills. The worker
advances the process row after bounded
[projection](glossary.md) rebuild batches and projection-apply units. This
keeps long, actively progressing work live without using a detached timer that
could mask a stuck operation. A missing
process-scoped row therefore means that instance's loop never registered or
gracefully deregistered, while a present row older than the configured maximum
age means the loop stopped or wedged after starting. For each service, the API
prefers an instance whose normal heartbeat or active long-operation phase is
within its configured age, then falls back to the newest stale evidence when
none is healthy. One live instance can therefore satisfy shared readiness
without being hidden by a newer retained instance that stopped. Each
container's `healthcheck` subcommand reads its own instance row, so another
instance cannot hide a stopped process. These rows are mutable operational signals. They are
not raw facts, [replay](glossary.md) checkpoints, chain checkpoints, or
projection freshness evidence.

Full worker rebuild heartbeat routes are explicit:

| Rebuild step | Bounded progress heartbeat | Named monolithic phases |
| --- | --- | --- |
| `name_current` | each completed name task; staged writes remain in 2,000-row batches | `name_current.load_inputs`, `name_current.publish` |
| `children_current` | each completed declared-child source; staged writes remain in 2,000-row batches | `children_current.count_existing`, `children_current.publish`, `children_current.count_published_parents` |
| `permissions_current` | each completed resource task; staged writes remain in 2,000-row batches | `permissions_current.count_existing`, `permissions_current.publish` |
| `record_inventory_current` | each completed resource task, staged in 500-row batches; text hydration also beats after each bounded 500-row page | `record_inventory_current.count_existing`, `record_inventory_current.publish` |
| `resolver_current` | each completed resolver target; staged writes remain in 1,000-row batches | `resolver_current.load_profile`, `resolver_current.load_targets`, `resolver_current.count_existing`, `resolver_current.publish` |
| `address_names_current` | each completed surface binding; staged writes remain in 2,000-row batches | `address_names_current.prepare`, `address_names_current.publish`, `address_names_current.count_published_addresses` |
| `primary_names_current` | each streamed tuple; legacy hydration beats during 1,000-candidate planning, configured provider batches, resolver-edge batches, and 1,000-row upserts | `primary_names_current.count_existing`, `primary_names_current.invalidate_execution_cache`, `primary_names_current.publish`, `primary_names_current.legacy_hydration.load_reverse_claim_candidates`, `primary_names_current.legacy_hydration.load_resolver_edge_candidates` |

A named phase is a distinct `scope_kind='phase'` row for the worker instance.
Its `heartbeat_at` is the phase start rather than a free-running timer, so a
crash or wedge still ages out. Worker and API checks use the separately
environment-tunable phase maximum (default 43,200 seconds) only while that row
exists; completing the phase removes it and refreshes ordinary process
evidence. Graceful worker shutdown first deletes its process row as a write
fence, then deletes the instance's remaining heartbeat rows; a new same-service
registration also clears phases left by a predecessor that exited without
running the hook. Ordinary worker heartbeat-write failures warn and
continue so a transient database write failure degrades liveness evidence and
remains due for retry, rather than converting the database blip into worker
restart churn. A
named phase is different: if its start marker cannot be persisted, the current
rebuild attempt fails before starting the monolithic work, so the worker never
runs a many-hour operation without the evidence used to interpret it.

Within `execution_*`, the API may write traces, steps, and normal
`execution_cache_outcomes` only for documented on-demand verified-resolution
product routes when a selected-snapshot cache miss is live-executed and
returned in the same response. That path uses execution persistence and does
not write projections, API state, manifests, discovery rows, normalized events,
identity rows, or adapter-owned facts. Synchronous indexer/reorg repair is the
other non-execution-worker write owner during chain reconciliation. It may
delete or invalidate reusable `execution_cache_outcomes` rows whose dependency
set includes an orphaned block identity. It does not write traces, steps,
normal outcomes, projections, API state, or manifest state.

For identity-row repair, the storage-owned `surface_bindings` update trigger is the bounded non-projection-worker writer for `projection_invalidations`. It enqueues `name_current` and `address_names_current` keys when repair updates change `active_to` or `canonicality_state` for an identity row. When a readable predecessor was closed by a now-orphaned successor binding, ENSv2 `RegistrationReleased`, ENSv2 replacement-reservation `SurfaceUnbound`, or ENSv1/Basenames `SurfaceUnbound`, losing-branch repair requires that orphaned boundary to equal the stored `active_to`. ENSv2 close evidence starts from the lineage block timestamp plus the normalized event's transaction/log offset and is clamped to at least one microsecond after the closing binding's `active_from`; ENSv1/Basenames `SurfaceUnbound` uses its recorded `active_to`. When block timestamps tie, successor ordering uses `(block_number, intra-block active_from)`, so a later block cannot be mistaken for an earlier successor solely because its timestamp is equal. Repair then chooses the earliest surviving readable same-chain successor start or canonical close event for the same logical name and resource after the predecessor start, and reopens the binding only when no surviving boundary remains. This preserves a winning-branch re-included release, prevents unrelated orphaned evidence from reopening a different boundary, and leaves the predecessor's stable creation anchor unchanged. The normalized-event upsert repair path has bounded stale-key invalidation exceptions: Basenames primary-claim source repair enqueues both old and repaired `primary_names_current` tuple keys when it rewrites an existing `RecordChanged(name)`/resolver claim observation from the old Basenames reverse-registrar tuple to the ENSv1 Base `L2ReverseRegistrar` tuple; ENSv1 reverse-name profile enrichment enqueues the added `primary_names_current` tuple but refuses a replay that would remove its durable claim source; ENSv1 registrar renewal resource repair enqueues old and repaired resource keys for affected resource-keyed projections when it repoints stale renewal/resource events; ENSv1 and Basenames Base registry/registrar event-time resource repair enqueues stale and repaired resource keys, or only the non-null key when one side of the repair has no resource anchor, for affected resource-keyed projections; ENSv1 same-transaction registration setup repair enqueues affected `name_current` and `permissions_current` keys when it repairs a `RegistrationGranted` pre-state and orphans leaked registry-only setup control rows; ENSv1 authority-epoch registry-owner repair updates existing deterministic `AuthorityEpochChanged` after-state rows when replay adds the registry owner field; ENSv1 authority-epoch resolver-boundary repair enqueues affected `record_inventory_current` keys when it repairs deterministic `ResolverChanged` boundary rows; ENSv1 registry resolver before-state repair enqueues affected `record_inventory_current` keys when it repairs anchored `ResolverChanged` before-state rows; and ENSv1 wrapper-token before-state repair updates existing deterministic `TokenControlTransferred` before-state rows when replay replaces a stale pre-wrapper authority kind or stale previous wrapper owner with the current replay-derived value. ENSv1 reverse primary-claim resolver before-state repair has no projection key to invalidate because the repaired row is intentionally unanchored; it records only a normalized-event change. These authority repair paths record normalized-event changes so downstream projections can refresh. Label-preimage insertion is another bounded storage-owned invalidation path: new retained labelhashes enqueue `children_current` keys for known parent surfaces that have historical canonical ENSv1 or Basenames registry child edges using that labelhash, so later projection rebuilds can replace unknown-label placeholders. Read-safe parent `name_surfaces` insertion or refresh also enqueues `children_current` for retained canonical registry child edges under that parent, so child enumeration does not depend on whether the registry edge, label preimage, or parent surface arrived first. `label_preimages` rows are proof-checked by normalizing the candidate label and recomputing the keccak labelhash; once retained, the mapping is durable even if the source event or surface later becomes noncanonical. Canonicality still gates the registry child edge and exact-name surface rows that projections publish. Adapters write identity rows and normalized events, plus only the adapter-owned transient resolver-profile run, target, and state-item staging rows listed above; they do not write projection rows directly.[^v1-l2rev-base-deploy][^v1-l2rev-base-args][^v1-l2rev-event][^bn-revreg-l12][^bn-revreg-l150]

ENSv1 same-resource registration-release repair updates existing synthetic `RegistrationReleased` before-state rows when replay recovers a different prior registrant for the same registrar resource. It records normalized-event changes for downstream projection refresh without changing resource keys.

For interval identity and normalized authority/permission events, adapters mint and reuse `resource_id`, `token_lineage_id`, and `surface_binding_id` per the architecture identity rules. Projection workers consume those rows; they do not infer alternate continuity or synthesize cross-resource permission carry.

## Manifests and discovery persistence

At minimum:

- `contract_instances` — one row per stable `contract_instance_id` with chain, contract kind, and provenance. Roots use the same identity family as other contract instances.
- `contract_instance_addresses` — time-ranged address attributes keyed by `contract_instance_id`. One id may carry multiple non-overlapping active ranges. Manifest-declared address ranges may carry nullable inclusive `start_block` metadata where the manifest supplied it.
- `discovery_edges` — keyed by `edge_id` with `from_contract_instance_id`, `to_contract_instance_id`, `edge_kind`, active range, provenance, canonicality.
- Materialized watch-plan rows keyed by `contract_instance_id` plus chain and range; root start nodes keyed by the root `contract_instance_id`. Address is a derived watch target, not the durable identity. An omitted `start_block` is persisted as null rather than coerced to zero.

Resolver-profile admission state (PublicResolver-generation profiles for ENSv1, `L2Resolver` compatibility for Basenames) is gated separately from contract-instance admission. It may be derived from existing discovery provenance, normalized resolver-discovery events, manifest contract roles, code-hash facts, and proxy/implementation edges; a dedicated profile-fact table is not required. Profile admission gates complete-family, resolver-overview, latest-only, authorization, and onchain-call parity claims for the affected resolver instance — not baseline generic resolver-event observation.[^v1-pres-l20][^v1-pres-l66][^bn-l2resolver-l4][^bn-l2resolver-l16][^bn-l2resolver-l29]

`manifest_alert_*` carries an observation identity, observation kind (`manifest_drift` or `proxy_implementation_drift`), lifecycle status, manifest version, source family, chain, contract-instance references, nullable proxy/implementation edge references, expected and observed code-hash or implementation-edge material, derived watch-plan metadata, first/last observed timestamps, and nullable remediation metadata. Writing it does not write `normalized_events`, mutate manifest truth, mutate discovery admission, change capability flags, or expose API state. A proxy implementation observation preserves the proxy `contract_instance_id`; implementation churn is represented by an observed or admitted edge, not by minting a replacement proxy identity.

## Backfill persistence

At minimum:

- `backfill_jobs` — one row per bounded backfill job with selected profile, chain, selector kind, resolved source identity, scan mode, declared range start and end, the atomically captured `raw_log_retention_generation`, idempotency key, lifecycle status, failure metadata, timestamps.
- `backfill_ranges` — child range records with declared range bounds, last-completed checkpoint, lease owner, lease token, lease expiry, attempt counters, lifecycle status, failure metadata, timestamps. A new range initializes its checkpoint to one block before the declared start so resume starts at `checkpoint_block_number + 1`.
- Monotonic helper-owned checkpoint fields that let a worker resume after crash without widening the original range or reclassifying already admitted facts.

Operational finalized catch-up uses these same families. It may create many finite chunks, but each chunk preserves one immutable job shape and idempotency key. Capacity preflight (current Postgres size, writable free disk, configured object-cache budget) records explicit failure or paused state in existing lifecycle/failure metadata when capacity is insufficient.

The selector identity fields on a job:

- `selector_kind` — `whole_active_watched_chain`, `source_family`, or `watched_target_set`
- `source_family` — the requested family for `selector_kind=source_family`, otherwise null
- `requested_watched_targets` — caller-supplied watched targets for `selector_kind=watched_target_set`, otherwise empty
- `selected_targets` — the resolved materialized target set sorted by `source_family`, `contract_instance_id`, normalized address, effective from-block, effective to-block
- `source_identity_hash` — digest of `selector_kind`, `source_family`, `requested_watched_targets`, and `selected_targets`

Very large source-family jobs and whole-active watched-chain jobs may persist compact selector identity instead of a full `selected_targets` array. Whole-active selectors use this compact form when the resolved target set has more than 10,000 targets. Compact identity sets `source_identity_payload_format=selected_targets_digest_v1`, keeps the selector fields including `source_family` (null for whole-active selectors), and carries `selected_target_count`, `selected_targets_digest_algorithm=keccak256`, `selected_targets_digest`, a first/last `selected_targets_sample`, and `source_identity_hash`. The digest input remains the sorted canonical `selected_targets` tuple; the compact payload is therefore `source_family` plus the target-set digest, not a source-family-only identity.

Idempotency validation has one compatibility bridge for jobs created before compact identity was introduced: a legacy full-payload identity and a `selected_targets_digest_v1` identity may match even when their `source_identity_hash` values differ, but only when every selector/provider field outside the selected-target representation and hash matches exactly and the compact count, digest, and sample recompute from the full `selected_targets` set. A different target set, topic plan, scan/provider field, range, chain, profile, scan mode, or idempotency key remains an immutable-job conflict.

When whole-chain or mixed-source backfill uses generic ENSv1 resolver topic scanning, the persisted identity records that scan in `generic_topic_scans` with `source_identity_payload_format=generic_resolver_event_topics_v1` and records the exact fetched topic set in `topic0s_by_source_family`. The address-scoped portion may be stored as `selected_targets_with_generic_topic_scans_v1` or, when compact, `selected_targets_digest_with_generic_topic_scans_v1`; in both forms `selected_targets`, `selected_target_count`, digest, and sample intentionally exclude the resolver-family targets covered by the generic topic scan while `source_identity_hash` covers the selected-target identity, generic scan declaration, and fetched topic set.

Manual backfill idempotency is derived from deployment profile, chain, finite range, scan family, and source identity. It must not include the local manifest root path: moving the same selected manifest corpus between filesystem locations does not create new raw backfill work. Automatic bootstrap and operations catch-up additionally append the raw-log retention generation captured atomically when the job is created. The logical selector/range may therefore produce new work after compaction instead of reusing a completed job from an older generation. Bootstrap checkpoint reuse matches persisted source identity, contiguous range coverage, and the exact current retention generation rather than the literal idempotency-key text.

Bootstrap planning clips new work only with checkpoints whose parent job and child range are both completed. A failed, expired, or otherwise incomplete child checkpoint remains resumability state inside its original full-range job; it is not durable fetch-coverage authority and cannot turn a restart into a suffix-only replacement job. Restarting reconstructs the original selector and range identity, reuses that idempotent job, and resumes its child checkpoint so the parent can record full-range coverage facts atomically at completion.

Historic checkpoint promotion consumes durable `backfill_coverage_facts`
rows (see below) instead of recomputing coverage from persisted job source
identities. Coverage is keyed by active `(source_family, address)` intervals,
so a tuple is required only for blocks where it is active, one source
family's coverage never credits another family at the same address, and
family-scope fact rows credit every address of that family over the fact
interval. Retained full-block payload metadata is cache evidence, not a
substitute for fetch coverage. Watched source families with no active ABI
event topics do not impose historical selected-log coverage, because there
are no selected log facts for backfill to prove. Event-silent
reverse-resolver direct-call indexing is scoped to ordinary live-tip
reconciliation: live intake retains direct-call transactions and receipts
from the current live block payload and records durable observations for
later current-state hydration, but historic stored-lineage promotion does not
require or synthesize per-block event-silent reverse-resolver state. That
reverse resolver data is latest-only by design.

`effective_to_block` is finite for every persisted selected target — backfill jobs are finite at creation time. The initial bootstrap planning snapshot includes eligible manifest-declared targets plus already-materialized finite-known-start ENSv2 root, registry, and resolver discovery targets; their ranges end at the provider finalized head latched for that chain's startup run. ENSv2 fixed-point recovery may add further finite-known-start targets through that same finalized head. It does not generalize recursive target enumeration to other source families: ENSv1 generic resolver and Basenames recursive registry history use their separate scan mechanisms. A watched target whose manifest-declared `start_block` is unknown is skipped by bootstrap; it leaves no synthetic block-zero, provider-history, recent-window, or job-start range in `backfill_*`.

### Backfill coverage facts

`backfill_coverage_facts` records, per completed job, which watched
`(source_family, address)` tuples had their logs fetched over which block
interval — durable fetch evidence derived from the job's immutable selector
plan at completion time. Facts and the job status flip commit in one transaction. Historic stored-lineage
checkpoint promotion consumes these rows
instead of recomputing selector plans from persisted identities. A row is
authoritative only while its parent job is `completed` and the whole fact
interval is contained by that job's declared range. Writers reject other
rows, and promotion applies the same checks so legacy or manually corrupted
rows fail closed.

Target-bounded ENSv1 and Basenames registry-discovery repair also consumes
these facts after a raw-log retention rotation. It loads every manifest-backed
current or closed historical registry-emitter interval through the winning
head and requires a gap-free union of exact-address or family-scope facts whose
completed parent jobs captured the current retention generation. Facts from an
older generation never compose into that proof, and a topic-filtered job must
still match the current manifest event-topic set. The proof records the
concrete discovery-admission epoch it inspected; the absence-aware discovery
writer compares that epoch under its writer fence. A chain-scoped raw-log
mutation guard remains held from proof loading through adapter persistence, so
generation drift, admission drift, or a same-chain raw-log mutation fails
closed before the winning checkpoint can advance.

When live target-bounded reconciliation finds that this proof is incomplete,
it returns the first uncovered `ens_v1_registry_l1` or
`basenames_base_registry` tuple with its retention generation, exact address,
and inclusive missing bounds. Poll recovery may create an ordinary raw-only,
hash-pinned provider job for exactly that historical watched target and range;
the recovery job records raw facts and current-generation coverage but does not
run adapters, mutate discovery, or advance the chain checkpoint. The unchanged
reconciliation attempt must then reload and validate the complete proof before
its absence-aware writer can proceed. Authority drift before or after provider
I/O causes a replan, while a missing or out-of-bounds target, widened selector,
provider failure, stale topic evidence, untyped adapter error, or failure to
converge within the bounded poll loop remains fail-closed.

Full-closure normalized-event replay consumes the same current-generation fact
authority for every closure family outside the ENSv2 root/registry retained-
history proof. It requires gap-free exact-address or family-scope coverage for
every current or closed historical interval through the replay target, rejects
older-generation facts and stale topic-filtered evidence, and rechecks the
raw-log input version and discovery-admission epoch while establishing the
proof. A migrated generation-one database can therefore recover replay
authority by completing generation-scoped historical backfill; a retained
suffix without that coverage still fails closed.

Stored-lineage promotion ([checkpoint promotion](glossary.md); "promotion"
unqualified in this section always means the checkpoint sense, never capability
promotion) persists a storage-owned operational [coverage frontier](glossary.md):
a saved proof of which [watched](glossary.md) block intervals have complete
backfill coverage. It is
durable across indexer restarts and independent of projection tables and replay
markers. It is not a raw fact, manifest or discovery authority, chain checkpoint,
projection, execution artifact, or API-readable state, and discarding it only
forces fail-closed re-verification. Stored-lineage reconciliation in
`bigname-indexer` is its only publication owner and writes it through
storage-owned persistence helpers; manifest and discovery queries remain the
authority for candidate requirements. Publishing, replacing, or invalidating a
frontier does not write canonicality, enqueue projection work, or invalidate an
execution-cache outcome.

`stored_lineage_coverage_frontiers` has one header row per chain. The header
contains a monotonically increasing `snapshot_revision`, a
`proof_format_version`, the `discovery_admission_epoch` used to build the
snapshot, inclusive `verified_from_block` and `verified_through_block` bounds,
the canonical map of active event topic0 values by source family, the exact
requirement-row count, a constant-state 128-bit integrity fingerprint over the
normalized requirement rows, and `updated_at`. The count and fingerprint detect
missing or altered child rows without materializing the complete snapshot in an
aggregate value; they are integrity metadata, not fetch evidence or a
cryptographic authority claim. Proof format `stored_lineage_coverage_v1` binds
the header to the watched-tuple and `backfill_coverage_facts` rules in this
section. Topic values
are 32-byte lower-hex topic0 values, sorted and deduplicated within each family;
the map omits families without active log-producing topics, and those families
create no tuple requirement. A header with inverted bounds, malformed
topics, or an unsupported proof format is not coverage authority. This release
hard-refuses every proof format other than `stored_lineage_coverage_v1`; it does
not overwrite or downgrade an unrecognized row. `updated_at` is audit metadata,
not a separate freshness signal.

`stored_lineage_coverage_frontier_requirements` stores one normalized row for
each `(chain_id, source_family, lowercased address)` in the header snapshot. Its
`required_intervals INT8MULTIRANGE` value represents inclusive block intervals,
with overlaps and adjacent intervals coalesced and every interval clipped to the
header bounds. Storage encodes an inclusive `from..=through` interval as the
canonical PostgreSQL half-open range `[from, through + 1)`; an inclusive upper
bound at the signed-64-bit maximum is rejected rather than becoming unbounded.
Empty, lower-unbounded, and upper-unbounded child multiranges are malformed.
The primary key is `(chain_id, source_family, address)`. An empty snapshot has no
requirement rows; it is not represented by synthetic block-zero requirements.
Current watched rows and finitely closed historical discovery rows participate
under the same active source and mapped target-manifest rules as direct fact
verification. Deprecated-profile intervals remain audit evidence but not
coverage authority, and a watch with an unknown start remains unknown.

Postgres builds the complete candidate requirement snapshot in a
transaction-local table from manifest and discovery authority. It also computes
the candidate difference against the saved requirement rows; the indexer does
not fetch the whole watched surface or submit a trusted snapshot or difference.
For a tuple whose source-family topic set is unchanged, the proof work is only
the candidate intervals not present in the saved interval set. For a new family
or a family whose topic set changed, every candidate interval for that family is
new proof work. A valid candidate retains the saved lower bound unless a newly
explicit watched interval begins earlier, and uses the attempted look-ahead
block as its upper bound. Extending that upper bound naturally adds the
uncovered suffix for each active tuple. Removed tuples and shortened interval
portions require no fact read, but they disappear from the candidate that will
replace the saved snapshot. A retroactive admission therefore proves its own
added history instead of inheriting an advanced checkpoint, while an unrelated
watched-set change does not rescan unchanged history.

Every added or topic-changed candidate interval must pass the indexed,
gap-free `backfill_coverage_facts` check before publication. The proof uses only
completed-job facts and the exact address-scope, family-scope, interval, and
topic-plan rules below. It never treats the prior frontier, a projection, a job
checkpoint, retained payload metadata, or stored lineage alone as fetch
evidence. The append-only fact discipline and the prohibition on pruning parent
jobs are therefore prerequisites for durable frontier reuse.

After proof succeeds, publication takes the chain's
`discovery_admission_epochs` row in shared mode, requires the candidate epoch to
remain current, and atomically replaces the header and all requirement rows.
The header write is a compare-and-swap (CAS) against the revision that produced
the difference, or against expected absence for a cold chain. The first publish
uses revision 1 and each replacement increments it by exactly one. Epoch drift
or a lost CAS publishes nothing. Missing state, malformed current-format state
(including a child count or fingerprint mismatch),
or a saved lower bound that does not contain the next promotion path is handled
as a cold proof: no saved intervals are reused, and promotion remains refused
until a complete current-format candidate has been proved and published. An
unsupported format remains a hard refusal. A CAS conflict reloads the winning
snapshot and replans; it does not let the losing attempt promote from its
unpublished proof.

A checkpoint regression whose next promoted path begins below
`verified_from_block` invalidates reuse of the whole saved frontier. The stale
row may remain as diagnostic state, but the indexer must perform the cold proof
from the earlier of the path and the earliest explicit watched start, then
publish a complete replacement before promotion. Closed historical discovery
intervals participate in that lower bound. Normal polls extend or
differentially replace the durable frontier, so a restart does not by itself
rescan unchanged history.

The frontier proves selected-log fetch coverage only. Provider hash and ancestry
validation, canonical child-path validation, same-height non-orphan fork checks,
selected-log [companion checks](glossary.md), and the final admission-epoch fence remain
per-promotion checks. Immediately before checkpoint advancement, promotion
reacquires the shared admission-epoch lock for the frontier's epoch and holds it
through the checkpoint write. That final transaction briefly excludes lineage
writers and repeats the fork check over the exact promoted path. Manifest sync
and discovery admission still take the owning chain's epoch row in exclusive
mode and bump it for every real watched-set mutation, so either frontier
publication or final checkpoint advancement refuses concurrent authority drift.

Topic-filtered coverage facts are valid relative to the manifest event topics
persisted by their job. A retained stale job does not by itself poison a range:
promotion refuses only when one of that job's facts would supply coverage for
a historically required `(source_family, address)` interval and no gap-free
union of current-topic or topic-unfiltered facts replaces that evidence.
Closed intervals remain required after the address or discovery edge is
deactivated while their source and mapped target manifests remain active.
This keeps immutable job history auditable while allowing one or more complete
reruns on the current manifest to restore promotion.

- Scope semantics: `scope=address` rows carry the lowercased emitting
  address and cover exactly that tuple. `scope=family` rows carry a NULL
  address and mean every address of the family is covered by a
  topics-complete fetch over the row's interval (ENSv1 generic resolver
  scans and Base Basenames registry topic scan-all, whether Coinbase SQL or
  hash-pinned).
- Promotion coverage is the gap-free union of the exact tuple's address rows
  and its family's family-scope rows. Overlapping or adjacent intervals from
  separate completed jobs compose; a missing block, a different address, or a
  different source family does not. This lets the default sequence of
  independently completed 32-block `ops-catchup` jobs prove a larger
  stored-lineage promotion slice without weakening the exact watched-tuple
  requirement.
- Stored-lineage companion validation uses the same scope semantics.
  Address-scoped facts select every log from the exact watched address during
  its active interval; family-scoped facts select only logs whose topic0 is in
  the family's current active manifest ABI. Same-transaction sibling logs are
  retained replay context, not independent code-observation requirements, while
  the transaction and receipt containing a selected log remain required.
- Derivation kinds: `job_completion` rows are written by the completing
  job from its validated in-memory plan;
  `legacy_full_payload_identity` rows are
  re-derived by `repair derive-backfill-coverage-facts` from persisted
  verbatim-target identities of already-completed jobs.
- Append-only discipline: no code path UPDATEs a fact row. Re-derivation is
  idempotent through `ON CONFLICT DO NOTHING` against the tuple key
  `(backfill_job_id, source_family, scope, address, covered_from_block,
  covered_to_block)` (`NULLS NOT DISTINCT`); the same derivation re-run
  inserts nothing.
- Family facts are clamped to the merged union segments of the family's
  target effective windows intersected with the job range — a deliberate
  under-claim relative to the raw job range, because the Coinbase SQL
  scan-all planner skips windows holding no targets. It cannot cause a
  spurious promotion refusal: requirement tuples are by construction inside
  the union of the family's target windows.
- Repair derivability: identities that persist the fetched target set
  verbatim (jobs created before compact digests, whose identities used
  fnv1a64 hashes and stored the full target payload) re-derive completely, as does the
  hash-pinned registry scan-all identity
  (`basenames_registry_scan_all_topics_v1`), which persists its topic0 set
  verbatim — repair derives a full-job-range family fact from it (unlike its
  Coinbase SQL counterpart below, which persists no spans and is refused). Compact digest
  identities (`selected_targets_digest_v1` and its generic-scan variant)
  are refused — the target set is unrecoverable from a digest. Family-scan
  identities that do not persist the scanned family's target windows
  (`basenames_registry_scan_all_event_signatures_v1`,
  `generic_resolver_event_topics_v1`, and
  `selected_targets_with_generic_topic_scans_v1` whose producers filtered
  the generic families' targets out of `selected_targets`) are refused
  rather than deriving partial coverage; those jobs must be re-run on
  fact-writing code.
- `backfill_coverage_facts.backfill_job_id` cascades on job delete. Once
  checkpoint promotion relies on facts, deleting or pruning completed
  `backfill_jobs` rows silently destroys promotion evidence — job pruning
  is forbidden.

### Backfill range checkpoint vs chain checkpoint

Backfill range checkpoints are operational state. They record only that bounded fetch/resume work reached a position in a declared range. They do not change any `canonicality_state` value and do not advance `canonical_head`, `safe_head`, or `finalized_head`.

Backfill raw admission still writes canonicality for the facts it admits. When the admitted historical range is already proven canonical, safe, or finalized by retained lineage or provider checkpoint evidence, new lineage, raw-fact, and normalized-event rows use `canonical`, `safe`, or `finalized` as appropriate rather than staying `observed` solely because the source was backfill. If the evidence is absent, the storage layer preserves the weaker state.

## Partitioning status

The current migrations create ordinary PostgreSQL tables for lineage, raw facts, normalized events, execution, identity, and projections. There is no checked-in table partitioning baseline yet. Row-volume control currently comes from explicit indexes, bounded backfill ranges, normalized-replay catch-up chunks, and retention/compaction policy. Any future partitioning change is a migration-bearing storage change and must update this section with the concrete table list and keys.

## Canonicality model

`chain_lineage` persists the recent reconciled block window keyed by `(chain_id, block_hash)`:

- `parent_hash`
- `block_number`
- `timestamp`
- checkpoint-promotion state

Header audit fields are optional retention. The default lineage contract omits `logs_bloom`, `transactions_root`, `receipts_root`, and `state_root`; reorg repair walks backward through `(block_hash, parent_hash)` until it reaches a stored matching ancestor, then marks the losing stored branch and dependent facts noncanonical from that point forward.

An auditable-header retention mode stores those fields in `chain_header_audit` keyed by the same `(chain_id, block_hash)` so inspection tooling can explain or cross-check fetched payloads. Their absence cannot prevent canonicality repair, checkpoint promotion, replay over retained selected facts, or projection rebuilds. When both stored and incoming audit rows carry the same field, mismatches are hard storage conflicts.

`raw_blocks` is not a durable table. Intake, replay, workers, adapters, audit helpers, and tests read block timestamps and canonicality from `chain_lineage` and read optional audit roots/bloom from `chain_header_audit` when auditable retention is enabled. Normal replay batches block-anchor admission once through the `chain_lineage` write boundary.

Every fact-derived row that can be invalidated by reorg carries `chain_id`, `block_number`, `block_hash`, `canonicality_state`, `observed_at`. `canonicality_state` values:

- `observed`
- `canonical`
- `safe`
- `finalized`
- `orphaned`

Rules:

- block hash is the identity anchor; block number is position only
- fork detection marks affected rows `orphaned`; it does not delete them
- reorg repair preserves lineage and normalized-event/identity canonicality for losing branches as audit truth; log-audit mode also preserves selected raw-log/transaction/receipt facts. Minimal raw-log retention may already have compacted those staging rows
- evictable payload-cache bytes or compacted staging rows do not erase canonicality, normalized-event provenance, or replay-critical evidence retained by the selected policy
- optional header audit fields are verified when both stored and incoming audit rows carry them. A minimal replay does not conflict with an existing auditable row solely because it omitted those fields
- projection rebuilds read rows that are `canonical`, `safe`, or `finalized` by default; history and audit tools may opt into `observed` and `orphaned` rows
- safe and finalized checkpoint promotion is monotonic per chain

## Reorg repair

Reorg repair preserves audit truth: orphaned rows persist for explanation and rebuild, not deletion. The losing branch's lineage, identity rows, and normalized events stay canonical-state `orphaned` so explain and history routes can still reconstruct what was observed.

Registry discovery authority is repaired before the winning chain checkpoint advances. Reconciliation forces complete canonical ENSv1, Basenames, and ENSv2 registry-source passes through the winning head even when that head is event-silent, so a losing `NewOwner`, `NewResolver`, `SubregistryUpdated`, or resolver assignment cannot remain in active discovery edges, contract addresses, or the watch plan merely because log-derived live scope is empty. ENSv1 and Basenames replay closed historical registry-emitter intervals through that boundary so a canonical subregistry branch closed by the losing fork can be restored in full. Raw facts after the winning head do not enter the repair, and existing non-orphaned later discovery assignments are preserved. Discovery mutations remain manifest/discovery-owned and use the normal reachability cascade and admission-epoch fence. Generation-zero retained input is replayable from its original boundary. After destructive retention rotation, ENSv1 and Basenames may recover this target-bounded repair only from gap-free current-generation coverage for every required registry-emitter interval under an unchanged discovery-admission epoch; ENSv2 uses its generation-bound root/registry proof. Missing or stale coverage, topic drift, generation drift, or epoch drift leaves the winning checkpoint unpublished for retry. Other stateful ENSv1 replay still requires its own documented generation-bound proof or versioned snapshot.

Execution cache rows follow the same hash-first canonicality rule. When reorg repair marks a block identity `orphaned`, synchronous indexer/reorg repair invalidates or deletes any reusable `execution_cache_outcomes` row whose dependency set includes that `(chain_id, block_hash)` or a boundary resolved through it. The invalidation makes the cached outcome ineligible for reuse; it does not delete raw facts, traces, steps, attachments, or any execution-owned audit artifact.

Resolver-profile inputs use a separate crash-safe handoff. Statement triggers compare the latest non-orphaned `raw_code_hashes` observation before and after an insert or semantic update and coalesce a changed `(chain_id, contract_address)` into `resolver_profile_input_changes`. Canonicality repair and guarded code-hash correction therefore use the same path as ordinary intake. Because raw code is observed for non-resolver watched contracts too, an ordinary queued address outside current resolver-profile authority is acknowledged as irrelevant without adapter or projection work. Each row keeps a monotonically increasing `generation` and a `processed_generation`. Duplicate notifications for the same final hash inside one transaction are suppressed; distinct committed writers may safely bump the generation again, while an explicit authority kick always does.

Manifest and discovery authority use `resolver_profile_authority_journal` for changes to the [resolver profile](glossary.md), rather than holding an in-memory before/after pair. Its singleton row is the compare-and-set header: it stores the per-chain `discovery_admission_epochs` snapshot captured with the journal and a monotonic revision. `resolver_profile_authority_journal_entries` stores one JSON payload per authority-entry identity. Its canonical `entry_key` is derived from the chain, source family, address, contract instance, source, source manifest, and active block interval; resolver-profile admission semantics and seed status remain in the payload, so changing either updates the same entry and still counts as an authority change. The normalization migration decomposes every legacy `authority_snapshot.entries` item into this table before dropping the whole-snapshot column.

A full journal attempt reads the discovery epochs, streams distinct current resolver addresses through bounded authority pages into a transaction-local table, and reads the epochs again; epoch drift during that scan discards the inconsistent capture. The target cursor shares the journal transaction's connection, while bounded admission reads use another pooled connection. Indexer processes retain one more connection for the Base normalized-event writer guard, so resolver-profile authority journaling requires a pool capacity of at least three and rejects a smaller pool before acquiring the journal transaction. SQL anti-joins compare the staged rows with persisted entries without loading either complete set into the indexer. Revision zero is an initialization marker: the first stable capture establishes the baseline without scheduling historical absence repair, because an upgraded database's generation-one raw-log corpus is explicitly of unknown completeness. The queue migration seeds historical targets only for generation-zero corpora, whose retained history has never crossed a destructive boundary.

After that baseline, changed entries directly target their old and new addresses. A changed seed entry also expands to every address in its chain and source family across both the persisted and staged entry sets, preserving removal cleanup. One storage transaction pages those unique targets through `resolver_profile_input_changes`, applies only changed entry upserts and deletes in bounded statements, and then compare-and-set advances the header revision and epoch snapshot. The handoff commits all three effects or none: a stale journal revision rolls back queue increments and entry mutations, then the indexer reloads the header and retries instead of publishing work derived from an obsolete diff. A crash after a later authority mutation but before this handoff is recovered by startup or the next epoch guard.

Ordinary live adapter sync does not load the whole resolver authority graph before and after every block. It compares only the current chain's cheap discovery-admission epoch with the journaled epoch before discovery work and again after a successful discovery call. An unchanged epoch performs zero authority scans. Drift performs the full stable diff even when the retry's discovery summary reports zero mutations, covering a prior discovery transaction that committed and then returned an error. Manifest sync and broad startup/timer adapter reconciliation run the full journal unconditionally. An authority diff force-enqueues the same generation-fenced queue even when the effective code hash did not change; the persisted prior snapshot preserves a removed resolver address long enough for absence cleanup.

The indexer classifies a bounded set of dirty inputs by loading only their matching rows from `resolver_profile_authority_journal_entries`; it never reconstructs the complete authority set for a queue drain. A dirty seed resolver expands to every active target whose [resolver profile](glossary.md) derives from that seed: all current and legacy ENSv1 PublicResolver-generation seeds fan out across `ens_v1_resolver_l1`, and the Basenames `L2Resolver` seed fans out across `basenames_base_resolver`. A non-seed candidate remains address-scoped. Seed-family addresses are keyset-paged from the journal into the adapter's exact per-chain target table. After every target page for one chain is staged, one server-side cursor derives projection invalidation keys from that exact set and persists them through bounded inserts before adapter publication can orphan their source events. The absence-aware adapter then performs one chronological reconciliation over the chain's inclusive resolver-emitter range, upserts observations still admitted by the current resolver profile, and orphans prior raw-log-backed normalized events that are now absent. The indexer upserts and removes the pre-captured invalidation keys in bounded statements on the same transaction as the normalized-event repair, so repaired events and claimable invalidations become visible together. The same-chain reconciliation lock remains held through that transaction's run cleanup and commit, so a later reconciliation cannot stage keys that an earlier publisher could consume. Those chain-keyed staging rows survive cleanup of an incomplete adapter run, so a crash after normalized-event publication cannot discard the only pre-repair key capture; a retry unions its capture with the retained keys. Only after the adapter work and direct projection invalidations are durable does the indexer acknowledge the generation it loaded. A concurrent generation makes the compare-and-set fail and remains pending, so a crash or race cannot mark unseen work complete.

Absence-aware resolver-profile replay currently has closure authority only for a never-destructively-rotated generation-zero raw-log corpus. When a chain has a later retention generation, resolver-profile convergence defers its affected input changes without acknowledging them; they remain durable pending work and still require a full database rebootstrap into a new generation-zero corpus. Deferred chains do not prevent eligible chains from reconciling and acknowledging their own generations, and the poll loop continues after emitting bounded operator-visible warnings. Deferral is not replay success, a completion checkpoint, or permission to infer absence from a retained suffix. The ENSv2 root/registry retained-history proof does not authorize this ENSv1/Basenames resolver-profile replay.

Reusable `execution_cache_outcomes` rows carry dependencies tied to explicit block-hash-bearing chain positions or boundaries. Rows that lack those dependencies fail closed.

## Replay semantics

Raw-fact normalized-event replay is indexer-owned orchestration over the adapter-owned `normalized_events` boundary. It selects bounded canonical raw facts and asks adapters to perform an upsert-only resync; it advances only its own `normalized_replay_*` cursor.

Whole-range replay is the default. Automatic bootstrap and automatic catch-up share one all-source chain cursor over persisted canonical raw facts in block order — adapter-owned identity histories combine registry, registrar, wrapper, resolver, and reverse-claim signals into one storage write boundary, so independent per-source-family cursors would tear those histories.

For a chain that requires stateful or dependency-ordered replay, automatic catch-up runs two ordered phases while holding the existing deployment-profile/chain replay ownership fence. First it runs the block-derived stateless producers that the following full-history pass does not re-emit, using the same selected canonical raw logs, manifest/source identity, decoder, event identity, provenance, and canonicality rules as live adapter sync. That stateless work uses the configured canonical raw-log candidate cap and whole-block boundaries for physical pages, but the ownership and raw-log input-version fences remain session-wide. It then runs the existing stateful and dependency-ordered pass through the latched target. The all-source cursor is the completion fence for both phases: it advances only after the second phase succeeds and the raw-log input version is accepted. A process exit after the first phase leaves that cursor pending, so restart repeats the idempotent stateless upserts before resuming the second phase; projection workers and live-poll handoff therefore cannot observe an intermediate phase as completed replay.

That completion marker does not record whether the binary that advanced it ran one
phase or two. A `raw_fact_normalized_events` cursor completed by a pre-two-phase
indexer remains complete after upgrade, so the upgraded loop is idle and does not
backfill stateless label-preimage rows omitted from that completed span. Operators
must use the [manual normalized-event replay stopgap](deployment.md#single-phase-to-two-phase-normalized-replay-upgrade)
for the exact completed range. Because cursor completion may already have
permitted raw-log compaction, that stopgap first requires current-generation
retained-history authority; operators must restore it with provider-backed
backfill or rebuild a clean generation-zero database before replay. A pending
cursor upgraded in place does run the full stateless phase before resuming
stateful closure from the existing adapter checkpoints because both images use
the same replay checkpoint context. Deploying the two-phase image before an
in-progress cursor completes avoids the manual stopgap, at the cost of that one
full stateless pass.

Normalized events are adapter-owned semantic transition rows, not guaranteed-stateless decorations on individual raw logs. Some adapters can derive every emitted row from the selected raw fact alone; those stateless adapters may be replayed from a block-hash selection. Stateful adapters derive `before_state`, resource continuity, authority metadata, resolver state, wrapper state, registrar expiry, and permission provenance from the chronological adapter history. For those adapters, replay that emits or compares transition rows must start from a valid closure boundary and carry adapter state across every physical page in the replay.

Where a stateful adapter's replay may start depends on whether raw-log history
was ever destroyed — the retention [generation](glossary.md):

- At generation zero (raw-log staging history never destroyed — no delete,
  truncate, or destructive identity/payload update), the
  current valid [closure](glossary.md) boundary is the earliest retained
  canonical raw fact for that adapter/source graph. If the required source
  graph has no retained canonical raw fact at all, a generation-zero replay
  beginning at block zero is a genuinely empty closure: there is nothing to
  replay, and generation zero itself proves the absence.
- After deletion, truncation, or another destructive generation change, a
  retained suffix of history is not a closure boundary. Replay then requires
  current-generation, gap-free `backfill_coverage_facts` through the target
  for every historically authoritative interval of each participating closure
  family, or a documented durable versioned adapter-state snapshot anchored to
  that generation and input revision.
- The persisted [retained-history proof](glossary.md) tuple remains the
  stronger authority for ENSv2 root/registry closure; those two families do
  not substitute ordinary tuple facts for that proof.
- Target-bounded live reorg repair carries a transient ENSv1/Basenames
  registry proof and discovery-admission epoch into its destructive writer.
  That transient writer token is not reusable as general replay authority: a
  full-closure replay separately validates current-generation facts for all of
  its non-ENSv2 closure families, including resolver, registrar, wrapper,
  authority, and permission inputs.
- Existing `normalized_events`, `surface_bindings`, `resources`, projection
  rows, or API-visible state are not semantic input for deterministic stateful
  replay and must not be used as implicit snapshots.

Durable ENSv1 adapter checkpoints record the raw-log retention generation and commit-ordered input revision from which their private state was derived. Each checkpointed ENSv1 adapter invocation holds the chain-scoped raw-log semantic-mutation fence and an `ACCESS SHARE` lock through final publication. On resume, a retention-generation change or a later raw-log mutation touching an already consumed block resets the adapter-private checkpoint to its replay start; a revision change confined after the consumed boundary may advance the stored version and continue. The global automatic replay cursor records the same input version. At rewind inspection it latches the current version, then rewinds to the earliest already-consumed block changed by a newer committed revision, or to the range start when the retention generation changed. Cursor publication treats the completed iteration boundary (the latched target for a full-closure pass) as inclusive: it may advance the stored input version when per-block revision metadata proves that every newer mutation is strictly above that boundary and the retention generation is unchanged. Those later raw facts remain explicit subsequent-page or backlog work and do not widen or starve the completed pass. A newer mutation at or below the boundary, a missing per-block revision witness, or a retention-generation change still fails publication and is rewound on the next iteration. Publication performs that boundary check while holding the chain-scoped mutation fence and writes the cursor plus newest accepted input version through the same fenced transaction, so a concurrent commit cannot be silently acknowledged. Raw-log commit revisions, rather than `observed_at` timestamps or sequence allocation order, are the durable rewind authority.

The raw-staging boundary classifier distinguishes an accepted newer input version from retention-generation drift and a witnessed mutation at or before an inclusive boundary. Missing per-block evidence, revision rollback, and storage failures remain integrity errors rather than ordinary stale-readiness results. The multi-chain form holds one transaction, takes chain advisory locks in sorted order, and holds one `ACCESS SHARE` lock on `raw_logs`, so final handoff validation observes one raw-input snapshot without consuming one connection per chain.

The `post_replay_live_adapter_backlog` cursor stores the raw-log retention generation and commit revision accepted for every consumed block through `next_block_number - 1`, including empty advancement. A legacy/default-version cursor or a cursor whose accepted version predates the replay prefix resets to `replay_target + 1` at the replay cursor's accepted version and reprocesses instead of waiting forever. A later same-generation mutation touching the consumed post-target range rewinds to its earliest affected block; replay-prefix drift returns ownership to full replay, while a mutation strictly after the consumed range becomes later backlog work. Adapter execution occurs outside the raw-log fence, but page publication reacquires the fence and either publishes the cursor at the observed version, rewinds and retries, or defers to replay. Final ownership uses the sorted multi-chain guard to validate replay prefixes, backlog cursors, and current canonical raw maxima, grants only a one-poll adapter permit while that guard is held, and repeats the proof on every poll. Raw-only writers queued behind the final guard commit on the far side of that permit and are therefore detected by the next cycle.

Full-closure replay may persist adapter-private checkpoints under `normalized_replay_adapter_checkpoints` and `normalized_replay_adapter_checkpoint_items`. These rows are replay orchestration state: they may contain staged adapter observations, scan watermarks, and versioned payloads needed to resume an in-progress closure pass, but they are not raw facts, manifest truth, identity rows, projection input, or API state.

A checkpoint can make process restarts resumable only for the adapter and checkpoint payload version that wrote it. One cross-process ownership fence per deployment profile and chain serializes automatic and operator full-closure sessions. Automatic catch-up owns the `raw_fact_normalized_events` checkpoint namespace; an unscoped operator block-range replay may use full closure only from the retained closure boundary and uses a deterministic range-scoped `manual_raw_fact_normalized_events:<from>:<to>` namespace. That manual session loads historically watched emitters, including closed canonical discovery intervals, retains its checkpoint after failure for an exact-range retry, and clears it only after successful completion. Source-restricted and block-hash selections remain restricted repair sessions.

The post-bootstrap startup adapter sync uses the separate
`startup_adapter_owned_raw_log_state` cursor namespace and
`startup_adapter_sync` checkpoint scope. It latches the greatest canonical
stored block or raw-log block for the chain and uses the existing ENSv1
adapter-private checkpoint formats without sharing rows with full-closure
replay. Each startup checkpoint payload also records the chain's discovery
admission epoch, which versions the manifest-declared and discovered watched
surface; a retained checkpoint resets instead of resuming when that authority
has changed. ENSv1 subregistry discovery stages only one raw-log page's changed
assignments at a time, finalizes through the streamed full-source discovery
reconcile, and emits normalized events from checkpoint pages. The streamed
reconcile therefore applies its existing
`BIGNAME_INDEXER_DISCOVERY_FULL_RECONCILE_MAX_DEACTIVATIONS` guard to startup
as well. ENSv1 unwrapped-authority startup sync likewise uses checkpointed
raw-log pages, but keeps each page's normalized events in adapter-private
checkpoint items until its name surfaces, resources, and bindings have been
materialized. It then publishes and deletes those staged events in pages of
20,000, so a continuously running projection worker cannot consume an event
before its identity rows exist. A failed startup retains its rows for the next
boot, including a stream-complete checkpoint whose target is extended by a
later boot. The indexer deletes completed startup-scoped checkpoint rows only
after every requested startup family and the resolver-profile authority journal
update have completed successfully.

For `ens_v1_unwrapped_authority`, the durable checkpoint payload is the adapter's private closure snapshot: dirty name histories, reverse-claim histories, learned name metadata, pending namehash observations, migrated-registry markers, flushed normalized-event counters, and the block-boundary watermark. To keep full-closure replay bounded, that replay lane may flush already-emitted normalized events through the adapter-owned `normalized_events` upsert boundary at checkpoint boundaries, then persist the checkpoint with those event buffers cleared. Those full-closure rows are not projection readiness, public API readiness, identity-row finalization, or a cursor boundary; projection workers still wait for the global `raw_fact_normalized_events` cursor and identity finalization. Startup uses the private event staging and post-materialization paged publication described above instead; this distinction leaves the replay-catch-up lane unchanged.

If a process exits after a flush but before the matching checkpoint save, restart may replay and upsert the same event identities again, and any differing payload remains a hard storage mismatch. A completed snapshot may remain after cursor advancement so the next closure target can extend from that private adapter boundary. Transient adapter checkpoints may be cleared after a successful closure pass only when they are not declared durable snapshot boundaries.

Full-closure replay chooses physical pages by canonical raw-log event candidate count while preserving whole-block boundaries; adapter routing may then filter that page down to the watched or generic source events that the closure pass consumes. Implementation scan guards may limit one database range probe, but they are throughput guards rather than semantic 512-block replay windows. If a single block exceeds the configured candidate-log cap, the full block is still replayed as one page; the cap is not allowed to split a block or create a replay cursor. When a scan guard is reached before the candidate-log cap, the page may advance through empty or low-density whole blocks because no semantic boundary is created until the closure target completes.

The global `raw_fact_normalized_events` cursor advances only after the stateless phase and every stateful or dependency-ordered adapter finalize their adapter-owned writes through the requested target block. Automatic full-closure catch-up latches that requested target when the cursor is created and does not widen the same closure pass just because newer live raw facts arrive while it is running or after it completes; a later closure target requires an explicit cursor rewind/reset or a documented adapter-state snapshot boundary.

A completed automatic catch-up may be followed by a separate `post_replay_live_adapter_backlog` operational cursor that live-normalizes canonical raw-log blocks already persisted after the latched replay target before normal live polling resumes adapter sync. That cursor scopes adapter routing from the selected raw-log emitters, is not a closure replay cursor, does not change the full-closure target, and remains replay-safe because it uses the same deterministic adapter-owned upsert path as live polling. Backlog normalization never replaces provider-backed live intake; the following live reconciliation still admits raw payloads for canonical blocks that were not already persisted.

Source-scoped live and backlog discovery writes are bounded manifest/discovery mutations: they reconcile touched observation keys and the affected descendant branch only, while full-source discovery carry-forward remains a closure/full-reconciliation operation. A complete ENSv2 closure loads both current emitters and closed canonical discovery intervals that intersect retained history, so replacing a discovered registry does not make its earlier registry facts unreplayable. Its source-wide reconciliation also treats absence from the complete canonical replay as deletion; source-scoped live or backlog passes never do.

ENSv2 registry/resource replay runs before ENSv2 registrar and resolver replay so contextual rows see stable registry/resource outputs; ENSv2 permissions replay then runs over the retained resolver-family raw-log history. These ENSv2 closure passes do not currently publish durable adapter-private snapshots, so a restart reruns the topologically ordered closure pass from the retained closure boundary and relies on idempotent normalized-event and identity upserts.

Ordinary ENSv2 registry live polling may retain lifecycle state in a best-effort process-local cache with at most 32 entries and an estimated 32 MiB state-weight limit per entry. The process-local cache is not a snapshot or replay boundary. When a result exceeds its 32 MiB budget, the live adapter instead writes the same lifecycle state to a versioned adapter-private checkpoint under the deployment profile, chain, live cursor, adapter, and payload-version scope; that checkpoint remains replay orchestration state and does not establish projection or API readiness.

Reuse requires exactly one selected non-orphaned target, a cached anchor on that target's exact parent-hash path, and an unchanged discovery-admission epoch. The cache stores the commit-ordered raw-log input revision it observed. Per-block-hash revisions must prove that no later raw-log mutation touched the cached ancestor path; a lower-`raw_log_id` late commit, canonicality repair, or newly admitted log on that path forces complete hydration, while a mutation confined to an unselected fork does not. Incremental hydration consumes every retained registry log after the anchor on that exact path, independent of a caller's narrower page scope.

A process-cache miss first attempts a completed durable live checkpoint. Process startup, cache eviction, and subsequent overweight polls can therefore resume incrementally when the checkpoint passes the same exact-path, input-revision, retention-generation, discovery-epoch, and closure checks. A missing, incomplete, version-incompatible, corrupt, non-advancing, or stale checkpoint, target ambiguity, an ancestor-path mutation, or discovery drift triggers complete hydration over the exact selected path through the target, but only when retained-history metadata proves that raw-log closure is complete.

Non-live registry sync deletes the process and durable live state under the chain registry fence before rewriting adapter output. After raw-log deletion or supported staging compaction, a cold or forced full hydration fails closed rather than treating the retained suffix as a complete source replay and deactivating discovery from omitted history. Unregister and regeneration transitions remove unreachable registrations, aliases, and subregistry suffixes from cached state.

A chain-scoped database advisory fence serializes every ENSv2 registry sync entrypoint; before publishing cached or durable state, the sync verifies that the raw-log input revision and discovery epoch changed only by its own reconciliation. An overweight snapshot is first staged as incomplete while that fence is held and becomes reusable only in the transaction that advances the retained-history proof. If staging or finalization cannot persist the snapshot, the live poll fails explicitly instead of reporting success and forcing another full hydration.

Contextual adapters are not stateful because of `before_state`; they are contextual because their emitted identity, row set, or payload depends on another adapter-owned identity/discovery output being stable. Empty `before_state` is not proof of stateless replay. Replay for these adapters is deterministic only after dependency closure is complete and stable, or inside a documented topologically ordered closure replay.

Batching is only a physical IO and throughput detail. Chunk size, log-count caps, whole-block replay pages, block-hash paging, process restarts, and cursor checkpoints do not create semantic replay boundaries for stateful or contextual adapters. If automatic replay cannot resume with a durable adapter snapshot, it restarts from the closure boundary. Source-scoped, per-target, and block-hash replay for a stateful or contextual adapter is operational repair only and fails closed unless the requested selection is proven closure-complete.

Current raw-fact normalized replay allows restricted block-hash/source-scoped replay only for adapters classified `stateless_raw_fact`. `stateful_closure_required` and `contextual_dependency_required` adapters fail closed for restricted replay unless the requested range starts at the retained closure boundary and the adapter has an implemented closure/dependency replay session. The central code contract is mirrored here:

| Adapter / producer | Model | Raw-fact restricted replay | Reason and proof |
| --- | --- | --- | --- |
| `block_derived_normalized_events` | `stateless_raw_fact` | Allowed | Preimage rows are decoded from selected canonical raw logs, manifest/source metadata, and decoder constants. Covered by idempotent normalized replay and block-derived adapter tests. |
| `ens_v1_reverse_claim` | `stateless_raw_fact` | Allowed | `ReverseChanged` rows are derived from selected reverse raw logs and immutable manifest/source metadata. ENSv1 Mainnet uses `ReverseClaimed`; Basenames primary-name value intake uses the ENSv1 Base `L2ReverseRegistrar` `NameForAddrChanged` log and emits the companion `RecordChanged(name)` claim observation from the same raw fact.[^v1-l2rev-event] Covered by block-range, source-scoped, and block-hash replay tests. |
| `ens_v1_subregistry_discovery` | `contextual_dependency_required` | Restricted replay denied; full retained closure replay allowed | Normalized rows include discovery-edge contract-instance context; raw-log selection alone is not stable dependency closure. |
| `ens_v1_unwrapped_authority` | `stateful_closure_required` | Restricted replay denied; full retained closure replay allowed | Authority transitions, `before_state`, resource continuity, resolver state, wrapper state, registrar expiry, and permission provenance require one ordered in-memory history across registry, registrar, wrapper, resolver, and related Basenames families. |
| `ens_v2_registry_resource_surface` | `stateful_closure_required` | Restricted replay denied; full retained closure replay allowed | Token/resource identity, suffix state, bindings, discovery observations, and regeneration intervals depend on canonical ordered registry history through the replay target. |
| `ens_v2_registrar` | `contextual_dependency_required` | Restricted replay denied; full retained closure replay allowed | Registrar rows resolve `logical_name_id` and `resource_id` from stable ENSv2 registry/resource output replayed through the same target. |
| `ens_v2_resolver` | `contextual_dependency_required` | Restricted replay denied; full retained closure replay allowed | Resolver rows resolve name/resource links from stable `name_surfaces` and `surface_bindings` replayed through the same target. |
| `ens_v2_permissions` | `stateful_closure_required` | Restricted replay denied; full retained closure replay allowed | Permission resources and role events depend on prior resolver resource-hint observations in canonical order through the replay target. |
| `manifest_normalized_events` | `contextual_dependency_required` | Not a raw-fact replay participant | Manifest rows derive from manifest, capability, code-hash, and discovery-edge corpus state rather than selected raw logs. |

Source-scoped or per-target replay is an operational repair mode. It narrows the raw-log selection and adapter source scope; it does not narrow canonicality, change persisted backfill job identity, delete raw facts from other sources, mutate discovery or manifests, or promote any coverage to supported. Storage helpers, projections, API code, and inspection tooling do not synthesize normalized events outside this boundary.

Replay reads canonical durable hot facts first. It may use a retained durable cold payload only when an explicitly retained replay contract requires that payload. For block-scoped payloads it may use provider re-fetch only through the digest-checked, fail-closed cache-fill path.

Adapter-private replay checkpoint payloads are resumability state, not canonical event payloads. They may use versioned, lossless encodings for strings that PostgreSQL `jsonb` cannot store directly, and adapters must decode those snapshots before continuing deterministic replay.

Replay does not delete stale `normalized_events` or replace existing payloads for an already persisted normalized-event identity. The upsert path inserts absent rows and refreshes canonicality for matching identities; conflicting payloads remain mismatches except for the explicitly documented adapter-repair fields below. Adapter-owned identity rows may be marked `orphaned` only when those rows have no backing normalized event, were produced by the same adapter boundary, and would otherwise overlap the incoming identity interval.

Replay does not mutate `chain_*`, `raw_*`, `backfill_*`, `projection_*`, `execution_*`, manifests, discovery rows, public API state, or checkpoint promotion state.

### Adapter repair

Explicit adapter repair is narrower than replay and exists for deterministic adapter bugs where existing normalized-event rows can be proven to be the same adapter output but a documented field or adapter-derived identity component was encoded lossily. The triggering conflicted row matches the retained source identity; related-row repair is constrained to the same adapter, chain, logical name, canonical state, and documented repair boundary. Repair updates only the fields, identity components, stale-row orphaning, and stale-key invalidations listed below. In minimal raw-log deployments, repair may fetch exact historical logs directly from the configured provider or same-host Reth substrate without re-materializing `raw_logs`.

New repair work lands in the Rust repair framework, or in shared SQL functions invoked from that framework when SQL is the natural expression of the proof. Migration-only repair rewrites are reserved for schema/index/trigger changes or tightly bounded one-time invocations of the same guarded repair logic; they must not depend on `_sqlx_migrations.installed_on` or wall-clock cutoffs. Repair code that rewrites `event_identity` must include the same collision handling as the Rust framework, and repair code that widens or reopens a `surface_bindings.active_to` interval must be explicitly listed in this section with the proof that no successor interval is invalidated, the non-overlap guard, and the downstream invalidations it records. Historical pre-framework SQL repair migrations that widened or reopened intervals are remediation artifacts, not precedent for future repair policy; the known artifacts are `20260508203000_ens_v1_registrar_live_renewal_resource_repair.sql`, `20260508204000_ens_v1_registrar_registry_boundary_repair.sql`, and `20260514110000_ens_v1_recent_renewal_resource_repair.sql`.

The admitted interval repair is losing-branch surface-binding closure repair. Its proof is an orphaned successor binding whose start equals the stored close, an orphaned ENSv2 `RegistrationReleased` or replacement-reservation `SurfaceUnbound` whose lineage timestamp plus transaction/log offset equals that close, or an orphaned ENSv1/Basenames `SurfaceUnbound` whose recorded `active_to` equals that close; event evidence must match the predecessor logical name and resource. It changes only that readable predecessor's `active_to`: the repaired value is the earliest surviving readable same-chain successor start or canonical close-event boundary strictly after the predecessor start, or `NULL` when none exists. Exact equality to the orphaned boundary prevents unrelated evidence from reopening the interval, while canonical close-event evidence preserves a winning-branch re-included release. The resulting earliest boundary is the non-overlap guard. The storage-owned surface-binding update trigger enqueues `name_current` and `address_names_current` invalidations for the changed interval.

Orphaned stable-binding reobservation is the other admitted interval reopen. Its proof is that the stored row is already `orphaned` and the winning observation matches its stable binding id, logical name, resource, kind, `active_from`, and provenance. Reobservation replaces the orphaned `active_to` with the winning observation's derived value instead of applying readable-row monotonic close rules; an open winning registration therefore removes a losing-branch unregister close, while a winning unregister in the same replay supplies its own close. The surface-binding non-overlap constraint remains the guard, and the storage-owned update trigger enqueues `name_current` and `address_names_current` invalidations when the interval changes.

The currently admitted normalized-event field repairs are:

- ENSv1 PublicResolver-compatible `TextChanged` payload repair: legacy generic `RecordChanged` rows with `record_family=text`, `record_key=text`, `selector_key=null` are rewritten to selector-specific `text:<key>` rows; selector-specific text rows missing a retained value have that value filled when the source log verifies against the indexed key hash.[^v1-text-l5][^v1-text-l21]
- ENSv1 registrar renewal resource repair: `ExpiryChanged`/`RegistrationRenewed` rows whose event identity, name, and payload match may update `resource_id` when replay recovers the stable registrar/wrapper resource anchor that an earlier replay encoded incorrectly. The old and repaired `resources` rows must be canonical ENSv1 registrar anchors for the same mainnet logical name and labelhash; when the normalized event payload does not carry `labelhash`, as with expiry-only rows, the resource provenance provides that equality check. The same repair map may update `before_state.expiry` on the repaired renewal/expiry row when the stale row had copied the renewal after-expiry into the before-state and the repaired resource provenance, or an earlier canonical registrar grant/renewal/expiry event on the repaired resource, proves the replayed pre-renewal expiry. It may also repoint later authority events on the stale resource to the repaired resource, rewrite `PermissionChanged` grant/revocation authority keys, preserve each current replay-batch renewal/expiry row's own replayed `before_state`, refresh older related renewal/expiry `before_state.expiry` from the repaired replay proof, and rewrite a stale `RegistrationReleased` event identity from the old authority key to the repaired authority key. If that repaired release identity already exists, the stale release row is marked `orphaned` instead of rewritten. The repair also orphans stale synthetic registrar grant/surface events plus old `resources`/`surface_bindings` scaffolding when no earlier canonical backing event still uses the old resource, and enqueues old and repaired resource keys for resource-keyed projections whose stale key is no longer derivable after the row is repointed.
- ENSv1 renewal before-state repair: `ExpiryChanged`/`RegistrationRenewed` rows may update only `before_state.expiry` when the source identity, namespace, logical name, registrar resource, source family, derivation kind, and `after_state` are unchanged. The stale before-expiry may be the renewal after-expiry, a later/current expiry retained on the same registrar resource, or an earlier stale expiry in the same retained registrar-resource history when both stale and replayed before-expiries are numeric and strictly less than the unchanged renewal after-expiry. The canonical mainnet registrar resource must still anchor the same logical name and labelhash, but its provenance expiry is not the proof for the repaired before-expiry because the resource can carry the current post-renewal expiry for the same registrar authority. Outside the bounded numeric same-resource case, the replayed before-expiry must match an earlier canonical ENSv1 unwrapped-authority registrar grant, expiry, or renewal event on the same resource. The repair records a normalized-event projection change and does not rewrite resource keys.
- ENSv1 registration-release before-state repair: synthetic raw-block `RegistrationReleased` rows may update only `before_state.registrant` when the namespace, logical name, registrar resource, source identity, `before_state.expiry`, and full `after_state` are unchanged, both old and replayed registrants are non-empty, and the registrar resource is a canonical mainnet resource for that logical name. The repair records a normalized-event projection change and does not rewrite resource keys.
- ENSv1 registry/registrar event-time resource repair: resolver, record, and permission rows may update `resource_id` when replay recovers the registry-only resource that was authoritative at the event block but an earlier replay attached the row to a later registrar/wrapper resource, to a legacy registry-only resource keyed only by labelhash, or to no resource anchor; they may also drop a later registrar/wrapper resource when replay proves there is no resource anchor at the event block. Repaired registry-only resources must be canonical mainnet resources for the same logical name and leftmost labelhash; legacy registry-only sources are admitted only as stale labelhash-key collisions and are rewritten to the node/namehash-scoped registry-only resource. A nullable repaired resource is admitted only when the stale resource is a later mainnet registrar/wrapper anchor for the same logical name and the source identity plus before/after state still match. A null-to-resource repair is admitted only when the source identity, logical name, and event kind still match; before/after state must still match except registry `ResolverChanged` rows may combine the resource repair with a `before_state.resolver` update from JSON `null` to a lower-hex resolver address when the after-state namehash and resolver are lower-hex values. The referenced `resource_id` remains normal adapter-owned identity output, and downstream projection publication remains gated by the corresponding identity rows. Same-transaction registration setup is excluded from that registry-only rewrite: when block-scoped replay preloads an older registry-only authority, `RegistryOwnerChanged` and `ResolverChanged` observations before a later `RegistrationGranted` in the same transaction are deferred to that new registrar resource. Record writes, renewals, and wrapper observations retain event-time attribution. Registrar-family permission rows may also be repointed from a stale registry-only resource to that registrar resource only when the registrar resource, authority key, and later `RegistrationGranted` row prove the same block/transaction ordering. Selector-specific text `RecordChanged` repairs may combine a `resource_id` update with a `value`-only after-state repair; the value-bearing after-state is preserved so event-time anchor repair does not erase a previously retained or newly replayed text value. If the renewal resource repair already repointed a related resolver/record row earlier in the same upsert transaction, the registry/registrar event-time repair treats the row as repaired when the current row now matches the replayed resource and state. `AuthorityTransferred` repairs may update only `before_state.owner` when the source identity, canonical mainnet resource, logical name, `resource_id`, and `after_state` are unchanged; a JSON `null` owner may be upgraded to a concrete owner, while an incoming JSON `null` owner is accepted as compatible but must not erase a retained concrete owner. The admitted Basenames Base in-place extension is the 2026-07-03 node/namehash re-keying ratification (recorded as "option (i)" in that decision) applied to the `basenames_base_registry` observation class on `base-mainnet`: `AuthorityTransferred`, log-scoped `PermissionChanged`, and observation-scoped `ResolverChanged` rows whose identities do not embed the registry-only authority key. It covers retained rows whose legacy registry-only resource or authority state used the pre-12bcea0 labelhash-scoped key and whose replayed row uses the current node/namehash-scoped registry-only resource. The repair may rewrite `resource_id` and only derivation-affected observation state fields: `PermissionChanged` grant/revocation source authority objects; observation `ResolverChanged` changes only `resource_id` when state is otherwise unchanged. `AuthorityEpochChanged`, boundary `PermissionChanged`, `SurfaceBound`, `SurfaceUnbound`, and `ResolverChanged` rows whose `after_state.source_event` is `AuthorityEpochChanged` are excluded from this in-place repair because their event identities include the registry-only authority key. The same guarded ENSv1 mainnet source-family class remains admitted for structurally identical event-time repairs. No `event_identity` rewrite is admitted. `RecordVersionChanged` repairs may update only `before_state.record_version`, with or without a `resource_id` change, between `null` and the immediately previous numeric version when `after_state.record_version` is unchanged, the numeric previous version is exactly `after_state.record_version - 1`, and the source identity is unchanged. Permission repairs may update only `grant_source` and `revocation_source` authority objects between the old and repaired resource provenance, and they enqueue stale and repaired resource keys, or only the non-null key for nullable repairs, for affected resource-keyed projections.
- Basenames Base registry boundary derivation-change supersession: for the same 2026-07-03 re-keying class, `basenames_base_registry` `AuthorityEpochChanged`, boundary `PermissionChanged`, `SurfaceBound`, `SurfaceUnbound`, and boundary `ResolverChanged` rows with `after_state.source_event = AuthorityEpochChanged` use identity-aware canonicality supersession instead of in-place repair. When replay inserts or re-observes a current node/namehash-scoped boundary row and a canonical stale row exists for the same raw-block anchor, logical name, block, source family, and event kind, storage verifies the old and current registry-only resources are canonical `base-mainnet` resources for the same logical name and labelhash, that the stale resource authority key is `registry-only:base-mainnet:<labelhash>`, that the current resource authority key is `registry-only:base-mainnet:<namehash>`, and that only derivation-affected state fields differ. Boundary `PermissionChanged` state verification is limited to `grant_source` and `revocation_source` authority objects changing from the stale registry resource provenance to the current registry resource provenance; subject, scope, powers, transfer behavior, inheritance path, and source event kind must otherwise match. The disclosed second widening under that standing re-keying ratification also admits the `basenames_base_registrar` `AuthorityEpochChanged` subclass whose stale identity embeds a labelhash-scoped registry-only `before_state.authority_key` while the event `resource_id` is the registrar resource. That subclass resolves the stale before authority key to its legacy registry-only resource by key, resolves the current registry-only counterpart by the same logical name and labelhash under the node/namehash derivation, verifies the stale and current rows share the same canonical registrar resource and registrar after-state, and accepts the current replay shape where `before_state` is either the node/namehash registry-only key or explicit `authority_kind=null`/`authority_key=null` when replay defers the transient registry-only epoch. The registrar before-key proof is materialized from the distinct stale before keys in each repair batch and requires the concurrent `resources_basenames_registry_authority_key_idx` and `resources_basenames_registry_logical_labelhash_idx` indexes before the supervised run. The stale row is marked `orphaned`; its `event_identity`, payload, resource, and provenance are never rewritten. This path is not gated on persisted `surface_bindings` rows. The current replayed row remains the only canonical boundary timeline, and the canonicality update is recorded through the normalized-event projection-change trigger.
- ENSv1 registry resolver before-state repair: anchored mainnet `ResolverChanged` rows from `ens_v1_registry_l1`/`ens_v1_unwrapped_authority` may update only `before_state.resolver` between JSON `null` and a lower-hex resolver address, or between lower-hex resolver addresses when the replayed before resolver equals the unchanged lower-hex `after_state.resolver`, when the source identity, logical name, canonical mainnet resource, `after_state`, and all other `before_state` fields are unchanged. The unchanged `after_state` must carry a lower-hex namehash and resolver address, and the resource provenance must still anchor the same logical name with `registrar`, `wrapper`, or `registry_only` authority. The repair records a normalized-event projection change and enqueues the affected `record_inventory_current` resource key.
- ENSv1 reverse primary-claim resolver before-state repair: unanchored mainnet `ResolverChanged` rows from `ens_v1_registry_l1`/`ens_v1_unwrapped_authority` may update only `before_state.resolver` between JSON `null` and a lower-hex resolver address when the source identity, `after_state`, and lack of `logical_name_id`/`resource_id` are unchanged. The unchanged `after_state` must carry an ENS primary-claim source whose reverse node equals the row namehash and whose claim provenance is the ENSv1 reverse registrar. The repair records a normalized-event projection change and does not enqueue resource-key invalidations because no stale projection key exists on the row.
- ENSv1 reverse primary-claim source enrichment: an unanchored mainnet `RecordChanged(name)` row from `ens_v1_resolver_l1`/`ens_v1_unwrapped_authority` may add only a structurally valid `after_state.primary_claim_source` when every other field is unchanged and the replayed resolver profile is supported. The repair never removes or replaces a retained claim source. Explicitly unsupported classification derives a separate `record-change-unsupported` event identity without claim provenance; resolver-profile publication marks the prior claim row `orphaned` rather than rewriting it, and supported reactivation makes that same claim identity readable again. Pending or unclassified evidence fails resolver-profile reconciliation before normalized-event publication. The enrichment records a normalized-event change and enqueues the added `primary_names_current` tuple.
- ENSv1 authority-epoch resolver-boundary repair: deterministic raw-block `ResolverChanged` rows whose `after_state.source_event` is `AuthorityEpochChanged` may update only `after_state.resolver` when the source identity, canonical mainnet resource, logical name, `resource_id`, `before_state`, and the rest of `after_state` are unchanged. The repair records a normalized-event projection change and enqueues the affected `record_inventory_current` resource key.
- ENSv1 same-transaction registration setup repair: legacy rows may update a `RegistrationGranted.before_state` from an inferred registry-only authority to no prior authority when replay proves earlier registry owner observations in the same transaction were deferred setup for that registration. Under the completion of the 2026-07-03 re-keying ratification, the same guarded before-state repair is admitted for `basenames_base_registrar` `RegistrationGranted` rows on `base-mainnet` when the registrar resource is canonical for the same logical name and labelhash and the stable-identity row moves from the adapter-produced keyless `{"authority_kind":"registry_only","registrant":null}` pre-state to the replayed deferred-setup no-authority pre-state. Retained leaked setup rows are not required for the Base before-state rewrite; when present, the repair may orphan same-transaction `AuthorityTransferred` and `PermissionChanged` rows plus synthetic registry-only boundary rows that were minted from the setup observation against a registry-only resource for the same logical name. It enqueues the repaired name key and affected registry-only/registrar resource keys for projection rebuilds. For the older known-name derivation, deployment alone changes no retained row: replay repairs rows whose stable event identities are emitted again, but an old transient setup event that the new derivation no longer emits is left standing unless a separately ratified repair or sweep supersedes it. The attribution rule therefore governs newly derived output and full-versus-block-scoped replay parity; it does not claim a historical rewrite of vanished setup events.
- ENSv1 wrapper-token before-state repair: deterministic `TokenControlTransferred` wrapper rows may update only `before_state.authority_kind` between stale `registrar`, `registry_only`, or JSON `null` values and current replay-derived `registrar`, `registry_only`, or JSON `null` values, or only `before_state.from` between lower-hex previous-owner addresses, when the source identity, metadata, `after_state`, and all other `before_state` fields match. The repair records a normalized-event projection change so downstream projections can refresh.
- Basenames primary-claim source repair: `RecordChanged(name)` claim-observation rows for `basenames_base_primary` may update only `after_state.primary_claim_source` when the stored tuple uses the old Basenames `ReverseRegistrar`/coin type `60`, while replay recovers the ENSv1 Base `L2ReverseRegistrar`/coin type `2147492101` tuple for the same address, namespace, reverse node, and reverse name.[^bn-readme-base-revreg][^v1-l2rev-base-deploy][^v1-l2rev-base-args][^v1-l2rev-event][^bn-revreg-l12][^bn-revreg-l150]

2026-07-03 Basenames Base registry-only derivation-change repair record: commit 12bcea0 intentionally moved registry-only ENSv1 authority resource derivation from a labelhash-scoped key to the current node/namehash-scoped key. Its checked-in repair record admitted only the Basenames `AuthorityTransferred` parity case; widening beyond that was not anticipated there. The ratified re-keying method ("option (i)") is the sanctioned Rust repair machinery, not an ad-hoc SQL migration. The completing live census checked every 12bcea0 embedding path in canonical/safe/finalized `basenames_base_registry` and `basenames_base_registrar` rows on `base-mainnet`: `resource_id`, event identity, top-level boundary `authority_key`, nested `grant_source` and `revocation_source` authority objects, and `inheritance_path[*].authority_key`. `inheritance_path` had zero hits. The complete affected-class matrix is: `basenames_base_registry` `AuthorityTransferred` 6,924 log-scoped rows, covered by in-place repair; `PermissionChanged` 275,964 log-scoped rows, covered by in-place repair; boundary `PermissionChanged` 1,164,540 rows, added here to boundary supersession; observation `ResolverChanged` 162,495 rows, covered by in-place repair; boundary `ResolverChanged` 617,928 rows, covered by supersession; `AuthorityEpochChanged` 623,707 rows, covered by supersession; `SurfaceBound` 623,706 rows, covered by supersession; `SurfaceUnbound` 41,284 rows, covered by supersession. For `basenames_base_registrar`, `RegistrationGranted` has 3,716,130 stable-identity rows with the adapter-produced keyless registry-only pre-state and is added here to the before-state repair from that keyless shape to replayed no-authority deferred setup; `AuthorityEpochChanged` has 3,750,941 stale before-key rows and remains covered by the registrar supersession subclass. `basenames_base_registrar` `PermissionChanged`, `ResolverChanged`, `SurfaceBound`, and `SurfaceUnbound` had zero old labelhash-scoped key hits. The checked-in repository does not contain the deployment corpus needed to recompute these counts.

Preflight event-class census query:

```sql
SELECT
  CASE
    WHEN source_family = 'basenames_base_registrar'
     AND event_kind = 'AuthorityEpochChanged'
     AND transaction_hash IS NULL
	    AND log_index IS NULL
	    AND before_state->>'authority_kind' = 'registry_only'
	      THEN 'registrar_authority_epoch_before_registry_only'
	    WHEN source_family = 'basenames_base_registry'
	     AND event_kind = 'PermissionChanged'
	     AND transaction_hash IS NULL
	     AND log_index IS NULL
	      THEN 'registry_boundary_permission_changed'
	    WHEN source_family = 'basenames_base_registry'
	     AND event_kind = 'PermissionChanged'
	      THEN 'registry_log_permission_changed'
	    WHEN source_family = 'basenames_base_registry'
	     AND event_kind = 'ResolverChanged'
	     AND transaction_hash IS NULL
     AND log_index IS NULL
     AND after_state->>'source_event' = 'AuthorityEpochChanged'
      THEN 'registry_boundary_resolver_changed_population'
    WHEN source_family = 'basenames_base_registry'
     AND event_kind = 'ResolverChanged'
      THEN 'observation_resolver_changed'
    ELSE concat(source_family, ':', event_kind)
  END AS repair_class,
  COUNT(*) AS row_count,
  MIN(block_number) AS min_block,
  MAX(block_number) AS max_block
FROM normalized_events
WHERE namespace = 'basenames'
  AND chain_id = 'base-mainnet'
  AND source_family IN ('basenames_base_registry', 'basenames_base_registrar')
  AND derivation_kind = 'ens_v1_unwrapped_authority'
  AND canonicality_state IN ('canonical', 'safe', 'finalized')
  AND (
      (source_family = 'basenames_base_registry'
       AND block_number BETWEEN 17577060 AND 46945560)
      OR
      (source_family = 'basenames_base_registrar'
       AND block_number BETWEEN 18715358 AND 46945560)
  )
GROUP BY 1
ORDER BY 1;
```

Registrar before-key verification spot-check query:

```sql
WITH registrar_aec AS (
  SELECT
    event_identity,
    logical_name_id,
    resource_id AS registrar_resource_id,
    block_number,
    before_state->>'authority_key' AS stale_before_authority_key,
    after_state->>'authority_key' AS registrar_authority_key
  FROM normalized_events
  WHERE namespace = 'basenames'
    AND chain_id = 'base-mainnet'
    AND source_family = 'basenames_base_registrar'
    AND derivation_kind = 'ens_v1_unwrapped_authority'
    AND event_kind = 'AuthorityEpochChanged'
    AND transaction_hash IS NULL
    AND log_index IS NULL
    AND before_state->>'authority_kind' = 'registry_only'
    AND canonicality_state IN ('canonical', 'safe', 'finalized')
    AND block_number BETWEEN 18715358 AND 46945560
)
SELECT
  COUNT(*) AS registrar_aec_rows,
  MIN(block_number) AS min_block,
  MAX(block_number) AS max_block,
  COUNT(*) FILTER (WHERE legacy_registry.resource_id IS NULL) AS missing_legacy_key_resource,
  COUNT(*) FILTER (WHERE current_registry.resource_id IS NULL) AS missing_current_key_resource,
  COUNT(*) FILTER (WHERE registrar.resource_id IS NULL) AS missing_registrar_resource
FROM registrar_aec event
LEFT JOIN resources legacy_registry
  ON legacy_registry.chain_id = 'base-mainnet'
 AND legacy_registry.canonicality_state IN ('canonical', 'safe', 'finalized')
 AND legacy_registry.provenance->>'authority_kind' = 'registry_only'
 AND legacy_registry.provenance->>'authority_key' = event.stale_before_authority_key
 AND legacy_registry.provenance->>'authority_key' =
     concat('registry-only:', legacy_registry.chain_id, ':', legacy_registry.provenance->>'labelhash')
LEFT JOIN resources current_registry
  ON current_registry.chain_id = 'base-mainnet'
 AND current_registry.canonicality_state IN ('canonical', 'safe', 'finalized')
 AND current_registry.provenance->>'authority_kind' = 'registry_only'
 AND current_registry.provenance->>'logical_name_id' = event.logical_name_id
 AND lower(current_registry.provenance->>'labelhash') =
     lower(legacy_registry.provenance->>'labelhash')
 AND current_registry.provenance->>'authority_key' =
     concat('registry-only:', current_registry.chain_id, ':', current_registry.provenance->>'namehash')
LEFT JOIN resources registrar
  ON registrar.resource_id = event.registrar_resource_id
 AND registrar.chain_id = 'base-mainnet'
 AND registrar.canonicality_state IN ('canonical', 'safe', 'finalized')
 AND registrar.provenance->>'authority_kind' = 'registrar'
 AND registrar.provenance->>'authority_key' = event.registrar_authority_key;
```

The earlier `664,470 resources total` figure was an event-conflict distinct-resource count from the blocker query, not the live legacy-resource census. The live legacy registry-only resource census reviewed for this ratification was 3,707,844 rows. Operators must rerun and archive this resource-census query before the supervised repair:

```sql
SELECT COUNT(*) AS legacy_registry_only_resource_count
FROM resources
WHERE chain_id = 'base-mainnet'
  AND canonicality_state IN ('canonical', 'safe', 'finalized')
  AND provenance->>'authority_kind' = 'registry_only'
  AND provenance->>'authority_key' =
      concat('registry-only:', chain_id, ':', provenance->>'labelhash')
  AND COALESCE(provenance->>'labelhash', '') <> ''
  AND (
      NOT (provenance ? 'namehash')
      OR provenance->>'authority_key' IS DISTINCT FROM
          concat('registry-only:', chain_id, ':', provenance->>'namehash')
  );
```

The in-place observation repair verifies the same event identity, logical name, chain, source family, event kind, canonical resource anchors, and legacy/current labelhash relationship, then rewrites only `resource_id` and derivation-affected observation state fields listed above. The boundary supersession path verifies the same raw-block anchor, logical name, chain, source family, event kind, manifest metadata, canonical resource anchors, and legacy/current labelhash relationship, then marks only the stale old-identity row `orphaned`; it does not rewrite identity or payload. The registrar-family subclass verifies the legacy before key through canonical `resources` provenance rather than through the event's registrar `resource_id`. Before the supervised registrar-family run, operators must apply the concurrent resources provenance-index migrations, wait for both index builds to finish valid, then archive `EXPLAIN (ANALYZE, BUFFERS)` for a representative registrar batch. The plan must use `resources_basenames_registry_authority_key_idx` for `candidate.stale_authority_key = resource.provenance->>'authority_key'` and `resources_basenames_registry_logical_labelhash_idx` for the current registry-only counterpart lookup by `resource.provenance->>'logical_name_id'` plus `lower(resource.provenance->>'labelhash')`; any plan that repeats `resources` scans per input row remains a hard stop. The repair appends normalized-event projection changes and directly enqueues stale and repaired resource keys for `permissions_current` on in-place observation `AuthorityTransferred`/log-scoped `PermissionChanged` and observation-scoped `record_inventory_current` on `ResolverChanged`. Boundary supersession rows publish through normalized-event insert/canonicality changes. After the supervised repair run, operators must drain replay/apply or rebuild `name_current`, `address_names_current`, `permissions_current`, and `record_inventory_current`. `name_current` consumes normalized-event insert/canonicality changes for the boundary class by logical name, `address_names_current` depends on the repaired authority/name state after `name_current`, and the two resource-keyed projections consume the directly enqueued stale and repaired resource keys for observation rows plus normalized-event changes for boundary permission rows. `children_current`, `primary_names_current`, `resolver_current`, and execution caches are not direct rebuild targets for this repair unless a separate run changes their inputs. Basenames manifest metadata mismatches are a hard stop: the existing boundary manifest-metadata mismatch allowance is ENS L1 only, the boundary supersession path rejects mismatched persisted Basenames metadata, and the operator must verify the replayed `basenames_base_registry` and `basenames_base_registrar` `manifest_version` and `source_manifest_id` match persisted rows before the supervised run; this ratification does not admit Basenames manifest metadata repair.

Repair does not write `raw_*`, `backfill_*`, projections, manifests, discovery rows, execution rows, or public API state directly. Field repairs append a normalized-event projection change. Repair paths also enqueue bounded key invalidations when a projection key is added or changed by repair or when an anchored resource projection should be refreshed immediately: Basenames primary-claim source repair enqueues old and repaired `primary_names_current` keys; ENSv1 reverse-name profile enrichment enqueues the added `primary_names_current` key; ENSv1 registrar renewal and ENSv1 or Basenames Base registry/registrar event-time resource repairs enqueue stale and repaired resource keys for affected resource-keyed projections, with nullable-resource repairs enqueueing only the non-null resource key; ENSv1 registry resolver before-state and authority-epoch resolver-boundary repairs enqueue affected `record_inventory_current` resource keys; and ENSv1 same-transaction registration setup repair enqueues the repaired `name_current` key plus affected `permissions_current` resource keys. Unanchored ENSv1 reverse primary-claim resolver before-state repair records only the normalized-event projection change. ENSv1 renewal repair updates to `surface_bindings` use the storage-owned surface-binding repair trigger to enqueue affected name/address keys.

### Bulk-load index deferral

During fresh normalized replay — current projection tables empty, normalized replay cursor not at target — the indexer may defer normalized-event indexes that exist only for projection/API readback while keeping replay-required indexes for event identity, reverse-claim lookup, and latest resolver/version preloads. The retained temporary latest-resolver, latest-record-version, and latest-registrar indexes are the database marker that this absence is intentional. Index deferral/restoration and post-migration index repair use one cross-process advisory fence: `worker migrate` does not recreate a deliberately deferred record-inventory replay index while any marker remains, but it repairs an invalid or missing index when replay is not deferring it. Replay removes the markers only after every deferred index is ready. Deferred indexes are therefore recreated before projection rebuilds or API-ready declared reads complete.

`current_projection_replay_status` rows let worker restarts resume from the first unfinished projection family instead of restarting bootstrap/full replay from the start. They are worker-owned operational progress: not API truth, not projection data, not live-readiness state, and ignored unless the recorded replay version is still current and the recorded normalized target covers the requested replay target. The API does not read this table. Automatic bootstrap holds its cross-process replay lock from apply-cursor baseline selection through family replay and creates a missing `projection_apply_cursors` row immediately before releasing that lock. If a target-covering current-version marker exists without that cursor, restart seeds the cursor at change `0` rather than acknowledging a later watermark that the completed family may not have applied. Continuous apply therefore cannot observe the handoff cursor before the protected replay has completed.

`projection_invalidations` rows are the durable key-scoped work queue for projection refreshes. `projection_normalized_event_changes` is the append-only downstream input for normalized-event inserts and canonicality-state updates; migrations install the forward log and trigger without bulk-copying historical `normalized_events`. Its identity-assigned `change_id` values are allocation-ordered, not assumed to be commit-ordered. Before bootstrap captures its initial cursor or continuous derive chooses a batch, storage captures a finite complete-prefix bound in a short transaction: it takes a `SHARE` table lock, waits out prior `ROW EXCLUSIVE` change-log insert transactions for at most 100 milliseconds, reads `MAX(change_id)`, and commits to release the lock before derivation. New insert writers cannot allocate a change id while that bound is queued or held, but the timeout bounds that writer barrier. A capture that times out fails without entering the cursor-advancement transaction; the worker retries later from the unchanged cursor. Derive processes only rows above its cursor and at or below a successfully captured bound; later inserts remain explicit subsequent work, and unused identity values remain harmless gaps. A cursor therefore cannot skip a lower-id transaction that was still in flight when the bound was selected.

The complete-prefix-capture migration takes the automatic-bootstrap replay lock, pre-locks and rewinds an existing `normalized_events_to_projection_invalidations` cursor, then takes `ACCESS EXCLUSIVE` on the change log to drain old derive readers and insert writers in cursor-then-change-log order. While that cutover lock remains held, it removes the obsolete global insert-order advisory trigger, installs the reader-side capture function, and repeats the targeted cursor rewind. If the cursor was initially absent, an in-flight pre-cutover derive either publishes before the exclusive lock and is caught by the second rewind, or resumes afterward from a safe zero lower bound. Deployments therefore pay one full, idempotent change-log derivation after this migration. A follow-up migration attaches the 100-millisecond function-scoped `lock_timeout`; it does not rewrite the already-applied cutover migration or relax complete-prefix correctness. `projection_apply_cursors` rows track consumed `change_id` watermarks for that input. Manifest, execution, and other non-normalized-event invalidation producers write the same queue directly. The primary key is `(projection, projection_key)`; repeated invalidations for the same key update the row generation, clear retry metadata, return the row to `pending`, and release any stale claim so an older apply cannot erase newer work. Projection workers claim and apply rows in projection dependency order, then delete only the claimed generation. Claims are leases with retry recovery, so rows claimed by a stopped worker become eligible again after the retry delay rather than requiring manual queue repair. Rows that fail the same claimed generation five times are removed from the live queue and copied to `projection_invalidation_dead_letters` with `state='dead_letter'`, the failure reason, timestamps, attempt count, and original queue identity for operator inspection. Dead-letter rows are durable operational evidence, not claimable work.

## Projection storage rules

Every current-state projection row carries provenance pointers, manifest version, relevant chain positions, canonicality summary, and last-recomputed timestamp.

Current projection timestamp fields are representable Unix-second values or `null`. ENSv2 `type(uint64).max` expiry observations project as `null` rather than a fabricated far-future timestamp; upstream uses that value for never-expiring reverse names, while registry renewal can carry any non-decreasing `uint64` expiry.[^v2-reverse-max-expiry][^v2-registry-renew-expiry] Numeric values that do not fit the projection timestamp representation are not converted into public projection timestamps.

Projection tables may be truncated and rebuilt from canonical facts plus normalized events.

`permissions_current_resource_summary` is the projection-owned per-resource companion to `permissions_current`. It persists permission-authority support classification, an optional ENSv2 registry-root anchor, and chain-position/canonicality evidence from the authority inputs even for resources with zero holder rows. Its JSONB coverage column has a typed storage boundary: only the documented `full`/`authoritative`, `partial`/`best_effort`, and `unsupported`/`not_applicable` combinations and permission-specific unsupported reasons decode. Unknown vocabulary and inconsistent status/exhaustiveness/reason combinations are storage read errors rather than implicit public downgrades. Keyed rebuild replaces one resource's holder rows and summary in one transaction. Full rebuild target discovery additionally selects canonical zero-event resources with positive source-family/manifest-version identity evidence, then stages and publishes both families in one transaction. For a zero-event current resource, the worker may derive summary evidence from either the normal resource provenance keys (`source_family`, `manifest_version`) or the ENSv1 binding-authority keys (`binding_source_family`, `binding_manifest_version`); these are identity evidence for the projection rebuild, not an API fallback. Base normalized-event rederive deletes affected summaries in the same permission-projection delete transaction, and deleting an identity resource cascades its summary. Public role and permission reads use this companion for support metadata and fail closed when it is absent; they do not recover that metadata from adapter-owned `resources` provenance.

`permissions_current_publication` has one row keyed by `projection='permissions_current'`, with positive `publication_version`, positive monotonic `data_revision`, and `published_at`. Version 2 denotes the holder-row plus typed per-resource-summary publication contract. The full staged rebuild upserts the compatible version and advances the revision in the same transaction that replaces both projection families. A keyed resource rebuild advances the revision in its row-and-summary transaction only when the existing publication version is already exact-current; it neither creates the row nor upgrades an old version. Public permission-backed reads require the exact current version, capture its revision before reading, and verify the same revision before returning. Missing or incompatible versions and an interleaved revision change return `409 stale` before an assembled response is exposed. Readers do not use the revision or `published_at` as a freshness signal, and this artifact does not replace operational replay markers, apply cursors, invalidation draining, or deployment coordination. The supervised Base normalized-event correction is the only direct permission-projection deletion path outside these publishers: it runs with the API drained and does not advance the revision, then requires a full projection rebuild and compatible publication before reads are restored. Exported row/summary upsert and delete helpers are low-level storage/test construction boundaries, not public-generation publishers; production worker publication uses the keyed or full transaction.

When a code change widens the normalized-event set consumed by a projection,
already-consumed change cursors do not by themselves revisit old current rows.
Replay version 9 therefore forces an all-current worker replay that seeds the
permission publication version/revision and discovers canonical zero-event
permission resources. That all-current replay also rebuilds `name_current`: for
an ENSv1 name whose current binding is a wrapper resource, the worker stores
explicit unsupported control instead of carrying pre-wrap control facets into
the public exact-name summary. Accepting a version-8 completion marker could
preserve an existing apply cursor, skip bootstrap, and leave the publication
artifact empty. Version 9 retains both the version-8 resource-summary backfill
and current-wrapper unsupported control behavior, plus version-7 ENSv2
exact-name-profile evidence. The replay changes no raw fact, normalized event,
or identity row; the normal worker rebuild remains the projection write owner
and re-evaluates canonicality and manifest evidence from retained inputs. Replay
versioning is a bootstrap gate, not a mixed-version writer fence. Deployments
must stop or drain every version-8 worker before any version-9 worker starts,
keep public reads drained until every version-9 marker (including
`name_current`) is current and all pending invalidations have drained, and never
run the two worker versions concurrently; an older worker can otherwise publish
the superseded result after the new replay.

Replay version 10 retains the version-9 outputs and rebuilds
`primary_names_current` to materialize `claim_name_is_normalized` from the
untrimmed reverse claim under the pinned normalizer. The migration adds the
non-null flag with a false default, so existing successful rows are deliberately
not readable as verified successes until replay recomputes them. Full rebuild
compares the current and staged claim rows, including this flag, and deletes
request-matching `verified_primary_name` cache outcomes for changed tuples in one
set-based statement before publishing the staged projection. Targeted rebuilds
keep the existing transaction-scoped tuple invalidation. Because this field has
no normalizer-version-keyed repair, a pinned-normalizer change requires another
projection replay-version bump. Deployments must not run version-9 and version-10
workers concurrently and must keep public reads drained until every version-10
marker is current and pending invalidations have drained.

Permission-backed v1/v2 routes and permission-derived address-name expansions
enforce `permissions_current_publication` version 2 and return `409 stale` when
it is absent or old. Address-name reads without the permission expansion remain
available. This compatibility gate prevents readers from decoding a legacy
permission row/summary contract, but it does not prove freshness. Exact-name
and primary-name reads have no corresponding replay-version gate, so the drain
remains required for the `name_current` and `primary_names_current` replays and
pending invalidations as well as the permission cutover.

Historical projection materializations are projection-owned caches, not truth. When a worker materializes an `at` or `chain_positions` snapshot, the rows are keyed by the normal projection key plus exact chain-position context or an equivalent snapshot key. They may be bounded and evicted by policy; absence returns `stale`. A historical materialization must never overwrite a newer current row in place, and the API must never fill a missing historical projection from raw facts or provider data.

Exact-name snapshot selection is a storage read boundary, not a new family. The API resolves `at`, explicit `chain_positions`, and `consistency` to one concrete `ChainPositions` object, then reads only projection rows and execution outputs eligible for that exact object. `name_current`, `coverage_current`, `surface_bindings_current`, `permissions_current` with its transactionally co-published resource summary, and `record_inventory_current` retain enough chain-position and canonicality context for the API to reject mismatched joins rather than combine rows from different snapshots.

If the selected positions are valid but no eligible projection or persisted execution output exists, the serving path returns the documented `stale`, `unsupported`, or `not_found` API state. It does not read raw facts, adapter-owned identity/event rows, or provider data directly to fill the public response.

## Execution storage

Inline in Postgres for small payloads:

- request metadata
- response digests
- decoded final values
- failure reasons

Large gateway bodies, metadata responses, and trace attachments are not persisted to a separate object store today. Execution may retain digests and trace metadata in Postgres, but adding durable external payload storage would be a migration-bearing storage change.

`execution_traces` and `execution_steps` preserve what was executed and why.
Normal `execution_cache_outcomes` writes record whether a verified outcome can
be reused under its request key, manifest versions, and block-hash-bearing
dependency boundaries. The API on-demand route exception and the
reorg-invalidation exception above are the only non-execution-worker write paths
for these execution-owned rows.

Verified-primary materialized outcomes remain fenced by the exact
`primary_names_current(address, coin_type, namespace)` claim row and its
normalization/content state. The ENS/60 route-local producer is the bounded
missing-row case: it may persist only while that exact row is absent, records
the stored selected checkpoint in its cache identity, and is reusable only while
the row remains absent and the route selects the same checkpoint. A route-local
trace is never admitted through the materialized-row fence, and a materialized
trace is never admitted through the missing-row fence.

Because the missing tuple owns no projected name surface or backing resource,
its `topology_version_boundary` and `record_version_boundary` JSON fields both
use `{boundary_kind: "selected_checkpoint", chain_position}`. That internal
execution-cache variant records the exact stored block number, hash, and
timestamp without inventing a `logical_name_id`, `resource_id`, or normalized
event. Materialized outcomes continue to use the ordinary projected
`VersionBoundary` shape.

Because PostgreSQL cannot row-lock an absent tuple, route-local persistence and
readback hold a short `SHARE` lock on `primary_names_current` while they check
absence and write or read the execution rows in the same transaction. Projection
inserts and updates wait at that boundary, so a response cannot combine a
missing-row decision with a trace read from the other side of a projection write.

Exact block-anchored `raw_call_snapshots` used by verified resolution stay in
the intake-owned `raw_*` family. Execution persistence, including the API
on-demand route exception, may hand off candidate snapshots only through the
raw-fact boundary, only for the exact requested chain position, and only for
support classes that admit them. `execution_traces`, `execution_steps`, and
`execution_cache_outcomes` do not own those rows.

Before a verified-resolution selector persists as a supported reusable outcome, execution reloads from storage the exact manifest versions for the request, the same declared topology snapshot a mixed route would serve, and any resolver-profile admission state required by participating resolver-local fact families. The frozen support class derives from those stored inputs and matches the persisted trace and cache key. If those inputs are absent or do not re-establish one frozen class, the trace remains a durable audit artifact but the selector does not persist as a supported reusable outcome.

## Read-only inspection tooling

Worker-owned, read-only operational tooling reads storage audit helpers and renders stable JSON. It does not create public `v1` routes, mutate state, fetch fresh chain data, or bypass API read boundaries.

- `bigname-worker inspect canonicality --chain-id <id> --block-hash <hash>` — for a stored block: lineage, parent hash, block number, canonicality state, optional header-audit presence, raw fact counts, payload-cache metadata counts/digests where retained, normalized-event counts.
- `bigname-worker inspect stored-lineage-range --chain-id <id> --from <block> --to <block>` — lists only lineage rows already stored for the requested chain and finite block range, ordered by `(block_number, block_hash)`. Renders chain id, block number, block hash, parent hash, canonicality state, timestamp, and stored promotion markers per observed block. Nullable fields render as `null`. Does not infer missing heights, gaps, span-wide canonicality, or completeness.
- `bigname-worker inspect backfill-job --backfill-job-id <id>` — resolves one persisted job and its child ranges. Renders job lifecycle, declared range, selector kind, resolved source identity, idempotency key, timestamps, failure metadata, and a `ranges` array sorted by range bounds and id.
- `bigname-worker inspect execution-trace --execution-trace-id <id>` — reads `execution_traces`, `execution_steps`, and trace-attachment metadata for one stored trace.
- Manifest-drift and proxy-alert inspection — joins stored alert observations to manifest/discovery identifiers, code-hash facts, proxy/implementation edges, and derived watch-target metadata. Does not fetch fresh chain state, create alerts, mutate alert lifecycle, mutate manifest truth, or change capability flags.

## Migration rules

- Schema changes land through checked-in migrations only.
- Append-only tables prefer additive changes over destructive rewrites.
- Backfill job and range checkpoint storage lands as additive `backfill_*` tables or additive columns; it does not overload `chain_lineage`, projection job state, or public API tables.
- Projection tables may be recreated when the rebuild path already exists.
- Migrations that change a shared interface require the companion doc update first.
- If `CREATE INDEX CONCURRENTLY` leaves an `INVALID` index, the runbook is a later `-- no-transaction` migration that `DROP INDEX CONCURRENTLY IF EXISTS` for the invalid name before recreating or replacing it; do not rebuild by editing an already-applied migration.

## Repository ownership

- Storage owns migrations and query primitives.
- Storage owns backfill job/range helper primitives for idempotent create, reserve, advance, complete, and fail transitions.
- Worker/backfill code owns operational writes to `backfill_*` through those helpers, including finalized catch-up chunk creation and capacity pause/failure metadata.
- Adapters own inserts into identity and `normalized_events` tables. During indexer-orchestrated resolver-profile reconciliation, adapters also own transient writes to `resolver_profile_reconciliation_runs`, `resolver_profile_reconciliation_targets`, and `resolver_profile_reconciliation_state_items`; those rows stage the exact emitter target set, private page state, and candidate normalized events until final publication and cleanup.
- Indexer resolver-profile convergence owns dirty-input selection, per-chain replay invocation, crash-safe operational `resolver_profile_reconciliation_invalidation_keys`, projection invalidation, and generation-fenced acknowledgement or deferral; it does not turn adapter staging rows into replay authority or public state.
- Projection workers own materialized read models.
- Execution workers own trace and step writes plus normal cache outcome writes,
  with the API on-demand verified-resolution product-route exception for
  selected-snapshot cache misses.
- Synchronous indexer/reorg repair owns only `execution_cache_outcomes` deletes/invalidations tied to orphaned block dependencies.
- Raw-fact normalized-event replay is indexer-owned orchestration over the adapter-owned `normalized_events` boundary; selected-target replay scopes are operational scan bounds and do not change adapter ownership.
- Normalized replay cursor and adapter-checkpoint storage is indexer-owned operational state for resuming bounded replay; it does not define canonicality, widen backfill jobs, or change adapter event ownership.
- Intake owns durable hot raw-fact writes, including admitted exact-block
  `raw_call_snapshots` handed off by execution persistence, plus optional
  payload-cache metadata. Replay and inspection tooling may dereference
  object-backed cache or re-fetch provider payloads only through the
  digest-checked, fail-closed boundary.
- API code does not query raw-fact tables directly except for explicit audit endpoints.
- Canonicality, raw-fact, stored-lineage-range, backfill-job, and execution-trace inspection tooling is worker-owned and read-only over storage audit helpers; none expose public `v1` routes.
- Manifest drift and proxy alerting tooling is worker-owned observation over `manifest_*`, `discovery_*`, code-hash facts, proxy/implementation edges, and derived watch targets. Its live audit path writes only `manifest_alert_*`; its read-only inspection path renders those observations as operational JSON without writing `normalized_events` or mutating manifest/discovery/projection/API state.

---

[^v1-pres-l20]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f)
[^v1-pres-l66]: (upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L66 @ ens_v1@91c966f)
[^v1-text-l5]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/ITextResolver.sol:L5 @ ens_v1@91c966f)
[^v1-text-l21]: (upstream: .refs/ens_v1/contracts/resolvers/profiles/TextResolver.sol:L21 @ ens_v1@91c966f)
[^v1-registrar-grace]: (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L17 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L101 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L161 @ ens_v1@91c966f)
[^v1-l2rev-base-deploy]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L2 @ ens_v1@91c966f)
[^v1-l2rev-base-args]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L391 @ ens_v1@91c966f)
[^v1-l2rev-event]: (upstream: .refs/ens_v1/deployments/base/L2ReverseRegistrar.json:L98 @ ens_v1@91c966f)
[^bn-readme-base-revreg]: (upstream: .refs/basenames/README.md:L33 @ basenames@1809bbc)
[^bn-revreg-l12]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L12 @ basenames@1809bbc)
[^bn-revreg-l150]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L150 @ basenames@1809bbc)
[^bn-revreg-l193]: (upstream: .refs/basenames/src/L2/ReverseRegistrar.sol:L193 @ basenames@1809bbc)
[^ens-subgraph-label-null]: (upstream: .refs/ens_subgraph/src/utils.ts:L76 @ ens_subgraph@723f1b6)
[^ens-subgraph-name-null]: (upstream: .refs/ens_subgraph/src/resolver.ts:L85 @ ens_subgraph@723f1b6)
[^ensnode-null-label]: (upstream: .refs/ensnode/packages/enssdk/src/lib/types/ens.ts:L92 @ ensnode@2017ae6)
[^graph-ens-rainbow-table]: (upstream: .refs/ens_rainbow/src/main.rs:L36 @ ens_rainbow@bc44492)
[^graph-ens-rainbow-hash]: (upstream: .refs/ens_rainbow/src/main.rs:L50 @ ens_rainbow@bc44492)
[^v2-reverse-max-expiry]: (upstream: .refs/ens_v2/contracts/src/reverse-registrar/StandaloneReverseRegistrar.sol:L175 @ ens_v2@48b3e2d) (upstream: .refs/ens_v2/contracts/src/reverse-registrar/StandaloneReverseRegistrar.sol:L176 @ ens_v2@48b3e2d)
[^v2-registry-renew-expiry]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L214 @ ens_v2@48b3e2d) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L225 @ ens_v2@48b3e2d) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L226 @ ens_v2@48b3e2d) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L228 @ ens_v2@48b3e2d)

[^bn-l2resolver-l4]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L4 @ basenames@1809bbc)
[^bn-l2resolver-l16]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L16 @ basenames@1809bbc)
[^bn-l2resolver-l29]: (upstream: .refs/basenames/src/L2/L2Resolver.sol:L29 @ basenames@1809bbc)

[^v2-iperm-l34]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L38 @ ens_v2@48b3e2d)
[^v2-iperm-l67]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L71 @ ens_v2@48b3e2d)
[^v2-iperm-l72]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L76 @ ens_v2@48b3e2d)
[^v2-events-l69]: (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L82 @ ens_v2@48b3e2d)
[^v2-pr-l28]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L29 @ ens_v2@48b3e2d)
[^v2-pr-l203]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L452 @ ens_v2@48b3e2d)
[^v2-pr-l216]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L464 @ ens_v2@48b3e2d)
[^v2-pr-l237]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L201 @ ens_v2@48b3e2d)
[^v2-pr-l451]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L528 @ ens_v2@48b3e2d)
[^v2-pr-l536]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L637 @ ens_v2@48b3e2d)
