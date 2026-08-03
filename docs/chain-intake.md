# Chain Intake

Chain intake is split into explicit per-chain phases. The checked-in runtime
currently implements `ingest` and `interpret`; `project`, `verify`, and `live`
remain unavailable until their later ports land. The old monolithic indexer,
its provider reconciliation loop, its persisted backfill scheduler, and its
normalized-event replay driver are no longer part of the source tree.

The architecture model lives in [`architecture.md`](architecture.md), storage
ownership in [`storage.md`](storage.md), and manifest and discovery authority in
[`manifests.md`](manifests.md).

## Implemented phase boundary

For each configured chain, the implemented path is:

1. `ingest` resolves provider heads and source ranges, fetches selected chain
   data, and writes lineage plus immutable [raw facts](glossary.md#raw-fact).
2. `interpret` waits on the ingested range and writes schema-v2 identity rows,
   discovery edges, and [normalized events](glossary.md#normalized-event).

The runner persists phase and per-source cursors. A phase advances only through
the exact block-hash markers returned by its implementation. `interpret` never
fetches missing provider data or calls an old adapter; its input is the raw fact
range already admitted by `ingest`.

Manifest synchronization uses the schema-v2 repository and checks the selected
[deployment profile](glossary.md#deployment-profile) fingerprint against the
interpreter content hash before a phase runs.
Manifest declarations and current discovery edges determine admission and the
watch filter. Discovery does not infer missing historical facts: a newly
admitted source must return to `ingest` for its required range before
`interpret` can derive it.

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

The current source kinds and seed-basis vocabulary are validated by the phase
runner configuration layer. Unsupported combinations fail as configuration
errors rather than falling back to a different provider or range.

## Redo

An explicit finite redo is selected with:

```sh
cargo phase -- redo \
  --chain <chain> \
  --phase ingest \
  --from-block <inclusive-start> \
  --to-block <inclusive-end> \
  --source <descriptor>

cargo phase -- redo \
  --chain <chain> \
  --phase interpret \
  --from-block <inclusive-start> \
  --to-block <inclusive-end> \
  --source <descriptor>
```

Redo state is persisted and range-bound. An interpret redo prepares the
schema-v2 derived range, replays it from retained raw facts, and resumes after
interruption. It does not use the deleted normalized-event upsert, repair,
supersession, adapter-checkpoint, or coverage-authority machinery.

## Canonicality and replay facts

Block hash is identity and block number is position. Canonicality is stored
explicitly in lineage and raw fact rows; consumers do not infer it from row
insertion order. Raw facts remain the durable input boundary. Schema-v2
identity and normalized-event rows are derived output and can be recreated by
an explicit interpret redo over a complete ingested range.

Execution may persist exact block-anchored call snapshots through the admitted
raw-fact boundary. That surviving path does not make execution a general chain
intake owner.

## Current Stage B limitation

`PhaseSet::with_ingest_and_interpret` deliberately installs unavailable
implementations for `project`, `verify`, and `live`. Consequently,
`phase-runner run` can execute the implemented phases but terminates when it
reaches `project`; it is not yet a complete continuously serving deployment.
The surviving worker still owns projection rebuild/apply and verified execution,
and the API still reads those projections and execution artifacts. Their port
to the phase pipeline is deferred to the project/live stage.

The historical `backfill_*`, `normalized_replay_*`, resolver-profile
reconciliation, raw-log revision/proof, and startup-checkpoint SQL tables remain
in migration history. This source tree has no old-runtime writer for them.
Storage exposes only the read paths still used by the worker or API, including
historical backfill-job inspection and the normalized replay cursor reads used
by the still-unported projection/compaction boundary.
