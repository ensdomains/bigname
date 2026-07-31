# A2 — phase runner design (for maintainer review, 2026-07-31)

The one new component of the rewrite. Everything else is a port; this is
the spine they hang from. Style: STE. Authority: build plan § A2 +
amendments E/F; audit § Rewrite obligations.

## Shape

One binary. It replaces the indexer and worker run modes. Per chain, it
runs a supervisor. Each supervisor owns its chain completely and cannot
touch the other chain. A fatal error stops one supervisor. The process
stays up and the other chain continues.

## Phases

Each chain moves through five phases. One phase runs at a time per chain,
except `verify` and `live`, which may overlap because `verify` only reads.

1. **ingest** — bulk-download raw history from the chain source (Ethereum:
   local reth; Base: Coinbase SQL) into the raw tables. Recount each bulk
   load against the source's claimed count/checksum. Record the end block.
2. **interpret** — run the interpreter over stored raw logs in order.
   Write normalized events and identity rows. Discovery happens here:
   an announced contract (RegistryCreated, resolver signature, Upgraded)
   extends the address-scoped watch set forward from its announcement.
3. **project** — build/refresh the projection tables from normalized
   events. A full build publishes every table in one projection family
   together through a single rename-swap transaction; live updates apply
   incrementally. Hydration belongs to this phase: multicall reads pinned
   at the chain's canonical head hash for the event-silent legacy set.
4. **verify** (Base: dRPC sweep behind finality; Ethereum: one-time sweep
   against reth) — read-only. On mismatch: mark the chain's status,
   stop that chain's supervisor, leave a diagnosis bundle. Never repairs.
5. **live** — follow the head via RPC: reorg walk, orphaning, gap fill,
   then incremental interpret+project per new block range.

Phase transitions are explicit rows in `chain_phase_state`, keyed by
(`chain_id`, `phase_name`). `started_at` and `finished_at` bracket a run;
the current, target, and live-handoff block-number/hash columns record its
positions. The ingest→live handoff datum is "ingest ended at block N";
`live` starts its cold walk from N.

## Writer rules (structural, audited at D5)

- The phase runner is the ONLY pipeline writer binary. The separate API has
  one bounded write exception: an API-triggered lookup may write or clear the
  `resolution_divergences` table defined by build-plan amendment H. A
  `chain_lineage` canonicality change also clears affected active rows through
  a database invariant; it is not a second writer binary.
- Raw-table writes exist only in ingest modules. Derived-table writes
  exist only in interpret/project modules. The verifier has no write
  capability (compile-level: no pool with write role).
- Semantic event interpretation and projection code must never live in
  `apps/phase-runner`: the runner is deliberately outside the interpreter
  content-hash roots and only orchestrates the hashed implementations.
- One Postgres advisory lock per (chain, phase). A second process
  attempting the same phase fails loudly. This is the entire
  writer-exclusion apparatus (~20 lines).
- Every connection stamps the interpreter content hash (A3). A binary
  whose hash differs from the one recorded in `chain_phase_state` for the
  current interpret/project epoch refuses derived writes and reports.
  This is the deploy-race guard both review lenses required.

## Status and heartbeats

- The runner publishes per-chain head markers (latest/safe/finalized) —
  the successor to `chain_checkpoints`; API snapshots and /v2/status read
  these.
- A checkpoint jump promotes lineage through each legal state in order in the
  same head-publication transaction: observed→canonical→safe→finalized.
  Re-canonicalization first moves orphaned→canonical. The port must not reuse
  the retained helper's direct assignment of one target state across a path.
- Heartbeat rows: (`service_name='phase-runner'`, `instance_id`, `chain_id`,
  `phase_name`, `heartbeat_at`),
  written by each phase loop at most every 5s. /healthz checks DB
  reachability + newest heartbeat age. /v2/status derives per-chain state
  from `chain_phase_state` + head markers + heartbeat age, and reports the trust
  label: Ethereum node-checked; Base quick-synced → cross-checked.
- `/healthz` and `/v2/status` route wiring lands in Stage C; Stage A2 owns
  only the state and heartbeat writes those routes will read.
- Every phase batch must finish within the configured heartbeat staleness
  window so a healthy runner is not reported stale while one batch is active.
- The ops_catchup disk/DB capacity guard is re-homed here: every phase
  loop checks free-disk and database-size floors between batches and
  pauses (does not die) below the floor, with a status flag.

## Redo integration

The redo debug command drives the same five phase implementations plus
`recompute-flags` (normalization flag recompute, no replay), scoped by
chain + range + phase. It takes the same advisory locks. Rewind is redo's
undo half for reorg-shaped repairs. Redo progress is stored separately from
the normal phase cursor, and the pre-redo lifecycle state remains durable
across retries; an unfinished redo remains marked and blocks normal resume
until the named redo command is run again.

There is no redo abort. A partially applied redo cannot be safely un-applied,
so the only path back to normal operation is a redo that covers the unfinished
range and completes. If a redo fails deterministically, fix the code and run
that redo again.

## What does not exist here

No checkpoints keyed to migrations or versions. No retention generations
or revision counters. No fences beyond the one lock + hash stamp. No
standing planners, queues, dead letters, or watermarks. No recovery: a
failed phase is restartable from its cursor; a data-integrity failure is a
stop, a human, and (worst case) wipe-and-resync.

## Review outcomes (maintainer-approved 2026-07-31)

1. Restart policy: non-data fatal errors (network, DB outage) restart the
   phase with capped backoff; only verify-mismatch and data-integrity
   errors stop a chain.
2. Ethereum's one-time reth sweep runs BEFORE live follow (local, fast).
3. The API stays a separate binary. It is read-only except for the bounded
   `resolution_divergences` write above; chain canonicality changes enforce
   the automatic clearing invariant described there.
4. Deployment note: hard blast isolation, when wanted, comes from running
   this one binary once per chain (two containers, same code) — isolation
   by chain, never by pipeline half. The indexer/worker split does not
   return.
5. Manifest sync keeps the authored `deployment_epoch` field and persists it
   one-to-one as `manifest_versions.deployment_label`; queries use the
   storage name after the Stage B port.
