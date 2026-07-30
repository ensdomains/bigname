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
   events (stage-and-swap for full builds, incremental applies while
   live). Hydration belongs to this phase: multicall reads pinned at the
   chain's canonical head hash for the event-silent legacy set.
4. **verify** (Base: dRPC sweep behind finality; Ethereum: one-time sweep
   against reth) — read-only. On mismatch: mark the chain's status,
   stop that chain's supervisor, leave a diagnosis bundle. Never repairs.
5. **live** — follow the head via RPC: reorg walk, orphaning, gap fill,
   then incremental interpret+project per new block range.

Phase transitions are explicit rows in `phase_state` (chain, phase,
started_at, ended_at, end_position). The ingest→live handoff datum is
"ingest ended at block N"; `live` starts its cold walk from N.

## Writer rules (structural, audited at D5)

- The phase runner is the ONLY writer binary.
- Raw-table writes exist only in ingest modules. Derived-table writes
  exist only in interpret/project modules. The verifier has no write
  capability (compile-level: no pool with write role).
- One Postgres advisory lock per (chain, phase). A second process
  attempting the same phase fails loudly. This is the entire
  writer-exclusion apparatus (~20 lines).
- Every connection stamps the interpreter content hash (A3). A binary
  whose hash differs from the one recorded in `phase_state` for the
  current interpret/project epoch refuses derived writes and reports.
  This is the deploy-race guard both review lenses required.

## Status and heartbeats

- The runner publishes per-chain head markers (latest/safe/finalized) —
  the successor to `chain_checkpoints`; API snapshots and /v2/status read
  these.
- Heartbeat rows: (service='phase-runner', chain, phase, beat_at),
  written by each phase loop at most every 5s. /healthz checks DB
  reachability + newest beat age. /v2/status derives per-chain state
  from phase_state + head markers + beat age, and reports the trust
  label: Ethereum node-checked; Base quick-synced → cross-checked.
- The ops_catchup disk/DB capacity guard is re-homed here: every phase
  loop checks free-disk and database-size floors between batches and
  pauses (does not die) below the floor, with a status flag.

## Redo integration

The redo debug command drives the same five phase implementations plus
`recompute-flags` (normalization flag recompute, no replay), scoped by
chain + range + phase. It takes the same advisory locks. Rewind is redo's
undo half for reorg-shaped repairs.

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
3. The API stays a separate read-only binary.
4. Deployment note: hard blast isolation, when wanted, comes from running
   this one binary once per chain (two containers, same code) — isolation
   by chain, never by pipeline half. The indexer/worker split does not
   return.
