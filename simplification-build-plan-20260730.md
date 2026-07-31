# bigname simplification build plan (2026-07-30)

Converts `simplification-audit-20260730.md` (census + decisions, all seven
maintainer questions decided) into an ordered build. Read that doc first;
this one does not restate rationale.

**Context that shapes everything:** the deployment is currently stopped by
maintainer order. There is no uptime to preserve and no live writers to
coordinate with. The rewrite lands as one cutover, not a rolling migration.

**Shape:** new thin phase-runner binary in this repo, fresh schema, keep-set
crates ported onto it with their internal cuts applied. The old runtime
paths are deleted, not maintained in parallel. Raw downloaded data is
carried over; every derived table is rebuilt from it by the new pipeline.

---

## Stage A — Foundations

**A1. Fresh schema.** One new baseline migration set containing only product
tables: raw facts (logs/txs/receipts/headers), chain lineage +
canonicality, per-chain head markers (latest/safe/finalized — the successor
to `chain_checkpoints`; API snapshots/status read this), normalized events
(plain, no repair/supersession columns), identity reshaped for the
normalization-flag model (raw labels stored; per-label
`normalized_under_version` flag; shadow rows for unnormalizable names),
the eight projection tables (support-status columns kept; exhaustiveness
accounting columns dropped), label_preimages (+ flag), service heartbeats,
manifest declarations (no capability-flag table), the new divergence ledger
(live-vs-indexed disagreements, small), ingest cursors + phase state.
Nothing else. Target ≈ 3–4k SQL.

**A2. Phase runner.** New binary core (~3–5k):
- Per-chain supervisor; chains fully isolated (one chain's fatal error
  stops that chain only).
- Phases per chain: `ingest → interpret → (verify ‖ live)`; verifier is a
  reader and may run beside live. Explicit handoff datum: "ingest ended at
  block N; live starts there."
- One advisory lock per phase (~20 lines) + interpreter content-hash
  stamped at connect — the minimal writer-exclusion both review lenses
  required. No other fences.
- Single crate owns derived-table writes; ingest path alone writes raw
  tables; verifier has no write capability. (Rewrite-obligations audit
  checks these structurally at Stage D5.)

**A3. Semantic tripwires.** Interpreter content-hash (ponder-style: hash
the interpreter, manifest-authority, and projection sources plus complete
manifest event blocks; hash change ⇒ redo trigger). Complete event blocks
cover decode and mapping semantics: `fragment`, `emitter_roles`, and
`normalized_events`. Watch-set-only changes remain ingest work under
amendment A and do not rely on this hash.
The file-level scan conservatively hashes inline `#[cfg(test)]` modules in
production files, and B-stage internal cuts are expected to bump the hash
during the rewrite. Golden fixture harness in CI: fixed raw-event corpus →
expected interpreter output, committed; any output change anywhere fails CI;
intentional changes update fixtures and thereby bump the hash. Reuses the
conformance inliner/fixture pattern.

## Stage B — Port the keep-set

Order within B matters only where noted; B1–B4 are the sync spine, B5–B7
hang off it.

**B1. Ingest.** Port fetching engine + Coinbase SQL client + ingest write
path (incl. the event-silent candidate enumerator). Add the cheap
ingest-time recount (provider-said-N vs stored-N per bulk load — the
118-log-class catcher). Ethereum source: local reth. Base source: Coinbase
bulk (quick-sync) → dRPC at the seam.

**B2. Interpret.** Port adapters with internal cuts applied (checkpoint/
completeness/fence/recovery modules deleted; plain `sync_*` entry points
only; `_with_progress` threading stripped). **In the same change:** delete
`ens_v1_subregistry_discovery` AND land its resolver replacement — the
existing v1 generic-resolver match-all scope becomes the sole v1 resolver
source (census: partially built already). v2 `RegistryCreated` discovery
kept as-is. Index proxy `Upgraded` as a history event. Preimage capture
switches to raw-label + flag.

**B3. Live follow.** Port the reorg core (head walk, orphaning, gap fill)
onto the live phase; dRPC for Base, reth for Ethereum. Rewind CLI ported
minus its two cut couplings. Multicall hydration at the canonical head
hash for event-silent legacy reverse names + valueless legacy text
records; refresh as the head advances; no historical passes.

**B4. Verify (Base).** Re-point the stored-verification scanner at dRPC
behind the finality line; drop its attestation-persistence half. Mismatch ⇒
fatal stop of that chain, human diagnosis; repair path is wipe-and-resync.
Status route reports per chain: quick-synced (provider-trusted) /
verified (node-checked) / live at block N.

**B5. Projections.** Port the seven builders + stage-and-swap; strip
heartbeat threading, claim/dead-letter/watermark referee (single-writer),
and the standing hydration planners. Support-status computation kept;
"authoritative/full" wording in emitted JSON re-trued to what the
mechanism proves (wire-contract change, documented). Profile
reconvergence: admission of a resolver triggers an event-driven scoped
redo of its classification inline (no journal). Resolver classification:
declared list only (v1 builds; v2 implementations via `Upgraded`).

**B6. Lookup engine.** Port execution crate minus validation, revalidation,
persistence-side request validation, durable traces, and the outcome
cache. New small divergence-ledger write path: store a result only when it
disagrees with indexed state; never durably cache CCIP. Keep the
row-unchanged concurrency guard on ledger writes.

**B7. Redo command.** Generic per-phase debug tool: redo ingest /
interpret / project / hydrate / verify for a chain+range or all; checks
raw-data presence before interpret-redo; reuses replay core + rewind as
the undo half.

## Stage C — API and surfaces

**C1. Absorb pass (before any deletion).** Extract `serve()`/router
assembly from `openapi/server.rs` into main. Move the six v1-module pieces
v2 depends on (query parsing, primary-name execution stack,
permissions_support, snapshot scope, coverage/normalization helpers,
record-key types) into v2/support.

**C2. v1 deletion + v2 trims.** Delete v1 routes/handlers/responses/types/
tests (~15.6k prod + 32.3k tests). Collapse the indexed/verified/auto
tri-flavor to one flavor per route. Drop per-response completeness
envelope; keep per-name support status. Collapse capability flags
(declared = supported); simplify namespace routes. Re-derive the bounds
lane classifier for the surviving routes.

**C3. Edge flip.** Same deploy as C2: public edge allows v2 + GraphQL,
v1 gone. (Census hazard: today's edge *denies* v2 — shipping C2 without
C3 takes the public API dark.)

**C4. Generated v2 OpenAPI** from route definitions; docs link it.

**C5. Contract suites.** Retarget conformance harness to v2 + GraphQL
(GraphQL currently has zero contract tests and the manager depends on it —
new suite). Drop v1/backfill/chaos suites (~half of 30k). Keep the
manager-graphql-compat CI job and schema fixture.

## Stage D — Data, cutover, docs

**D1. Data carry-over.** Copy raw facts + lineage + label_preimages (+
rainbow imports) into the fresh schema. Everything derived is rebuilt by
the new interpret/project phases — that rebuild is the redo command's
first production run. Top-up ingest for the gap since the July stop
(Base: CDP quick-sync from extent 46,954,147→48,428,000 was already
downloaded — verify presence, then CDP/dRPC for the stop-gap; Ethereum:
reth).

**D2. e2e retarget.** Re-point the 20 scenario files asserting /v1 to /v2;
keep fault injection and catch-up-vs-live equivalence. e2e green is the
cutover gate.

**D3. Cutover.** New stack runs both chains to full sync; Base verifier
sweep completes; status shows verified+live; flip the edge; retire old
containers, binaries, and the old schema. Old CI guard steps
(startup-adapter-versions, conformance runner, v1 OpenAPI drift) removed
in the same change their subjects die.

**D4. Docs (runs alongside every stage, doc-first for public semantics).**
STE rewrite: architecture, storage (a fraction of 2,062 lines), api (v2 +
generated spec), manifests, upstream (add the two ratified divergences),
glossary pruned to surviving terms, AGENTS/CLAUDE boundaries updated (the
outcome-cache write exception is gone; verification wording re-trued;
guarantee = mechanism everywhere).

**D5. Finalization gate** (from the audit doc): rewrite-obligations
structural audit (phase ownership, verifier read-only, single derived-
writer), /v2/lookup latency under manager workloads, live-execution +
CCIP gateway load, plus: golden-fixture corpus covers every interpreter,
`claim_name_is_normalized` covered by the flag recompute, heads-marker
consumers all moved, edge flip verified from outside.

---

## Execution model

- Bounded ports/deletions run as codex lanes with tight packets; census
  tables define each lane's scope and its tendril checklist. Maintainer
  eyes on: schema (A1), phase-runner design (A2), every wire-contract
  change (B5 wording, C2), cutover (D3).
- Every semantic PR keeps the two-lens review gate (fable + kimi) + CI
  green on final head. PR sizing per existing scope discipline — stages
  land as series of small PRs, not monoliths; B2's
  discovery-cut+replacement is the one deliberately atomic change.
- Deletions cite the census row that authorizes them; anything found
  off-census stops and reports instead of guessing.

## Post-review amendments (fable + kimi adversarial reviews, 2026-07-31 — binding; where an amendment conflicts with a stage above, the amendment wins)

**A. Historical data for newly-watched signatures (both lenses, top finding).**
The carried raw corpus only contains what was previously watched. Three
decided watch-sets have NO historical raw data, and "rebuild from raw"
cannot conjure them: proxy `Upgraded` (never watched), v2 `RegistryCreated`
(NOT built today — current v2 discovery is SubregistryUpdated/reachable-
from-root; the decided match-all is new work AND a deliberate semantic
widening ratified by the maintainer — doc-first note in D4), and Base
resolver match-all (the existing generic scope is Ethereum-only). D1 gains
a mandatory step: one-time historical fetch of every newly-watched
signature BEFORE the derived rebuild. B2 is relabeled accordingly.

**B. Verify carried raw before deleting its coverage record.** The old
coverage/job tables are the only record of which (address, topic, range)
sets were ever fetched. Before A1's schema retires them: run a per-source
verification of carried raw against them, and seed the new per-chain/
per-source ingest cursors explicitly (Base seam 48,428,000; new-signature
ranges from amendment A; Ethereum head). The B4 sweep also runs once over
carried *Ethereum* raw against reth (free), not just Base.

**C. Ordering fixes.** (1) e2e retarget (old D2) moves BEFORE v1 deletion —
new order: C1 absorb → C-e2e retarget → C2 delete (+ v1 conformance suites
die in the same change; the conformance CI job itself survives retargeted).
(2) The edge flips ONCE, at D3 cutover; C3 becomes "prepare inverted edge
config + flip public-edge-smoke assertions + add CORS preflight matcher for
POST /v2/lookup, deployed at D3." (3) Old-binary co-deletion rule: every
crate cut lands leaf-first WITH its old-binary consumers in the same PR
series; the old indexer/worker runtime trees are deleted during Stage B as
their replacements land, not parked until D3.

**D. Status-label honesty (razor 3).** The dRPC sweep earns
"cross-checked (independent provider)", NOT "verified (node-checked)".
Three-state Base label: quick-synced (provider-trusted) → cross-checked
(second provider) → node-checked reserved for a future real Base node.
Ethereum: node-checked (reth) legitimately.

**E. Unowned decided items, now owned.** /v2/lookup live-joins rewire of
identity_facade → C1 (it reads the sidecar counts table today). GraphQL
golden/contract suite pulled forward to A3 (today only schema validation
exists; the manager is the hardest external dependency). Normalization-flag
recompute is a sixth redo mode in B7 (`recompute-flags`, runs without
replay; covers claim_name_is_normalized). Audit-layer inspection windows
(entry 9 KEEP) port in B7 alongside the redo/debug tooling, tables in A1.
v2/diagnostics coverage/trace routes die in C2 with their backing tables.
Disk-pressure guard re-homes into the phase runner (A2); D1 checks free
headroom first (raw is double-stored until D3).

**F. Specs pinned.** A2 phases are five: ingest → interpret → project
(+hydrate) → verify ‖ live; redo covers the same five + recompute-flags.
Ownership rule restated structurally: ONE writer binary; raw writes only in
ingest modules, derived writes only in interpret/project modules — D5
audits that formulation. A3 content-hash input = all adapter sources,
worker sources minus explicit wiring/test exclusions,
manifest-authority sources, and each whole `[[abi.events]]` block, EXCLUDING
the normalizer version (flag recompute path owns that). New watched signatures
still require amendment A's ingest before rebuild. A2 defines the
heartbeat/readiness contract:
per-chain per-phase heartbeat rows under new service names; /v2/status and
health re-derive from them (old indexer/worker names retire). C4 mechanism:
annotation-derive OpenAPI (utoipa-style), not hand-assembly. C1 checklist
extends to: declared-state builders used by v2 diagnostics,
build_resolution_verified_state, status_freshness.

**G. Cutover soak.** D3 retains the old schema and stopped containers for a
soak window (default 7 days) after the edge flip; spot-diffs of
name_current/primary_names old-vs-new run before retirement; destruction is
a separate, final change.

**H. Minor corrections folded in.** Divergences documented in D4 are three
(owner-discovery narrowing, no non-proxy code-change detection, head-only
hydration). A1's projection-table set is enumerated at schema-writing time
from the census, not counted ("seven"/"eight" both wrong: 7 builder modules,
~10 tables). D1 carries the `ens_names` rainbow side table explicitly.
AGENTS boundary rewords (not deletes) the execution-write exception: the
divergence ledger is an API-triggered durable write. The event-silent
candidate enumerator stays with hydration (worker/live phase), not ingest.
label_preimages' migrate-time backfill scan retires. The
manifest_normalized_events ABSORB remainder lands in B2's manifest sync.
D4 adds the reference-pin triage + ens_v2 citation-hash sweep. B4/finalization
gate adds: measure the dRPC sweep's query volume/cost before D3.

## Stage A review decisions (maintainer, 2026-07-31)

- GraphQL gains `text(key:)` on Resolver in Stage C (manager main already
  queries it; pin bump follows).
- Lookup execution pins to the newest processed block (same rule as
  hydration); divergence positions therefore always cite ingested blocks.
- The reorg auto-clear rule on the divergence ledger is maintainer-ratified.
- Namechain cancellation (upstream, maintainer-stated): single-chain name
  binding stands; the L1↔L2 cross-chain concern is void.

## Risk register (top five)

1. **ENSv1 interpreter port regressions** — richest semantics, 15k lines.
   Mitigation: golden fixtures before port (A3 precedes B2), e2e lifecycle
   scenarios, kimi+fable diff review per PR.
2. **Derived rebuild fidelity at D1** — new pipeline must reproduce name
   state from raw. Mitigation: rebuild on the e2e corpus first; spot-diff
   name_current/primary_names against the old schema's tables before
   retiring them; product-level cross-audit vs ensnode/subgraph (#199–201)
   as the independent check.
3. **Ordering hazards** — edge flip, server-bootstrap extraction, resolver
   replacement atomicity, head-marker successor. Mitigation: each is a
   named step above; CI + e2e enforce two of the four.
4. **Scope creep back toward machinery** — the razor and AGENTS "prove the
   operating path first" rule govern every packet; reviewers instructed to
   reject reintroduced self-verification.
5. **Base stop-gap ingest** (data between the July stop and cutover) —
   CDP spend small but nonzero; budget cap honored; measured before D3.
