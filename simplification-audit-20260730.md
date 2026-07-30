# bigname simplification audit — working doc (started 2026-07-30)

Joint scrutiny pass (maintainer + Claude), subsystem by subsystem. Not a plan
yet — an honest inventory of what exists, what it claims, what it actually
does, and whether it earns its production cost. Findings here are
evidence-backed or marked pending.

## The razor

1. **External realities earn runtime defenses.** Reorgs, provider outages,
   crashes mid-write, actual data gaps — the world misbehaves, code may defend
   at runtime.
2. **Our own bugs earn tests, not runtime bureaucracy.** No production surface
   whose purpose is to re-check our own code against itself. If we don't trust
   a write path, the fix is tests/simplification of the write path — not a
   second system that audits the first and a third that repairs the second.
3. **No guarantee stated stronger than its mechanism.** If the mechanism
   proves "matches the provider," the docs must not say "complete" or
   "independent." Misleading precision is worse than absence.

## Headline numbers (2026-07-30)

- Total Rust: **519,573 LOC**. Test-path files: ~264,700. Production: roughly
  **~230–255k LOC** (332 files also carry inline `#[cfg(test)]` modules).
- Checked-in docs: **10,240 lines** of markdown; `docs/storage.md` alone is
  2,062 lines.
- Self-verification / trust apparatus, production code only, conservative:

| Subsystem | prod LOC |
| --- | --- |
| indexer backfill + CDP checksum verification + receipts | 13,897 |
| indexer reconciliation (boot checkpoints, promotion, closure scan, replay classification) | 14,589 |
| indexer repair CLIs | 2,953 |
| storage: backfill_jobs + closure coverage + recovery-failure tracking | 6,005 |
| storage: base_normalized_rederive | 6,199 |
| storage: raw staging revisions/generations | 1,196 |
| storage: startup_adapter_sync + checkpoints | 1,988 |
| storage: audit | 1,932 |
| manifests: watched-requirement/frontier views | 3,475 |
| **Subtotal** | **~52,200** |

That subtotal is the *core* of the apparatus — more is woven through triggers,
migrations, adapter-sync scoping, e2e fault scenarios, and the docs. **At
least a fifth of production code exists so the system can convince itself of
things**, and its flagship claim (Base completeness) is not real (entry 1).

## Candidate trust model (maintainer proposal, 2026-07-30 — pending sizing)

- **Ethereum:** own validating reth node is source and truth (already real).
- **Base:** Coinbase SQL becomes an explicit **quick-sync** bootstrap — fast,
  cheap, provider-trusted, and *labeled as such* in the status API. A real
  Base node is the eventual verifier and live source: once it exists, one
  diff pass of stored logs against the node upgrades status from
  "quick-synced (provider-trusted)" to "verified (node-checked)", and the node
  becomes the ongoing source. Same pattern as snap-sync in Ethereum clients:
  fast first, heal to full trust behind a visible flag.
- Consequence: the attestation apparatus is unnecessary even on its own
  terms — verification against the node is a one-time local diff, not a
  standing bureaucracy. Open question before adopting: Base node disk/host
  sizing (root FS already tight; likely a second disk or host).

## Rewrite obligations (maintainer, 2026-07-30)

The deletions are justified by claims about how the rewritten system behaves.
Each claim must be **structurally enforced and audited once at rewrite time**
— not asserted in docs, not re-verified continuously at runtime:

- One writer at a time: the phase runner owns the DB write handles; ingest,
  verify, and live phases cannot run concurrently by construction.
- Verifier is read-only: no write capability in its module.
- Only the interpretation crate writes derived tables; only the ingest path
  writes raw tables (visibility/ownership, same pattern as the entry-2
  golden-fixture boundary).
- Audit the rewrite against this list before deleting the old guards is
  called done.
- **Per-chain isolation:** each chain's indexing runs independently — a fatal
  error on one chain (e.g. bad Base provider data → deliberate crash) stops
  that chain only; the other keeps syncing and serving. Cheap: all storage is
  already chain-keyed and lanes are per-chain; this is process/supervisor
  structure, not data-model work. Cross-chain reads (ENSv2 L1↔L2) join at
  the projection/API layer, not during per-chain intake.

## Normalization as a gate, not stored identity (maintainer, 2026-07-30)

Raw truth (labels as registered, hashes) is what the DB stores. Normalization
is an inclusion/exclusion decision: a per-label visibility flag ("raw form
byte-equals its ENSIP-15 normalized form under normalizer version N"), not a
stored rewritten value. Non-normalizable names remain in the DB as shadow
(deactivated) rows. When Unicode/normalizer rules change (annually), recompute
the flag column over distinct labels and flip visibility — no name-tree
rebuild, no event replay; at most a scoped recompute of projections whose
membership changed. Kills the normalizer-version repair walker.

## Redo command spec (maintainer, 2026-07-30)

- Debug/operator tool, not a runtime subsystem.
- Generic: redo any one phase (ingest / interpret / project / hydrate /
  verify) for a chain and range, or all phases, if cheap to implement —
  natural fit once the phase runner owns phases explicitly.

## Legacy event-silent reverse names (maintainer, 2026-07-30)

No historical hydration at all. The candidate set (reverse claims pointing at
event-silent legacy resolvers) is enumerable from indexed events; read the
current values with multicall batches at the live chain head, and refresh the
same way while following the head (silent writes are still possible on the
old contracts). Current state is the only truthful and useful answer for
silent data — no pinned-block historical passes, no standing planner.

## Proxy upgrades (maintainer, 2026-07-30)

Index the standard proxy `Upgraded` event for declared/announced contracts
(v2 registries and resolvers are mostly upgradeable) and surface it as a
generic history event type; monitoring alerts derive from that indexed event.
Replaces code-hash polling, drift-alert synthetic events, and the
raw_code_hashes store + repair tool.

## Rewrite finalization gate

Deferred-until-measured items that must be explicitly checked (not
remembered) before the rewrite is called done:

- [ ] /v2/lookup latency measured under realistic manager workloads (batch
      sizes the manager actually sends); add the small materialized feed
      only if measurements miss the bar (question 3 decision).
- [ ] Live-execution volume + CCIP gateway load measured with no outcome
      cache (question 4 decision); if gateways need protection, add a
      seconds-scale in-memory TTL only — never durable storage.

## Reference-pin triage (TODO)

ens_v2 repinned 2026-07-30 to `post-audit-2` head (`ccaeb58b`, was `48b3e2d`;
fast-forward; contract delta small: DNSTLDResolver tweak, ApprovedUpgradeGate
removed, WrapperRegistry edits — EAC still declares only `EACRolesChanged`).
The other 10 pins need the same triage: are they current, and does the
simplified plan still need them at all (e.g. graph_node/reth as
"cross-check" references may no longer earn their place once the
attestation apparatus is gone). Citations in existing docs reference the old
ens_v2 commit hash and need a sweep when docs are rewritten.

## Reference comparison (measured 2026-07-30 on pinned checkouts)

Same method: test paths excluded, `target/`/`node_modules/`/`dist/` excluded.
ponder@c8f6935, graph_node@aefe173, ensnode pinned per `.refs/MANIFEST.toml`.

| | Scope | Prod LOC | Test LOC | Self-verification machinery |
| --- | --- | --- | --- | --- |
| **bigname** | ENS + ENSv2 + Basenames, nothing else | **~230–255k Rust** | ~265k | **~52k+** (entry 1 apparatus) |
| **graph-node** | *any* subgraph on *any* supported chain: WASM runtime for user code, full GraphQL engine, multi-chain adapters, network features | ~194.6k Rust | ~28.5k | ~0.9k (proof-of-indexing digest, `graph/src/components/subgraph/proof_of_indexing/`) |
| **ponder core** | generic indexing framework, any contracts/chains | ~43.7k TS | ~28.3k | ~1.4k (realtime reorg handling + bloom, `packages/core/src/sync-realtime/`) |
| **ensnode (whole monorepo)** | ENS+Basenames indexer *on* ponder, incl. admin UI, API, rainbow | ~92.5k TS | ~22.8k | inherits ponder's |

Reading, with the Rust-vs-TS verbosity caveat (call it 1.5–2×):

- bigname, single-purpose, is **larger than graph-node**, a general-purpose
  platform that ships a virtual machine for arbitrary user programs and a
  GraphQL query engine.
- The full competing stack (ponder framework + all of ensnode ≈ 136k TS)
  covers our product scope and more with roughly half our line count and
  **~1/35th of our trust machinery**.
- Our test corpus alone (~265k) exceeds graph-node's by ~9×; much of it
  tests the apparatus, not name semantics.
- Both references trust their data source and defend only against external
  realities (reorgs); their entire "verification" layers are 1–2 weekend-sized
  modules.

## Inventory — verdicts

| # | Subsystem | Status | Verdict |
| --- | --- | --- | --- |
| 1 | Completeness verification (CDP checksums, coverage facts, generations, recovery waves) | **reviewed 2026-07-30** | Overstated guarantee; per-address repair economically unbounded; see below |
| 2 | Startup checkpoints / boot re-walks (invalidated by any migration deploy; ~12h re-walks) | **reviewed 2026-07-30** | Replace with ponder's model: content-hash of interpretation code triggers redo, plain resume otherwise. Boundary enforced by single-crate write ownership + golden fixture tests in CI (intentional output change = fixture update = the version bump). Cuts ~6–7k prod LOC (checkpoint tables, seven invalidation paths, version-guard script); adds ~200 (hash + trigger). Net strongly down. |
| 3 | Retention generations + raw-log staging revisions | **reviewed** | CUT — concurrent-writer referee; phases (one writer at a time) remove the need. Residue: "raw data present?" existence check inside the redo command |
| 4 | Full-closure discovery | **reviewed** | CUT owner-based discovery (3.87M base / 1.12M eth phantom instances; zero real emitters found). KEEP resolver discovery + v2 `RegistryCreated`. See § Discovery design |
| 5 | Stored-lineage promotion / coverage frontier | **reviewed** | CUT whole — cache of deleted proofs feeding a deleted walk |
| 6 | ENSv2 reconciliation fences/locks | **reviewed** | CUT — guards against self-inflicted concurrency; same-block attribution carried by `RegistryCreated`'s exact log position |
| 7 | base_normalized_rederive | **reviewed** | CUT whole (one-shot mission complete); absorbed by generic redo command. Its writer guard is on EVERY indexer+worker DB connect — mechanical untangle |
| 8 | Repair CLIs | **reviewed** | CUT coverage tools; normalization walker → flag recompute; text_records → hydration-at-head; raw_code_hashes dies with drift |
| 9 | Audit layer | **reviewed** | KEEP block/raw-event inspection windows (~1/3); CUT drift/cache views |
| 10 | Execution/verified-resolution layer | **reviewed** | KEEP lookup engine (wildcard/CCIP/forward-check — the differentiator); CUT durable step traces + validation + revalidation (census: also persistence-side request validation, ~6k total); outcome cache with event-driven eviction |
| 11 | Hydration (event-silent contracts) | **reviewed** | Multicall-at-head only; no historical hydration; no standing planner. See § Legacy event-silent reverse names |
| 12 | API surface | **reviewed** | v1 REST removed; keep v2 + GraphQL (manager); per-response completeness accounting → per-chain status route; one flavor per route |

## Discovery design (decided 2026-07-30)

Indexable = announced by an on-chain event, forward-only (ponder factory
semantics). No per-address discovery backfill exists as a concept.
- v1/Basenames: registries are singletons; owners are leaves; NO registry
  discovery. Registry/registrar/wrapper contracts come from manifests.
- v1 resolvers: match-all on the ENS-unique resolver event signatures
  (any address may be a resolver; no announcement exists).
- v2 registries: match-all on `RegistryCreated` (first event in the
  constructor — upstream: .refs/ens_v2/contracts/src/registry/
  PermissionedRegistry.sol:L112 @ ens_v2@ccaeb58b); after announcement the
  address is indexed address-scoped from its creation block. Linkage to a
  parent decides *authority*, never *indexability*.
- v2 resolvers: announced via registries' `ResolverUpdated`; unique
  signatures match-all; shared-standard signatures (`ApprovalForAll`)
  address-scoped after announcement (tiny per-address fetch at link time).
  Revisit when upstream adds a resolver creation event.
- Proxy `Upgraded` indexed for declared/announced contracts as a history
  event; replaces all code-hash polling.
| 13 | Docs themselves (10k lines; register, redundancy, overclaims) | **reviewed 2026-07-30** | Full rewrite alongside the refactor. Style: approximately ASD-STE100 Simplified Technical English — short sentences, one statement each, active voice, one meaning per term, no stacked qualifier chains. Keep vendor/manager requirement docs. Guarantee wording must match mechanism (razor rule 3). Target: a fraction of 10k lines. Biggest rule: no slop. |

## Entry 1 — completeness verification (reviewed 2026-07-30)

**Claim as stated:** durable, checksum-backed proof that stored raw logs are
complete, verified against "the independent query"; absence of a stored row is
proof an event never happened (`docs/storage.md:413` — "generation-bound fetch
coverage is the absence proof").

**What the mechanism actually proves:** identity-set equality with the
ingestion provider — the docs admit this themselves in the fine print
(`docs/storage.md:905` — "Equality proves identity-set equality only; it does
not prove that every stored payload field is correct"). On Base the
"independent" reference (`docs/storage.md:908`) is Coinbase — the same source
we ingested from. Asking the source twice is not a second opinion. On
Ethereum the reference is our own validating reth node, so the completeness
claim is real there — but it's inherited from the node, not produced by this
apparatus.

**Observed failure (what triggered this audit):** Base's retention generation
bumped 0→1 during the tail resync. Every historical proof was generation 0;
the coverage rule (`crates/manifests/src/lib/views/watched/coverage/uncovered.rs:63`)
refuses old-generation proofs. Result: the system silently went from "history
proven" to "history unproven," and its only repair path re-bought proof
**one discovered contract at a time** from the same provider: 488 jobs,
483 of which found **zero logs**, 12,191 paid queries ≈ **$93**, pool
effectively unbounded (requirement windows some from block 0, before the
contracts existed). All paid verification spend ever recorded was this wave —
the original download left no surviving proof at all.

**A real completeness proof exists and this isn't it:** block headers commit
to all receipts (and thus all logs) per block; verifying receipts against
headers chained to one trusted head proves completeness cryptographically,
rolling, provider-independent. What we built instead is attestation
bookkeeping that can be voided by our own state changes and re-purchased
without gaining information.

**Claim-check addendum (2026-07-30, adversarial pass at maintainer request):**

- *"The apparatus never caught anything real" — FALSE (correcting Claude's own
  earlier statement).* Recovery job 1831 (Basenames registry
  `0x03c4…dd9a`, window 0–46,954,147) detected a real **118-log hole in our
  stored copy** — one verification bucket, 70 blocks in 35,323,981–35,389,420 —
  and filled it from the provider at 2026-07-29 07:14–07:36 (raw_logs
  observed_at matches the job run; all rows finalized). The provider had the
  rows; our store didn't. One real catch in the apparatus's lifetime, and it
  was an ingestion-side gap (our class of bug), not a provider gap.
- *Failure census:* zero data-mismatch failures ever recorded on either chain.
  All failure reasons are operational (stale claims, provider caps, operator
  restarts, fence artifacts). Base: 57 failed jobs, all operational. Ethereum:
  36, same.
- *Double-purchase:* jobs 1831/1906 and 1832/1907 are identical
  (same address, window, log count) — the wave bought at least two proofs
  twice.
- *"No evidence of missing provider logs" — true but structurally vacuous:*
  on Base the only reference is the provider itself, so a provider gap is
  undetectable by construction. Absence of evidence was guaranteed either way.
- *Soundness overclaim (both parties):* we do NOT prove stored logs are
  canonical-chain members on Base. We bind logs to provider-reported block
  hashes and check lineage contiguity plus agreement between two services
  (live RPC vs Coinbase bulk) where they meet; log-in-block inclusion is never
  verified. Ethereum soundness is real (validating reth). Base soundness =
  cross-service consistency, not proof.
- *What the 118-row catch actually justifies:* a cheap post-load recount
  (checksum-at-ingest, ~hundreds of LOC), which would have caught the same
  hole. It does not justify generations, closure scans, per-address receipts,
  or absence-proof claims.

**Verdict:** the guarantee as documented does not exist on Base. Options
(decision pending): (a) delete the apparatus, keep a cheap ingest-time
integrity check, state provider-trust honestly, like every reference indexer;
(b) replace with real header/receipts verification at ingest; (c) keep but
repair at family granularity and fix the docs. Cost of the status quo:
~14k LOC (backfill/verification) + ~10k LOC (coverage/requirements/recovery)
+ unbounded paid-query exposure + it is currently the thing blocking sync.

## Full module census (fable ×6 + kimi ×2, launched 2026-07-30)

### crates/adapters (fable)

| module | prod LOC | what it does | verdict |
| --- | --- | --- | --- |
| ens_v1_subregistry_discovery | 5,142 | owner-based discovery + its own checkpoint/replay machinery | CUT — but see tendril below |
| checkpoint_context / checkpoint_codec / startup_versions / startup_progress | ~820 | boot-walk resume tokens, version pins, heartbeat plumbing | CUT (entry 2) |
| manifest_normalized_events | 698 | config-derived history rows + code-hash drift alerts | ABSORB — drift half CUT (proxy `Upgraded` replaces), ~300 LOC manifest-sync remainder |
| normalized_event_support | 315 | chunked event upserts + re-derive arbitration variants | ABSORB — plain upsert stays; arbitration dies with single-writer |
| ens_v1_unwrapped_authority | 18,428 | THE ENSv1/Basenames interpreter | KEEP, with ~3–4k internal cuts (checkpoint/, self-repair passes, resolver_profile_reconciliation → redo command) |
| ens_v2_registry | 11,020 | ENSv2 interpreter incl. event-driven registry discovery (discovery.rs IS the decided mechanism) | KEEP, ~3.5k internal cuts (live/completeness, live/checkpoint, fence, recovery) |
| ens_v2_permissions / ens_v2_resolver / ens_v2_registrar / ens_v1_reverse_claim / ens_v2_common / block_derived_normalized_events / registry_migration_cache / adapter_manifest / evm_abi / lib | ~8,600 | direct chain semantics, label-preimage harvest, migration-cache reality, ABI utils | KEEP (preimage capture switches to raw-label + visibility flag) |

**Critical tendril:** ens_v1_subregistry_discovery is ALSO today's sole
producer of ENSv1 **resolver** discovery (assignment.rs:146 builds resolver
edges from NewResolver; the ENSv1 interpreter's active_emitters scoping reads
those edges). The replacement (resolver-signature-scoped selection) must land
in the same change or ENSv1 loses its resolver log stream.

### crates/manifests + domain + metrics (fable)

CUT (~8.6k): discovery/reconciliation 4.9k (full-closure edge replay),
watched/selection 845 (job identity receipts), views/drift 781 (code-hash
alerts), watched/coverage 780 + frontier 678 + historical 480 (attestation
views), admission_epoch 117 (counter/fence).
ABSORB (~5.1k → ~1.5k): managed_edges 1.3k → manifest declarations;
sync 1.0k → plain load→upsert at phase start; discovery loading/persistence/
admission/provenance ~1.2k → the event-driven admission write path (persistence.rs
is the live path in miniature — survives simplified); watched views 0.9k →
static list + admitted rows; bootstrap.rs → ingest planning.
KEEP (~2.7k): model/repository/attribution (manifest schema + file
validation), views/abi (the event-subscription source), snapshot, domain
crate (range math + ENSIP-15 wrapper — core of the visibility-flag model),
metrics crate.
NEEDS-MAINTAINER: (a) resolver_profiles 1.4k — bytecode-matching to classify
legacy resolvers; its evidence source (code-hash observations) dies with
drift; does multicall-at-head candidate selection need bytecode ID at all?
(b) capability flags — keep per-family supported/shadow/unsupported, or
collapse to "declared = supported"? (c) execution_owner — rides entry 10's
final shape.
Tendrils: admission-epoch consumers spread across 7 indexer files; manifest
sync has a hard tie into base_normalized_rederive status enums; watch-plan
loaders consumed throughout indexer backfill/bootstrap (die together).

### apps/worker + crates/execution (fable)

CUT (~6.0k): execution validation/ 1.9k + revalidation/ 1.4k (second/third
systems auditing the first), execution/primary_name ~0.8k of request/trace
validation (**mislabeled in entry 10 — it's persistence-side validation, not
lookup engine**; only the 265-LOC row-unchanged guard survives into the
cache write), worker/inspect 856 (4 of 6 subcommands inspect deleted
machinery), worker/manifest_drift 384 (code-hash polling CLI),
json_helpers 156 (all consumers deleted), standing hydration planner loops
560.
ABSORB (~5.5k → much smaller): automatic_projection_replay → the phase
runner; replay/ rebuild-in-order logic → redo/project phase (its
version-keyed checkpoints + fences die with entry 2/3);
persistence.rs → plain outcome-cache upsert; rebuild_heartbeat → phase
status reporting; hydration paths (~2.5k across primary_name +
record_inventory) → multicall-at-head (the multicall primitives already
exist: ens_reverse_names.rs, ens_text_records.rs).
KEEP (~20k): all seven derived-table builders (name_current,
projection_apply, record_inventory, primary_name core, address_names,
permissions, resolver, children), the real lookup engine
(ens_resolution*, ens_primary_name, ccip, rpc, abi), commands/cli/health.
NEEDS-MAINTAINER: worker/raw_facts (323) — does the phase model keep a
raw-staging compaction step, or are raw tables the single durable store?
Tendrils: (a) **builders emit per-row "coverage" JSON claiming
"full/authoritative" — API wire contract; wording must be re-trued to the
surviving mechanism (razor 3)**; (b) heartbeat threading touches every KEEP
builder (mechanical strip); (c) every worker DB entry point routes through a
base_normalized_rederive writer guard — entry 7's cut touches all worker
subcommands; (d) event-driven cache eviction already exists inline in the
primary-name builder — the target model's eviction is partly built.

### apps/indexer (fable)

CUT (~19k): reconciliation bulk ~10.1k (locks/promotion/closure/authority
gating/code-hash polling), repair 2.5k, ops_catchup 2.5k (**its
capacity.rs disk-pressure guard is the ONLY one in the codebase — re-home
before deleting**), coverage_recovery 1.6k (the $93 wave engine),
resolver_profile_convergence 1.6k, stored_verification+coverage_facts 1.6k,
replay_handoff latch 0.6k, drop_rederive 0.5k, run_mode matrix.
ABSORB (~17k → phase runner): backfill job/lease ledger 4.7k → per-chain
ingest cursor; runtime 3.5k → phase-runner loop (~40% logging boilerplate);
bootstrap_backfill 2.8k → ingest phase (recovery.rs 585 dies with
generations); normalized_replay_catchup 2.2k → interpret phase (cursor keeps
(start,next,target), drops revision/generation columns; its fatal coverage
fence is what has been blocking sync); reorg core ~2.0k → live phase (real
keeper: head-walk, orphaning, gap fill); ingest write path 1.6k
(event_silent.rs = multicall candidate enumerator); replay core 0.9k → redo
primitive; run supervision 1.1k → **currently one lane's fatal kills BOTH
chains — invert to per-chain isolation**; heartbeat/activity 0.6k → trivial
periodic beat (real cost is the `_with_progress` twin of nearly every
function across three crates).
KEEP (~17k): provider layer 7.2k (RPC + reth-db backends; ~560 of bytecode
reading dies), fetching engine 4.2k, coinbase_sql client 3.4k (evidence.rs
542 = the honest cheap integrity check), metrics/cli/main/healthcheck/rewind
(reorg rollback CLI doubles as redo's undo half — strip its two CUT
couplings), event-signature constants.
NEEDS-MAINTAINER: runtime discovery refresh (~440) — resolved by § Discovery
design: live watch plan updates from announced events only; the refresh loop
dies with per-target discovery edges.
Notes: bootstrap→live handoff needs an explicit "ingest ended at N, live
starts there" datum; normalized-replay cursor schema carries the deleted
counters; main/tests ~7.4k largely test CUT machinery.

### apps/api + suites + migrations + scripts (fable)

API: CUT the whole v1 surface (~15.6k prod: responses 7.0k, handlers 4.6k,
openapi 1.7k, routes 1.1k, types/query/pagination) + v1 tests 32.3k + the
verified-flavor duplicate route + ~200 lines of completeness plumbing.
KEEP: v2 (~13.6k) + graphql 0.7k + bounds/status_freshness/metrics/wiring +
v2/graphql tests ~21.6k. **Ordering hazards:** (1) openapi/server.rs contains
the actual server bootstrap/router assembly — extract before deleting
openapi; (2) v2 compiles against six pieces of v1 modules (query_parsing,
primary-name execution stack, permissions_support, snapshot scope, coverage/
normalization helpers, record-key types) — absorb pass precedes deletion;
(3) **the public edge currently allows v1+GraphQL and DENIES /v2 — flip the
allowlist in the same change or the public API goes dark.**
Suites: conformance harness 29.6k + fixtures + 640-line source-inliner CUT
(pure v1 parity rig; rebuild a small v2 contract suite instead). e2e KEEP —
the rig that matches the trust model (real chains, real processes, fault
injection); 20 scenario files assert /v1 responses and re-point to /v2.
Migrations (144 files / 15.7k lines): ~75–80% is machinery, v1 support, or
self-repair history. Fresh schema carries forward only the product tables
(~3–4k of content). NEEDS-MAINTAINER: identity-feed sidecar (~4.1k of
migrations + storage counterpart) — /v2/lookup reads it today; keep a small
materialized feed or drop to live joins?
Scripts/CI: CUT identity-10k check, subgraph doc-samples check,
check-startup-adapter-versions (entry 2), raw-backfill index helper, v1
OpenAPI drift step, the dedicated conformance CI runner (largest machinery
CI cost). ABSORB release/rollback smoke shells; rewrite public-edge-smoke
inverted. KEEP sync-refs, migrate, test-db, dev-up, migration-version check,
manager-graphql-compat (the manager contract, executable), core
static/test/e2e/docker jobs. NEEDS-MAINTAINER: file-size ratchet.
v2/diagnostics (1.6k) straddles entries 1/10: its coverage/trace routes
re-serve apparatus blobs — follows those verdicts, not the v1 one.

### crates/storage (fable)

~26k of the crate's ~64k prod LOC goes. CUT outright (~18.6k): rederive
6.2k, backfill_jobs attestation parts, coverage_recovery_failures 1.5k,
startup_adapter_sync 1.1k, full_closure_coverage 1.1k, stored_lineage
frontier 1.0k, raw_code 0.9k (**no product consumer — verified**),
checkpoints 0.8k (but see head-marker gap below), raw_payload_cache ledger
0.7k, staging revisions 0.6k, resolver_profile_input_changes queue 0.4k
(+ its DB triggers), connection version-stamp fence 0.4k.
Internal cuts: **normalized_events upsert carries ~4.9k of inline
retroactive repair passes** (14 repair files + supersession) — the core
spine table is really ~1.5–2k; audit's drift views 1.3k; execution's durable
traces ~0.5k.
ABSORB (~5.2k): execution outcome cache (keep reorg invalidation, drop
boundary/manifest families), authority journal → scoped redo on discovery
change, migration_indexes → migrations + redo, projection_staging
stage-and-swap (keep) minus its fence half, versions helper.
KEEP (~30k): all product projections (name_current, address_names,
permissions, history, children, record_inventory, primary_name, resolver,
identity 4.2k — reshaped for the visibility-flag model since NameSurface
currently stores normalized identity), raw facts (raw, raw_children 1.8k),
lineage 1.3k (the reorg model), snapshot_selection, identity_facade,
resolution_support (home of "unsupported is explicit" + the event-silent
list), label_preimages (natural home of the per-label flag; retire its
migrate-time backfill scan), service_heartbeats, helpers.
NEEDS-MAINTAINER: raw_calls (340) — keep per-call evidence snapshots as raw
facts, or outcome-cache only?
Tendrils: (a) **head-marker gap** — API snapshots/status + worker read
`load_chain_checkpoint`/`chain_checkpoints`; the phase runner must publish an
equivalent per-chain latest/safe/finalized position or /status breaks;
(b) coverage JSONB columns on four projection tables + PermissionCoverage
enums + v2/vocab's apparatus-table list = wire-contract changes;
(c) every pool constructor stamps the replay-version fence and takes the
rederive shared lock — unpick in lib.rs;
(d) adapters' v2 completeness/checkpoint modules consume these guards
(cascade already counted in the adapters census).

### Kimi K3 second-opinion lenses — adjudicated (2026-07-30)

Kimi's censuses broadly matched fable's. Material disagreements, adjudicated:

**Accepted (plan amended):**
1. **Verifier reuse:** `backfill/stored_verification` scans are ~exactly the
   decided dRPC sweep (bucketed count/digest vs a provider). ABSORB, not CUT:
   re-point at dRPC behind finality, drop the attestation-persistence half.
2. **Hydrate at the canonical checkpoint, not the live head:** current
   multicall already pins `{blockHash, requireCanonical}`; reading at the
   provider's live head could write rows a reorg erases with no event to
   evict them (event-silent targets!). Amendment to § Legacy event-silent
   reverse names: multicall at the indexer's canonical head hash, refreshed
   as it advances. Same cost, keeps provenance.
3. **Keep minimal writer-exclusion:** both lenses independently flagged that
   "one writer by construction" is process-internal; a stale pod/second
   process during deploys isn't stopped by structure. Keep ONE advisory lock
   per phase (~20 lines) + a content-hash stamp at connect, replacing the
   364-line fence + AST test + lock zoo. Added to rewrite obligations.
4. **Outcome cache needs TTL/size-cap too:** most cached tuples never see
   another event; event-driven eviction alone = unbounded growth + immortal
   wrong entries. Trivial TTL added to the entry-10 design.
5. **"Coverage" conflates two things — split before cutting:** per-name
   *support status* ("this name's setup is unsupported") is product,
   load-bearing in /v2/lookup, and stays (guardrail: unsupported is
   explicit). The *exhaustiveness accounting* is apparatus and goes.
6. **Conformance harness retargets rather than dies** (adjudicated middle):
   the inliner + hand-seeded fixtures are exactly the golden-fixture vehicle;
   keep harness + fixture pattern, retarget suites to v2+GraphQL (GraphQL has
   NO contract suite today and the manager depends on it), drop v1/backfill/
   chaos suites (~half its 30k).
7. **Profile reconvergence needs a named replacement:** resolver-profile
   updates fire on admission changes too (more churn under match-all).
   Replacement: admission of a resolver triggers a scoped, event-driven redo
   of that resolver's classification inline in the phase — no standing
   journal. Added to rewrite obligations.

**Accepted as documented divergences (not plan changes):**
- Owner-based discovery deletion loses NewOwner streams of undeclared
  custom registry-like contracts. Their events were never canonical-ENS
  authority; wildcard subnames are served via resolvers + execution (kept);
  reference indexers behave the same. Goes in docs/upstream.md § divergences.
- No non-proxy code-change detection after drift polling dies (post-Dencun,
  in-place code change is ~extinct; wrong-address manifests are a config bug
  for golden tests). Residual questions live in the resolver_profiles item.
- Head-only hydration drops the (existing) retention of historical
  event-silent call evidence — accepted; silent data has no truthful
  history.

**Useful confirmations:** v1 generic-resolver match-all already exists
(`GENERIC_SOURCE_SCOPE_ADDRESS="*"` + 14 signatures) — decided model partly
built; name_surfaces already stores normalizer_version per row — the flag
model is a reshape, not greenfield; a per-chain rederive run-state machine
exists as a base for wipe-and-resync; `claim_name_is_normalized` must be
covered by the flag recompute or normalizer upgrades stale it silently.

## Maintainer question list (consolidated, for decision)

1. ~~resolver_profiles~~ **DECIDED 2026-07-30: declared list only, no
   fingerprinting.** Evidence: 0 of 1,617 discovered resolvers ever matched a
   fingerprint; every supported resolver got there via the declared list.
   v1: declared list of known builds. v2: declared list of *implementations*
   — a proxy resolver is known iff its current implementation (tracked via
   the indexed `Upgraded` event) is declared. Unknown resolvers: indexed by
   events, answered live by the lookup engine.
2. ~~Capability flags~~ **DECIDED 2026-07-30: collapse to declared =
   supported.** Sync state lives on the per-chain status route; per-name
   honest-unsupported status stays; resolver classification is separate
   (question 1). Deletes the flag table, its sync path, and namespace-route
   plumbing.
3. ~~/v2/lookup identity feed~~ **DECIDED 2026-07-30: live joins first, no
   materialized feed.** TRACKED at the finalization gate (below): measure
   lookup latency under realistic manager workloads before ship; add a small
   feed table only against a measured miss.
4. ~~raw_calls / outcome cache~~ **DECIDED 2026-07-30 (maintainer redesign):
   no durable outcome cache at all.** Live execution answers live; a result
   is stored ONLY when it disagrees with indexed state — a small divergence
   ledger marking that name/resolver as unreliably resolvable by indexing
   (feeds the per-name support status). CCIP results are never durably
   cached (no on-chain eviction event exists — caching them was always
   wrong). raw_calls dies entirely; supersedes the TTL amendment (nothing
   to expire). Rider at the finalization gate: if gateway load needs it, a
   seconds-scale in-memory TTL is an ops mitigation, never durable state.
5. ~~Raw staging compaction~~ **DECIDED 2026-07-30: no compaction — raw
   tables are the permanent durable store.** Simpler, and the raw corpus is
   what makes redo/wipe-and-resync cheap. Revisit only if disk genuinely
   becomes the constraint; the compact-log-staging tool dies with its
   cursor-machinery gate.
6. ~~File-size ratchet~~ **DECIDED 2026-07-30: keep for now.** Allowlist
   shrinks naturally as oversized files die in the rewrite.
7. ~~v2 OpenAPI~~ **DECIDED 2026-07-30: generate the spec from the route
   definitions.** No hand-written spec; docs rewrite links it.

All seven questions are decided. The two documented divergences (custom
subregistry narrowing; no historical event-silent evidence) were ratified by
the maintainer's entry-4 and hydration decisions and stand unless revisited.

## Process note

This apparatus grew under heavy multi-lens review (two model lenses + codex +
CI on every PR), and every lens reviewed each PR *within the design's own
premises* — none asked whether the premise (self-attestation as completeness)
was sound. Review depth is not a substitute for questioning the frame.
