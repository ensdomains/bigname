# Storage

Persistence boundaries for [raw facts](glossary.md), identity, [normalized events](glossary.md), [projections](glossary.md), and execution. Project-specific terms are defined in the [glossary](glossary.md). Architecture model in [`architecture.md`](architecture.md); intake detail in [`chain-intake.md`](chain-intake.md); manifest schema in [`manifests.md`](manifests.md); read model in [`projections.md`](projections.md); execution layout in [`execution.md`](execution.md).

## Invariants

- Durable raw facts are immutable. Evictable payload-cache entries and non-audit raw-log staging rows lose their system-of-record status once the replay contract that retained them is satisfied.
- Projections are disposable and rebuildable from canonical raw facts plus normalized events.
- Canonicality is explicit, never inferred from "latest row wins."
- Execution traces and steps are durable audit artifacts except for the bounded ENS/60 missing-tuple route described under execution storage; cache outcomes are reusable only while their dependencies remain canonical.
- One write owner per storage family.

## Corrections

Raw-fact corrections are explicit, auditable events. They are not normal replay,
do not weaken the default immutability rule, and must name the corrupted field
set, cause, proof source, rewrite owner, acceptance checks, and ratification
record in this section before or with the tool that applies them. A correction
tool must be idempotent, resumable, fail closed on verification disagreement,
and update only the ratified fields. Any wider rewrite requirement is a new
doc-first storage task.

### 2026-07-03 raw code-hash padding correction (retired)

The ratified pre-cut correction rewrote only `raw_code_hashes.code_hash` and `code_byte_length` after node-backed verification. Its old-indexer command and the storage selection/update helpers have now been deleted. Historical results remain raw facts; there is no current Rust correction writer for this class. Any future raw-fact rewrite requires a new doc-first correction record and implementation.

### 2026-07-03 Base normalized-event drop-and-rederive correction (retired)

This ratified pre-cut correction and its `bigname-indexer drop-and-rederive-base-normalized-events` implementation are retired. The command, its Base-tail rederive state machine, and its Rust storage helpers were deleted with the old runtime. The `base_normalized_rederive_*` tables remain only in immutable migration history and have no current writer or repair authority.

Schema-v2 interpretation now owns Base identity and normalized-event output. A required re-derivation is an explicit finite `interpret` redo over a complete ingested range; it does not resume or consult the retired correction tables.

## Storage layers

During Stage B, these layers are split across two explicit schemas in one
PostgreSQL database. The surviving API and worker remain on immutable
`migrations/` history in `public`, while phase-runner ingest, interpretation,
and projection use the fresh `schema-v2/` baseline in `bigname_phase`. The
`phase-runner init-schema` command accepts only an empty phase schema until a
reviewed upgrade or rebuild mechanism exists. Serving the fresh projections is
deferred to the worker/API port and cutover. The shared transaction domain is
intentional: it lets head publication invalidate retained execution-cache
eligibility atomically with phase-lineage orphaning.

The system of record splits into six layers.

1. `chain_lineage` — block ancestry, fork points, hash-first reconciliation, head checkpoint publication, one durable header-anchor row per observed block hash.
2. `raw_facts` — hot indexed replay facts: selected/admitted target logs, the minimum transaction/receipt fields needed to decode them, fetched call snapshots, optional header/log audit extensions, compact payload-cache metadata. Legacy `public.raw_code_hashes` rows and resolver-profile readers remain compiled only for the old worker's public-schema service until Stage C; no current runtime produces new rows, and schema-v2 projection does not read them. The replacement classifier uses manifest declarations and canonical ERC-1967 upgrade history; see [`manifests.md`](manifests.md#discovery-admission).
3. `manifests_and_discovery` — source manifests, discovered edges, rollout flags.
4. `identity_and_events` — `NameSurface`, `SurfaceBinding`, `resources`,
   `token_lineages`, and current-interpretation-epoch `normalized_events`.
   Normal interpretation appends events; an explicit bounded interpret redo
   replaces its selected event range as described below.
5. `projections` — current-state and collection read models.
6. `execution` — durable traces and steps, `execution_cache_outcomes`, invalidation records.

Layers 1–5 rebuild current declared state. Layer 6 replays verified answers and explains them.

Worker-owned manifest/proxy alert observations live alongside these layers as an operational audit family. They record drift findings; they are not manifest truth, discovery admission, projection state, or interpret-phase events.

## Storage substrates

Postgres is the hot indexed and replay-focused store. It retains:

- lineage and header anchors needed to reconcile forks, prove ancestry, promote checkpoints, audit canonicality
- selected/admitted target logs and the minimal transaction and receipt fields while they are needed to decode those logs, route them through adapters, and append normalized events
- block-scoped call snapshots and enrichments retained by an explicit replay contract for normalized events, projections, or execution artifacts
- durable event-silent resolver call observations used as projection-invalidation inputs after selected transaction and receipt staging rows are compacted
- legacy code-hash observations used only by old public-schema
  resolver-profile and manifest-drift views; no current runtime produces new
  observations, and schema-v2 projection does not consume them
- compact metadata and optional digests for full payloads fetched as cache

There is no deployed object-storage layer in the current schema or compose stack. When the system retains fetched payload metadata, Postgres stores the metadata and optional digests needed to validate later cache use; fetched bytes outside durable replay facts are cache-owned and may be absent.

## Raw facts and retained staging

The schema-v2 `ingest` phase owns chain lineage and selected `raw_*` writes. Raw facts are immutable inputs to `interpret`; canonicality is explicit and block-hash anchored. The deleted old runtime no longer maintains raw-log input revisions, retention generations, retained-history proofs, coverage facts, or adapter startup checkpoints. Their SQL tables remain migration history and have no current Rust authority.

`raw_logs`, selected `raw_transactions`, and selected `raw_receipts` still support the surviving minimal-versus-audit retention policy. In minimal mode, `bigname-worker raw-facts compact-log-staging` remains because the worker has not yet been ported. It still reads the historical normalized replay cursor before compacting. That read-only compatibility boundary is deferred to the worker port; it does not reactivate the deleted normalized replay writer or prove schema-v2 interpret completeness.

Exact block-anchored `raw_call_snapshots` also survive because verified execution persists and reuses them through the admitted raw-fact boundary. Worker inspection reads selected raw facts and lineage for audit output. The API does not acquire a general raw-fact read path.

## Evictable payload cache

Large block payloads and non-selected transaction or receipt bodies are evictable cache once no admitted replay contract needs them. Retained cache metadata may record a digest, size, encoding, source, observation time, and canonicality. A later provider fill is explicit, block-hash scoped, digest checked when a digest exists, and fail-closed; it is not a substitute for lineage, normalized events, identity rows, projections, or execution artifacts.

Local execution-client storage is provider or cache substrate, not a durable bigname identity namespace. Client table keys, cursors, and data-directory paths do not become normalized-event provenance or projection inputs.

## Identity strategy

### Deterministic namehash IDs

`logical_name_id = "<namespace>:<namehash>"` — stable and derivable without a database lookup. `namehash` is the lowercase `0x`-prefixed 32-byte node computed from the verbatim on-chain label path.

The ENSIP-15 normalizer is an inclusion gate, not an identity transform. Each verbatim label records the normalizer version and whether the raw bytes equal the normalized result under that version. A name containing a rejected or changed label remains as a deactivated shadow identity row. A normalizer-version change recomputes those flags without changing `logical_name_id` or rebuilding the name tree, as ratified by the audit's [normalization-as-a-gate decision](../simplification-audit-20260730.md#normalization-as-a-gate-not-stored-identity). Names that remain active or remain shadow need no chain replay; names that cross between active and shadow are handed to the standard Interpret and Project redo path so bindings are created or retracted by their existing owner. `primary_names_current` separately preserves its documented raw-claim and `invalid_name` behavior.

ENSv1 registrar labels and wrapper DNS labels that contain embedded NUL bytes, are not UTF-8, contain a dot, or exceed the DNS one-label limit remain attacker-controlled chain truth rather than interpretation failures. The controller's registration predicate checks only a minimum decoded length and keys registration in BaseRegistrar by `keccak256(bytes(label))`, while its string-length helper advances from the leading byte without validating UTF-8 continuation bytes. NameWrapper emits the DNS name as bytes and its namehash helper hashes each length-delimited label directly. (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L191 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L192 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L247 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L250 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L288 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L290 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/utils/StringUtils.sol:L13 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/utils/StringUtils.sol:L16 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/utils/StringUtils.sol:L20 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/utils/StringUtils.sol:L25 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L27 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L29 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/utils/NameCoder.sol:L126 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/utils/NameCoder.sol:L134 @ ens_v1@91c966f) The interpreter stores their byte preimages and a deactivated `NameSurface` keyed by the raw chain-native namehash, without creating a surface binding. When a label cannot be represented safely as PostgreSQL UTF-8 text, the shadow row's text display inputs are empty; `label_preimages.raw_label`, the namehash, and, where encodable, `dns_encoded_name` retain the byte identity.[^ens-subgraph-label-null][^ens-subgraph-name-null][^ensnode-null-label]

Solidity `string` values in resolver and reverse-name events are likewise decoded from the ABI as raw bytes. ENSv1 and ENSv2 resolver contracts store and emit the authorized caller's name, text key, and text value without a content-validation step, and the ENSv1 standalone reverse registrar stores and emits its supplied name unchanged. (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L13 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L18 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/TextResolver.sol:L15 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/TextResolver.sol:L21 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/StandaloneReverseRegistrar.sol:L28 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/reverseRegistrar/StandaloneReverseRegistrar.sol:L30 @ ens_v1@91c966f) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L467 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L472 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L479 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L488 @ ens_v2@ccaeb58) A string-valued `after_state` field is an ordinary JSON string only when those bytes are valid UTF-8 and contain no NUL. Otherwise the same field is a lossless object of the form `{"encoding":"hex","bytes":"0x..."}`; no replacement-character text is retained. Indexed text-key hashes are checked against the original key bytes. Name-shaped values split on raw dot bytes for label derivation, and any undecodable or unnormalizable label produces byte preimages plus a deactivated shadow identity rather than halting interpretation. A leading, trailing, consecutive, or bare dot creates an empty segment and therefore a shadow identity keyed by the exact raw dot-segment hash path. Only its non-empty segments may enter `label_preimages`; empty segments never do. Because an interior zero-length DNS label is indistinguishable from the root terminator, these names store an empty `dns_encoded_name` byte string to mean that no valid DNS wire encoding exists, while `raw_name`, `raw_labels`, and the hash path retain the identity observation. For the existing record-projection selector contract, a selector that reaches normalized output but cannot be represented as a nonblank PostgreSQL-safe string is classified under a `<family>_opaque` record family: `selector_key` is a non-textual hex identity, while `raw_selector_key` carries the canonical decoded-string-or-tagged-bytes representation. That family is explicitly unsupported for projection and cannot be mistaken for a queryable text selector; valid UTF-8 selectors, including leading `~`, remain unchanged. A reverse-name claim with hostile bytes similarly stores `raw_name = null` and retains the tagged bytes in `raw_name_bytes`, so the primary-name projection observes no text claim instead of a JSON serialization masquerading as one.

ENSv2 resolver `AliasChanged` and `Named*Resource` payloads with a structurally malformed DNS wire name remain available in the immutable raw log but produce no normalized name or preimage observation: without a valid length-delimited label path, the interpreter cannot derive a chain-native namehash. A structurally valid DNS name whose individual labels contain hostile bytes instead follows the shadow-identity rule above.

`label_preimages` is a hash-to-verbatim-label fact table, not a name-surface table. The schema-v2 interpret phase writes label preimages alongside admitted name observations; the surviving worker may also import them operationally from a rainbow-table source such as Graph Protocol's `ens-rainbow` dump. The old migrate-time scan over retained normalized events and name surfaces has been deleted. The pinned generator emits a prepared `ens_names(hash, name)` table and computes `hash` as `keccak256(name)`.[^graph-ens-rainbow-table][^graph-ens-rainbow-hash] Each retained label must hash back to the stored labelhash. Its current normalizer result is stored as `normalized_under_version` plus an error when false; rejection does not discard the preimage. Label preimages can improve projection readability, but they do not by themselves create ownership, resolver topology, record facts, or primary-name truth, and normalized display text remains a read-time derivation.

`bigname-worker label-preimages import-ens-rainbow` is worker-owned operational tooling. Operators must first load the pinned `ens-rainbow`-shape source table `ens_names(hash, name)` into the bigname database. The command reads that table in hash order, validates and stores only labels that normalize as one ENS label and hash back to the supplied `hash`, and enqueues `children_current` invalidations for known parents that have matching canonical ENSv1 or Basenames registry child edges.

### Opaque UUIDs

- `resource_id`
- `token_lineage_id`
- `contract_instance_id`
- `surface_binding_id`
- `execution_trace_id`

UUID values are internal identities, not user-generated strings. `resource_id` and `token_lineage_id` survive projection rebuilds. Token IDs, node hashes, and resolver addresses are attributes, not identity anchors.

### Monotonic row IDs

Raw fact rows, normalized event rows, and projection job rows use
`bigint generated always as identity`. Allocation is monotonic and deleted
values are not reused; this is an identifier-allocation rule, not a promise
that an interpret redo retains superseded normalized-event rows.

### Continuity rules

`logical_name_id`, `resource_id`, `token_lineage_id`, and `contract_instance_id` continuity is shared with [`architecture.md`](architecture.md) — see the identity model section there for the rules adapters must follow when minting and reusing IDs across ENSv1 wrap/unwrap/re-registration, ENSv2 token regeneration, and proxy implementation churn.

The storage-side guarantees those rules depend on:

- One admitted contract address on one chain maps to one stable `contract_instance_id` across all admission epochs. Re-admission after an inactive gap reuses the prior id and records a new non-overlapping active range.
- Proxy contracts and their implementations are separate `contract_instance_id`s. Implementation churn updates the proxy/implementation discovery edge, not the proxy id.
- Contract addresses are time-ranged attributes for raw-fact lookup, log routing, and watch-plan materialization. Addresses are never the primary key of the source graph.
- Stable adapter identity rows for `token_lineages`, `resources`, and `name_surfaces` are idempotent across retained replay anchors. Replaying a compatible readable resource or name-surface row with the same stable identity and identity-defining fields from a later raw-log anchor may be accepted as an existing identity without rewriting the original anchor, anchor provenance, or `observed_at`; incompatible identity fields remain hard conflicts. The hash-covered adapter seam emits a token-lineage row only when that lineage first enters interpreter state, and replay of that row requires the retained chain/block anchor and provenance to match exactly. Once the stored row is explicitly `orphaned`, re-observing the same stable identity on the winning branch replaces its chain/block anchor and anchor provenance with the winning observation while preserving immutable identity fields. A `name_surfaces` re-anchor also replaces `deactivated_at` from that winning observation alongside the block anchor, provenance, and canonicality. For `name_surfaces`, compatibility requires the stable namespace/namehash ID and the same verbatim raw name, labels, DNS wire encoding, and labelhash path. Normalizer version, flags, visibility, and errors are recomputable metadata, not identity. ENSv1 registrar resources materialized only from a closed surface-binding segment after the lease has been released intentionally carry binding-derived provenance: `released_at` is the binding close time, `expiry` is that time minus the ENS grace period, and the prior registrant is not reconstructed into the resource row unless an unreleased current or superseded registrar lease survives finalization.[^v1-registrar-grace]
- Every emitted normalized event or surface binding with a `resource_id` must reference either a resource already persisted for that chain or a resource emitted earlier in the same identity-and-events transaction. In particular, when an ENSv1 or Basenames registrar release activates a retained direct-registry fallback that has never been materialized, the adapter emits that resource with the raw-block release-boundary anchor before the writer opens the replacement binding. Re-emitting an already-persisted compatible direct-registry resource is idempotent and preserves its original anchor.
- Normalizer-version changes are owned by Interpret redo's `recompute-flags` mode. It updates normalization metadata in `label_preimages` and `name_surfaces` while preserving stable identity and chain-observation anchors. Same-class names are updated without event replay. For an active-to-shadow or shadow-to-active change, it records the affected block range as ordinary Interpret and Project redo instead of creating, reopening, closing, or retracting a `surface_bindings` row itself.
- The schema-v2 interpret phase loads the prior-state fold once per chain run or redo session and keeps that compact fold resident while it advances across physical 500-block batches. This accepted memory cost is proportional to the distinct retained interpreter state keys and their live-lineage dependency anchors; it avoids reloading the full prior normalized-event history for every batch. Completing a redo evicts every resident fold for that chain so the restored normal cursor reloads the rewritten database state.
- Redo preparation marks stable identity rows anchored in the requested range `orphaned` before the later physical batches have necessarily re-observed them. If a multi-batch redo exits after that first transaction, identities not yet repopulated intentionally remain orphaned until the same redo resumes and completes; the interrupted redo state prevents an unrelated redo from treating that intermediate database state as complete.
- For interval identity rows like `surface_bindings`, `active_from`, identity-defining fields, and the observation anchor of every readable row are immutable; `active_to` is replay-derived. The only anchor exception is reorg replacement: an already `orphaned` row may adopt the winning branch's chain/block observation anchor when the stable identity, `active_from`, kind, and provenance still match, while a readable row rejects the same change. That replacement also discards the orphaned row's replay-derived close and uses the winning observation's `active_to`; a winning registration with no unregister evidence therefore restores the stable binding as open. Canonical historical replay may tighten an existing non-null `active_to` to an earlier close point when older or more complete facts reveal an earlier end. Normal replay and identity upsert paths do not extend or reopen a readable closed interval. Any future interval widening or reopen requires a new doc-first rule with its proof, overlap guard, and invalidation behavior. Replay batches that both close an existing interval and open a replacement at the same boundary apply the existing interval update before inserting the replacement, so the non-overlap invariant is enforced without relying on implicit snapshots.

For ENSv2, `resource_id` keys by `(chain_id, registry_contract_instance_id, upstream_eac_resource)` after observing the upstream EAC resource — not by the current ERC-1155 token id. Upstream exposes both `getResource(anyId)` and `getTokenId(anyId)`, emits `TokenResource(tokenId, resource)` when a token links to a resource, and emits `TokenRegenerated(oldTokenId, newTokenId)` when role changes burn and mint a replacement token while leaving the resource unchanged.[^v2-iperm-l34][^v2-iperm-l67][^v2-iperm-l72][^v2-events-l69][^v2-pr-l216][^v2-pr-l451] Unregister/re-register increments both `eacVersionId` and `tokenVersionId` and mints fresh `resource_id` and `token_lineage_id`.[^v2-pr-l28][^v2-pr-l203][^v2-pr-l237][^v2-pr-l536]

## Table families and write ownership

| Family | Current owner | Purpose and Stage B status |
| --- | --- | --- |
| `chain_*` | `ingest` | Block-hash lineage and explicit canonicality. Legacy revision/proof tables have no current Rust writer. |
| selected `raw_*` | `ingest`; execution persistence for admitted exact-block call snapshots | Immutable interpretation inputs and execution snapshot facts. Worker audit and compaction reads survive. |
| `manifest_*` | schema-v2 manifest synchronization | Authored declarations, admission, and capability versions. |
| `discovery_*` | schema-v2 `interpret` | Canonical discovered edges and current watch-plan expansion. |
| `name_surfaces`, `surface_bindings`, `resources`, `token_lineages` | schema-v2 `interpret` | Stable identity anchors. |
| `normalized_events` | schema-v2 `interpret` | Derived protocol events written transactionally with identity and discovery output. |
| `label_preimages` | schema-v2 interpretation plus surviving worker or operator imports | Verified labelhash-to-label facts used by projections. |
| `bigname_phase.*_current` | schema-v2 `project`, including its canonical-head hydration step | Retained replacement current read models; the event-derived core is rebuildable from canonical identity and normalized events, with head-pinned hydration layered into two documented surfaces. |
| `public.projection_*`, `public.*_current`, replay staging and apply cursors | worker and storage triggers | Surviving legacy public-schema read models, rebuild/apply progress, and invalidation journals until Stage C. |
| `manifest_alert_*` | worker audit | Manifest-drift and proxy observations; not admission truth. |
| `service_loop_heartbeats` | worker | Current worker liveness. The API still reads retained old-indexer process/chain rows until its readiness port. |
| `execution_*` | legacy execution worker | Durable traces, steps, cache outcomes, and invalidation records retained for worker use and diagnostic readback. API serving paths do not write this family. |
| `resolution_divergences` | schema-v2 lookup engine, including v2 verified product reads and the Tier-3 diagnostics records route | Rows in the [resolution divergence ledger](glossary.md#resolution-divergence-ledger): active rows represent direct live/indexed disagreements only, and restored agreement may clear a matching row. The compared exact `record_inventory_current` row is guarded through commit; CCIP answers are excluded. |
| `backfill_*` | no current writer | Immutable migration-era jobs and ranges; storage retains read-only worker inspection. |
| `normalized_replay_*` | no current writer | Migration-era replay/checkpoint state. The worker still reads selected cursors for projection readiness and raw staging compaction. |
| `base_normalized_rederive_*`, resolver-profile queues/journals/reconciliation, retained-history/coverage/frontier tables, startup adapter checkpoints | no current writer | Stranded transitional schema retained only because migrations are immutable. These rows are not current admission, readiness, replay, or repair authority. |
| `name_surface_normalization_repair_findings` | no current writer | Historical audit rows from the deleted indexer repair command. |

The API is otherwise read-only over projections and execution output. V2
verified record routes use the schema-v2 lookup engine and may perform only the
divergence-ledger write described below; v2 primary-name verification performs
no serving-path write. Neither path grants a raw-fact or legacy
operational-table fallback.

The worker continues to update its process and named rebuild-phase heartbeat
rows at bounded projection progress points. Existing API readiness code also
loads the old indexer heartbeat shape. That read and the underlying rows are
kept because the API/worker port has not landed; no process in this source tree
publishes new old-indexer heartbeat state.

Legacy projection invalidation journals, replay attempts, staging checkpoints,
and dynamic stage tables remain worker-owned only for the public schema. The
schema-v2 project phase has none of them: it stages in connection-local tables
and publishes the affected projection set in one transaction.

## Manifests and discovery persistence

At minimum:

- `contract_instances` — one row per stable `contract_instance_id` with chain, contract kind, and provenance. Roots use the same identity family as other contract instances.
- `contract_instance_addresses` — time-ranged address attributes keyed by `contract_instance_id`. One id may carry multiple non-overlapping active ranges. Manifest-declared address ranges may carry nullable inclusive `start_block` metadata where the manifest supplied it.
- `discovery_edges` — keyed by `edge_id` with `from_contract_instance_id`, `to_contract_instance_id`, `edge_kind`, active range, provenance, canonicality.
- Materialized watch-plan rows keyed by `contract_instance_id` plus chain and range; root start nodes keyed by the root `contract_instance_id`. Address is a derived watch target, not the durable identity. An omitted `start_block` is persisted as null rather than coerced to zero.

Schema-v2 resolver classification is separate from contract-instance
admission. ENSv1 and Basenames require the exact emitter address in the active
resolver manifest. ENSv2 requires the resolver proxy's latest canonical
`Upgraded` implementation in the active manifest's
`resolver_implementations` list. Unknown emitters remain unsupported even when
generic resolver events were retained; no code-hash fact participates. A
matching `SourceManifestUpdated` event scopes inline classification
reconvergence during project publication. The old public-schema profile views
retain their code-hash behavior only until Stage C.[^v1-pres-l20][^v1-pres-l66][^bn-l2resolver-l4][^bn-l2resolver-l16][^bn-l2resolver-l29]

`manifest_alert_*` carries an observation identity, observation kind (`manifest_drift` or `proxy_implementation_drift`), lifecycle status, manifest version, source family, chain, contract-instance references, nullable proxy/implementation edge references, expected and observed code-hash or implementation-edge material, derived watch-plan metadata, first/last observed timestamps, and nullable remediation metadata. Writing it does not write `normalized_events`, mutate manifest truth, mutate discovery admission, change capability flags, or expose API state. A proxy implementation observation preserves the proxy `contract_instance_id`; implementation churn is represented by an observed or admitted edge, not by minting a replacement proxy identity.

## Historical backfill inspection

The old persisted backfill scheduler has been deleted. No surviving Rust path creates, leases, advances, completes, fails, repairs, or publishes coverage from `backfill_jobs`, `backfill_ranges`, or their companion tables. Historical range work is represented in the new runtime by explicit finite phase redo state instead of by these migration-era jobs.

Storage retains only `load_backfill_job` and `load_backfill_ranges` because `bigname-worker inspect backfill-job` is a surviving read-only audit command. The returned lifecycle, range, selector, source identity, timestamps, and failure metadata describe rows written by an older binary. They do not establish current ingest completeness, interpret readiness, checkpoint promotion, or projection freshness.

The old coverage facts, source selectors, frontier calculations, recovery failures, job leases, retention-generation fences, and stored-lineage proofs have no current Rust owner. Their SQL migrations remain unchanged. Any future deletion or replacement of those tables belongs to the worker/API port and migration-baseline work, not this code deletion.

## Partitioning status

The current migrations create ordinary PostgreSQL tables for lineage, raw facts, normalized events, execution, identity, and projections. There is no checked-in table partitioning baseline yet. Row-volume control currently comes from explicit indexes, phase batching, projection batching, and retained-staging compaction policy. Any future partitioning change is a migration-bearing storage change and must update this section with the concrete table list and keys.

## Canonicality model

`chain_lineage` persists the recent reconciled block window keyed by `(chain_id, block_hash)`:

- `parent_hash`
- `block_number`
- `timestamp`
- checkpoint-promotion state

Header audit fields are optional retention. The default lineage contract omits `logs_bloom`, `transactions_root`, `receipts_root`, and `state_root`; reorg repair walks backward through `(block_hash, parent_hash)` until it reaches a stored matching ancestor, then marks the losing stored lineage noncanonical. Every dependent fact becomes unreadable as canonical through its mandatory lineage join; reorg repair does not require per-table orphan writes.

An auditable-header retention mode stores those fields in `chain_header_audit` keyed by the same `(chain_id, block_hash)` so inspection tooling can explain or cross-check fetched payloads. Their absence cannot prevent canonicality repair, checkpoint promotion, replay over retained selected facts, or projection rebuilds. When both stored and incoming audit rows carry the same field, mismatches are hard storage conflicts.

`raw_blocks` is not a durable table. Intake, replay, workers, adapters, audit helpers, and tests read block timestamps and canonicality from `chain_lineage` and read optional audit roots/bloom from `chain_header_audit` when auditable retention is enabled. Normal replay batches block-anchor admission once through the `chain_lineage` write boundary.

Every fact-derived row that can be invalidated by reorg carries `chain_id`, `block_number`, `block_hash`, `canonicality_state`, `observed_at`. `canonicality_state` values:

- `observed`
- `canonical`
- `safe`
- `finalized`
- `orphaned`

For a block-anchored, chain-derived row, canonical readability is defined only
by joining its `(chain_id, block_hash)` anchor to `chain_lineage`. The row-local
`canonicality_state` records the replay/verification checkpoint-promotion
lifecycle from `canonical` through `safe` and `finalized`; it may lag a reorg
and is never a standalone readability predicate. Every consumer of a
block-anchored row must apply the lineage join.

Manifest-derived control events such as `SourceManifestUpdated` have no block
anchor and therefore cannot join lineage. They use their row-local finalized
state for project admission and are not reorg-addressable chain observations.
This unanchored control-event rule never permits a consumer of a block-anchored
row to skip the lineage join.

Rules:

- block hash is the identity anchor; block number is position only
- head publication marks displaced lineage `orphaned`; it never deletes the
  losing lineage, and that one lineage mutation makes every anchored derived
  row unreadable as canonical without per-table orphan writes
- between head publication and the required interpret redo, losing-fork
  normalized events remain physically present but are excluded from the
  canonical-readable universe by the lineage join
- interpret redo deletes the selected normalized-event range and re-derives it
  from readable lineage; superseded losing-fork derivations are not retained
  after that redo completes
- reorg repair preserves permanent audit truth in lineage and durable raw
  facts. Log-audit mode also preserves selected raw-log/transaction/receipt
  facts; minimal raw-log retention may already have compacted non-audit
  staging rows
- evictable payload-cache bytes or compacted staging rows do not erase
  permanent lineage anchors or replay-critical raw evidence retained by the
  selected policy. Normalized-event provenance describes the current
  interpretation epoch
- optional header audit fields are verified when both stored and incoming audit rows carry them. A minimal replay does not conflict with an existing auditable row solely because it omitted those fields
- projection rebuilds read rows that are `canonical`, `safe`, or `finalized` by default; history and audit tools may opt into `observed` and `orphaned` rows
- canonical readability of block-anchored derived rows and normalized events requires both readable row-local state and a readable `chain_lineage` join on `(chain_id, block_hash)`; the row-local column records only the replay/verify canonicality-promotion lifecycle and may lag reorg state, so readers must never consult it without that lineage join. The only exemption is interpret's [discovery-edge](glossary.md#discovery-graph--discovery-edge) admission reads: they consume discovery-authority state with activation anchors and are healed by the stamped interpret redo; adding readable-lineage validation for each edge's `(chain_id, active_from_block_hash)` activation anchor remains ticketed hardening
- before stamped redo replaces its range, audit tools may still inspect the
  retained losing rows while labeling them from their orphaned lineage
- normal phase targets use only that readable path. An `observed` suffix written before head publication is intake staging, not a downstream target
- safe and finalized checkpoint promotion is monotonic per chain
- `chain_heads.lineage_orphaning_epoch` is the per-chain [lineage orphaning epoch](glossary.md#lineage-orphaning-epoch). It increases only when head publication moves previously readable lineage to `orphaned`; ordinary head advancement and checkpoint promotion preserve it

## Reorg and redo boundary

The phase runner stores canonicality by block hash and marks displaced readable
lineage orphaned. It also marks conflicting `observed` intake staging rows
orphaned only through the proposed latest height, but derives downstream redo
ranges only from displaced rows that had already been readable. Higher observed
rows remain staging until a later provider snapshot proves or displaces them.
Raw facts remain immutable; interpretation selects them
by joining against readable lineage. In the same head-publication transaction,
the runner increases the lineage orphaning epoch when it orphans a displaced
readable suffix, and
storage removes rows from retained `public.execution_cache_outcomes` whose block
dependencies are orphaned in `bigname_phase.chain_lineage`. Their durable
execution traces and steps remain in `public`; later canonical recovery does
not recreate an evicted cache row. If the orphaned suffix starts at or below an
`interpret` or `project` cursor, that transaction also stamps the existing redo
fields for the inclusive affected suffix through the phase's recorded cursor.
The deleted
indexer reconciliation tree no longer performs its synchronous multi-family
adapter repair, coverage proof, normalized-event supersession, or
resolver-profile convergence.

Derived schema-v2 repair is an explicit `interpret` redo over a complete
ingested range. Head publication first orphans the losing lineage, immediately
excluding its anchored normalized events through the mandatory lineage join
while retaining those event rows physically. Their row-local canonicality may
lag during this window. Redo preparation then deletes normalized events in the
selected range, orphans derived identities anchored there, replays the range
through the schema-v2 interpreter, and re-anchors stable identities when the
winning facts reproduce them. An interrupted multi-batch redo remains explicit
persisted redo state and must resume; its intermediate state is not a completed
projection boundary.

This bounded delete-and-re-derive behavior is the intentional
[plain-events redo](glossary.md#plain-events-redo) rule and a deliberate
divergence from the previous wording that promised permanent retention of
losing normalized events. Keeping superseded derivations across interpreter
versions would require the event revision and supersession machinery removed
in Stage B and would otherwise accumulate stale copies of history. Durable raw
facts plus permanent competing chain lineage are the audit trail; normalized
events are the current interpreter epoch's derivation of the readable universe.
`recompute-flags` is the bounded normalizer-version repair mode. Under the
Interpret and Project phase locks, it first uses the existing scoped Project
machinery to refresh `primary_names_current.claim_name_is_normalized`, then
recomputes `label_preimages` and `name_surfaces` normalization metadata with the
current normalizer; correctness does not depend on this order because Project
derives claim normalization with the current normalizer when it builds the
projection. A name that remains active or remains shadow takes this flags-only
path: normalized events, identity anchors, and surface bindings are not
re-derived. A name whose visibility class changes is enumerated and reported to
the operator, and its affected chain range is merged into the standard Interpret
redo and required Project continuation. The recompute path never fabricates,
reopens, closes, or retracts a surface binding. After a shadow-to-active
recompute commits, the surface has active visibility while its bindings and
projections remain at the pre-transition class until the stamped Interpret and
Project redo runs. The API serves the conservative pre-transition projection
state in that window, and the stamped markers block normal Interpret work. The
operator must run the stamped redo to make transitions visible; until then,
affected names serve their pre-transition state.
An interrupted recompute retains its resumable marker. Its own queued scoped
Project refresh is distinguishable and resumable. After that refresh completes,
the Project marker remains in a distinct "Interpret flags pending" state until
Interpret completion atomically restores it or replaces it with the ordinary
transition redo; there is no unmarked Project-to-Interpret crash handoff. An
unrelated ordinary Project redo that was already pending is widened or left
pending rather than consumed as recompute work. The label scope includes
in-range name surfaces, label provenance, and retained in-range
`PreimageObserved` evidence, so resolver label observations remain repairable
even when they do not materialize a name surface.

This is an explicit design divergence from amendment E of the simplification
plan, whose bare rule said that `recompute-flags` runs without replay. A shadow
name intentionally has no active binding. Moving shadow to active therefore
requires the standard interpreter to derive a binding, and moving active to
shadow requires the same replay path to retract it. Only names that stay in the
same visibility class can honestly complete without replay; class transitions
are stamped for ordinary Interpret and Project redo to preserve derivation
ownership and replay purity.

Verify redo uses the same marker and persists the
[verification level](glossary.md#verification-level) reported by its phase
implementation. The production phase rechecks the requested
inclusive range only inside the recorded verification extent; its end cannot
exceed the current verify cursor. Each batch is additionally constrained to
finalized lineage. Blocks above the verify cursor are covered by normal
verification resume, never by redo. Redo completion restores the pre-redo
normal extent; a partial production redo retains its level, while a redo
covering the full retained extent can change it. A normal verification run
freezes its target at the finalized head and can run beside live follow. Its
start comes from durable ingest cursors rather than replacement runtime
descriptors; a resumed normal scan retains the weaker whole-extent level if the
reference level changed. Base
additionally caps the independent dRPC comparison at the
Coinbase-to-dRPC ingest seam. Its separately credentialed database handle must
have SELECT on every `bigname_phase` relation and is rejected at startup if its
login can write any application relation, create schemas/database objects, has
elevated role attributes, belongs to another role, or was reached by assuming a
reader role from a different session user. The verifier never receives a
writer-role pool, and its connection must report the same PostgreSQL system
identifier, database OID, and database name as that pool.
Unsupported historical `live` redo requests fail before the runner writes a
redo marker. Verification configuration is checked before a redo marker is
written. A failed verification redo retains the normal resumable redo marker;
rerunning the same range after repair resumes it. These refusals and failures
cannot leave an unresumable state row.

A verification mismatch is a non-retryable chain failure. The verify row's
`last_error` stores the block number, differing field, stored value, and
reference value. No attestation or repair row is written. Normal verification
does not advance past the last successful batch. That cursor is safe only while
the lower stored history is unchanged: wipe-and-resync repair must also reset
the chain's phase state or run verify redo from the durable ingest start through
the retained verified extent (the current verify cursor), because normal resume
skips every block below its cursor. That full-extent redo rechecks all retained
history and records the level fixed by its source kind again; normal resume
then checks the re-ingested blocks above the cursor. A mismatch in the
first-ever verify batch leaves no recorded extent, so no verify redo range is
expressible and a full phase-state reset is the only repair. The live row
records that verification stopped its paired live loop.

System-required redo stamps reuse `chain_phase_state.redo_*`; there is no reorg
queue or scheduler table. A stamp is created only when the phase cursor reaches
the orphaned suffix, and multiple affected ranges merge. The runner consumes an
interpret stamp before project and restores each phase's normal cursor to the
winning block hash. The existing ownership marker distinguishes a pending
system stamp from a replay that has acquired its phase writer slot. Only the
pending form is ignored while selecting dependencies or filling the winning
gap; the active form participates in the normal non-verify writer exclusion.
Successful interpret redo atomically stamps project for the
same actual replayed suffix, including a same-hash operator data repair. An
already active operator redo remains explicit and is extended rather than
discarded.

`phase-runner rewind` takes the ingest, interpret, project, and live advisory
locks and republishes an exact stored readable ancestor without crossing the
safe marker. It performs no raw or normalized write: suffix orphaning, cache
invalidation, and downstream redo stamping are all effects of the same
head-publication transaction used by live follow. A later live cycle must load
the winning path through the stamped upper bound before the downstream redo can
run.

Storage still exposes manifest-, topology-, and record-boundary execution
invalidation used by the worker. The retained orphan-block invalidator is
narrower: phase-runner head publication invokes it in the canonicality
transaction, and it changes only cache eligibility while preserving durable
execution evidence.

The resolver-profile queue, journal, and reconciliation tables remain migration-era data with no Rust storage API or runtime consumer. Rows left by an older binary do not gate phase progress, worker replay, API reads, or current manifest admission.

## Interpretation replay semantics

Schema-v2 `interpret` is the sole current writer of identity rows, discovery output, and normalized events from retained chain facts. Normal execution advances only through the completed ingest boundary. A redo selects one inclusive finite block range and reconstructs it chronologically from the admitted raw facts and manifest state.

Interpretation loads the prior state needed before the range and keeps the compact fold across physical batches. When the lineage orphaning epoch is unchanged, the next load checks only block anchors newly retained by the preceding fold. When the epoch changes, it checks every retained anchor once and reloads prior state if any anchor is no longer readable. The write transaction still locks and revalidates the resume marker plus every current batch block before writing identity, discovery, and normalized events, so an orphaning concurrent with interpretation cannot commit stale derived state. Stable identity fields remain conflict checked. Redo uses the same loader and may replace only the selected derived range; it uses explicit orphan/re-anchor behavior rather than the deleted old-schema upsert, field-repair, canonicality-supersession, closure-proof, or adapter-checkpoint machinery.

The old `normalized_replay_*` cursor and checkpoint tables are not interpret progress. They have no writer in this source tree. Selected reads remain solely because the unported worker uses them when deciding projection readiness and raw staging compaction.

### Projection replay durability

`current_projection_replay_status` rows let worker restarts resume from the first unfinished projection family instead of restarting bootstrap/full replay from the start. They are worker-owned operational progress: not API truth, not projection data, not live-readiness state, and ignored unless the recorded replay version and full-replay input revision are current and the recorded normalized target covers the requested replay target. The API does not read this table. The full-replay input singleton also retains the activation state and minimum projection replay version admitted to projection-owned writes, even when a later direct-input repair invalidates all markers and the automatic attempt. Every new database connection stamps the binary's compiled replay version. Replay-state writers lock the singleton exclusively, reject a compiled version below the stored minimum or any persisted attempt, checkpoint, or marker version, and activate or raise the fence in the same transaction. Statement triggers on the invalidation queue and cursor, current projection and companion tables, replay state, and the singleton itself normally take the shared lock before every write. Before activation they serialize any already-running pre-fence writer with the first fence-aware replay. After activation, they reject both a lower stamp and a missing stamp from a pre-fence binary. A statement trigger that finds replay admission already holding the singleton fails immediately rather than waiting after its table lock and creating a reverse lock-order deadlock. A current, validly stamped writer receives the one retryable admission error. An unstamped or already-outdated process receives the fatal outdated-process error; missing singleton state and invalid stamps are also fatal fence failures, but are not classified as outdated. Claimed apply also holds an explicit shared fence through projection publication and queue completion, while claim, claim-heartbeat, invalidation derive, and hydration transactions perform their own typed checks. An earlier shared writer therefore commits before newer replay admission, while an outdated writer arriving afterward fails fatally without claiming or publishing. Dynamic stage-table writes are coupled to a protected checkpoint mutation in the same transaction. A direct non-event source repair advances the input revision and invalidates markers and the automatic attempt in its source-update transaction without lowering the replay-version minimum. Automatic bootstrap holds its cross-process replay lock from apply-cursor baseline selection through family replay. The manual `replay all-current-projections` command tries to acquire that same lock and exits with a clear ownership error instead of competing with automatic replay. Projection-specific one-shot rebuild commands also acquire it before clearing a marker or entering durable full-family staging, and fail without changing shared replay state when another process owns the lock. Once admitted, a manual all-current command first reuses a compatible persisted attempt and its target. Without an attempt, it starts one at the same normalized-replay and chain-checkpoint head used by automatic bootstrap when either head exists, resumes only unfinished families, and writes that real target on every completion marker. Those target-bearing markers can therefore satisfy the later automatic handoff. When no attempt and neither head exist, the manual command instead proceeds without creating an attempt and writes `NULL`-target checkpoints and completion markers. No automatic attempt exists to consume that targetless progress, and a later attempt with a concrete target does not treat those markers as covering it. The final automatic transaction locks the input revision, verifies all seven compatible markers, creates a missing `projection_apply_cursors` row at the persisted attempt baseline, and consumes the attempt. Continuous apply therefore cannot observe the handoff cursor before the protected replay has completed.

Fence-aware workers exit on every fatal replay-version fence failure and retry
only the explicit current-version admission-race error. A pre-fence binary
cannot be made to exit by code it does not contain: it may keep retrying, but
the database still prevents every protected write from committing until the
process is replaced.

Stamped invalidation-queue DML is the narrow exception to singleton row
locking. It reads the committed activation state and version floor without
waiting. An ingestion transaction can already hold `ROW EXCLUSIVE` on a staging
input journal, while replay holds the singleton exclusively and waits for
`SHARE` on that journal; making the enqueue wait for the singleton would close
a deadlock cycle. If the enqueue commits before replay captures the journal,
the replay drift check sees it. If it commits afterward, the durable queue row
and retained direct-invalidation revision make it post-replay incremental apply
work. A stamped enqueue can therefore cross a concurrent floor raise based on
the previously committed floor, but it publishes no projection or replay state;
the current worker reapplies the requested key, and a later statement from a
now-lower version is rejected. The trigger enforces `READ COMMITTED` for this
exception so that the next queue-writing statement sees a newly committed
floor; a transaction using a longer-lived snapshot fails fatally rather than
widening the crossing-write window. Queue `TRUNCATE` and unstamped queue
writers remain non-waiting.

Identity canonicality and readability changes on `resources`,
`name_surfaces`, `surface_bindings`, and `token_lineages` can recompute
`address_names_current_identity_counts` and
`address_names_current_identity_feed`. Those mutable sidecars do not use the
queue's committed-floor exception. Address-name full replay truncates and
rebuilds both from the published projection and current identity state, but an
older statement snapshot allowed to cross publication could later overwrite
that rebuild without leaving a durable work item for another recomputation.
Waiting for the singleton is also unsafe: the identity transaction can already
hold a normalized-event, permission-resource, or direct-invalidation journal
lock while the replay owner holds the singleton and waits to take `SHARE` on
that journal. The sidecar triggers therefore retain the non-waiting admission
check. Current-version schema-v2 identity transactions retry the entire failed
transaction with a fresh `READ COMMITTED` snapshot and bounded backoff; an
admission collision is not recorded as a cursor or range failure unless that
retry budget is exhausted. Canonicality flips may consequently retry during
full-replay admission windows without publishing partial identity or sidecar
state.

`current_projection_staging_checkpoints` is the earlier, per-family stage of that worker progress. One row records the projection replay version, a staging-schema version, the exact normalized target, the `current_projection_full_replay_input_revision` value, the normalized-event change-log prefix, direct-invalidation revision, and permission-resource revision validated for its completed source range, dynamically named logged stage tables, the last completed ordered source key, source and output counts, and `running` or `staging_complete` state. Before each fresh keyset-paged source query, the worker captures complete watermarks and checks projection-relevant keys through the stored cursor. A page transaction commits its stage rows, cursor, counts, and new validated watermarks together. After an empty final page, it captures fresh watermarks and repeats the check across the full source range before marking the stage complete. Immediately before replacing the live projection, the publication transaction captures fresh watermarks and repeats that full-range check again. Its `SHARE` locks on all three input journals remain held through the live-table commit, so a relevant invalidation or permission-resource change that was in flight after staging completion either commits first and forces a fresh stage or waits until publication commits and remains incremental apply work. The dynamic tables intentionally survive connection loss and worker or database-backend termination; they are not temporary or unlogged tables. Their extra `inserted_at` value is stage-only and uses a deterministic epoch default, while publication selects only the declared projection columns and lets the target table own its insertion timestamp.

The worker reuses a checkpoint only when every compatibility field, cursor shape, and stage table matches. Missing tables, changed full-replay input revisions or targets, and replay- or staging-version mismatches cause transactional deletion of the old stage and creation of a fresh one. The replay input revision remains the worker fence for any admitted direct source mutation. The old name-surface normalization repair producer has been deleted; no current runtime advances that revision through that command. Staging-page and publication transactions hold a shared lock on the matching revision, so they cannot commit stale progress across a concurrent revision advance.

Normalized-event change-log growth by itself is deliberately not a checkpoint compatibility mismatch. A relevant family-specific or manifest-derived change at or before the completed source cursor discards that family's running stage for a fresh restage; a later-key change is included by a subsequent fresh page. Direct invalidations use the same key-range rule through their retained generation revision. Publication repeats the full-range check from the completion watermarks inside the replacement transaction, including invalidations whose live queue row has already been consumed. Drift discards the completed stage and starts a fresh one without touching the live table. Changes whose writers begin only after guarded publication commits remain key-scoped invalidations for handoff. `current_projection_replay_attempt` durably preserves the original target and pre-replay apply baseline across kills, allowing concurrent schema-v2 interpretation and chain-head advancement during a long projection replay without treating every new event as a global restart. A completed stage remains through publication and replay-owned hydration. The worker then writes its revision-bound completion marker and removes the consumed checkpoint and logged tables in one transaction. Published-family skip and completed handoff clean residue from older non-atomic workers. A running worker revalidates marker/revision handoff before continuous apply and re-enters bootstrap if a direct-input repair invalidates it. Neither the API nor continuous projection apply reads replay attempts, checkpoints, or dynamic stage tables, and they are not projection truth, freshness evidence, or a replacement for invalidation catch-up.

`projection_invalidations` rows are the durable key-scoped work queue for projection refreshes. `projection_normalized_event_changes` is the append-only downstream input for normalized-event inserts, admitted semantic content updates, and canonicality-state updates. Migration-owned triggers record schema-v2 interpretation replacements as `content_update` and canonicality transitions as `canonicality_update`; when one update changes both, the normalized-event trigger appends both records. Historical one-time repair migrations that ran before `content_update` existed retain their legacy `canonicality_update` labels, and consumers treat both kinds as invalidation input. Migrations install the forward log and trigger without bulk-copying historical `normalized_events`. Its identity-assigned `change_id` values are allocation-ordered, not assumed to be commit-ordered. Before bootstrap captures its initial cursor or continuous derive chooses a batch, storage captures a finite complete-prefix bound in a short transaction: it takes a `SHARE` table lock, waits out prior `ROW EXCLUSIVE` change-log insert transactions for at most 100 milliseconds, reads `MAX(change_id)`, and commits to release the lock before derivation. New insert writers cannot allocate a change id while that bound is queued or held, but the timeout bounds that writer barrier. A capture that times out fails without entering the cursor-advancement transaction; the worker retries later from the unchanged cursor. Derive processes only rows above its cursor and at or below a successfully captured bound; later inserts remain explicit subsequent work, and unused identity values remain harmless gaps. A cursor therefore cannot skip a lower-id transaction that was still in flight when the bound was selected.

The complete-prefix-capture migration takes the automatic-bootstrap replay lock, pre-locks and rewinds an existing `normalized_events_to_projection_invalidations` cursor, then takes `ACCESS EXCLUSIVE` on the change log to drain old derive readers and insert writers in cursor-then-change-log order. While that cutover lock remains held, it removes the obsolete global insert-order advisory trigger, installs the reader-side capture function, and repeats the targeted cursor rewind. If the cursor was initially absent, an in-flight pre-cutover derive either publishes before the exclusive lock and is caught by the second rewind, or resumes afterward from a safe zero lower bound. Deployments therefore pay one full, idempotent change-log derivation after this migration. A follow-up migration attaches the 100-millisecond function-scoped `lock_timeout`; it does not rewrite the already-applied cutover migration or relax complete-prefix correctness. `projection_apply_cursors` rows track consumed `change_id` watermarks for that input. Normalized-event derive writes both family-specific and manifest-derived keys; execution and other non-normalized-event producers write the same queue directly. A queue trigger assigns every direct inserted or advanced generation a monotonic revision and upserts its key into `projection_direct_invalidation_revisions`; normalized-event derive sets a transaction-local marker to avoid duplicating changes already covered by the normalized-event journal. The retained row is not apply work and survives deletion or dead-lettering of the live queue row. Its watermark capture uses the same short `SHARE`-lock pattern and 100-millisecond bound as normalized-event capture, making unenumerated future direct producers part of the staging fence automatically. The queue primary key is `(projection, projection_key)`; repeated invalidations for the same key update the row generation, clear retry metadata, return the row to `pending`, and release any stale claim so an older apply cannot erase newer work. Projection workers claim and apply rows in projection dependency order, then delete only the claimed generation. Claims are leases with retry recovery, so rows claimed by a stopped worker become eligible again after the retry delay rather than requiring manual queue repair. Rows that fail the same claimed generation five times are removed from the live queue and copied to `projection_invalidation_dead_letters` with `state='dead_letter'`, the failure reason, timestamps, attempt count, and original queue identity for operator inspection. Dead-letter rows are durable operational evidence, not claimable work.

## Projection storage rules

Every current-state projection row carries provenance pointers, manifest version, relevant chain positions, canonicality summary, and last-recomputed timestamp.

The schema-v2 project phase reads only canonical-lineage identity rows and
normalized events. It derives all seven builder families into connection-local
temporary tables, then replaces the affected chain or redo scope in one
transaction; concurrent readers therefore see the complete prior set or the
complete successor set. Projection JSON that carries a `coverage` object uses
`status = "projected"` and `exhaustiveness = "not_asserted"`. Those values mean
only that the row was derived from the stored canonical inputs. Product support
is stated separately by `support_status` and `unsupported_reason`; the JSON
wording is not an assertion that history or enumeration is complete. The
legacy public-schema worker vocabulary below remains unchanged until the API
switches storage in Stage C.

Canonical-head [hydration](glossary.md#hydration) is a post-publication step of
that project phase, not an additional rebuild input. On Ethereum, it selects
existing ENS/60 primary-name tuples whose latest canonical reverse claim and
resolver edge name a configured event-silent resolver (upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L311 @ ensnode@2017ae6) (upstream: .refs/ensnode/packages/datasources/src/mainnet.ts:L316 @ ensnode@2017ae6), plus supported ENSv1
text entries whose event did not retain a value or whose prior hydration needs
refresh. Multicall reads are pinned to the exact `chain_heads` number and hash;
the publication transaction takes a shared lock on the same marker and retries
if it changed. The project publication marker must equal that canonical head;
a behind-head bounded redo defers hydration until normal project catch-up.
Reverse results update only `primary_names_current`; text results
update only `record_inventory_current.entries`. Hydration provenance records the
exact head and resolver/node where applicable, plus the event-derived fields
that the enrichment replaced. Failed calls restore those fields, remove stale
hydration metadata, and keep the project attempt retryable at the same head. A
previously hydrated reverse tuple that loses legacy-resolver eligibility also
restores those fields without issuing another resolver call.
There are no raw-fact, identity, normalized-event,
execution-trace, or historical-hydration writes.

`children_current` remains node-complete when a current registry edge reveals
only hashes. Such a row keeps non-null `labelhash` and `namehash`, but its
`raw_label`, `decoded_label`, `raw_name`, and `decoded_name` are null until
verbatim bytes are observed. Decoded text is forbidden without the matching raw
bytes, and a display placeholder is derived only when Stage C reads the row. A
later preimage rebuild upgrades the same row with bytes and exact decoded text.

Current projection timestamp fields are representable Unix-second values or `null`. ENSv2 `type(uint64).max` expiry observations project as `null` rather than a fabricated far-future timestamp; upstream uses that value for never-expiring reverse names, while registry renewal can carry any non-decreasing `uint64` expiry.[^v2-reverse-max-expiry][^v2-registry-renew-expiry] Numeric values that do not fit the projection timestamp representation are not converted into public projection timestamps.

Projection tables may be truncated and rebuilt from canonical facts plus normalized events.

`permissions_current_resource_summary` is the projection-owned per-resource companion to `permissions_current`. It persists permission-authority support classification, an optional ENSv2 registry-root anchor, and chain-position/canonicality evidence from the authority inputs even for resources with zero holder rows. Its JSONB coverage decoder accepts the documented legacy `full`/`authoritative`, `partial`/`best_effort`, and `unsupported`/`not_applicable` combinations and the schema-v2 derivation-only `projected`/`not_asserted` pair. The derivation-only pair does not assert permission support; until a reader consumes schema-v2's separate `support_status` and `unsupported_reason`, public permission consumers fail closed to partial/best-effort support. Unknown vocabulary and inconsistent status/exhaustiveness/reason combinations are storage read errors rather than implicit public upgrades or downgrades. Keyed rebuild replaces one resource's holder rows and summary in one transaction. Full rebuild target discovery additionally selects canonical zero-event resources with positive source-family/manifest-version identity evidence, then stages and publishes both families in one transaction. For a zero-event current resource, the worker may derive summary evidence from either the normal resource provenance keys (`source_family`, `manifest_version`) or the ENSv1 binding-authority keys (`binding_source_family`, `binding_manifest_version`); these are identity evidence for the projection rebuild, not an API fallback. A storage trigger records insertion, deletion, key changes, anchor/provenance changes, and canonicality changes in `projection_permissions_resource_input_revisions`, so durable full-rebuild staging does not depend on a normalized event existing for the resource. Deleting an identity resource cascades its summary. Public role and permission reads use this companion for support metadata and fail closed when it is absent; they do not recover that metadata from interpret-phase `resources` provenance.

`permissions_current_publication` has one row keyed by `projection='permissions_current'`, with positive `publication_version`, positive monotonic `data_revision`, and `published_at`. Version 2 denotes the holder-row plus typed per-resource-summary publication contract. The full staged rebuild upserts the compatible version and advances the revision in the same transaction that replaces both projection families. A keyed resource rebuild advances the revision in its row-and-summary transaction only when the existing publication version is already exact-current; it neither creates the row nor upgrades an old version. Public permission-backed reads require the exact current version, capture its revision before reading, and verify the same revision before returning. Missing or incompatible versions and an interleaved revision change return `409 stale` before an assembled response is exposed. Readers do not use the revision or `published_at` as a freshness signal, and this artifact does not replace operational replay markers, apply cursors, invalidation draining, or deployment coordination. The retired Base normalized-event correction was the only direct permission-projection deletion path outside these publishers. Its command and storage helpers are deleted; current production writes use the keyed or full publishers. Exported row/summary upsert and delete helpers are low-level storage/test construction boundaries, not public-generation publishers; production worker publication uses the keyed or full transaction.

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
versioning is both a bootstrap gate and, after the fence migration, a
mixed-version writer fence. The version-8/version-9 handoff predates that
migration and remains the historical example that motivated structural
enforcement. The first fence-aware replay-owner transaction now activates the
durable minimum; invalidation claim and heartbeat, derivation, apply
publication, hydration, replay publication, marker, checkpoint, cleanup, and
replay-reset writes from a lower-version or unstamped pre-fence process then
fail fatally. A write already holding the shared database fence finishes before
replay admission, so the later replay supersedes it. Deployment automation must
still stop or drain every old worker before a newer worker starts and keep
public reads drained until every new-version marker is current and all pending
invalidations have drained. The deleted indexer no longer participates in this
handoff. Schema-v2 interpretation is the current normalized-event producer, and
migration-owned change-log triggers make those writes visible to the surviving
worker. The drain steps remain the supported rollout and freshness gate rather
than the sole protection against an outdated worker.

Replay version 10 retained the version-9 outputs and rebuilt
`primary_names_current` to materialize `claim_name_is_normalized` from the
untrimmed reverse claim under the pinned normalizer. The migration adds the
non-null flag with a false default, so existing successful rows are deliberately
not readable as verified successes until replay recomputes them. Full rebuild
compares the current and staged claim rows, including this flag, and deletes
request-matching `verified_primary_name` cache outcomes for changed tuples in one
set-based statement before publishing the staged projection. Targeted rebuilds
keep the existing transaction-scoped tuple invalidation. Later pinned-normalizer
changes use the bounded `recompute-flags` repair described above instead of
requiring another projection replay-version bump. Deployments must not run
version-9 and version-10 workers concurrently and must keep public reads drained
until every version-10 marker is current and pending invalidations have drained.

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

If the selected positions are valid but no eligible projection or persisted execution output exists, the serving path returns the documented `stale`, `unsupported`, or `not_found` API state. It does not read raw facts, interpret-phase identity/event rows, or provider data directly to fill the public response.

## Execution storage

The schema-v2 successor used by v2 has no execution cache, durable trace,
reusable outcome, revalidation state, or persisted request-validation state.
Every v2 verified record request executes again after the API admits the
current authoritative projection position. A cross-chain execution chain must
be in the selected API scope, but the lookup engine derives the exact
hash-pinned execution position from the canonical projected row. That position may
be older than the API's newest generic checkpoint for the execution chain, but
it cannot be newer and must match the admitted hash at the same height; the
lookup result returns the actual authoritative and execution positions so the
v2 response metadata can expose them. For direct and alias paths, it
reads the exact `record_inventory_current` row identified by the projected
record-version boundary's `resource_id`, compares each direct hash-pinned
answer with that row's exact record entry, and calls
`write_resolution_divergence` for every comparable direct answer. Disagreement
may create or replace an active row; restored agreement creates no row but may
clear a matching active row. The guarded writer derives the indexed answer from
that exact inventory row and verifies that the requested name, selected
resolver, record selector, and record boundary still match the current name
projection; callers cannot supply a different indexed answer or target an
unrelated ledger row. The Rust caller independently derives the same indexed
answer to interpret the writer's action. If those derivations disagree, the
writer result is rejected as an internal database error and the request fails
closed with `500` rather than exposing or accepting the inconsistent decision.
When comparison and live execution use different blocks
on one chain, the ledger retains separate `indexed` and `live` position slots so
a reorg of either dependency clears the active row. These internal role slots
do not change v2 response metadata. A supported wildcard
lookup can execute without an exact inventory row; it then has no comparison
target and performs no ledger write or clear rather than comparing the request
with its wildcard ancestor's inventory. The read captures the completed
project-generation row, the exact `name_current` row when record topology is
involved, and every selected manifest version and contract declaration. The
serving transaction first calls `revalidate_resolution_lookup_state` after live
execution. It locks the authoritative head, the unchanged project generation,
every observed canonical position, the exact name row, and the optional
inventory row; it also verifies the selected manifest rows. A phase restart,
manifest-driven project invalidation, same-height projection publication,
name-topology replacement, or manifest replacement therefore rejects the
result instead of combining generations. A shared manifest-sync advisory lock
is held through the serving commit, so active or admitted shadow declarations
cannot change between validation and commit. The name lock precedes the
inventory lock, matching projection publication order. The guard runs even when
wildcard or CCIP behavior precludes a ledger mutation, and CCIP still guards an
inventory row it read. The writer receives the same captured authority plus the
inventory primary key and `xmin`, then repeats the guard before deriving or
mutating an answer. Both functions are fixed-`search_path`, security-definer
interfaces whose default `PUBLIC` execution privilege is revoked; the API role
receives only explicit `EXECUTE`, not direct write privileges on the guarded
relations or ledger. Before
inserting a disagreement or clearing one after restored agreement, it also
locks every observed canonical-lineage row; a reorged observation rejects the
mutation. Active divergence positions are
automatically cleared by a later reorg. CCIP participation short-circuits
the mutation-specific writer before any ledger write or clear, while the
serving transaction still performs the general head and position guard above.
ENS/60 primary-name verification uses the same schema-v2 lookup engine and
current readable Ethereum position and revalidates that head, lineage, project
generation, and both selected manifest declarations after its live calls, but
it has no indexed record comparison and therefore never writes this ledger.
This narrow write is authorized by
[`simplification-build-plan-20260730.md` § B6](../simplification-build-plan-20260730.md#stage-b--port-the-keep-set).

The remaining execution tables and rules in this section describe the legacy
crate and retained storage only. No v1 API route serves them.

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

Unlike resolution-on-demand, which starts from an indexed name row, the ENS/60
missing-tuple route accepts any syntactically valid Ethereum address. Addresses
that never acquire a projected primary-name claim therefore create a much wider
persistence domain. The worker bounds that storage: on each normal poll it
deletes at most
`BIGNAME_WORKER_PRIMARY_NAME_ROUTE_CACHE_PRUNE_BATCH_SIZE` outcomes whose two
boundary fields use `selected_checkpoint` and whose Ethereum checkpoint is
more than
`BIGNAME_WORKER_PRIMARY_NAME_ROUTE_CACHE_RETENTION_CHECKPOINTS` blocks behind
the stored canonical checkpoint. The defaults retain 50,000 checkpoints and
delete at most 5,000 outcomes per poll. Deleting the outcome also deletes its
otherwise-unreferenced route-local trace and steps. A second independently
bounded batch removes old route-local traces already orphaned by same-identity
outcome replacement. Materialized primary-name outcomes and all other execution
traces remain outside this cleanup. A later request for an evicted address
executes against the then-current stored checkpoint.

Because the missing tuple owns no projected name surface or backing resource,
its `topology_version_boundary` and `record_version_boundary` JSON fields both
use `{boundary_kind: "selected_checkpoint", chain_position}`. That internal
execution-cache variant records the exact stored block number, hash, and
timestamp without inventing a `logical_name_id`, `resource_id`, or normalized
event. Materialized outcomes continue to use the ordinary projected
`VersionBoundary` shape.

Because PostgreSQL cannot row-lock an absent tuple, route-local persistence,
route-local readback, and the projection writer take the same
transaction-scoped PostgreSQL advisory lock derived from the normalized
`(current_database, address, namespace, coin_type)` identity. Including the
database keeps independent bigname databases on one PostgreSQL cluster from
sharing a fence. The projection writer takes the lock before reading the old
row and holds it through cache invalidation and projection commit. If fallback
persistence commits first, the later projection writer invalidates that outcome
before publishing the row; if projection publication commits first, fallback
persistence or readback observes the row and stops. Different tuples use
different locks and do not serialize. Full projection replacement uses an
exclusive database-scoped maintenance advisory lock while tuple operations join
that maintenance boundary in shared mode, avoiding an unbounded set of tuple
locks during a rebuild.

The retained legacy execution code first joins that tuple and replacement advisory
fence, then invokes the fixed-`search_path`, security-definer
`bigname_lock_primary_name_anchor` function. The function row-locks and returns
only the requested projection anchor; when no row exists, the earlier advisory
lock protects that absence. Its default `PUBLIC` execution privilege is
revoked. This preserves the anchor through the execution-artifact commit
without granting the API direct `UPDATE` access to the worker-owned projection
table.

The migration that introduces this protocol also installs projection-table
write triggers for rolling-version compatibility. A writer from the preceding
release is forced onto the same tuple/maintenance keys before its DML becomes
visible, and a changed legacy write repeats verified-primary invalidation after
it acquires the fence. Because a legacy writer obtains its PostgreSQL table lock
before a row trigger can run, a conflicting advisory lock returns SQLSTATE
`40001` instead of waiting with the reversed lock order; the projection loop
retries the rolled-back write. Legacy full replacements are recognized by their
disabled bulk-publication sidecar triggers, take the maintenance fence, and
conservatively invalidate verified-primary outcomes once. Updated writers take
the advisory lock before their table lock and keep the narrower exact-change
invalidation path.

For an identical verified-primary cache identity, a stored `success` outcome
wins over a later `execution_failed` attempt; the failed attempt does not write
an unreferenced trace. On both on-demand execution paths, configured API RPC
response timeouts remain in-band durable failures. Provider connect-phase
timeouts and other RPC transport failures, such as DNS or TLS errors, abort
before trace or outcome persistence so the next request retries. The CCIP-Read
gateway leg follows the same split — configured gateway-client response
timeouts are durable in-band, while gateway connect-phase timeouts and other
gateway transport failures abort before persistence — and is reachable only
from execution that follows CCIP-Read, which today is the ENS/60 primary-name
fallback; record-route verified reads execute block-pinned without a gateway
leg.

Exact block-anchored `raw_call_snapshots` used by verified resolution stay in
the intake-owned `raw_*` family. Execution persistence, including the API
on-demand route exception, may hand off candidate snapshots only through the
raw-fact boundary, only for the exact requested chain position, and only for
support classes that admit them. `execution_traces`, `execution_steps`, and
`execution_cache_outcomes` do not own those rows.

Before a verified-resolution selector persists as a supported reusable outcome, execution reloads from storage the exact manifest versions for the request, the same declared topology snapshot a mixed route would serve, and any resolver-profile admission state required by participating resolver-local fact families. The frozen support class derives from those stored inputs and matches the persisted trace and cache key. If those inputs are absent or do not re-establish one frozen class, the trace remains a durable audit artifact but the selector does not persist as a supported reusable outcome.

## Read-only inspection tooling

`phase-runner inspect` owns three bounded schema-v2 windows and renders JSON
from a read-only repeatable-read transaction. It does not create public API
routes, mutate state, or fetch fresh chain data.

- `block-canonicality --chain <id> --from-block <n> --to-block <n>` lists every stored fork by `(block_number, block_hash)`, its explicit canonicality, optional header-audit presence, and retained raw-fact and normalized-event counts.
- `stored-lineage --chain <id> --from-block <n> --to-block <n>` lists stored lineage and optional `chain_header_audit` fields in stable order. It does not infer missing heights, completeness, or canonicality for absent rows.
- `raw-events --chain <id> --from-block <n> --to-block <n>` lists retained raw logs with their raw transaction, optional receipt, header-presence, lineage canonicality, and matching normalized-event context.

All three include orphaned forks with `canonicality_state='orphaned'`. Retired
drift, payload-cache, execution-trace, and watch-plan views have no schema-v2
phase-runner commands.

## Migration rules

- Schema changes land through checked-in migrations only.
- Tables explicitly documented as append-only prefer additive changes over
  destructive rewrites. `normalized_events` is not one of those tables: it is
  the current interpretation epoch, and a bounded interpret redo intentionally
  deletes and re-derives its selected range.
- Old-schema migrations, including `backfill_*`, normalized replay, coverage,
  reconciliation, and repair tables, remain immutable even after their Rust
  writers are deleted.
- Projection tables may be recreated when the rebuild path already exists.
- Migrations that change a shared interface require the companion doc update first.
- If `CREATE INDEX CONCURRENTLY` leaves an `INVALID` index, the runbook is a later `-- no-transaction` migration that `DROP INDEX CONCURRENTLY IF EXISTS` for the invalid name before recreating or replacing it; do not rebuild by editing an already-applied migration.

## Repository ownership

- Storage owns migrations, surviving read/query primitives, projection and
  execution persistence primitives, and test-only fixture insertion helpers.
- `ingest` owns lineage and selected raw-fact production. Execution persistence
  may hand admitted exact-block `raw_call_snapshots` through that raw-fact
  boundary.
- Schema-v2 `interpret` owns identity, discovery, and normalized-event writes.
  Protocol adapters provide interpretation behavior; they do not write
  projection rows.
- Projection workers own current read models, replay staging, apply cursors,
  invalidation journals, and worker heartbeats.
- Legacy execution workers own traces, steps, and normal cache outcomes. The
  schema-v2 lookup engine owns only guarded divergence-ledger writes; its API
  caller remains otherwise read-only and never writes legacy execution rows.
- The API reads projections and execution output. It has no general raw-fact or
  legacy operational-table fallback; explicit audit endpoints remain the only
  documented exception.
- Worker inspection of canonicality, stored lineage, historical backfill jobs,
  and execution traces is read-only.
- No current Rust owner writes `backfill_*`, `normalized_replay_*`, the Base
  rederive tables, resolver-profile reconciliation state, old coverage/frontier
  state, or old startup adapter checkpoints. Their removal or replacement is
  deferred to the worker/API port and migration-baseline work.

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
[^v2-registry-link-time-expiry]: (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L458 @ ens_v2@48b3e2d) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L464 @ ens_v2@48b3e2d) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L468 @ ens_v2@48b3e2d) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L214 @ ens_v2@48b3e2d) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L228 @ ens_v2@48b3e2d) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L229 @ ens_v2@48b3e2d)

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
