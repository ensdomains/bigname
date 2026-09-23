# ADR 0008: V1 Schema Freeze and Change Process

Status: Accepted
Date: 2026-08-15
Accepted: 2026-09-11

## Summary

- **What is frozen.** The `bigname_phase` schema for bigname's V1 release (its first stable read release, not ENSv1): the `schema-v2/baseline/` tree plus every schema-migration up to the head named under [The frozen artifact](#the-frozen-artifact).
- **For how long.** From acceptance on 2026-09-11 until the V1 release is signed off. 2026-09-11 is the day the last of the carve-out history landed (#885, carve-out 3's canonicality rule). The sign-off is recorded by amending the status line above with its date; until then the freeze applies.
- **What may still change.** The six pre-authorized carve-outs below, all of them decided: three were settled by work that has since merged and are recorded as history, and the rest are decisions this ADR makes. Any other schema change needs an amendment to this ADR before it merges.
- **What the freeze does not hold still.** Published data. Every rotation of the [interpreter content hash](../glossary.md#interpreter-content-hash) re-derives it, so work built on the schema keys on stable identifiers only.
- **How it is enforced.** `schema-v2/apply-check.sh` fails CI when the schema, its schema-migration history or the named head moves without being recorded. [`schema-v2/apply-check.md`](../../schema-v2/apply-check.md) states its rules in full.
- **History.** 38 schema-migrations landed while this ADR was a draft and are part of the frozen schema. 28 more landed after acceptance without a carve-out or an amendment. They are part of the frozen schema too, because that is the schema initialized databases hold, but they are recorded as the first breach of the freeze.

## Context

"V1" in this ADR is bigname's first stable read release, not ENSv1 and not the prospective V2 specification. The [V1 milestone](../glossary.md#v1-milestone) is the work that proves that release as a replacement for the retained legacy read surface, all of it built on top of the schema:

- parity coverage: the slice-1 full-re-walk acceptance comparison (a rebuild from raw chain data at a [re-derivation boundary](../glossary.md#re-derivation-boundary)) and the combined-boundary gate that [`consumer-capabilities.md`](../consumer-capabilities.md) requires;
- regression tests over the served routes;
- monitoring.

It ends when those gates pass and the release is signed off. That work is expensive to redo, so it needs a schema it can rely on. The requirement is not that the schema never changes, but that changes are listed in advance rather than discovered mid-milestone.

Two earlier decisions frame this. [ADR 0006](0006-api-v2-product-surface.md) fixed the v2 product surface and rejected GraphQL as the product contract. `consumer-capabilities.md` defines the ENSv1→ENSv2 delivery slices. When this ADR was drafted only slice 1 had landed; slices 2A–2E, 3A, 3B and the final activation have all merged since. Their re-derivation boundaries are behind us in code, but not necessarily on every deployment: a deployment takes a boundary as a full walk when it first runs the rotated hash. The keying rule below is written for artifacts that must survive those walks either way.

Three properties of the system decide what a freeze can promise:

- **Schema change and re-derivation are independent.** DDL does not rotate the interpreter content hash. These do:
  - a change to the covered production sources under `crates/project/src`, `crates/adapters/src`, `crates/interpret/src/write` and `crates/manifests/src`. An inline `#[cfg(test)] mod` inside a covered file is hashed with the file, so a test-only edit there rotates the hash; a `#[cfg(test)]`-gated module in a file of its own does not (`crates/content-hash/src/compute.rs`, `source_exclusion`, pinned by `crates/content-hash/src/tests.rs`);
  - a change to a manifest's `[[abi.events]]` declarations, the named semantic source files, or the pinned lockfile families.

  A manifest's `read_features` rotates a different value, the fingerprint recorded by the [manifest-authority marker](../glossary.md#manifest-authority-marker), with the interpreter hash unchanged. None of these is a narrower walk. A hash rotation forces the full-history Interpret and Project walk. A fingerprint change on an initialized chain blocks derived work until the same full-range, token-attested Interpret redo and stamped Project redo complete, after a stamped Ingest redo if the [watch plan](../glossary.md#watch-plan--watched-tuple) widened.

  A normalizer bump is two of these at once. The `ENS_NORMALIZER_VERSION` constant lives in `crates/domain/src/normalization.rs`, which the hash covers (`crates/content-hash/src/compute.rs`), so changing it is a hash rotation with its full-history walk. It also needs the `recompute-flags` pass over each chain's full retained range and a full-range Project redo to recompute the label verdicts. A manifest's `normalizer_version` field is not hashed, but it cannot move on its own. Manifest loading rejects a value that differs from the compiled constant (`crates/manifests/src/lib/repository.rs`, `validate_manifest_metadata`), so editing the field alone makes the manifest invalid, and a real bump changes both together ([`deployment.md`](../deployment.md) § Phase-runner configuration, the manifest-authority marker and normalizer-version paragraphs).
- **The content hash does not cover the schema.** It watches Rust sources, manifests and the lockfile, not `schema-v2/` or `migrations/`, so it cannot anchor the freeze.
- **Slices 2 and 3 each touch Project builders.** Both rotate the hash and invalidate artifacts keyed to a [projection generation](../glossary.md#projection-generation), whether or not they change any DDL.

A freeze that promised "no churn" without accounting for this would be false on the day it was signed.

## Decision

### The frozen artifact

The V1 schema contract is the pair:

- the `schema-v2/baseline/` tree, and
- the schema-migration head, `migrations/20260925120000_normalized_events_resolver_history_idx.sql`.

<!-- apply-check.sh reads the first schema-migration path in this file as the head. Keep the head above every other such path. -->

Two checked-in files record it. `schema-v2/frozen-schema.txt` is the catalog of everything the pair builds, and `schema-v2/migration-inventory.txt` lists every schema-migration file in order with the SHA-384 of its bytes, the checksum sqlx records when it applies one. `apply-check.sh` checks on every run that the head named here and the one [`storage.md`](../storage.md) names in its opening paragraph are the newest file in `migrations/`, so a merge that brings a later schema-migration fails until the head moves.

**A change to the schema that does not also change `apply-check.sh` and regenerate `schema-v2/frozen-schema.txt` is out of contract.** That coupling is what makes the freeze observable rather than aspirational.

### Changing the frozen schema

A schema change during the milestone is one of the pre-authorized carve-outs below, or an amendment to this ADR agreed before it merges. The change that lands it also:

- adds its schema-migration to the inventory after the current head, and advances the head named here and in `storage.md`;
- makes the same change in the baseline, so a fresh database and an upgraded one stay identical;
- regenerates `frozen-schema.txt` (`SCHEMA_V2_APPLY_CHECK_WRITE_FINGERPRINT=1`);
- updates `apply-check.sh`: every schema-migration that names `bigname_phase` joins the check's list of files it applies to an empty database and gets an application to an initialized schema (the baseline-first list or a predecessor-shape test), a new table joins its expected-table lists, and a table or column name the forbidden-name rule matches needs a maintainer allowlist entry. The check is extended further where the change needs something it refuses today, such as another object kind or a session setting.

A change that leaves any of these behind is out of contract. Whether a change really is an authorized carve-out, and not something that needs an amendment, is decided in review: CI catches an unrecorded schema change, not an unjustified one.

### What CI enforces

`schema-v2/apply-check.sh` runs in its own CI job. It fails when:

- **The schema moves unrecorded.** The schema the baseline builds on its own, and the one the schema-migrations build, must each match `frozen-schema.txt` line for line. The check also takes the previous revision's baseline and applies only the schema-migrations added since, as `sqlx migrate run` would on a database at that revision, and the result must match too. So a schema-migration without its baseline edit fails, and so does a baseline edit without its schema-migration. The same catalog must also come out of the check's scratch schema at the end of the run. By then its tests have filled that schema with rows and carried it through every schema-migration again, so a schema-migration whose DDL runs only on a populated table fails too.
- **The history is rewritten.** `migrations/` must equal the inventory byte for byte. Compared with the previous inventory, no file may be edited or deleted, and no new file may sort at or below the previous head. sqlx refuses to run against a database that recorded different bytes for a file, and applies a file that sorts below the head on every initialized database while the freeze would record nothing. After each replay, sqlx's own `_sqlx_migrations` ledger must equal that history exactly, and a schema-migration may not read the ledger's timing columns, which the check cannot reproduce.
- **A new kind of object appears.** The phase schema holds only the object kinds the baseline uses: tables, views, sequences, indexes, constraints, triggers, functions and procedures, enum and domain types, and comments on those. Any other kind is refused until a carve-out extends the rule and the catalog.
- **A table is missing, unexpected or badly named.** The phase schema's tables must be exactly the check's expected ones, the invariants it names must exist, and every table and column needs a comment. A table or column name that matches the forbidden-name patterns is refused unless the maintainer allowlist names it, and a table name ending in `_staging` or `_publication` is refused outright.
- **A schema-migration hides the phase schema's name.** Every schema-migration since the legacy schema's drop spells the schema exactly `bigname_phase`, and one that names no phase object may only drop indexes, so a name relative to the search path cannot slip past the check.
- **A schema-migration changes facts.** On a populated database a schema-migration changes the shape, not the facts. It may backfill these tables in place: the phase runner's coordination state (`chain_phase_state`, `service_heartbeats`), Interpret's redo coordination (`discovery_watch_admissions`, `project_redo_*`), manifest synchronization's `manifest_*` rows, the `resolution_divergences` ledger, and Project's rebuildable projections (the `*_current` families, `permissions_current_resource_summary` and `child_registration_events`). Every other table must keep its exact rows. That covers what Ingest recorded and Interpret derived (chain data, raw facts, contract instances, identity rows, discovery edges, label preimages, normalized events and Interpret's diagnostics), which a redo re-derives from or builds on rather than repairs. It also covers the operator's rainbow candidates and the audit of each [projection generation failure](../glossary.md#projection-generation-failure). No table may gain or lose rows, and no existing sequence may move, because a moved sequence would hand an ID out again. A table the list does not name is compared, so a new one is covered until review names it.
- **Columns move.** A schema-migration may add a column, which lands last, but may not reorder the columns a table already has, and a table it creates must match the baseline's layout.
- **A file would behave differently under sqlx than in the check.** A baseline file or schema-migration may not leave session state behind for the next file, use psql-only syntax, change the server outside its databases, run publication, subscription or event-trigger DDL, set a sequence's position or change how it counts, or write a system catalog directly. A schema-migration may not branch on who runs it or where, since the check cannot reproduce either, and may not catch every error. Since the legacy schema's drop, a schema-migration may not use `CASCADE` either, other than a foreign key's `ON DELETE` or `ON UPDATE CASCADE`: in production it also drops objects that depend on its target and that only production holds.
- **Something outside the phase schema changes.** The check normally renames `bigname_phase` to a scratch name, so it also replays the schema-migrations once more under the real name, as the configured database user, in a database of its own ([details](../../schema-v2/apply-check.md#replays-under-the-real-schema-name)). Those replays must leave every other object in that database unchanged, and no replay may change roles, databases, passwords or other server-wide state.

The check proves most of these rules on every run by planting examples of what it must refuse. [`schema-v2/apply-check.md`](../../schema-v2/apply-check.md) states each rule precisely, with the reason for it.

**Limits.** The check is built to catch a schema-migration that would behave differently under sqlx on a production database through ordinary SQL, including dynamic SQL whose effect it can observe at run time. It runs as a throwaway role and as the database user it is configured with, so a branch keyed to an identity neither has, such as a production-only role, is beyond it, as it is beyond any check that does not run as that role. So is a change to an object that only the production database holds outside the phase schema, such as an operator's own table, since no replay holds it. The text rules refuse what no schema-migration needs, such as publication, subscription and event-trigger DDL, the statements that set a sequence's position or change how it counts, and `CASCADE`; review is the control for the rest. SQL written to hide from a text rule what no run-time read observes, such as a keyword or role name assembled from pieces, is outside what a conformance check can close; review is the control for it. [`apply-check.md`](../../schema-v2/apply-check.md#limits) lists what the run-time comparisons do catch.

### How the frozen schema got here

The draft named `20260811120200_ens_v2_migration_slice_1_constraints.sql`, the head when it was written. Sixty-seven schema-migrations follow it up to the head above: 38 landed before acceptance, 28 after it, and the last is carve-out 6, which lands with this ADR.

**38 before acceptance.** These landed while this ADR was a draft, under the review-only process described under [Alternatives considered](#alternatives-considered). They predate the freeze, so they are part of it rather than carve-outs.

- Four are the ENSv1→ENSv2 slice schema this ADR anticipated: `20260814130000_surface_binding_authority_arm.sql` (slice 2A, #468), `20260814131000_project_generation_failure_audit.sql` (slice 2E, #497; carve-out 1 below), and `20260814132000_project_generation_failure_child_authority.sql` with `20260904120000_project_redo_child_registration_history.sql` (slice 3B's children publication invariant and its parent-path filter, #499 and #821).
- The other 34 are independent changes that would each have needed a carve-out or an amendment had the freeze been in force:
  - phase-runner coordination state: heartbeat liveness, unconfigured-phase settlement, Ingest redo source-boundary and manifest-authority markers, and the redo attempt generation (#427, #556);
  - Project incremental-scope and reverse-[hydration](../glossary.md#hydration) state (#415);
  - the raw-block preimage derivation swap (#519);
  - the Interpret decode-skip audit and the manifest applied-change counter (#583, #579);
  - `normalized_events` scope indexes and the legacy index drops (#612, #653, #762, #636), and the `name_current` [serving-resource](../glossary.md#serving-resource) column (#636);
  - the retirement of direct [resolution divergences](../glossary.md#resolution-divergence-ledger) for null-resolver names (#739);
  - the [discovery-watch admissions snapshot](../glossary.md#discovery-watch-admission-snapshot) (#747);
  - Project redo [expiry-root](../glossary.md#expiry-root) and expiry-resource seeds (#762);
  - registry operator [account permissions](../glossary.md#account-permission-state) (#815);
  - the last before acceptance, `20260906120000_exact_zero_addr60_default_derivation.sql` (#869, 2026-09-07), which replaces the `write_resolution_divergence` function so that an exact zero `addr:60` stays absent when a default derivation exists, a serving-semantics change.

**28 after acceptance.** These landed after acceptance and before this ADR merged, none as a carve-out or with an amendment. Under the 2026-09-11 effective date they are the first breach of the freeze, recorded here rather than reclassified as history. Several ship a concurrent prebuild installer under `ops/`. Several also check their indexes: `CREATE INDEX IF NOT EXISTS` matches on the name alone, so the run fails when an index exists under its name without being the reviewed, valid index. #907 and #902 make that check in a separate file that changes no object; the others make it in the same file.

- **#893 (2026-09-16), twelve files:**
  - `20260909120000`–`20260909120200` (`resolver_record_id_events`) widen the `normalized_events` event-kind CHECK in three steps, a constraint replacement on a populated table;
  - `20260911120000` and `20260911120200` add `normalized_events` and `name_current` indexes;
  - `20260911120100` creates the `address_records_current` projection table, `20260914120100` comments it, and `20260915120000` drops three of its NOT NULL constraints;
  - `20260913120000` and `20260914120000` replace `write_resolution_divergence` and add `revalidate_resolution_lookup_state`;
  - `20260913130000` and `20260913130100` replace the CHECKs on `permissions_current_resource_summary` and `account_permission_state_current`.
- **#897 (2026-09-17):** `20260916120000` adds the `surface_bindings (chain_id, logical_name_id)` index that Interpret redo preparation reads without a canonicality predicate.
- **#899 (2026-09-17):** `20260917120000` adds the partial `discovery_edges` observation-history index that recording a contract seen before reads, with its installer under `ops/discovery-history-index/`.
- **#907 (2026-09-17):** `20260917130000` adds the unfiltered `discovery_edges` reopen index, with its installer under `ops/discovery-reopen-index/`, and `20260917160000` checks both discovery indexes.
- **#902 (2026-09-18):** `20260917131000` adds six `normalized_events` indexes for Project's scoped history reads, and `20260917161000` checks them.
- **#905 (2026-09-18):** `20260917140000` replaces the `discovery_edges` self-edge CHECK, a constraint replacement on a populated table, and `20260917141000` renames it to the reviewed name where an earlier build left another.
- **#912 (2026-09-18):** `20260917150000` adds two `normalized_events` look-ahead indexes for ENSv1 interpretation.
- **#934 (2026-09-23):** `20260923120000` adds and checks three partial expression `normalized_events` indexes for the address history read, with its installer under `ops/address-history-indexes/`.
- **#936 (2026-09-23):** `20260923130000` adds and checks a `normalized_events (chain_id, block_number DESC NULLS LAST)` index for history and event pages read in chain order, with its installer under `ops/events-order-index/`.
- **#939 (2026-09-23):** `20260923150000` creates the Project-owned `child_registration_events` projection table with its CHECKs and two indexes, filled only by the full Project rebuild that #939's interpreter-hash rotation requires.
- **#940 (2026-09-23):** `20260922010000`, `20260922010100` and `20260923140000` add and check `normalized_events` and `name_surfaces` indexes for Project's scoped node and label reads, the last with the `label_hashes` function its GIN index keys on, with an installer under `ops/project-progressive/`.
- **#946 (2026-09-24):** `20260924120000` adds and checks a partial expression `normalized_events` index on the node each ENSv1 `ResolverChanged` event addresses, for Project's [ENSv1 mirror resolver](../glossary.md#ensv1-mirror-resolver-ensv1_mirror_resolver) lookups and the history reader, with its installer under `ops/mirror-pointer-index/`.

#902 also edited `20260917160000` in place after #907 had landed it, adding a comment and the `quote_all_identifiers` guard. The inventory now forbids that: sqlx records each file's checksum and refuses to run against a database that applied the earlier bytes, so a database that took #907's version before #902 merged must have that row's checksum corrected by hand before its next `sqlx migrate run`.

### What the freeze promises

- No schema change to the frozen artifact during the V1 milestone, except the pre-authorized carve-outs below.
- Each carve-out made under this freeze requires no re-derivation, and each is additive except the drop carve-out 6 authorizes: two indexes nothing reads, an access path and no contract. Carve-out 1's constraint replacement is not additive either, but it predates the freeze and is recorded, not authorized; a change like it now is an amendment, not a carve-out.
- Any change beyond the carve-outs requires an amendment to this ADR before it merges.

### What the freeze explicitly does not promise

Published data can change during the milestone even though the schema holds still. Every interpreter-hash rotation (the slice-2 and slice-3 re-derivation boundaries, the two later rotations under [Derivation-side changes](#derivation-side-changes-that-are-not-schema-changes), and any that follows) forces a full `interpret` and `project` walk on each deployment that first runs it, and the walk re-derives the published data.

This is the load-bearing clause: a freeze cannot protect work keyed to values the system already documents as unstable across a boundary. Dependent work must therefore key on stable identifiers only.

**Safe to key on:**

- `logical_name_id`, which is `<namespace>:<namehash>` (`architecture.md` § Identity). A bare namehash is not enough: it does not encode the namespace, and the supported `ens` and `basenames` namespaces can carry the same node.
- [`resource_id`](../glossary.md#resource), [`token_lineage_id`](../glossary.md#token-lineage) and [`contract_instance_id`](../glossary.md#contract-instance).
- Across the re-derivation boundaries this freeze permits, the [raw fact](../glossary.md#raw-fact) position alone (chain, block hash, transaction hash, log index), keyed to the set of events emitted for it. An expectation about one particular event is scoped to one interpreter hash, and is re-derived rather than carried across a rotation.

**Safe only within one database:** `event_identity`, under a fixed manifest set and interpreter content hash, which is the contract `architecture.md` gives it.

- It incorporates the [derivation kind](../glossary.md#derivation-kind), identity suffix and emission ordinal (`crates/adapters/src/schema_v2/normalized.rs`, `raw_log_event_identity`), so a covered adapter change can alter it for a raw log that did not change.
- It also embeds the numeric `source_manifest_id`, an identity-column value (`schema-v2/baseline/04_manifests.sql`) that is not a cross-database contract. Installing the same manifests into a fresh database may assign the same numbers, since the paths are collected and upserted in filename order, or different ones, and nothing promises either (`consumer-capabilities.md` says as much of an empty-schema replacement).
- An artifact that must survive a rebuild carries the retained manifest-ID mapping with it, or does not key on `event_identity` at all.

**Record only beside its identity:** a derived normalized name, which is not an identity at all. `architecture.md` and [ADR 0002](0002-surface-resource-identity.md) make normalization results read-time attributes, and a normalizer-version walk, which this freeze permits, can change, remove or collide them. An artifact may record a normalized name only beside the `logical_name_id` it was derived for and the normalizer version it was derived under.

**Not safe to key on:** `normalized_event_id` numeric values, cursor bytes, a specific interpreter content hash, projection generation numbers, or row counts for a given generation.

### Pre-authorized carve-outs

1. **`project_generation_failures`**, the append-only audit for a projection generation failure, a projection-blocking invariant failure. When this ADR was drafted, [`storage.md`](../storage.md) and [`architecture.md`](../architecture.md) already described the table as part of the ownership map, but it did not exist. Proposed as additive, with no re-derivation.

   **Decided: landed as slice 2E's own schema-migration (#497, 2026-08-20), after slices 2A–2C.** `migrations/20260814131000_project_generation_failure_audit.sql` creates the table, which the baseline now carries as `schema-v2/baseline/12_project_generation_failures.sql`, and `20260814132000_project_generation_failure_child_authority.sql` extends it. That extension was not additive: it drops and recreates the populated table's `failure_kind` CHECK to admit `dual_current_child_authority`, the same constraint replacement on a populated table that carve-out 2 classifies as beyond a carve-out. It is recorded as a historical non-additive exception, made while this ADR was a draft, and it required no re-derivation. The three prerequisites beyond the table itself are in place: it is in both expected-table lists in `apply-check.sh`, and in the maintainer allowlist that exempts it from the forbidden-name rule on `generation`.

2. **`migration_candidate_identity_effects.correlation_kind`**, pinned by CHECK to a single value. If slice 3's shape for [ENSv1→ENSv2 migration](../glossary.md#ensv1ensv2-migration) of child names had needed another value, widening it would have been a constraint replacement on a populated table, not an additive change.

   **Decided: no widening was needed, and none is authorized.** Slice 3A reuses `authority_transition`, so the CHECK on `migration_candidate_identity_effects` still pins that single value and `migration_discovery_associations` still pins `migration_registry_creation` (`schema-v2/baseline/05_normalized_events.sql`). The candidate-side `migration_candidate_discovery_effects` accepts any nonblank `correlation_kind` and is not the constraint this decision guards. The interpreter writes exactly those two kinds. A future shape that needs a third is a constraint replacement on a populated table, and therefore a new ADR rather than a carve-out under this one.

3. **[Canonicality](../glossary.md#canonicality) of the ENSv1→ENSv2 migration associations.** The four ENSv1→ENSv2 correlation tables keep rows whose anchor block is orphaned, nothing maintains their `canonicality_state`, and slice 2 adds readers over these tables.

   **Decided: document the reader rule; do not change reorg-time writes.** Merged in #885. Any reader that treats one of these rows as current must anchor the row's own `(chain_id, block_number, block_hash)` on `chain_lineage` with a [readable](../glossary.md#readable--read-safe)-state predicate. The draft proposed an `event_identity` join instead, but only `migration_event_associations` has that column, so three of the four tables could not satisfy it. `storage.md` carries the rule and the reorg test that pins the runner-level outcome.

   `storage.md` also records a consequence rather than hiding it: on today's two publishing readers the anchor is never the reason a row is withheld, because both also require the [`registry_announcement` edge](../glossary.md#registry-announcement-edge-registry_announcement) joined on the association's own block, and the same Interpret redo orphans both. The rule governs new readers, where it is the only guard.

4. **[Label-preimage](../glossary.md#preimage-observation--label-preimage) indexes.** **Decided: out of the freeze until measured. No index is pre-authorized and none is ruled out.**

   The two named paths differ. The children join reaches rows through the primary key: `preimage.labelhash = lower(...)` in `crates/project/src/builders/children.rs`, which lowers the other side so the key stays usable. The recompute after a normalizer bump does not. Its `load_labels` query (`crates/interpret/src/recompute.rs`) selects every `label_preimages` row that matches any of four `OR` branches (three correlated `EXISTS` lookups scoped to the chain and block range, and `source_kind = 'ens_rainbow_import'` outright), orders them by `labelhash` and locks them `FOR UPDATE`.

   It has no `normalizer_version` predicate, so the baseline's `label_preimages_normalization_idx (normalizer_version, normalized_under_version, labelhash)` cannot serve it. The primary key (`labelhash`, `schema-v2/baseline/07_labels.sql`) matches the `ORDER BY`, so the planner's real choices are an ordered primary-key scan that filters on the four branches, or a sequential scan plus a sort, and neither narrows the rows. After a bulk rainbow import, the `source_kind` branch alone returns the whole import on every recompute range.

   What that costs is a question for `EXPLAIN` against an imported table, and the plan must show which of the two choices PostgreSQL makes before an index is judged against it. If the import dominates, the cost is row volume and no index changes it. If it does not, an index is additive and can be authorized when the measurement says so. #364 tracks the `source_kind` filter in `crates/project/src/scope/labels.rs`, a primary-key join over a bounded array that is not the concern here. A bulk import during the milestone does not by itself require a schema change.

5. **Serving indexes the projections never had.** No index in `bigname_phase` supports a name-text filter or a name sort, which is all this ADR asserts about `/v1/search` and the GraphQL `name_contains` and name-ordered paths. What the planner does instead is unmeasured: a namespace-scoped request can walk `name_current_lookup_idx (namespace, namehash, logical_name_id)` before filtering `raw_name`, and a sort spills only past `work_mem`. Whether these paths scan and spill is #404's `EXPLAIN` to record, not a claim here.

   The predecessor indexes are not where the draft looked. Every index on the retired `public.address_names_current` led with `address`, including the one prefix index that named `normalized_name` (`address_names_current_address_normalized_name_prefix_idx`, `migrations/20260627120000_address_names_q_sort_read_indexes.sql`), so that table served name text only within one address. The global predecessors were on `public.name_current`, which the search and name-ordered paths read: `name_current_app_namespace_name_idx (namespace, normalized_name)` and `name_current_app_global_name_idx (normalized_name, namespace)` (`migrations/20260502170000_app_facing_rest_indexes.sql`). Those two shapes are #404's starting point, benchmarked against the current `raw_name` queries rather than copied.

   Separately, `normalized_events` has no index leading with `namespace`, but that does not mean the default `/v1/events` page has no candidate index. A request with no `event_type` still injects the product history event kinds (`apps/api/src/v2/events.rs`, `product_history_event_kinds()`), and `normalized_events_projection_idx (event_kind, canonicality_state, chain_id, block_number, normalized_event_id)` leads with `event_kind`, so it is a candidate access path for the real default query. Whether it serves the page or degrades into a scan over the kinds is a benchmark question.

   **Decided: the name-text index is in; the `/v1/events` index (#402) needs the measurement first.** The name-text gap is confirmed: no index in `schema-v2/baseline/06_projections.sql` supports a name-text filter or name sort (#404), and the two `name_current` predecessors above are the shapes to measure first. For #402, the carve-out authorizes an index only after an `EXPLAIN` of the default query against the existing `normalized_events_projection_idx`; a `namespace`-leading index that duplicates or misshapes that path is not authorized on the strength of this ADR. Monitoring and parity work exercise exactly these paths, so the latency should be found by a benchmark rather than by the milestone's own measurements. Both are additive and require no re-derivation.

6. **The four resolver-history indexes no schema-migration carried.** #415 (2026-08-14) added `normalized_events_pointer_after_resolver_history_idx`, `…_pointer_before_…`, `…_permission_after_…` and `…_permission_before_…` to `schema-v2/baseline/05_normalized_events.sql` without a schema-migration. Their predicates name `consumer_visibility`, which slice 1's `20260811120000` adds, so a database that took slice 1 in place and was never rebuilt from the baseline has the column but not the indexes. The conformance check found this the first time it compared the frozen catalog with the schema its own tests fill, rewind to older shapes and upgrade again. A fresh schema could not show it, because there a baseline object needs no schema-migration to be present.

   Review then asked who reads them:

   - The `pointer_*` pair serves the resolver-anchored event feed (`GET /v1/events?resolver=<chain>:<address>`, `crates/storage/src/history/paging.rs`), which selects activated canonical `ResolverChanged` events by chain and the lower-cased resolver the pointer moved to or from. `EXPLAIN` over a 400,000-row `normalized_events` answers it with a `BitmapOr` over both indexes, and without them with a scan of every canonical `ResolverChanged` on the chain.
   - The `permission_*` pair has no reader. #415 built them for Project's resolver scoping (`crates/project/src/scope`, `crates/project/src/stage.rs`), but that code derives the resolver address through a `CASE` inside a lateral `VALUES` list. No expression index serves that shape: `EXPLAIN` of it reads the primary key and filters. No other read filters `PermissionChanged` by scope resolver.

   **Decided: keep the `pointer_*` pair and drop the `permission_*` pair, in this ADR's change.** `migrations/20260925120000_normalized_events_resolver_history_idx.sql` builds each kept index where it is missing, and where one is present refuses an invalid index, another definition or a table under the name rather than adopting it. It drops each retired index that exists and refuses a retired name held by anything that is not an index. The two retired definitions leave the baseline in the same change.

   That file's build is an ordinary write-blocking `CREATE INDEX` and its drop takes the table's exclusive lock. On a large database the operator therefore first prebuilds and drops concurrently with `ops/resolver-history-indexes/install.sql`, as [`deployment.md`](../deployment.md) and the production runbook list. That script checks each retired name again right before dropping it, because a name that was free can be taken during the hours the builds may run.

   Access paths only; no re-derivation. The drop is the one non-additive step under this freeze: an index nothing reads costs storage and write maintenance and carries no contract. Carve-out 5's rule applies to bringing one back: first an `EXPLAIN` of the read it serves. Whether other objects #415 and its neighbours added to the baseline without a schema-migration are missing on some initialized database is a question for the deployment that would hold it, because the check's populated comparison sees only what its tests rewind. The fresh baseline is the artifact, and a database that differs from it is rebuilt, or carried to it by a schema-migration under this process.

### Derivation-side changes that are not schema changes

These rotate the interpreter content hash rather than touching DDL, so the freeze does not cover them, but they had to be sequenced against the same boundaries rather than landing ad hoc. Both were open when this ADR was drafted, which proposed slice 2's boundary to carry them. Neither made it: both landed after slice 3, each as its own content-hash rotation (#745 on 2026-08-31 and #813 on 2026-09-02). They are recorded here as outcomes, not as pending work:

- `ROLE_WAS_RESERVED` (bit 32) is in the ENSv2 registry role vocabulary. It was the one registry role constant upstream declares (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L48 @ ens_v2@a971bd64) with no entry in the adapter table; `REGISTRY_ROLE_BITS` in `crates/adapters/src/schema_v2/protocol/permissions.rs` now carries `(32, "was_reserved")`.
- The empty-value guard for ENSv1 and Basenames `contenthash` and non-ETH `addr` records, which published a cleared record as `status: "success"`, is corrected: `crates/project/src/builders/record_inventory.rs` classifies those as `not_found`, pinned by the `v1-record-clears.json` interpreter fixture. The classification follows the contracts: both resolver families store the supplied byte payload verbatim and their reads return the stored bytes, so an empty payload is what a clear leaves behind and what a read then returns (upstream: .refs/ens_v1/contracts/resolvers/profiles/ContentHashResolver.sol:L14-L28 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L47-L85 @ ens_v1@91c966f) (upstream: .refs/basenames/src/L2/resolver/ContentHashResolver.sol:L32-L43 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L57-L99 @ basenames@1809bbc); [`api-v1.md`](../api-v1.md#status-vocabulary) § Status Vocabulary carries the read-side rule.

Neither needs scheduling again.

### Explicitly out of scope

- **V2 schema sign-off.** The V2 spec is still being shaped. This ADR records V1 only; V2 direction is acknowledged but not frozen.
- **Chunked projection publication.** **Decided: out of the milestone.** The work has not started: `crates/project/src/engine.rs` still publishes in one `REPEATABLE READ` transaction. Landing it would void the freeze outright, because durable per-unit progress needs either reopening the `chain_phase_state` coherence CHECK or adding a table the schema check forbids by name. Keeping a freeze at all means deferring it past the milestone. It stays tracked as its own readiness item; a decision to pull it forward supersedes this ADR rather than amending it.

## Upstream anchors

This ADR governs bigname's own schema; its upstream dependencies are the two derivation-side outcomes above:

- (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L47-L48 @ ens_v2@a971bd64) anchors `ROLE_WAS_RESERVED` (bit 32) as an ENSv2 registry role, mirrored by `REGISTRY_ROLE_BITS` in `crates/adapters/src/schema_v2/protocol/permissions.rs`. The bit itself is mirrored, not diverged. What bigname does with it, exposing the token-only marker as `was_reserved` in `effective_powers` although it grants no authorization, is the divergence `upstream.md` § Known divergences already records as "ENSv2 reservation-history marker appears in the permission vocabulary"; this ADR adds no second entry for it.
- (upstream: .refs/ens_v1/contracts/resolvers/profiles/ContentHashResolver.sol:L14-L28 @ ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L47-L85 @ ens_v1@91c966f) (upstream: .refs/basenames/src/L2/resolver/ContentHashResolver.sol:L32-L43 @ basenames@1809bbc) (upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L57-L99 @ basenames@1809bbc) anchor the verbatim byte storage and reads behind the record-clear `not_found` classification above. Mirrored, not diverged.

## Consequences

**Positive.** Parity, regression and monitoring work gets a named contract with a conformance check behind it. The re-derivation boundaries are documented events with an invalidation rule instead of surprises: merged as code, and taken as a full walk by each deployment that adopts them. The split between stable and unstable identifiers gives downstream authors a rule they can follow without understanding the whole replay model.

**Negative.** Tooling enforces only the mechanics of the freeze. `apply-check.sh` fails CI when the schema the baseline and inventoried schema-migrations build no longer matches the frozen catalog, when `migrations/` differs from the inventory, and when a schema-migration lands without advancing the head this ADR and `storage.md` name. Whether a change is an authorized carve-out or a substantive amendment is still decided by review: nothing fails CI when the catalog and head are regenerated but the ADR's decision is not recorded. The freeze also front-loads decisions that would otherwise be made inside the slices, which costs time now.

**Newly possible failure mode.** A carve-out landing without its `apply-check.sh` allowlist entry fails CI with a forbidden-table error that does not itself name this ADR. The allowlist's comment does: it records each entry as a carve-out argued here.

## Rollout

Doc-first in intent; in practice three carve-outs and 34 unrelated schema-migrations landed while this ADR was still a draft. Carve-out 1 shipped as slice 2E's reviewed schema-migration on 2026-08-20, carve-outs 2 and 3 were settled by slice 3 and #885, and this ADR records them rather than authorizing them in advance. That is a process miss worth naming: the schema was observable the whole time through `apply-check.sh`, but the written contract trailed it by three weeks.

The miss then repeated. Between acceptance and this ADR's merge, the 28 schema-migrations listed under [How the frozen schema got here](#how-the-frozen-schema-got-here) landed under the review-only process, with no carve-out and no amendment. #893's twelve brought a new projection table, constraint replacements on populated tables and function replacements. The other sixteen were eleven index files (one also creating the function its index keys on), two check-only files, a constraint replacement with its rename, and another projection table. They are part of the frozen artifact and the head advanced to the last of them, because the artifact has to be the tree that exists, but they are not retroactively authorized.

From this ADR's merge the process is the one it describes: a schema change is a listed carve-out or an amendment, and `apply-check.sh` is where an omission fails. Carve-out 6 lands in this change, listed here before its schema-migration is inventoried. Carve-out 5 is the one still ahead, and it follows the intended order: this ADR first, then the schema-migration that references it.

Ownership follows [`workstreams.md`](../internal/workstreams.md): Storage and Domain own the schema-migrations and `apply-check.sh`; Projections and API own the downstream keying rule; Conformance and Fixtures own re-keying any existing artifact that violates the stable-identifier list.

## Alternatives considered

**Freeze on the interpreter content hash.** The obvious candidate, and wrong: the hash does not cover `schema-v2/` or `migrations/`, so it would freeze derivation semantics while leaving the schema free, the inverse of what is needed. It also rotates at both slice boundaries, which would make the freeze appear violated by planned work.

**Freeze the schema-migration head alone.** Simpler, but a schema-migration head does not describe the schema's shape, so a baseline edit could pass unnoticed. Pairing it with `apply-check.sh` and the frozen catalog closes that.

**No freeze; rely on review.** This is the status quo, and it is what produced a table documented in the authoritative ownership map that did not yet exist. Review catches changes; it does not catch omissions.

**Sign off V1 and V2 together.** Not possible while the V2 spec is being shaped. The combined statement would be unfalsifiable.

## References

- [ADR 0006](0006-api-v2-product-surface.md): v2 product surface
- [`consumer-capabilities.md`](../consumer-capabilities.md): slice definitions
- [`storage.md`](../storage.md): table families and replay classification
- [`schema-v2/apply-check.md`](../../schema-v2/apply-check.md): every rule the conformance check enforces
