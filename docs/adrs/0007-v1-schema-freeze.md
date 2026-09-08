# ADR 0007: V1 Schema Freeze and Change Process

Status: Accepted
Date: 2026-08-15
Accepted: 2026-09-06

> Every carve-out below is decided. Three were settled by work that has since
> merged and are recorded here as history; the rest are decisions this ADR makes.

## Context

"V1" in this ADR is bigname's first stable read release — not ENSv1, and not
the prospective V2 specification. The V1 milestone is the work that proves
that release as a replacement for the retained legacy read surface: parity
coverage (the slice-1 full-re-walk acceptance comparison and the
combined-boundary gate that
[`consumer-capabilities.md`](../consumer-capabilities.md) requires),
regression tests over the served routes, and monitoring, all built
on top of the schema. It ends when those gates pass and the release is signed
off; the sign-off is recorded by amending the status line of this ADR with its
date, and until that amendment the freeze applies. That work is expensive to
redo, so it needs a schema it can rely on. The requirement is not that the
schema never changes — it is that changes are enumerated in advance rather
than discovered mid-milestone.

Two prior decisions frame this. [ADR 0006](0006-api-v2-product-surface.md) fixed
the v2 product surface and rejected GraphQL as the product contract.
[`consumer-capabilities.md`](../consumer-capabilities.md) defines the ENSv1→ENSv2
delivery slices. When this ADR was drafted only slice 1 had landed; slices 2A–2E,
3A, 3B and the final activation have all merged since, so the re-derivation
boundaries this ADR anticipated are behind us rather than ahead.

Three properties of the current system determine what a freeze can and cannot
promise:

- **Schema change and re-derivation are independent axes.** DDL does not rotate
  the [interpreter content hash](../glossary.md#interpreter-content-hash). A
  change to the covered production sources under `crates/project/src`,
  `crates/adapters/src`, `crates/interpret/src/write`, and
  `crates/manifests/src`, to a manifest's `[[abi.events]]` declarations, to the
  named semantic source files, or to the pinned lockfile families does.
  `#[cfg(test)]` modules under those roots and a manifest's
  `normalizer_version` do not (`crates/content-hash/src/tests.rs` pins both),
  and a manifest's `read_features` rotates the separate fingerprint recorded
  by the [manifest-authority marker](../glossary.md#manifest-authority-marker)
  with a byte-identical interpreter hash (`deployment.md` § manifest-authority
  marker). Only an interpreter-hash rotation forces a
  full-history re-walk; the normalizer and manifest-authority paths have their
  own, narrower redo.
- **The content hash does not cover the schema.** It watches Rust sources,
  manifests, and the lockfile — not `schema-v2/` and not `migrations/`. It cannot
  serve as the freeze anchor.
- **Slices 2 and 3 each touch Project builders.** Both therefore rotate the hash
  and invalidate generation-keyed artifacts, independently of whether either
  changes a line of DDL.

A freeze that promises "no churn" without accounting for this would be false on
the day it was signed.

## Decision

### The frozen artifact

The V1 schema contract is the pair:

- the `schema-v2/baseline/` tree, and
- the schema-migration head at
  `migrations/20260906120000_exact_zero_addr60_default_derivation.sql`.

The draft named `20260811120200_ens_v2_migration_slice_1_constraints.sql`, which
was the head when it was written. The 38 schema-migrations that landed between
the two carried slices 2 and 3, whose schema work this ADR anticipated rather
than forbade; the head is restated here so the frozen artifact is the tree the
milestone actually builds on.

An authorized carve-out that lands as a schema-migration becomes the new head,
and the change that lands it must advance the head named above and the head
[`storage.md`](../storage.md) names in its opening paragraph in the same
change; a carve-out that leaves either behind is out of contract, exactly as a
schema change that leaves `apply-check.sh` behind is. The frozen artifact at
any moment is therefore the baseline tree plus the head this line names.

`schema-v2/apply-check.sh` is the conformance test for that contract. It already
gates its own CI job and asserts table inventory, column presence, constraint
shape, and a forbidden-name policy. **A change to the schema that does not also
change `apply-check.sh` is out of contract.** That coupling is what makes the
freeze observable rather than aspirational.

### What the freeze promises

- No schema change to the frozen artifact during the V1 milestone, except the
  pre-authorized carve-outs below.
- Each carve-out is additive and requires no re-derivation.
- Any change beyond the carve-outs requires an amendment to this ADR before it
  merges.

### What the freeze explicitly does not promise

The published data will change during the milestone even though the schema holds
still. The ENSv1→ENSv2 slice-2 and slice-3 [re-derivation
boundaries](../glossary.md#re-derivation-boundary) each rotate the interpreter
content hash and force a full `interpret` and `project` walk.

Dependent work must therefore key on stable identifiers only:

**Safe to key on:** `logical_name_id`, `resource_id`, `token_lineage_id`, and
`contract_instance_id`. `event_identity` is safe only within one database
under a fixed manifest set and interpreter content hash, which is the contract
`architecture.md` gives it: it incorporates the derivation kind, identity
suffix, and emission ordinal (`crates/adapters/src/schema_v2/normalized.rs`,
`raw_log_event_identity`), so a covered adapter change can alter it for a raw
log that did not change — and it embeds the numeric `source_manifest_id`,
which is sequence-assigned, so installing the same baseline into a fresh
database changes every identity even with nothing else changed
(`consumer-capabilities.md` says as much of an empty-schema replacement). An
artifact that must survive a rebuild carries the retained manifest-ID mapping
with it or does not key on `event_identity` at all. Across
the re-derivation boundaries this freeze permits, the anchor is the raw-fact
position alone — chain, block hash, transaction hash, log index — keyed to
the set of events emitted for it; an expectation about a particular event is
scoped to one interpreter hash and re-derived, not carried, across a
rotation. A namehash is safe only
together with its namespace — which is what `logical_name_id` is
(`<namespace>:<namehash>`, `architecture.md` § Identity) — because the hash
does not encode the namespace, and the supported `ens` and `basenames`
namespaces can carry the same node. A derived normalized name is not an
identity at all: `architecture.md` and
[ADR 0002](0002-surface-resource-identity.md) make normalization results
read-time attributes, and a normalizer-version walk — which this freeze
permits — can change, remove, or collide them. An artifact may record a
normalized name only beside the `logical_name_id` it was derived for and the
normalizer version it was derived under.

**Not safe to key on:** `normalized_event_id` numeric values, cursor bytes, a
specific interpreter content hash, projection generation numbers, or row counts
for a given generation.

This is the load-bearing clause. A freeze cannot protect work that is keyed to
values the system already documents as unstable across a boundary.

### Pre-authorized carve-outs

1. **`project_generation_failures`** — the append-only audit for a
   projection-blocking invariant failure. When this ADR was drafted it was
   already described in [`storage.md`](../storage.md) and
   [`architecture.md`](../architecture.md) as part of the ownership map but did
   not exist; the baseline now carries it as
   `schema-v2/baseline/12_project_generation_failures.sql`. Additive; no
   re-derivation.
   Landing it requires three things beyond the table itself: entries in both
   expected-table lists in `apply-check.sh`, and an entry in the maintainer
   allowlist, because the table name matches the forbidden-name regex on
   `generation`.

   **Decided: landed, ahead of slice 2, as its own schema-migration.**
   `migrations/20260814131000_project_generation_failure_audit.sql` creates the
   table and `20260814132000_project_generation_failure_child_authority.sql`
   extends it. All three prerequisites are in place: the table appears in both
   expected-table lists in `apply-check.sh` and in the maintainer allowlist that
   exempts it from the `generation` forbidden-name regex.

2. **`migration_candidate_identity_effects.correlation_kind`** — currently
   pinned by CHECK to a single value. If slice 3's child-migration shape is not
   that value, widening it is a constraint replacement on a populated table, not
   an additive change.

   **Decided: no widening was needed, and none is authorized.** Slice 3A reuses
   `authority_transition`, so the CHECK on
   `migration_candidate_identity_effects` still pins that single value and
   `migration_discovery_associations` still pins `migration_registry_creation`
(`schema-v2/baseline/05_normalized_events.sql`); the candidate-side
`migration_candidate_discovery_effects` accepts any nonblank
`correlation_kind` and is not the constraint this decision guards. The
   interpreter writes exactly those two kinds. A future shape that needs a third
   is a constraint replacement on a populated table and therefore a new ADR, not
   a carve-out under this one.

3. **Migration-association canonicality** — the four ENSv1→ENSv2 correlation
   tables retain rows whose anchor block is orphaned, and nothing maintains their
   `canonicality_state`. Slice 2 adds readers over these tables.

   **Decided: document the reader rule; do not change reorg-time writes.**
   Merged in #885. The rule is a **`chain_lineage` anchor** requirement, not the
   `event_identity` join this draft proposed: `event_identity` exists only on
   `migration_event_associations`, so three of the four tables cannot satisfy an
   identity join at all. Any reader that treats one of these rows as current must
   anchor the row's own `(chain_id, block_number, block_hash)` on `chain_lineage`
   with a readable-state predicate. `storage.md` carries the rule and the reorg
   test that pins the runner-level outcome.

   One consequence is recorded there rather than hidden: on today's two
   publishing readers the anchor cannot be the reason a row is withheld, because
   both also require the `registry_announcement` edge joined on the association's
   own block, and the same Interpret redo orphans both. The rule governs new
   readers, which is where it is the only guard.

4. **Label-preimage indexes** — **decided: out of the freeze, pending a
   measurement; no index is pre-authorized and none is ruled out.** The two
   named paths differ. The children join reaches rows through the primary key:
   `preimage.labelhash = lower(...)` (`crates/project/src/builders/children.rs`,
   which lowers the other side precisely so the PK stays usable). The
   post-normalizer-bump recompute does not. Its `load_labels` query
   (`crates/interpret/src/recompute.rs`) selects every `label_preimages` row
   matching any of four `OR` branches — three correlated `EXISTS` lookups
   scoped to the chain and block range, and `source_kind =
   'ens_rainbow_import'` outright — then orders the union by `labelhash` and
   locks it `FOR UPDATE`. It carries no `normalizer_version` predicate, so the
   baseline's `label_preimages_normalization_idx (normalizer_version,
   normalized_under_version, labelhash)` cannot serve it, and the PK is not its
   access path either. `source_kind` is therefore on this path, and after a bulk
   rainbow import that branch alone returns the whole import on every recompute
   range.

   What that costs is a question for `EXPLAIN` against an imported table, not
   for this ADR: if the import dominates, the cost is row volume and no index
   changes it; if it does not, an index is additive and can be authorized when
   the measurement says so. #364 tracks the `source_kind` filter in
   `crates/project/src/scope/labels.rs`, which is a PK join over a bounded array
   and not the concern here. A bulk import during the milestone does not by
   itself require a schema change.

5. **Serving indexes the projections never had** — no index in
   `bigname_phase` supports a name-text filter or a name sort, so `/v2/search`
   and the GraphQL `name_contains` and name-ordered paths are sequential scans
   plus external sorts. The predecessor is not where the draft looked for it:
   every index the retired `public.address_names_current` carried led with
   `address`, including the one prefix index that named `normalized_name`
   (`address_names_current_address_normalized_name_prefix_idx`,
   `migrations/20260627120000_address_names_q_sort_read_indexes.sql`), so that
   table served name text only within one address. The global predecessors
   were on `public.name_current`, which is what the search and name-ordered
   paths read: `name_current_app_namespace_name_idx (namespace,
   normalized_name)` and `name_current_app_global_name_idx (normalized_name,
   namespace)` (`migrations/20260502170000_app_facing_rest_indexes.sql`). Those
   two shapes are the starting point for #404, benchmarked against the current
   `raw_name` queries rather than copied. Separately,
   `normalized_events` has no index leading with `namespace`. That is not the
   same as the default `/v2/events` page having no index: a request with no
   `event_type` still injects the product history event kinds
   (`apps/api/src/v2/events.rs`, `product_history_event_kinds()`), and
   `normalized_events_projection_idx (event_kind, canonicality_state, chain_id,
   block_number, normalized_event_id)` leads with `event_kind`, so it is a
   candidate access path for the real default query. Whether it serves the
   page or degrades into a scan over the kinds is a benchmark question. Both
   items are additive; no re-derivation.

   **Decided: in for the name-text index; #402 needs the measurement first.**
   The name-text gap is confirmed present: no index in
   `schema-v2/baseline/06_projections.sql` supports a name-text filter or name
   sort (#404), and the two `name_current` predecessors above are the shapes
   to measure first. For `/v2/events` (#402), the carve-out authorizes an index
   only
   after `EXPLAIN` of the default query against the existing
   `normalized_events_projection_idx`; a `namespace`-leading index that
   duplicates or misshapes that path is not authorized on the strength of this
   ADR. Monitoring and parity work exercise exactly these paths, so the latency
   should be found by a benchmark rather than by the milestone's own
   measurements. Both are additive and require no re-derivation.

### Derivation-side changes that are not schema changes

These rotate the interpreter content hash rather than touching DDL, so the
freeze does not cover them — but they had to be sequenced against the same
boundaries rather than landing ad hoc. Both were open when this ADR was drafted,
which proposed slice 2's boundary as their carrier. Neither made it: both landed
after slice 3, each as its own content-hash rotation (#745 on 2026-08-31 and
#813 on 2026-09-02). They are recorded here as outcomes, not as pending work:

- `ROLE_WAS_RESERVED` (bit 32) is in the ENSv2 registry role vocabulary. It was
  the one registry role constant upstream declares (upstream:
  .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L48 @
  ens_v2@a971bd64) with no entry in the adapter table; `REGISTRY_ROLE_BITS` in
  `crates/adapters/src/schema_v2/protocol/permissions.rs` now carries
  `(32, "was_reserved")`.
- The empty-value guard for ENSv1 and Basenames `contenthash` and non-ETH `addr`
  records, which published a cleared record as `status: "success"`, is
  corrected: `crates/project/src/builders/record_inventory.rs` classifies those
  as `not_found`, pinned by the `v1-record-clears.json` interpreter fixture.

Neither needs scheduling again.

### Explicitly out of scope

- **V2 schema sign-off.** The V2 spec is still shaping. This ADR records V1 only;
  V2 direction is acknowledged but not frozen.
- **Chunked projection publication.** **Decided: out of the milestone.** The
  work has not started — `crates/project/src/engine.rs` still publishes in one
  `REPEATABLE READ` transaction — and landing it would void the freeze outright,
  because durable per-unit progress requires either reopening the
  `chain_phase_state` coherence CHECK or adding a table the schema check forbids
  by name. Keeping a freeze at all means deferring it past the milestone. It
  stays tracked as its own readiness item; a decision to pull it forward
  supersedes this ADR rather than amending it.

## Consequences

**Positive.** Parity, regression, and monitoring work gets a named contract with
a conformance test behind it. The two re-derivation boundaries become scheduled
events with a documented invalidation rule instead of surprises. The
stable/unstable identifier split gives downstream authors a rule they can follow
without understanding the whole replay model.

**Negative.** The freeze is enforced by review, not by tooling — nothing fails CI
when a schema change lands without amending this ADR, beyond the existing
requirement that `apply-check.sh` move with the schema. It also front-loads
decisions that would otherwise be made inside the slices, which costs time now.

**Newly possible failure mode.** A carve-out landing without its `apply-check.sh`
allowlist entry fails CI with a forbidden-table error that does not obviously
point back to this ADR. Worth a comment in the allowlist referencing it.

## Rollout

Doc-first in intent; in practice three carve-outs landed while this ADR was still
a draft. Carve-out 1 shipped as its own reviewed schema-migration in August,
carve-outs 2 and 3 were settled by slice 3 and #885, and this ADR records them
rather than authorizing them in advance. That is a process miss worth naming: the
freeze was observable the whole time through `apply-check.sh`, but the written
contract trailed the schema by three weeks. Carve-out 5 is the one still ahead,
and it follows the intended order — this ADR first, then the schema-migration
referencing it.

Ownership follows [`workstreams.md`](../internal/workstreams.md): Storage and
Domain own the schema-migrations and `apply-check.sh`; Projections and API own
the downstream keying rule; Conformance and Fixtures own re-keying any existing
artifact that violates the stable-identifier list.

## Alternatives considered

**Freeze on the interpreter content hash.** The obvious candidate, and wrong: the
hash does not cover `schema-v2/` or `migrations/`, so it would freeze derivation
semantics while leaving the schema free — the inverse of what is needed. It also
rotates at both slice boundaries, which would make the freeze appear violated by
planned work.

**Freeze the schema-migration head alone.** Simpler, but a schema-migration
head does not describe schema *shape*, so a baseline edit could pass unnoticed.
Pairing it with `apply-check.sh` closes that.

**No freeze; rely on review.** This is the status quo, and it is what produced a
table documented in the authoritative ownership map that did not yet exist. Review
catches changes; it does not catch omissions.

**Sign off V1 and V2 together.** Not possible while the V2 spec is shaping. The
combined statement would be unfalsifiable.

## References

- [ADR 0006](0006-api-v2-product-surface.md) — v2 product surface
- [`consumer-capabilities.md`](../consumer-capabilities.md) — slice definitions
- [`storage.md`](../storage.md) — table families and replay classification
