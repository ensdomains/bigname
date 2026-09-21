# ADR 0007: V1 Schema Freeze and Change Process

Status: Accepted
Date: 2026-08-15
Accepted: 2026-09-11

> Every carve-out below is decided. Three were settled by work that has since
> merged and are recorded here as history; the rest are decisions this ADR makes.
> The freeze is effective from the acceptance date, 2026-09-11 — the day the
> last of that history landed (#885, the canonicality rule of carve-out 3;
> the pre-acceptance head landed on 2026-09-07). Every schema-migration up to
> that day predates the freeze; the twenty-four that landed after it, before this
> ADR merged, are the first breach and are recorded as such below.

## Context

"V1" in this ADR is bigname's first stable read release — not ENSv1, and not
the prospective V2 specification. The [V1
milestone](../glossary.md#v1-milestone) is the work that proves that release
as a replacement for the retained legacy read surface: parity coverage (the
slice-1 full-re-walk acceptance comparison — a rebuild from raw chain data at a
[re-derivation boundary](../glossary.md#re-derivation-boundary) — and the combined-boundary gate that
[`consumer-capabilities.md`](../consumer-capabilities.md) requires),
regression tests over the served routes, and monitoring, all built on top of
the schema. It ends when those gates pass and the release is signed off; the
sign-off is recorded by amending the status line of this ADR with its date,
and until that amendment the freeze applies. That work is expensive to redo,
so it needs a schema it can rely on. The requirement is not that the
schema never changes — it is that changes are enumerated in advance rather
than discovered mid-milestone.

Two prior decisions frame this. [ADR 0006](0006-api-v2-product-surface.md) fixed
the v2 product surface and rejected GraphQL as the product contract.
[`consumer-capabilities.md`](../consumer-capabilities.md) defines the ENSv1→ENSv2
delivery slices. When this ADR was drafted only slice 1 had landed; slices 2A–2E,
3A, 3B and the final activation have all merged since, so the [re-derivation
boundaries](../glossary.md#re-derivation-boundary) this ADR anticipated are
behind us as code. They are not necessarily behind every deployment: a
boundary is a full walk that a deployment takes when it first deploys the
rotated hash, so a deployment still running an older hash has those walks
ahead of it, and the keying rule below is written for artifacts that must
survive them either way.

Three properties of the current system determine what a freeze can and cannot
promise:

- **Schema change and re-derivation are independent axes.** DDL does not rotate
  the [interpreter content hash](../glossary.md#interpreter-content-hash). A
  change to the covered production sources under `crates/project/src`,
  `crates/adapters/src`, `crates/interpret/src/write`, and
  `crates/manifests/src`, to a manifest's `[[abi.events]]` declarations, to the
  named semantic source files, or to the pinned lockfile families does.
  A `#[cfg(test)]`-gated *external* module file under those roots does not
  (`crates/content-hash/src/compute.rs`, `source_exclusion`, pinned by
  `crates/content-hash/src/tests.rs`), but an inline `#[cfg(test)] mod` inside
  a covered file is hashed with the file, so a test-only edit there rotates
  the hash like any other edit; a manifest's `normalizer_version` does not
  rotate it either,
  and a manifest's `read_features` rotates the separate fingerprint recorded
  by the [manifest-authority marker](../glossary.md#manifest-authority-marker)
  with a byte-identical interpreter hash. The triggers are independent, and
  none of them is a narrower walk: an interpreter-hash rotation forces the
  full-history Interpret and Project walk; a fingerprint change on an
  initialized chain blocks derived work until the same full-range,
  token-attested Interpret redo and stamped Project redo complete, with the
  stamped Ingest redo first if the [watch
  plan](../glossary.md#watch-plan--watched-tuple) widened. A normalizer bump is
  two of these at once: the `ENS_NORMALIZER_VERSION` constant lives in
  `crates/domain/src/normalization.rs`, which the content hash covers
  (`crates/content-hash/src/compute.rs`), so changing it is an
  interpreter-hash rotation with the full-history walk that entails, and it
  additionally requires the `recompute-flags` pass over each chain's full
  retained range and the full-range Project redo that recompute the label
  verdicts. Only the manifest field `normalizer_version` is hash-insensitive,
  and it cannot move on its own: manifest loading rejects a value that differs
  from the compiled constant (`crates/manifests/src/lib/repository.rs`,
  `validate_manifest_metadata`), so an edit to the field alone is an invalid manifest, not
  a no-op, and a real bump changes the field in lockstep with the constant
  ([`deployment.md`](../deployment.md) § Phase-runner configuration, the
  manifest-authority marker and normalizer-version paragraphs).
- **The content hash does not cover the schema.** It watches Rust sources,
  manifests, and the lockfile — not `schema-v2/` and not `migrations/`. It cannot
  serve as the freeze anchor.
- **Slices 2 and 3 each touch Project builders.** Both therefore rotate the hash
  and invalidate artifacts keyed to a [projection
  generation](../glossary.md#projection-generation), independently of whether
  either changes a line of DDL.

A freeze that promises "no churn" without accounting for this would be false on
the day it was signed.

## Decision

### The frozen artifact

The V1 schema contract is the pair:

- the `schema-v2/baseline/` tree, and
- the schema-migration head at
  `migrations/20260924120000_normalized_events_resolver_history_idx.sql`.
  `schema-v2/apply-check.sh` asserts on every run that this line and the one
  in [`storage.md`](../storage.md) name the newest file in `migrations/`, so a
  merge that brings a later schema-migration fails the conformance job until
  the head is advanced here.

The draft named `20260811120200_ens_v2_migration_slice_1_constraints.sql`, which
was the head when it was written. Sixty-three schema-migrations follow it up to the
head named above: 38 landed before acceptance, while this ADR was a draft,
under the review-only process § Alternatives describes, so none of them is a
carve-out under this ADR — they entered the frozen artifact by predating the
freeze — twenty-four landed after acceptance, which the next paragraphs
record, and the last is carve-out 6 below, which this ADR lands with itself.
The head is restated so the frozen artifact is the tree the
milestone actually builds on. The 38 are not all slice work. Four are the
ENSv1→ENSv2 slice schema this ADR anticipated:
`20260814130000_surface_binding_authority_arm.sql` (slice 2A, #468),
`20260814131000_project_generation_failure_audit.sql` (slice 2E, #497;
carve-out 1 below), and
`20260814132000_project_generation_failure_child_authority.sql` with
`20260904120000_project_redo_child_registration_history.sql` (slice 3B's
children publication invariant and its parent-path filter, #499 and #821).
The other 34 are independent changes that would each have needed a carve-out
or an amendment had the freeze been in force: phase-runner coordination state
— heartbeat liveness, unconfigured-phase settlement, Ingest redo
source-boundary and manifest-authority markers, and the redo attempt
generation (#427, #556) — and Project incremental-scope and
reverse-[hydration](../glossary.md#hydration) state (#415); the raw-block
preimage derivation swap (#519); the Interpret decode-skip audit and the
manifest applied-change counter (#583, #579);
`normalized_events` scope indexes and the legacy index drops (#612, #653,
#762, #636) and the `name_current`
[serving-resource](../glossary.md#serving-resource) column (#636); the
retirement of direct [resolution
divergences](../glossary.md#resolution-divergence-ledger) for null-resolver
names (#739); the [discovery-watch admissions
snapshot](../glossary.md#discovery-watch-admission-snapshot) (#747); Project
redo [expiry-root](../glossary.md#expiry-root) and expiry-resource seeds
(#762); and registry operator [account
permissions](../glossary.md#account-permission-state) (#815). The head itself
is one more independent change:
`20260906120000_exact_zero_addr60_default_derivation.sql` (#869, landed
2026-09-07) replaces the `write_resolution_divergence` function so that an
exact zero `addr:60` stays absent when a default derivation exists — a
serving-semantics change, and the last schema-migration before acceptance.

The remaining twenty-four landed after acceptance and before this ADR merged,
twelve in #893 (2026-09-16), one each in #897 and #899, two in #907 (all
2026-09-17), two in #902, two in #905 and one in #912 (2026-09-18), and one
each in #934, #936 and #939 (2026-09-23), none as a carve-out or with an amendment: under the effective date above they are the first
breach of the freeze, recorded here rather than reclassified as history.
`20260909120000`–`120200_resolver_record_id_events` widen the
`normalized_events` event-kind CHECK in three steps — a constraint
replacement on a populated table; `20260911120000` and `20260911120200` add
`normalized_events` and `name_current` indexes; `20260911120100` creates the
`address_records_current` projection table, `20260914120100` comments it,
and `20260915120000` drops three of its NOT NULL constraints;
`20260913120000` and `20260914120000` replace `write_resolution_divergence`
and add `revalidate_resolution_lookup_state`; `20260913130000` and
`20260913130100` replace the CHECKs on `permissions_current_resource_summary`
and `account_permission_state_current`. #897's `20260916120000` adds the
`surface_bindings` `(chain_id, logical_name_id)` index that Interpret redo
preparation reads without a canonicality predicate; #899's `20260917120000`
adds the partial `discovery_edges` observation-history index that recording a
contract seen before reads, with a concurrent prebuild installer under
`ops/discovery-history-index/`. #907's `20260917130000` adds the unfiltered
`discovery_edges` reopen index, with its installer under
`ops/discovery-reopen-index/`, and its `20260917160000` changes no object: it
fails the run when either discovery index exists under its name but is not
the reviewed, valid index, since `CREATE INDEX IF NOT EXISTS` matches on the
name alone. #902's `20260917131000` adds six `normalized_events` indexes
for Project's scoped history reads and its `20260917161000` checks them the
same way; #905's `20260917140000` replaces the `discovery_edges` self-edge
CHECK — a constraint replacement on a populated table — and its
`20260917141000` renames it to the reviewed name where an earlier build left
another; #912's `20260917150000` adds two `normalized_events` look-ahead
indexes for ENSv1 interpretation. #902 also edited `20260917160000` in place
after #907 had landed it — a comment and the `quote_all_identifiers` guard —
which the inventory below now forbids: sqlx records each file's checksum and
refuses to run against a database that applied the earlier bytes, so any
database that took #907's version before #902 merged must have that row's
checksum corrected by hand before its next `sqlx migrate run`. #934's
`20260923120000` adds three partial expression `normalized_events` indexes
for the address history read and checks them in the same file, with a
concurrent prebuild installer under `ops/address-history-indexes/`; #936's
`20260923130000` adds a `normalized_events` `(chain_id, block_number DESC
NULLS LAST)` index for history and event pages read in chain order, checked
the same way, with its installer under `ops/events-order-index/`; #939's
`20260923150000` creates the Project-owned `child_registration_events`
projection table with its CHECKs and two indexes, filled only by the full
Project rebuild that #939's interpreter content hash rotation requires. Then
`20260924120000`, carve-out 6, is this ADR's own. The head named above is
the last of them, so the frozen artifact is the tree an initialized database
actually holds; the breach is the subject of the Rollout section below.

Since #849 `apply-check.sh` applies every schema-migration that names a
`bigname_phase` object and fails on one it does not list. Its inventory is
that literal token, so a schema-migration written against the connection's
search path would not be in it; the same script therefore closes that door
by rule rather than by parsing: a schema-migration newer than the
legacy-schema drop that names no `bigname_phase` object may consist only of
`DROP INDEX` statements whose every target is `schema.name`, written with
plain identifiers and nothing quoted — no strings, quoted identifiers,
dollar quoting, or block comments — and without `CASCADE`, since a cascading
drop would take any dependent `bigname_phase` object with it unlisted. Every
other drop kind is refused here because `RESTRICT` protects only the
dependencies PostgreSQL records: a `bigname_phase` PL/pgSQL routine that
calls `public.helper()`, selects from `public.helper_view`, or reads
`nextval('public.helper_seq')` from its body records none, so such a drop
succeeds and the routine fails at its next call. An index is the one target
no routine body can depend on that way. A schema-migration that drops
anything else names `bigname_phase`, which has it inventoried, applied and
observed here.
`DROP TABLE` is refused even with `RESTRICT`: a table in another schema can
be an inheritance child or a partition of a `bigname_phase` table, and
PostgreSQL drops it without complaint, taking the rows visible through the
phase parent. Any other statement, any expression, any routine
call, and any spelling of the phase schema other than `bigname_phase` is
refused, so a search-path-relative name cannot be written outside the
inventory whatever statement carries it. `schema-v2/migration-inventory.txt`
lists every schema-migration file in order with the SHA-384 of its bytes — the
checksum sqlx records on apply and rejects on any later mismatch — and the
directory must equal it exactly, bytes included, so an edit to an applied
file fails here before it fails every initialized database; and because the
inventory is itself editable, the check also reads
the previous inventory — the base branch's on a pull request, the parent
commit's on a push — and refuses any new entry that sorts at or below the
previous head, any previous entry that is gone, and any previous entry whose
bytes changed: sqlx applies whichever
versions a database has not recorded, so a file named to sort below the
head would run on an initialized database while the freeze recorded
nothing. A schema-migration lands by joining the inventory after the head
and advancing the head in the same change. Inside the inventory the
rewrite is textual — the literal `bigname_phase` becomes the scratch schema —
so it cannot see a name a schema-migration assembles at run time (`'bigname_' ||
'phase.…'` inside `EXECUTE`, or a search-path-relative name in a `DO` body).
The check therefore applies every batch on a connection of its own, a
per-run login that owns the scratch schema and holds no privilege on
`bigname_phase` and no `CREATE` on the database — provisioned by the
configured user, who needs `CREATEROLE` for that login and, on an external
server, `CREATEDB` as well, because the check runs in a database of its own
there (created at start, dropped at exit) rather than asking for `CREATE` on
the database the URL names; the schemas are created for the login rather
than granted to it, and the only statements that run on the configured
user's own connection are the baseline's two reviewed `CREATE EXTENSION`
lines, matched whole, and the setup of sqlx's bookkeeping table the replays
record into (refused when the database already has one): whatever the rewrite misses
fails on the production schema instead of changing it unobserved. The check
proves itself on every run against a planted set of the forms it refuses and
the one it accepts, including an assembled production name that must be
refused — after `RESET ROLE` too — and its rewritten twin that must succeed.
Finally the frozen artifact itself is a checked-in catalog:
`schema-v2/frozen-schema.txt` is the baseline's extension declarations and
every relation (with its privileges, storage parameters, row-level-security
flags, replica identity, partitioning and parents), column (type,
nullability, default, identity, generation, collation, storage, compression,
statistics target, privileges, and the value rows that predate the column
read when it is no longer the default), constraint (with whether it is
defined locally or inherited, which decides whether `NO INHERIT` removes it),
index (with its validity), view,
routine (its full argument list with defaults, execution modes, planner cost
and rows, privileges and a digest of its body), trigger (with its firing
state), sequence (its whole range, cache, cycle and owning column), type,
domain, comment and the schema's own privileges, of the
baseline plus the inventoried schema-migrations, built into a fresh schema on every
run and compared line for line, so a change to a baseline file or a
schema-migration that moves the schema fails until the catalog is
regenerated (`SCHEMA_V2_APPLY_CHECK_WRITE_FINGERPRINT=1`) in the same
change — whatever the object is called. The catalog is taken twice, after
the baseline alone (what a fresh database gets, the schema-migrations being
no-ops before it exists) and after the schema-migrations (what an
initialized database gets), and the two must agree: a schema-migration
without its baseline edit is refused on that comparison before the frozen
file is consulted. A baseline edit without its schema-migration can pass it,
every schema-migration being a no-op on the edited baseline, so the check
also migrates the previous baseline — read at the same point as the previous
inventory — the way `sqlx migrate run` migrates a database at that revision:
every version in the previous `migrations/` directory is recorded without
being run, a recorded file that is gone or whose bytes changed is refused as
sqlx would refuse it, and only the phase schema-migrations added since are
applied, so an older file rerun cannot seem to carry a baseline-only edit, and
the result must be the current artifact. The previous baseline's catalog heads
with its own extension declarations and the migrated one with those plus each
`CREATE EXTENSION` a schema-migration added since carries, since the
configured user installs the current declarations before any replay and the
database alone would report an extension an initialized database lacks; a
baseline that gains an extension without a schema-migration that creates it
is refused on that header. Removing an extension is not modelled: the previous
baseline's own declaration would run as the per-run login and fail. That fresh artifact holds no rows, so a
schema-migration whose DDL runs only when a table has data would leave it
unchanged; the check therefore also takes the catalog of its scratch schema
at the end of the run — populated by every predecessor-shape and behavior
proof, rewound to older shapes by those proofs and carried back through the
whole inventoried sequence, as sqlx would carry an initialized database —
and that must be the frozen artifact too. Every such replay applies the
sequence the way `sqlx migrate run` does: through one session, each file in
its own transaction unless it opens with `-- no-transaction`, with sqlx's
own `_sqlx_migrations` bookkeeping recorded inside that transaction by its
unqualified name, so a setting one file commits is in force for the files
after it and a file that moves `search_path` breaks the bookkeeping exactly
where sqlx would; the check plants a sequence on every run to prove those
properties. Neither a baseline file nor a schema-migration may change session
state — a statement-leading `SET` or `RESET` of any setting, a custom
placeholder or quoted name, or a quoted or `format`-built `SET` run through
`EXECUTE` included, the SQL-standard
`SET TIME ZONE`, `SET SCHEMA`, `SET NAMES`, `SET XML OPTION` and
`SET SESSION CHARACTERISTICS`, `SET [SESSION | LOCAL] ROLE`, `SET SESSION AUTHORIZATION`, or
`set_config(..., false)`, in a routine body included — because the baseline
session and the sqlx run carry it into every later file; a routine that needs
a setting uses `set_config(..., true)` and restores it. Nor may either hold a
psql backslash command outside quoted text or a comment: the replay feeds each
file to psql, which runs the command on the client, while sqlx sends the file
to the server, which rejects it, so the check would pass a file deployment
refuses, and a `\set ON_ERROR_STOP 0` would hide every later file's errors.
Because sqlx applies only the files a database has not recorded, and a
catch-up can be split across several runs, what one file leaves in the session
reaches the next file in the replay but not in a deployment that starts that
file on a fresh connection. After each applied file's commit and bookkeeping
the check therefore probes the replay connection and refuses, naming the file,
a session-level setting however it was made (PostgreSQL lists no custom
placeholder setting, which only the statement rule sees), a temporary table,
routine or type, a prepared statement, a holdable cursor, a session advisory
lock, a `LISTEN`, an assumed role, a transaction a `-- no-transaction` file
leaves open, or a change to the login's own connection
defaults, memberships or default privileges since the sequence began — the
last because a later file that reverts it hides it from the end-of-replay
snapshot, while a deployment interrupted between the two keeps it. What ends
with the file's transaction — `ON COMMIT DROP`, a temporary table the file
drops again, a transaction-level advisory lock, `set_config(..., true)` —
passes; the one planted sequence that commits a setting on purpose, to prove
the replay is a single session, runs without the probe, and the probe proves
itself on planted files. A phase schema-migration may not read who runs it
either — `current_user`, `session_user`, `current_role`, `system_user`,
`getpgusername`, `pg_get_userbyid`, `pg_has_role`, a `has_*_privilege`
function, `pg_roles`, `pg_user`, `pg_authid`, `pg_auth_members`, `pg_shadow`,
`to_regrole` or `regrole`, `rolname`, `rolsuper`, `usename`, `is_superuser`,
`session_authorization`, the `information_schema` role and privilege views,
or a caught `insufficient_privilege` or `undefined_object` by name or SQLSTATE
— because the replay runs as the per-run login and
deployment as the writer database user, so a branch on identity, role
existence or privilege takes a path here that deployment does not; bare `user`
is left alone because the rule reads quoted prose too, and bare `role` because
`manifest_contract_instances.role` is a column. Nor may it read where it runs
— `current_database`, `current_catalog`, `pg_database`, `datname`, an
`information_schema` `*_catalog` column or `catalog_name`, or the server or
client address or port through the `inet_*` functions, `pg_stat_activity`,
`pg_stat_database` or the `port`, `listen_addresses`,
`unix_socket_directories` and `cluster_name` settings — since on an external server the replay runs in a
database of its own, or read the session's temporary namespace through
`pg_my_temp_schema`, `current_schemas` or `pg_is_other_temp_schema`: a file
that creates and drops a temporary table leaves that namespace allocated for
the files after it in one `sqlx migrate run` but not in a catch-up split
across runs, and the probe cannot refuse it, because nothing frees it before
the session ends and the frozen `20260917141000` allocates it. The predecessor baseline, once migrated, is held to the closed-kind
rule like the fresh and exercised schemas, and that rule also refuses a
foreign key whose referenced table has more than one unique index that could
back it: PostgreSQL picks the first valid one in index OID order and the
catalog does not print
which, so two histories that print alike would drop or cascade differently.
Column order is not part of the
artifact: a column a schema-migration adds sits last on an initialized
database and wherever the baseline lists it on a fresh one. What the check
enforces instead, reading the live order on each replay rather than putting an
ordinal in the artifact, is that a replay never moves a column a table already
had, that the columns it does add come after them, and that a table a
schema-migration creates from nothing matches the baseline's layout — so the
baseline and a schema-migration cannot lay the same table out differently,
which `SELECT *`, a positional `INSERT`, a composite value and `row_to_json`
would all read differently on a fresh and on an upgraded database. Every value
in the artifact that holds more than one element — privileges, storage
parameters, inherited parents, enum labels, a domain's constraints, a composite
type's attributes, a routine's configuration, a role's or database's connection
defaults — is encoded as a JSON array rather than joined with a delimiter,
since an element carrying the delimiter would let two different schemas
serialize identically. Each replay also ends by comparing sqlx's
`_sqlx_migrations` against the full expected history — one row per migration
file, with the SHA-384 of its bytes — and the cluster's role
configuration — connection defaults, every role attribute (`LOGIN`,
`SUPERUSER`, `BYPASSRLS`, `CREATEDB`, `CREATEROLE`, `REPLICATION`, `INHERIT`,
connection limit, expiry), role memberships, the default privileges of every
schema in the database rather than only the phase schema's, and, where the
check can read `pg_authid`, the password verifiers — against the snapshot taken
before the first replay. That last read is decided before the statement is
sent, because PostgreSQL checks the relation privilege when the scan opens and
the documented external-server login is not a superuser. A file that rewrites or deletes earlier bookkeeping, or changes
any of that however the statement is spelled, fails even though no catalog
line moves; the password form is also named by the statement rule, because a
run that cannot read `pg_authid` sees only one mask for every password.
From acceptance on, a
schema-migration of any of these kinds cannot land without moving the
conformance test, which is where the carve-out or amendment is checked for.

The check is built to catch a schema-migration that would behave differently
under sqlx on a production database through ordinary SQL, including dynamic
SQL whose effect it can observe at run time. SQL written to hide what it does
from the check — a keyword, schema name or role name assembled from pieces so
that no rule can see it — is outside what a conformance test can close, and
review is the control for it. Two such forms are declined on that ground: a
phase schema name assembled from pieces and compared as a value, which the
textual rewrite to the scratch schema cannot see, and an assembled password
change where the configured user is not a superuser and so cannot read the
password verifiers.

An authorized carve-out that lands as a schema-migration becomes the new head,
and the change that lands it must advance the head named above and the head
[`storage.md`](../storage.md) names in its opening paragraph in the same
change; a carve-out that leaves either behind is out of contract, exactly as a
schema change that leaves `apply-check.sh` behind is. The frozen artifact at
any moment is therefore the baseline tree plus the head this line names.

`schema-v2/apply-check.sh` is the conformance test for that contract. It already
gates its own CI job and asserts table inventory, column presence, constraint
shape, a forbidden-name policy, and the frozen catalog. **A change to the
schema that does not also change `apply-check.sh` and regenerate
`schema-v2/frozen-schema.txt` is out of contract.** That coupling is what makes the
freeze observable rather than aspirational.

The frozen catalog describes the object kinds the baseline uses — tables,
views, sequences, indexes, constraints, triggers, functions and procedures,
enum and domain types, and comments on those — and the phase schema is closed
to every other kind. `apply-check.sh` refuses, by kind, an aggregate or window
function, a range, multirange, composite or shell (declared but undefined) type, an operator, operator class
or family, a materialized view, a partitioned table or index, a typed table, a foreign table,
a rewrite rule, a row policy, an extended-statistics object, a collation, a
conversion, a text-search object, or a cast to or from a phase type, and
plants one of each on every run to prove it. A carve-out that needs one of
those kinds extends the rule and the catalog under this ADR; nothing outside
the catalog's vocabulary can enter the schema unobserved.

### What the freeze promises

- No schema change to the frozen artifact during the V1 milestone, except the
  pre-authorized carve-outs below.
- Each carve-out made under this freeze requires no re-derivation, and each
  is additive except the one drop carve-out 6 authorizes: two indexes nothing
  reads, an access path and no contract. The one non-additive change in the
  history recorded below — carve-out 1's constraint replacement — predates
  the freeze and is recorded, not authorized; a like change now is an
  amendment, not a carve-out.
- Any change beyond the carve-outs requires an amendment to this ADR before it
  merges.

### What the freeze explicitly does not promise

The published data can change during the milestone even though the schema
holds still. Every interpreter-hash rotation — the ENSv1→ENSv2 slice-2 and
slice-3 re-derivation boundaries, the two later rotations recorded under
[Derivation-side changes](#derivation-side-changes-that-are-not-schema-changes),
and any that follows — forces a full `interpret` and `project` walk on each
deployment that first deploys it, and the walk re-derives the published data.

Dependent work must therefore key on stable identifiers only:

**Safe to key on:** `logical_name_id`,
[`resource_id`](../glossary.md#resource),
[`token_lineage_id`](../glossary.md#token-lineage), and
[`contract_instance_id`](../glossary.md#contract-instance).
`event_identity` is safe only within one database under a fixed manifest set
and interpreter content hash, which is the contract `architecture.md` gives
it: it incorporates the [derivation kind](../glossary.md#derivation-kind),
identity suffix, and emission ordinal
(`crates/adapters/src/schema_v2/normalized.rs`, `raw_log_event_identity`), so
a covered adapter change can alter it for a raw log that did not change — and
it embeds the numeric `source_manifest_id`, an identity-column value
(`schema-v2/baseline/04_manifests.sql`) that is not a cross-database
contract: installing the same manifests into a fresh database may assign
the same numbers — the paths are collected in filename order and upserted in
that order — or different ones, and nothing promises either
(`consumer-capabilities.md` says as much of an empty-schema replacement). An
artifact that must survive a rebuild carries the retained manifest-ID mapping
with it or does not key on `event_identity` at all. Across
the re-derivation boundaries this freeze permits, the anchor is the [raw
fact](../glossary.md#raw-fact) position alone — chain, block hash,
transaction hash, log index — keyed to the set of events emitted for it; an
expectation about a particular event is scoped to one interpreter hash and
re-derived, not carried, across a rotation. A namehash is safe only together
with its namespace — which is what `logical_name_id` is
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
specific interpreter content hash, projection generation numbers, or row
counts for a given generation.

This is the load-bearing clause. A freeze cannot protect work that is keyed to
values the system already documents as unstable across a boundary.

### Pre-authorized carve-outs

1. **`project_generation_failures`** — the append-only audit for a
   [projection generation
   failure](../glossary.md#projection-generation-failure),
   a projection-blocking invariant failure. When this ADR was drafted it was
   already described in [`storage.md`](../storage.md) and
   [`architecture.md`](../architecture.md) as part of the ownership map but did
   not exist; the baseline now carries it as
   `schema-v2/baseline/12_project_generation_failures.sql`. Proposed as
   additive; no re-derivation.
   Landing it requires three things beyond the table itself: entries in both
   expected-table lists in `apply-check.sh`, and an entry in the maintainer
   allowlist, because the table name matches the forbidden-name regex on
   `generation`.

   **Decided: landed as slice 2E's own schema-migration (#497, 2026-08-20),
   after slices 2A–2C.**
   `migrations/20260814131000_project_generation_failure_audit.sql` creates the
   table and `20260814132000_project_generation_failure_child_authority.sql`
   extends it — and that extension was not additive: it drops and recreates
   the populated table's `failure_kind` CHECK to admit
   `dual_current_child_authority`, the same constraint replacement on a
   populated table that carve-out 2 below classifies as beyond a carve-out.
   It is recorded here as a historical non-additive exception, made while
   this ADR was a draft; it required no re-derivation. All three
   prerequisites are in place: the table appears in both expected-table lists
   in `apply-check.sh` and in the maintainer allowlist that exempts it from
   the `generation` forbidden-name regex.

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

3. **Migration-association [canonicality](../glossary.md#canonicality)** — the
   four ENSv1→ENSv2 correlation tables retain rows whose anchor block is
   orphaned, and nothing maintains their `canonicality_state`. Slice 2 adds
   readers over these tables.

   **Decided: document the reader rule; do not change reorg-time writes.**
   Merged in #885. The rule is a **`chain_lineage` anchor** requirement, not the
   `event_identity` join this draft proposed: `event_identity` exists only on
   `migration_event_associations`, so three of the four tables cannot satisfy an
   identity join at all. Any reader that treats one of these rows as current must
   anchor the row's own `(chain_id, block_number, block_hash)` on `chain_lineage`
   with a [readable](../glossary.md#readable--read-safe)-state predicate.
   `storage.md` carries the rule and the reorg test that pins the runner-level
   outcome.

   One consequence is recorded there rather than hidden: on today's two
   publishing readers the anchor cannot be the reason a row is withheld, because
   both also require the [`registry_announcement`
   edge](../glossary.md#registry-announcement-edge-registry_announcement)
   joined on the association's own block, and the same Interpret redo orphans
   both. The rule governs new readers, which is where it is the only guard.

4. **[Label-preimage](../glossary.md#preimage-observation--label-preimage)
   indexes** — **decided: out of the freeze, pending a measurement; no index
   is pre-authorized and none is ruled out.** The two
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
   normalized_under_version, labelhash)` cannot serve the selection. The
   primary key (`labelhash`, `schema-v2/baseline/07_labels.sql`) matches the
   `ORDER BY`, so the planner's real alternatives are an ordered primary-key
   scan with the four `OR` branches applied as filters, or a sequential scan
   plus a sort; neither narrows the rows. `source_kind` is therefore on this
   path, and after a bulk rainbow import that branch alone returns the whole
   import on every recompute range.

   What that costs is a question for `EXPLAIN` against an imported table, not
   for this ADR, and the plan it reports must show which of those two
   alternatives PostgreSQL picks before an index is judged against it: if the
   import dominates, the cost is row volume and no index changes it; if it
   does not, an index is additive and can be authorized when the measurement
   says so. #364 tracks the `source_kind` filter in
   `crates/project/src/scope/labels.rs`, which is a PK join over a bounded array
   and not the concern here. A bulk import during the milestone does not by
   itself require a schema change.

5. **Serving indexes the projections never had** — no index in
   `bigname_phase` supports a name-text filter or a name sort, which is all
   this ADR asserts about `/v1/search` and the GraphQL `name_contains` and
   name-ordered paths. What the planner does instead is unmeasured: a
   namespace-scoped request can walk `name_current_lookup_idx (namespace,
   namehash, logical_name_id)` before filtering `raw_name`, and a sort is
   external only past `work_mem`, so whether these paths scan and spill is
   #404's `EXPLAIN` to record, not a claim here. The predecessor is not where
   the draft looked for it:
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
   same as the default `/v1/events` page having no index: a request with no
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
   to measure first. For `/v1/events` (#402), the carve-out authorizes an index
   only
   after `EXPLAIN` of the default query against the existing
   `normalized_events_projection_idx`; a `namespace`-leading index that
   duplicates or misshapes that path is not authorized on the strength of this
   ADR. Monitoring and parity work exercise exactly these paths, so the latency
   should be found by a benchmark rather than by the milestone's own
   measurements. Both are additive and require no re-derivation.

6. **The four `normalized_events` resolver-history indexes no
   schema-migration carried** — #415 (2026-08-14) added
   `normalized_events_pointer_after_resolver_history_idx`,
   `…_pointer_before_…`, `…_permission_after_…` and
   `…_permission_before_…` to `schema-v2/baseline/05_normalized_events.sql`
   with no schema-migration. Their predicates name `consumer_visibility`,
   which slice 1's `20260811120000` adds, so a database that took slice 1 in
   place and was never replaced from the baseline has the column and not the
   indexes. The conformance test found this the first time it compared the
   frozen catalog with the exercised scratch schema — the one its
   predecessor-shape proofs rewind and re-upgrade — rather than with the
   fresh one, where a baseline object needs no schema-migration to be
   present. Review then asked who reads them. The `pointer_*` pair has a
   reader: the resolver-anchored event feed
   (`GET /v1/events?resolver=<chain>:<address>`,
   `crates/storage/src/history/paging.rs`) selects activated canonical
   `ResolverChanged` events by chain and the lower-cased resolver the
   pointer moved to or from, and `EXPLAIN` over a 400,000-row
   `normalized_events` answers it with a `BitmapOr` over both indexes, and
   without them with a scan of every canonical `ResolverChanged` on the
   chain. The `permission_*` pair has none: Project's resolver scoping
   (`crates/project/src/scope`, `crates/project/src/stage.rs`), which #415
   built them for, derives the resolver address through a `CASE` inside a
   lateral `VALUES` list, which no expression index serves (`EXPLAIN` of that
   shape reads the primary key and filters), and no other read filters
   `PermissionChanged` by scope resolver. **Decided: the `pointer_*` pair in,
   the `permission_*` pair out, with this ADR.**
   `migrations/20260924120000_normalized_events_resolver_history_idx.sql`
   builds each kept index when it is missing and, when one is present,
   refuses an invalid index, another definition, or a table under the name
   rather than adopting it; it drops each retired index that exists and
   refuses a retired name held by anything that is not an index; the two
   retired definitions leave the baseline in the same change. On a large
   database the operator prebuilds and drops concurrently with
   `ops/resolver-history-indexes/install.sql` first, as
   [`deployment.md`](../deployment.md) and the production runbook list,
   since the file's own build is an ordinary write-blocking `CREATE INDEX`
   and its drop takes the table's exclusive lock. Access paths only; no
   re-derivation. The drop is the one non-additive step under this freeze:
   an index nothing reads carries storage and write maintenance and no
   contract, and carve-out 5's rule applies to bringing one back — an
   `EXPLAIN` of the read it serves, first. Whether other objects #415 and
   its neighbours added to the baseline without a schema-migration are
   missing on some initialized database is a question for the deployment
   that would hold it, since the exercised comparison sees only what the
   proofs rewind; the fresh baseline is the artifact, and a database that
   differs from it is replaced or carried to it by a schema-migration under
   this process.

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
  The classification follows the contracts: both resolver families store the
  supplied byte payload verbatim and their reads return the stored bytes, so an
  empty payload is what a clear leaves behind and what a read then returns
  (upstream: .refs/ens_v1/contracts/resolvers/profiles/ContentHashResolver.sol:L14-L28 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L47-L85 @ ens_v1@91c966f)
  (upstream: .refs/basenames/src/L2/resolver/ContentHashResolver.sol:L32-L43 @ basenames@1809bbc)
  (upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L57-L99 @ basenames@1809bbc);
  [`api-v2.md`](../api-v2.md#status-vocabulary) § Status Vocabulary carries the read-side rule.

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

## Upstream anchors

This ADR governs bigname's own schema; its upstream dependencies are the two
derivation-side outcomes above:

- (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L47-L48 @ ens_v2@a971bd64)
  — anchors `ROLE_WAS_RESERVED` (bit 32) as an ENSv2
  registry role, mirrored by `REGISTRY_ROLE_BITS` in
  `crates/adapters/src/schema_v2/protocol/permissions.rs`. The bit itself is
  mirrored, not diverged. What bigname does with it — exposing the token-only
  marker as `was_reserved` in `effective_powers` although it grants no
  authorization — is the divergence `upstream.md` § Known divergences already
  records as "ENSv2 reservation-history marker appears in the permission
  vocabulary"; this ADR adds no second entry for it.
- (upstream: .refs/ens_v1/contracts/resolvers/profiles/ContentHashResolver.sol:L14-L28 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L47-L85 @ ens_v1@91c966f)
  (upstream: .refs/basenames/src/L2/resolver/ContentHashResolver.sol:L32-L43 @ basenames@1809bbc)
  (upstream: .refs/basenames/src/L2/resolver/AddrResolver.sol:L57-L99 @ basenames@1809bbc)
  — anchor the verbatim byte storage and reads behind the
  record-clear `not_found` classification above. Mirrored, not diverged.

## Consequences

**Positive.** Parity, regression, and monitoring work gets a named contract with
a conformance test behind it. The re-derivation boundaries are documented
events with an invalidation rule instead of surprises — merged as code, taken
as a full walk by each deployment that adopts them. The
stable/unstable identifier split gives downstream authors a rule they can follow
without understanding the whole replay model.

**Negative.** Tooling enforces only the mechanics of the freeze: `apply-check.sh`
fails CI when the schema the baseline and inventoried schema-migrations build
no longer matches the checked-in frozen catalog, when the `migrations/` directory
differs from the inventory, and when a schema-migration lands without
advancing the head this ADR and `storage.md` name. Whether a change is an
authorized carve-out or a substantive amendment is still decided by review —
nothing fails CI when the catalog and head are regenerated but the ADR's
decision is not recorded. It also front-loads decisions
that would otherwise be made inside the slices, which costs time now.

**Newly possible failure mode.** A carve-out landing without its `apply-check.sh`
allowlist entry fails CI with a forbidden-table error that does not obviously
point back to this ADR. Worth a comment in the allowlist referencing it.

## Rollout

Doc-first in intent; in practice three carve-outs and 34 unrelated
schema-migrations landed while this ADR was still a draft. Carve-out 1 shipped
as slice 2E's reviewed schema-migration on 2026-08-20, carve-outs 2 and 3 were
settled by slice 3 and #885, and this ADR records them rather than authorizing
them in advance; the 34 others are inventoried under the frozen artifact. That
is a process miss worth naming: the freeze was observable the whole time
through `apply-check.sh`, but the written contract trailed the schema by three
weeks. And the miss repeated once more: between acceptance and this ADR's
merge, #893 landed twelve schema-migrations — a new projection table,
constraint replacements on populated tables, function replacements — and
#897, #899, #907, #902, #905, #912, #934, #936 and #939 a thirteenth
through twenty-fourth — seven index files, two check-only files, a
constraint replacement with its rename, and a new projection table — under
the review-only process, with no carve-out and no amendment. They are
inventoried under the frozen artifact and the head advanced to the last of
them, because the artifact has to be the tree that exists; they are not
retroactively authorized. From this ADR's merge the process is the one it
describes: a schema change is a listed carve-out or an amendment, and
`apply-check.sh` is where the omission fails. Carve-out 6 lands in this
change, listed here before its schema-migration is inventoried; carve-out 5
is the one still ahead, and it follows the intended order — this ADR first,
then the schema-migration referencing it.

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
