# What `apply-check.sh` enforces

`schema-v2/apply-check.sh` is the conformance check for [ADR 0008](../docs/adrs/0008-v1-schema-freeze.md), the schema freeze for the [V1 milestone](../docs/glossary.md#v1-milestone). The ADR records what is frozen and why; this page states the rules the check enforces and the reason for each. The script's comments explain how each rule is implemented, and [`docs/development.md`](../docs/development.md) covers the database user it needs.

Terms used below:

- **Phase schema:** `bigname_phase`, the schema the baseline and the schema-migrations build.
- **Phase schema-migration:** a schema-migration that names `bigname_phase`.
- **Scratch schema:** a throwaway schema the check builds instead of touching `bigname_phase`. The check rewrites the literal `bigname_phase` in each file to the scratch name.
- **Login:** a throwaway role the check creates for one run and drops at exit.
- **Configured user:** the database user in the URL the check is given.
- **Writer:** the database user a deployment runs schema-migrations as.
- **Replay:** applying a sequence of schema-migrations the way `sqlx migrate run` does.
- **Predecessor-shape proof:** a test that rewinds part of the scratch schema to the shape it had before a schema-migration, applies that schema-migration, and checks the result.
- **Plant:** an example the check writes on purpose to prove a rule refuses it.

## How the check replays schema-migrations

- **As a throwaway login.** Apart from the replays under the real schema name, every baseline file and schema-migration the check applies runs on connections authenticated as the login, not as the configured user. The login owns the scratch schema and holds no privilege on `bigname_phase` and no `CREATE` on the database. The rewrite to the scratch name is textual, so it cannot see a name a schema-migration assembles at run time (`'bigname_' || 'phase.…'` inside `EXECUTE`, or a search-path-relative name in a `DO` body). Such a name fails on the production schema instead of changing it unobserved. A plant proves it: an assembled production name must be refused, after `RESET ROLE` too, and its rewritten twin must succeed.
- **Provisioned by the configured user.** That user needs `CREATEROLE` to create the login. It needs `CREATEDB` because the replays under the real schema name run in a database of their own, created for those replays and dropped when they finish. On an external server the whole check also runs in a database of its own, created at start and dropped at exit, rather than asking for `CREATE` on the database the URL names. The scratch schemas are created for the login rather than granted to it.
- **No schema text as the configured user.** Apart from the replays under the real schema name, no baseline or schema-migration text runs on the configured user's connection except the baseline's two reviewed `CREATE EXTENSION` lines, matched whole. That connection otherwise runs only the check's own setup (the login, its schemas and sqlx's bookkeeping table, refused if the database already has one), snapshots, plants and cleanup.
- **Like sqlx.** A replay runs all its files through one session, each in its own transaction unless it opens with `-- no-transaction`, with sqlx's `_sqlx_migrations` bookkeeping recorded inside that transaction by its unqualified name. So a setting one file commits is in force for the files after it, and a file that moves `search_path` breaks the bookkeeping exactly where sqlx would. A planted sequence proves both on every run.

## The frozen catalog

`frozen-schema.txt` is the catalog of the baseline plus the inventoried schema-migrations. Regenerate it with `SCHEMA_V2_APPLY_CHECK_WRITE_FINGERPRINT=1` in the change that moves the schema. It records:

- the baseline's extension declarations;
- every relation, with its persistence (logged, unlogged or temporary), the tablespace it is stored in, its access method, its privileges, storage parameters, row-level-security flags, replica identity, partitioning and parents;
- every column: type, nullability, default, identity, generation, collation, storage, compression, statistics target, attribute options, privileges, whether it is defined locally or inherited, and the value rows that predate the column read once it is no longer the default;
- every constraint, with whether it is defined locally or inherited, which decides whether `NO INHERIT` removes it;
- every index, with its tablespace (which `pg_get_indexdef` does not print), its validity and its replica-identity and `CLUSTER` flags, and every view;
- every routine: its full argument list with defaults, execution modes, planner cost and rows, privileges, and a digest of its body. The digest treats each run of whitespace outside quoted text and comments as one space, because `20260923140000_project_name_surfaces_label_indexes.sql` and the baseline indent `label_hashes` differently. String literals, quoted identifiers, dollar-quoted strings and comments are compared as written;
- every trigger with its firing state, including a foreign key whose internal triggers no longer all fire, which would stop enforcing it while its definition reads the same;
- every sequence: its whole range, cache, cycle and owning column;
- types, domains and comments;
- the schema's own privileges and its default privileges, both those set in the schema and the owner's global ones;
- ownership: any object not owned by the schema's owner is named, and the schema itself is named when the role that runs the schema-migrations no longer owns it. Privileges write that owner as `owner`, so the catalog reads alike whoever owns the schema.

Every value that holds more than one element (privileges, storage parameters, inherited parents, enum labels, a domain's constraints, a composite type's attributes, a routine's configuration, a role's or database's connection defaults) is encoded as a JSON array rather than joined with a delimiter. Otherwise an element carrying the delimiter would let two different schemas print identically.

So a change to a baseline file or a schema-migration that moves the schema fails until the catalog is regenerated in the same change, whatever the object is called. Column order is not in the catalog; see [Columns never move](#columns-never-move).

## Fresh, upgraded, older and populated databases

The catalog must come out the same whichever way a database reached the head:

- **Fresh:** after the baseline alone. The schema-migrations are no-ops before the schema exists.
- **Upgraded:** after the schema-migrations run over the baseline. This must equal the fresh catalog, so a schema-migration without its baseline edit fails here, before `frozen-schema.txt` is even consulted.
- **At the previous revision:** a baseline edit without its schema-migration passes the comparison above, because every schema-migration is a no-op on the edited baseline. So the check also migrates the previous revision's baseline the way `sqlx migrate run` migrates a database at that revision. The result must be the current catalog.
  - Every version in the previous `migrations/` is recorded without being run, a recorded file that is gone or whose bytes changed is refused as sqlx would refuse it, and only the phase schema-migrations added since are applied. Rerunning an older file could make it seem to carry a baseline-only edit, so older files are never rerun.
  - The previous baseline's catalog carries its own extension declarations, and the migrated one carries those plus each `CREATE EXTENSION` a schema-migration added since. The configured user installs the current declarations before any replay, so the database alone would report an extension an initialized database lacks. So a baseline that gains an extension without a schema-migration that creates it fails.
  - Removing an extension is not modelled: the previous baseline's own declaration would run as the login and fail.
- **Populated:** the fresh catalog holds no rows, so a schema-migration whose DDL runs only when a table has data would leave it unchanged. The check therefore also takes the catalog of its scratch schema at the end of the run, and it must match too. By then that schema has been populated by every predecessor-shape proof and behavior test, rewound to older shapes by those proofs, and carried back through the whole inventoried sequence, as sqlx would carry an initialized database.

**The previous revision** is the base branch on a pull request and the parent commit on a push. Locally it is `origin/main` while the branch is not yet part of it, and `SCHEMA_V2_PRIOR_INVENTORY_REF` names it explicitly. In CI a base commit that cannot be fetched or found fails the run. Without git history, or where the base commit carries no inventory or baseline, the check skips the previous-revision comparisons and says so; outside CI it does the same when the base commit is missing.

## The schema-migration history

- **The inventory.** `migration-inventory.txt` lists every schema-migration file in order with the SHA-384 of its bytes, the checksum sqlx records on apply and rejects on any later mismatch. `migrations/` must equal it exactly, bytes included, so an edit to an applied file fails here before it fails every initialized database.
- **File names.** Every file is named `<14-digit version>_<description>.sql`, and no two files share a version, because sqlx identifies a file by its version.
- **No rewriting history.** The inventory is editable too, so the check also reads the previous revision's inventory. It refuses a new entry whose version sorts at or below the previous head, a previous entry that is gone, and a previous entry whose bytes changed. sqlx applies whichever versions a database has not recorded, so a file that sorts below the head would run on every initialized database while the freeze recorded nothing. A schema-migration lands by joining the inventory after the head and advancing the head in the same change.
- **The named head.** The head ADR 0008 names and the one [`docs/storage.md`](../docs/storage.md) names must both be the newest file in `migrations/`. The check reads the first path of the form `migrations/<14-digit version>_<description>.sql` in each document, so nothing above the head may quote another schema-migration by that path.
- **Every phase schema-migration is exercised.** Each phase schema-migration must be applied both on an empty database, where it must be a no-op because the phase runner has not installed the baseline yet, and on an initialized-schema path. Both applications are written out in `apply-check.sh`: a new schema-migration joins the empty-database list, and it also joins the baseline-first list or gets a predecessor-shape or other specific test that applies it to an initialized schema. A file may be skipped only through a named, reasoned entry in the script's skip list.
- **The ledger.** After each replay, `_sqlx_migrations` must equal the full expected history: one row per schema-migration file, in order, with the SHA-384 of its bytes. A file that rewrites or deletes earlier bookkeeping fails even though no catalog line moves.
- **No reading the ledger's timing.** The replay records `installed_on` as its own clock and `execution_time` as zero, because the real values cannot be reconstructed. So a phase schema-migration may not read either column; one that needs to branch on history branches on the recorded version.

## How schema-migrations may name the phase schema

- **One spelling.** Every schema-migration newer than the legacy-schema drop must spell the phase schema exactly `bigname_phase`, in lowercase, everywhere in the file. PostgreSQL folds an unquoted `BIGNAME_PHASE` to the same schema, but the inventory match and the rewrite see only the lowercase literal.
- **No Unicode escapes.** Nor may such a file use a Unicode-escaped identifier or string (`U&"..."`, `U&'...'`), which can spell `bigname_phase` without containing the literal. No schema-migration needs the form.
- **Only index drops without the name.** Since #849 the check applies every schema-migration that names `bigname_phase` and fails on one it does not list. It finds them by that literal name, so a schema-migration written against the connection's search path would escape it. The check closes that by rule rather than by parsing: a schema-migration newer than the legacy-schema drop that names no `bigname_phase` object may consist only of `DROP INDEX` statements whose every target is written `schema.name`:
  - with plain identifiers and nothing quoted: no strings, quoted identifiers, dollar quoting or block comments;
  - without `CASCADE`, which would take a dependent `bigname_phase` object with it, unlisted;
  - with `CONCURRENTLY` only in a file that opens with `-- no-transaction` and only over one index, since PostgreSQL runs it only as a top-level statement and over one index.

Why only indexes:

- `RESTRICT` protects only the dependencies PostgreSQL records. A `bigname_phase` PL/pgSQL routine that calls `public.helper()`, selects from `public.helper_view` or reads `nextval('public.helper_seq')` in its body records none, so dropping that object succeeds and the routine fails at its next call. An index is the one target no routine body can depend on that way.
- `DROP TABLE` is refused even with `RESTRICT`: a table in another schema can be an inheritance child or a partition of a `bigname_phase` table, and PostgreSQL drops it without complaint, taking the rows visible through the phase parent.

Any other statement, expression or routine call is refused in such a file, so a search-path-relative name cannot be written outside the inventory. A schema-migration that drops anything else names `bigname_phase`, so it is inventoried, applied and observed.

## Closed object kinds

The phase schema holds only the kinds the catalog describes: tables, views, sequences, indexes, constraints, triggers, functions and procedures, enum and domain types, and comments on those. The check refuses, by kind:

- an aggregate or window function;
- a range, multirange, composite or shell (declared but undefined) type;
- an operator, operator class or operator family;
- a materialized view, a partitioned table or index, a typed table, or a foreign table;
- a rewrite rule or a row policy;
- an extended-statistics object, a collation, a conversion or a text-search object;
- a cast to or from a phase type.

On every run it plants an example of most of these kinds in a transaction it rolls back, and each must be named; a foreign table, an operator class and a shell type are planted only when the configured user is a superuser. A carve-out that needs one of these kinds extends the rule and the catalog under ADR 0008, so nothing outside the catalog's vocabulary can enter the schema unobserved.

The same rule is applied to the fresh and populated schemas and to the migrated previous-revision baseline. It also refuses:

- **A foreign key with more than one index that could back it.** PostgreSQL backs a foreign key with the first valid matching unique index in index OID order, and the catalog does not print which. With two candidates, two histories that print alike would drop or cascade differently.
- **An object that belongs to an extension or depends on one** (`ALTER FUNCTION ... DEPENDS ON EXTENSION`). `DROP EXTENSION` takes it along, and the catalog prints neither relationship.

## Tables, names and comments

- **The expected tables.** The phase schema's tables must be exactly the ones the check's expected-table lists name, so a missing or unexpected table fails, and a new table joins both lists.
- **Forbidden names.** A table or column whose name matches the forbidden-name patterns is refused, for example a table name containing `coverage`, `generation`, `checkpoint`, `queue` or `watermark`, or a column name containing `generation`, `repair`, `epoch` or `promotion`.
  - A table name authorized anyway goes into the table allowlist beside the pattern, whose comment makes each entry a carve-out argued under ADR 0008; `project_generation_failures` is the one table there today.
  - Column names have their own allowlist, whose comment asks only for maintainer authorization. It holds four columns today: `lineage_orphaning_epoch` on `chain_heads` and on `discovery_watch_admissions`, `manifest_authority_attestations.generation_token` and `chain_phase_state.redo_attempt_generation`.
  - A table whose name ends in `_staging` or `_publication` is refused outright; that rule has no allowlist.
- **Comments.** Every table and column carries a comment.
- **No repair or accounting apparatus.** `normalized_events` may have no column for repair, supersession, authority, revision or generation, and the current projections no exhaustiveness-accounting column.
- **Required invariants.** The raw tables keep `block_hash` in their primary keys, the registry-operator projection indexes exist, and a named list of behavioral constraints and indexes must exist. Removing one fails even when `frozen-schema.txt` is regenerated.
- **No btree over unbounded input.** A btree index may not cover the listed externally controlled columns, for example `name_current.raw_name`, `children_current.raw_name` or `normalized_events.after_state`. A name-text index over `name_current.raw_name` under carve-out 5 therefore cannot be a plain btree.

## Schema-migrations change the shape, not the facts

The populated pass, and the replay of the same rows under the real schema name, must keep:

- every table's row count;
- every existing sequence's position (its value and whether that value was handed out), since a reset sequence hands an ID out again;
- the exact contents of every table, compared over the columns it had before the replay, so a column the replay adds is not a change.

The exception is the tables a schema-migration may backfill in place, which keep only their row count:

- the phase runner's coordination state: `chain_phase_state` and `service_heartbeats`;
- Interpret's redo coordination: `discovery_watch_admissions` and `project_redo_*`;
- manifest synchronization's `manifest_*` rows;
- the [resolution divergence ledger](../docs/glossary.md#resolution-divergence-ledger), `resolution_divergences`;
- Project's rebuildable projections: the `*_current` families, `permissions_current_resource_summary` and `child_registration_events`.

The list follows the table ownership in [`docs/storage.md`](../docs/storage.md). Everything else is one of three things:

- what Ingest recorded or Interpret derived: chain data, [raw facts](../docs/glossary.md#raw-fact), [contract instances](../docs/glossary.md#contract-instance), identity rows, [discovery edges](../docs/glossary.md#discovery-graph--discovery-edge), [label preimages](../docs/glossary.md#preimage-observation--label-preimage), [normalized events](../docs/glossary.md#normalized-event) and Interpret's diagnostics. A redo re-derives from or builds on these rows rather than repairing them;
- the operator's rainbow candidates, `ens_names`;
- the audit of each [projection generation failure](../docs/glossary.md#projection-generation-failure).

A listed table, or a sequence, that the replay drops counts as retired; only a surviving sequence must keep its position. Any other table may neither be dropped nor lose a column. A table the list does not name is compared, so a new table is covered until review adds it.

The rule proves itself on the populated rows, inside a transaction it rolls back. It must see each of these: a deleted normalized event, a rewritten one, a rewritten resource, a rewritten row of the first populated discovery or identity table, a dropped `raw_logs`, a column dropped from `normalized_events`, and a restarted sequence.

## Columns never move

Column order is not in the catalog: a column a schema-migration adds sits last on an initialized database but wherever the baseline lists it on a fresh one. Instead, reading the live order on each replay, the check requires that:

- a replay never moves a column a table already had;
- the columns it adds come after them;
- a table a schema-migration creates from nothing matches the baseline's layout.

Otherwise `SELECT *`, a positional `INSERT`, a composite value and `row_to_json` would read differently on a fresh and on an upgraded database.

## Replays under the real schema name

The rewrite from `bigname_phase` to the scratch name is textual, so a name the rewrite cannot see escapes it, for example one assembled from pieces. Compared as a value, such a name takes one branch in the check and another in production. So the fresh baseline, the previous-revision transition and, where the configured user is a superuser, the populated rows are replayed once more without the rewrite, in a database of their own where the schema really is `bigname_phase`. They must give the same catalogs, object kinds and column order.

- **As the configured user.** They run as the configured user rather than the login. Where the two differ in what a file's branch tests, a file that branches on who runs it takes its other path here, however it reads the answer. It fails when that path changes what these replays compare. A path that only changes rows they do not compare, such as the contents of a table a schema-migration may backfill, or any rows when the populated replay is skipped, is left to review.
- **With the rows.** The populated replay copies the scratch schema's rows and sequence positions, with triggers suspended, and checks the row counts and positions equal on both sides before it replays. It is held to the row rule above, so a branch on rows and the name together is covered as far as those rows reach. Copying rows needs `session_replication_role`, a superuser setting, so a run whose configured user is not a superuser skips this replay and says so.
- **Nothing else in that database may change.** A failure the login meets can be swallowed and then succeed as the configured user. So nothing outside the phase schema may appear or change in that database, down to owners, privileges and the definitions of relations (columns, constraints, indexes, triggers, rules, policies), routines, types, operators, operator classes and families, statistics and collations. That includes the `_sqlx_migrations` ledger and the member objects of every extension.
- **Outside that database.** Such a file runs its other path with the configured user's privileges. What it does outside the database, to roles for example, is reported by the [snapshot of roles, databases and the server](#roles-databases-and-the-server) but not undone. A file like that is exactly what these comparisons exist to refuse.

## Files must behave the same under sqlx as in the check

### Statement rules

These apply to every baseline file and schema-migration. The check reads each file's statements with each comment read as a space, as PostgreSQL reads it, and quoted text kept, so a comment cannot hide a statement, and a string that looks like one is refused rather than trusted. A file it cannot split into statements is refused.

- **No session state.** The baseline session and the sqlx run would carry it into every later file. Refused:
  - a statement-leading `SET` or `RESET` of any setting, including a custom placeholder or quoted name, and a quoted or `format`-built `SET` run through `EXECUTE`;
  - the SQL-standard `SET TIME ZONE`, `SET SCHEMA`, `SET NAMES`, `SET XML OPTION` and `SET SESSION CHARACTERISTICS`;
  - `SET [SESSION | LOCAL] ROLE` and `SET SESSION AUTHORIZATION`;
  - any `set_config` whose last argument is not the literal `true`, in a routine body too.

  A routine that needs a setting uses `set_config(..., true)` and restores it.
- **No role or database defaults, and no passwords.** `ALTER ROLE`, `ALTER USER` or `ALTER DATABASE ... SET` or `RESET`, and a `PASSWORD` in `ALTER` or `CREATE ROLE`, `USER` or `GROUP`, are refused, in quoted text and `EXECUTE` strings too.
- **No psql backslash commands** outside quoted text or a comment. The replay feeds each file to psql, which runs the command on the client, while sqlx sends the file to the server, which rejects it. The check would pass a file deployment refuses, and a `\set ON_ERROR_STOP 0` would hide every later file's errors.
- **No psql variables.** No colon before a name outside quoted text or a comment (`:name`, `:'name'`, `:"name"`, `:{?name}`): psql replaces it with a variable it defines, `DBNAME` and `USER` among them, before sending, while sqlx sends the colon. A cast (`::`), `:=` and a numeric array slice bound pass; a slice bound that starts with a name takes a space after the colon.
- **Nothing outside the databases.** No catalog the check compares holds any of this. Refused:
  - `ALTER SYSTEM`, which PostgreSQL runs only as a top-level statement, so the text always shows it;
  - `COPY` to or from a file or a program, which reads or writes a file on the database server or runs a program there, and `lo_import` and `lo_export`, which read or write files on the database server. The file name may be a literal, a dollar quote or a `format()` slot;
  - a call to a server administration function: replication slots and origins (`pg_replication_*` and the `*_replication_slot*` functions, `pg_replication_slot_advance` and `pg_replication_origin_advance` among them), logical decoding (`pg_logical_*`), WAL and recovery control (`pg_switch_wal`, `pg_create_restore_point`, `pg_promote`, `pg_wal_replay_*`, `pg_backup_start` and `pg_backup_stop`, `pg_log_standby_snapshot`), `pg_reload_conf`, `pg_rotate_logfile`, `pg_terminate_backend`, `pg_cancel_backend` and `pg_stat_reset*`.
- **No publication or subscription DDL, and nothing that sets a sequence's position or changes how it counts.** A production database can hold an operator's own publication, subscription or sequence outside the phase schema, which no replay database has. A change guarded on one existing would pass every comparison and still change what production replicates or which values the sequence hands out. Refused in every statement but `COMMENT ON`, whose text never runs, in quoted text and `EXECUTE` strings too, with an `E'...'` newline or tab read as a space:
  - `CREATE`, `ALTER` or `DROP` of a `PUBLICATION` or `SUBSCRIPTION`;
  - `setval`, `ALTER SEQUENCE` and `TRUNCATE ... RESTART IDENTITY`;
  - an identity column's `RESTART`, its sequence options (`SET INCREMENT`, `SET MINVALUE`, `SET START` and the rest) and `DROP IDENTITY`;
  - `SET LOGGED` and `SET UNLOGGED`. `ALTER TABLE` applies them to a sequence too, and an unlogged sequence starts over after a crash. An unlogged table also drops out of a publication for all tables or for its schema.

  No current file uses them, and none has needed to: the row rule keeps every phase sequence at its position, and the frozen catalog records how every phase sequence counts and every table's persistence. A phase change that truly needs one, such as widening a phase sequence with `ALTER SEQUENCE ... AS bigint`, extends this rule under an ADR 0008 carve-out. `nextval` stays allowed, because inventoried schema-migrations call it and it can only skip values, which a sequence never promises not to do.
- **No direct writes to system catalogs.** An `UPDATE`, `INSERT`, `DELETE` or `MERGE` on a `pg_*` catalog table changes an object without the statement that names the change, so no text rule sees it, and on an object only production holds no comparison sees it either. Refused under the same reading as the rule above.
- **No `CASCADE` in newer schema-migrations.** A schema-migration newer than the legacy-schema drop may not use `CASCADE`, other than a foreign key's `ON DELETE` or `ON UPDATE CASCADE`. It reads the text the way the rule above does: every statement but `COMMENT ON`, quoted text and `EXECUTE` strings included, with an `E'...'` newline or tab read as a space. In production `CASCADE` also drops objects that depend on its target and that only production holds, such as an operator's view over a phase column or a table's entry in a publication whose column list or row filter names the column. The drop would pass here and be silent there. Without `CASCADE` the statement fails in production instead. No such schema-migration uses it.

### Nothing left in the session

sqlx applies only the files a database has not recorded, and a catch-up can be split across several runs. So what one file leaves in the session reaches the next file in the replay, but not in a deployment that starts that file on a fresh connection.

After each applied file's commit and bookkeeping, the check probes the replay connection and refuses, naming the file:

- a session-level setting, however it was made (PostgreSQL does not list custom placeholder settings, which only the statement rule sees);
- a temporary table, routine or type;
- a prepared statement or a holdable cursor;
- a session advisory lock or a `LISTEN`;
- an assumed role;
- a transaction that a `-- no-transaction` file leaves open;
- a change to the login's own connection defaults, memberships or default privileges since the sequence began. A later file that reverts it would hide it from the end-of-replay snapshot, while a deployment interrupted between the two keeps it.

What ends with the file's transaction passes: `ON COMMIT DROP`, a temporary table the file drops again, a transaction-level advisory lock, `set_config(..., true)`. The one planted sequence that commits a setting on purpose, to prove the replay is a single session, runs without the probe, and the probe proves itself on planted files.

The baseline installer runs the whole baseline as one transaction, so the check probes inside that transaction after each baseline file. It refuses, naming the file, a setting that no longer reads what it did once the installer's own `SET LOCAL`s ran, however it was made, a temporary object, a prepared statement, a cursor, an advisory lock and an assumed role. After the commit it also refuses a `LISTEN`, without naming which file issued it. Its plants include a `SET` assembled inside `EXECUTE`.

### No branching on who or where

The replay runs as the login, sometimes in a database of its own, while deployment runs as the writer. So a phase schema-migration that reads any of the following would take a path in the check that deployment does not take, and is refused by name, in quoted text too:

- **Who runs it:** `current_user`, `session_user`, `current_role`, `system_user`, `getpgusername`, `pg_get_userbyid`, `pg_has_role`, a `has_*_privilege` function, `pg_roles`, `pg_user`, `pg_authid`, `pg_auth_members`, `pg_shadow`, `to_regrole` or `regrole`, `rolname`, `rolsuper`, `usename`, `is_superuser`, `session_authorization`, the `information_schema` role and privilege views, or a caught `insufficient_privilege` or `undefined_object`, by name or SQLSTATE. Bare `user` is left alone because the rule reads quoted prose too, and bare `role` because `manifest_contract_instances.role` is a column.
- **Where it runs:**
  - the database: `current_database`, `current_catalog`, `pg_database`, `datname`, `datid`, `catalog_name`, or any other identifier ending in `_catalog` (except `pg_catalog`);
  - the server or client address or port: the `inet_*` functions, `client_addr`, `client_port`, `client_hostname`, `pg_stat_activity`, `pg_stat_get_activity`, the `pg_stat_get_backend_*` functions, `pg_stat_database`, `pg_stat_ssl`, `pg_stat_gssapi`, and the `port`, `listen_addresses`, `unix_socket_directories` and `cluster_name` settings;
  - the server's files: `pg_read_file`, `pg_read_binary_file`, `pg_stat_file` or a `pg_ls_*dir` function.
- **Settings:** a setting answers with the connection's defaults, which the login does not share with the writer (`ALTER ROLE ... SET`), and some name who and where (`session_authorization`, `port`). So the only settings a phase schema-migration reads are `search_path` and `quote_all_identifiers`, to put them back after `set_config(..., true)`, by a quoted literal passed to `current_setting`, quoted or not. That holds in a routine body it creates too, which the rule reads like the rest of the file, so a trigger that needs another setting lands under a carve-out that extends the rule. Any other read, `pg_settings`, `pg_show_all_settings`, `pg_file_settings`, and the word `SHOW` anywhere in the text are refused.
- **Swallowed failures:** a handler for every error (`WHEN OTHERS`) or every access-rule violation (`syntax_error_or_access_rule_violation`, `SQLSTATE '42000'`), which would swallow the privilege failure the login meets where the writer succeeds.
- **The temporary namespace:** `pg_my_temp_schema`, `current_schemas` or `pg_is_other_temp_schema`. A file that creates and drops a temporary table leaves that namespace allocated for the files after it in one `sqlx migrate run`, but not in a catch-up split across runs. The session probe cannot refuse this, because nothing frees the namespace before the session ends, and the frozen `20260917141000` allocates it.

## Roles, databases and the server

Before the first replay the check takes a snapshot of role, database and server state that the frozen catalog does not hold, and every replay must leave it unchanged:

- connection defaults set for any role or database, in any database, not only the one the check runs in;
- every role attribute: `LOGIN`, `SUPERUSER`, `BYPASSRLS`, `CREATEDB`, `CREATEROLE`, `REPLICATION`, `INHERIT`, connection limit and expiry;
- role memberships;
- the default privileges of every schema in the database, not only the phase schema's;
- every database's attributes: owner, connection limit, whether it accepts connections, the template flag, tablespace and privileges;
- privileges on server parameters (`GRANT SET` or `ALTER SYSTEM ON PARAMETER`);
- tablespaces (owner, privileges, options), replication slots and origins, subscriptions, and comments and security labels on shared objects. A slot or origin is recorded by what it is, not by its progress, which a live replica or subscriber moves on its own; the functions that move it are refused by the statement rule instead;
- where the check is a superuser, every line of the server's configuration files, including the `postgresql.auto.conf` that `ALTER SYSTEM` rewrites;
- where the check can read `pg_authid`, the password verifiers.

A change to any of these fails however the statement was spelled, even though no catalog line moves. Whether the check can read `pg_authid` is decided before the statement is sent, because PostgreSQL checks the relation privilege when the scan opens, and the documented external-server user is not a superuser.

**Passwords.** The check first tries its login with a wrong password. Where the server refuses it, the check also reconnects as its login after each replay, with the password it created the login with. That is all a run that cannot read `pg_authid` has, since `pg_roles` shows one mask for every password, and a run that can do neither refuses to start. The replays under the real schema name run as the configured user. There the check first confirms the server refuses that user a wrong password, then reconnects with the configured credential after each replay. It refuses to run those replays where it can neither do that nor read `pg_authid`. The check never tries a wrong password when psql runs inside the database container, so there it must be able to read `pg_authid`.

## The check tests itself

Most rules above prove themselves on every run. The check plants files, statements or changes it must refuse, and forms it must accept, and fails if a plant is missed or a form refused. Each snapshot plant is restored straight away, and cleanup restores one that a failed run leaves in flight. These rules have no plants: the history comparisons, the named head, the inventory match, the one-spelling rule, the rule that every phase schema-migration is exercised, the ledger contents, the ledger-timing rule, and the rules under [Tables, names and comments](#tables-names-and-comments).

The tablespace plants move a table and an index to a tablespace the check creates with `allow_in_place_tablespaces`, which PostgreSQL 15 and later provide so a test needs no server directory; it is dropped straight after.

Some plants are skipped, each with a note in the log: those that need a superuser when the configured user is not one, the `ALTER SYSTEM` plant when a configuration file already sets its parameter, and the `pg_global` tablespace-option plant when `pg_global` already has options. The run's last line reports how many refusal assertions passed out of the expected total, which the skipped plants lower.

## Limits

The check is built to catch a schema-migration that would behave differently under sqlx on a production database through ordinary SQL, including dynamic SQL whose effect it can observe at run time:

- an assembled phase schema name compared as a value, by the replays under the real name, as far as their rows reach;
- an identity read, however it is spelled, by the same replays run as the configured user. That works where the configured user differs from the login in what the branch tests, and where the other path changes the schema or anything outside it;
- an assembled password change, by the password-verifier snapshot or the reconnect.

Beyond it:

- a branch keyed to an identity neither the login nor the configured user has, such as a production-only role, is beyond any check that does not run as that role;
- likewise, an object only the production database holds outside the phase schema, such as an operator's own table, is beyond any replay that does not hold it. The statement rules refuse what no current file uses: publication and subscription DDL, the statements that set a sequence's position or change how it counts, direct catalog writes and `CASCADE`. Left to review:
  - any other change to such an object, such as dropping and re-creating an operator's sequence guarded on it existing, which the rules cannot refuse because inventoried schema-migrations use `DROP SEQUENCE` and `CREATE SEQUENCE`;
  - how an operator's publication for all tables or for the phase schema follows ordinary phase-schema changes: a table created, dropped, moved to another schema, attached or detached as a partition;
- SQL written to hide from a text rule what no run-time read observes, such as a keyword or role name assembled from pieces, is outside what a conformance check can close. Review is the control for it.
