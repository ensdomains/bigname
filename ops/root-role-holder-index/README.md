# Root role holder index

`GET /v1/registries?role_holder=<address>` lists the ENSv2 registries on which an address holds
registry root roles ([route](../../docs/api-v1-routes.md#get-v1registries)). Its page groups the
holder's live root grants by registry address, one chain at a time in chain order, in
lower-cased address order after the cursor (`crates/storage/src/families/topology/role_holder.rs`).
One partial index on `bigname_phase.project_grant` serves it:

| Index | Keys | Rows | Serves |
| --- | --- | --- | --- |
| `project_grant_root_role_holder_idx` | `subject COLLATE "C"`, `chain_id COLLATE "C"`, `lower(scope_detail ->> 'registry_address') COLLATE "C"` | grants with scope `root`, not revoked | the role-holder page on each chain |

The index condition carries the holder, the chain and the page's address bound: the cursor's
registry, or the start of the chain. The page reads the holder's live root grants on that chain
from the bound, in page order, and no other row, so it needs no sort.

`project_grant` also holds every per-name registry grant and every resolver grant, which far
outnumber the root grants, and the root grants that were revoked. Without the index the API
returns the same pages, but the statement reads every grant of the holder through
`project_grant_subject_resource_idx` and tests each row, so a holder of many names reads all of
their grants for each page.

Installing it changes an access path only: no grant, phase marker, or
[content hash](../../docs/glossary.md#interpreter-content-hash) input changes.

The index is advisory ([table ownership](../../docs/storage.md#table-ownership)). The phase
runner never builds it, and no walk drops it. Project keeps it current like the table's other
indexes. The API starts and serves without it, with a WARN at startup when it is not in place.

## Building the index

Build the index on every database the build is deployed to before its schema-migrations run.
Schema-migration `20261112170000_project_grant_root_role_holder_index.sql` adopts it by
name and refuses a populated table without it, so it never builds it in place: an ordinary
`CREATE INDEX` on a populated table would block writes to `project_grant` for the whole build.

Run `install.sql` by hand with the writer role and `psql -X -v ON_ERROR_STOP=1 -f install.sql`.
Do not wrap it in a transaction. It builds the index with `CREATE INDEX CONCURRENTLY`, so the
phase runner and the API keep running. The build reads the whole table twice and waits for
every transaction older than itself, including a running Project batch. Inspect
`pg_stat_progress_create_index` rather than restarting that batch. The script permits the wait
and bounds the build to six hours. The previous image does not read the index. Retain the
script's output in the deployment receipt.

The index holds one entry per live root grant, so its size grows with those grants, not with
the table. No `ANALYZE` is needed afterwards. If the same release carries another
`project_grant` index, build them one after the other in one session, so the table is read
for each build in one maintenance window.

## Checking the index

The script checks the name twice and fails, with a non-zero `psql` exit, instead of reporting
success over an index the page cannot use. Before it builds anything, it refuses the name when
it is already taken by an index that is not both `indisvalid` and `indisready`, an index on
another table, an index whose definition is not the reviewed one, or a table, view, or other
relation that is not an index. A name that resolves to nothing passes this first check. After
the build it makes the same check and also requires the index to exist. It prints the index row
before the last check, so the receipt shows the flags and the definition either way. Rerunning
the script on a database that has the index builds nothing and repeats the check.

Before the schema-migration runs on a database, the index must exist there, valid and ready,
with the reviewed definition:

```sql
SET search_path = pg_catalog;
SELECT c.relname, c.relkind, i.indrelid::regclass, i.indisvalid, i.indisready,
       pg_get_indexdef(i.indexrelid)
FROM pg_class AS c
LEFT JOIN pg_index AS i ON i.indexrelid = c.oid
WHERE c.relnamespace = 'bigname_phase'::regnamespace
  AND c.relname = 'project_grant_root_role_holder_idx';
RESET search_path;
```

It must return one row with `relkind` `i`, `indrelid` `bigname_phase.project_grant`, and
`indisvalid` and `indisready` true. The definition must be exactly this line, which is the
text the schema-migration and the script compare:

```text
CREATE INDEX project_grant_root_role_holder_idx ON bigname_phase.project_grant USING btree (subject COLLATE "C", chain_id COLLATE "C", lower((scope_detail ->> 'registry_address'::text)) COLLATE "C") WHERE ((scope = 'root'::text) AND (NOT revoked))
```

## Recovery

An interrupted concurrent build, for example one cancelled or stopped by the six-hour limit,
leaves an invalid index under the intended name. `IF NOT EXISTS` matches on the name alone, so
it does not repair that index. Rerunning the script stops at the first check and names it.
Nothing drops or rebuilds an index automatically. To recover, first confirm in
`pg_stat_progress_create_index` that no build is still running. Then drop only the named index
with `DROP INDEX CONCURRENTLY bigname_phase.project_grant_root_role_holder_idx`, as the
error's hint spells out, and rerun the script. Recover a valid index that fails the definition
check the same way. If the name belongs to a table, view, or another table's index, remove or
rename that relation first. Never drop a valid index with the reviewed definition merely
because an installation was retried.

The schema-migration ends with the same checks, read under the same settings and put back
before the block returns, so the SQLx run fails rather than recording success if the name is
not an index on `bigname_phase.project_grant`, is not valid and ready, or does not have the
reviewed definition. Recover as described above, then run the schema-migrations again.
`scripts/check-schema` proves the schema-migration's build on an empty table, its adoption of
this script's index, and each refusal.
