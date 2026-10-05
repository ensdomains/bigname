# ENSv2 registry token index

The API reads the latest published `TokenResourceLinked` or `TokenRegenerated` event for
each selected registration resource. `normalized_events_registry_token_idx` keys those
activated, readable ENSv2 events by chain, resource and descending block/transaction/log
position. Its predicate excludes other event kinds and events without real positions;
lineage eligibility remains a query check. It serves
`storage:normalized_events.registry_tokens` in `crates/storage/src/registry_token_ids.rs`.

This is an API-owned read-only access path on Interpret's table. It changes no rows,
projection state, event identity or interpreter semantics, and requires no backfill or
Interpret/Project redo. It belongs to the indexes dropped during a from-zero walk or
full-history Interpret redo and rebuilt before Project/serving resumes; see
[`ops/walk-index-set`](../walk-index-set/README.md).

A synthetic isolated PostgreSQL run with 800,000 events, 200 eligible token events and
1,000 newer non-token events per requested resource measured a 200-resource generic
prepared query at 188 ms / 205,293 token-probe buffers before the index and 0.97 ms / 600
buffers after it. No-evidence requests used 400 buffers. The index was 40 KiB, and its
local build including process startup took 0.12 seconds. These are synthetic measurements,
not a production sizing or build-time estimate. Transactional write probes measured 10,000
non-token inserts at 43.7→47.8 ms, 2,000 token inserts at 9.3→15.2 ms and 2,000 candidate
activations at 65.0→72.7 ms. The orphan update measurements were noisy (104.8→60.1 ms),
so they establish no performance improvement. Inserts/activation/canonicality updates
maintain the partial index; storage scales with eligible token events.

For a large initialized deployment, prebuild before applying the release's
`20261005090000_normalized_events_registry_token_index.sql` migration. With the writer
connection supplied through the normal operator environment, run from the release checkout:

```sh
psql "$BIGNAME_DATABASE_URL" -X -v ON_ERROR_STOP=1 -f ops/registry-token-index/install.sql
```

Run outside a transaction. `CREATE INDEX CONCURRENTLY` permits writes and may wait for a
running batch transaction; inspect `pg_stat_progress_create_index` rather than restarting
that batch. The installer allows that wait and sets a six-hour statement timeout. Preserve
its output and exit status in the deployment receipt. It prints the definition, size and
`indisvalid`/`indisready` flags, and succeeds only when the index belongs to
`bigname_phase.normalized_events` with the exact reviewed definition. Check progress with:

```sql
SELECT * FROM pg_stat_progress_create_index
WHERE relid = 'bigname_phase.normalized_events'::regclass;
```

The installer checks before and after building. It refuses invalid/not-ready indexes,
wrong definitions, another table's index, or a non-index relation under the same name.
The migration likewise fails instead of recording a successful adoption. After a successful
prebuild, run the release's usual schema migrations; adoption is idempotent. Fresh databases
receive the same definition from the baseline. Without a prebuild the migration uses ordinary
`CREATE INDEX`, which blocks writes for the build duration.

An interrupted or timed-out concurrent build can leave an invalid index. `IF NOT EXISTS`
does not repair it. Confirm no build is running, then drop only the failed index outside a
transaction and rerun the installer:

```sql
DROP INDEX CONCURRENTLY bigname_phase.normalized_events_registry_token_idx;
```

Use the same recovery for an index with the wrong definition. If another relation holds the
name, inspect and rename/remove that relation deliberately before retrying. Preserve a valid
index with the reviewed definition after an uncertain client response: rerunning the installer
will verify/adopt it. A failed migration must be retried only after the index passes installation.
`scripts/check-schema` verifies baseline/migration/prebuild equality, rerun adoption and refusal
of invalid, not-ready, wrong-table, wrong-key and non-index stand-ins.
