# Blue-Green Cutover Runbook

A deploy of this system is not just a new container. The containers are stateless;
all indexed state lives in PostgreSQL. So a release whose indexing logic changed
does not merely start running new code — it begins rewriting the database that is
currently answering queries, and every intermediate state is visible to readers.

Blue-green deployment moves that risk off the serving database. A second, isolated
PostgreSQL database (*green*) is filled by the new release and checked. Only once it
passes does the API stop reading the old database (*blue*) and start reading green.

Not every release needs this. The first section decides.

## When a green database is required

The question is whether the release only *adds* rows, or whether it can *rewrite*
what existing queries already return.

### Additive — deploy in place

A change is additive when its backfill only inserts rows that no existing answer
depends on, so every query that was correct before the deploy stays correct during
and after it.

The clear case is a new contract or source family with its own admitted start
block, whose events do not feed the derived state of any entity that is already
indexed. Backfill it in place, and gate any consumer that depends on the new
coverage on a readiness check rather than on the deploy finishing.

### Re-derivation — a green database is required

A change requires isolation when it can change the derived state of entities that
are already indexed. Served answers can regress while the work is in flight — a name
can briefly resolve to nothing, or to a stale owner — and those regressions are
visible to consumers.

Treat a change as re-derivation if any of the following is true.

- **An earlier manifest start block.** History before the old start block is
  indexed for the first time, and events in it can supersede the earliest state
  currently held for an existing name.
- **A new event on a contract that is already watched.** Existing entities acquire
  transitions they did not have, which can reorder or invalidate their current
  state.
- **A wider runtime watch scope.** Newly watched contracts re-backfill their
  history, and their events feed entities that already exist. This is
  re-derivation even though no manifest changed.
- **Adapter logic changes for an adapter that is not classified
  `stateless_raw_fact`.** Adapters classified `stateful_closure_required` or
  `contextual_dependency_required` replay through a full closure over their prior
  state rather than one raw fact at a time, so changing them re-derives entities
  beyond the events that changed. The classification is the discriminator; use it
  rather than judging by how large the diff looks.
- **A migration that rewrites existing rows**, as opposed to one that only adds
  columns or tables.
- **A projection rebuild.** Projections are rebuildable by design, but a rebuild
  empties and refills the tables the API reads. If the affected route is
  consumer-facing, isolate it.

There is a further reason to prefer green for a widening watch scope. Backfill jobs
select their targets from the set of contracts active *at the moment the job is
created*, so a contract discovered after a job began was never in that job's scope.
Repairing that in place is awkward. A fresh database, backfilled once with the
corrected scope, has no such ordering problem.

## Preconditions

Check all of these before filling green. The first two are hard gates: a green
database built without them is either impossible to store or silently wrong.

1. **The image must scope code observations to a block's selected log emitters.**
   An image that records the code of every watched contract on every block writes
   rows proportional to `watched contracts x blocks`, not to chain activity. Over a
   large watch set and a long history that exhausts the volume rather than filling
   it. See [`../chain-intake.md`](../chain-intake.md).
2. **The image's live watch plan must cover the contracts you expect to index.** If
   the runtime watch scope is narrower than the manifests and discovery edges
   declare, green will fill quietly and completely with the wrong corpus, and it
   will pass every check that compares one pipeline stage to the stage before it.
   Confirm with `bigname-worker inspect watch-plan --json`.
3. **Size green's storage against the corpus you intend to have, not against
   blue.** If the release exists to index contracts blue never watched, green will
   be larger than blue, possibly by a lot. Blue's current size is a floor, not an
   estimate.

## Filling green

Migrations do not run on boot. The indexer and worker healthcheck commands compare
applied migrations against the migration set compiled into the running binary and
fail closed on any mismatch, so green must be migrated before its services start.

1. Provision green as a separate PostgreSQL database with its own storage. It must
   not share a volume with blue.
2. Apply migrations: `bigname-worker migrate` against green.
3. Start an indexer and a worker against green, from the release being promoted.
   Leave blue's indexer and worker running against blue. Both pairs now follow the
   chain independently.
4. Let green backfill and then follow the chain head. Following head matters: green
   must be *current* at the moment of cutover, not merely finished with history.
   Because both databases track the chain independently, nothing is lost during the
   warm-up window however long it takes.

## The cutover gate

Green is promotable when it is data-complete:

```sh
bigname-worker inspect data-completeness \
  --database-url "$GREEN_DATABASE_URL" --json --fail-on-incomplete
```

A non-zero exit blocks the cutover. See
[`data-completeness.md`](data-completeness.md) for what each check means and how to
read a failure.

Do not substitute `/v1/status`. That endpoint derives its lag from the projection
queue and reports the stored canonical checkpoint whenever the queue is empty, so an
empty database and a complete one look identical to it.

As an advisory check alongside the gate, compare entity counts between green and
blue. Expect green to have *more* when the release widens coverage; investigate any
category where green has fewer. This is a sanity check on the shape of the data, not
a pass/fail condition, because a legitimate re-derivation can reduce a count.

## Cutting over

Only the API moves. Repoint its `BIGNAME_DATABASE_URL` at green and restart it.

**Do not stop blue's indexer and worker.** Leave them running against blue through a
bake window. This is the property that makes rollback safe: if blue keeps following
the chain, rolling back means pointing the API at a database that is *current*. If
blue is stopped at cutover, every minute of the bake window makes rollback worse,
because the only database you can roll back to is falling further behind head.

Rollback is therefore the cutover in reverse: repoint `BIGNAME_DATABASE_URL` at blue
and restart the API.

Retire blue only after the bake window closes and you are prepared to lose the
ability to roll back by configuration alone.

## What a passing gate does and does not prove

The gate is database-level. It proves that the database reconciled the chain
contiguously to head, that every contract the manifests and discovery edges declare
has actually been observed, that no pipeline stage is stalled or failing, and that
the projections are non-empty.

It does not exercise HTTP routes, compare answers between blue and green, or verify
that any specific name resolves. A passing gate is a necessary condition for a
cutover, not a sufficient one.
