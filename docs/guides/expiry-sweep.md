# Running an expiry sweep against `GET /v1/names`

This guide is for a service that sends expiry notifications (renewal
reminders, grace warnings, release notices) by walking
[`GET /v1/names`](../api-v1-routes.md#get-v1names) on a schedule. It adds no
rule of its own: every behaviour below is stated in the API contract, and each
section links the rule it relies on. Where this guide and the contract differ,
the contract wins.

A sweep is one pass over one expiry window, page by page:

```text
GET /v1/names?namespace=ens&parent=eth&expires_after=1793491200&expires_before=1793577600&page_size=200
GET /v1/names?namespace=ens&parent=eth&expires_after=1793491200&expires_before=1793577600&page_size=200&cursor=<next_cursor>
...
```

Each page is read from one *publication*: the indexed state the API serves,
which stands at one block per chain (see
[per-block publication](../glossary.md#per-block-publication)). The page names
that block in `meta.as_of`.

## 1. Windows

- `namespace` is required, and so is at least one of `expires_after` and
  `expires_before`. Both bounds accept decimal Unix seconds or RFC 3339.
- `expires_after` is inclusive and `expires_before` is exclusive, so
  consecutive windows `[t0, t1)`, `[t1, t2)` tile without overlap or gap.
  `expires_after` must be earlier than `expires_before`.
- Rows are sorted by `expires_at`, ascending by default (`order=desc` reverses
  it); ties are broken by namespace, name and namehash.
- `expires_at` is a decimal string of Unix seconds and can exceed the
  floating-point safe-integer range. Parse it as an exact integer before
  comparing ([timestamp format](../api-v1.md#timestamp-format-and-absent-expiry)).
- The window matches the row's served `expires_at`. From the
  [Universal Resolver cutover](../glossary.md#universal-resolver-cutover) a
  `.eth` second-level name with a live ENSv2 entry is listed by that entry's
  expiry, because resolution then starts at the ENSv2 root registry
  (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/UniversalResolverV2.sol:L55-L63 @ ens_v2_sepolia_20260916@366de741),
  and before it by its ENSv1 lease's
  (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L96-L98 @ ens_v1@91c966f).
  While ENSv1 decides the name, the lease's own date is `ens_v1.expires_at`
  ([expiry and grace](../api-v1.md#expiry-and-grace)).
- A row whose expiry is `null` never matches a window. That covers
  `expires_at_reason` `no_expiry` and `not_set`, and an ENSv2 registration
  ended by an explicit unregister (`expires_at_reason: "released"`), which
  therefore never reaches a sweep. A name with no registration context, or
  whose current authority is unsupported, is not listed either.
- An empty window is `200` with empty `data`, `has_more: false` and
  `next_cursor: null`. `page.total_count` is always `null`.

The rules are in the request, coverage and status bullets of
[`GET /v1/names`](../api-v1-routes.md#get-v1names).

## 2. Leases versus other registrations

Each request covers one namespace: `namespace=ens` or `namespace=basenames`.
Without a filter, an `ens` window mixes every finite registration in it:
`.eth` registrar leases, ENSv2 registrations, wrapped subnames and ENSv2
subnames. Two filters narrow it, and they combine:

- `parent=<name>` keeps only names one label below `<name>`. `parent=eth`
  selects the registrar-governed `.eth` second-level names on both sides of
  the cutover and excludes every subname. With `namespace=basenames`,
  `parent=base.eth` selects the Basenames second-level names.
- `authority=` keeps rows whose served
  [`authority`](../api-v1.md#naming-dictionary), the registry generation that
  decides the name, is one of the listed values (`ens_v0`, `ens_v1`,
  `ens_v2`, comma-separated). A row that serves no `authority` never matches,
  so a `namespace=basenames` sweep, whose rows serve none, does not use this
  filter.

For example, `parent=eth&authority=ens_v1,ens_v0` lists the `.eth` names whose
registration ENSv1 still decides. Every row carries its own `authority`, so a
sweep can also split one unfiltered window by row. Both filters are bound into
the cursor (section 5).

## 3. Expiry, grace and release: who to notify

Read four fields together: `expires_at`, `grace_ends_at`,
`registration_status` and `lapsed_registration`.

- `grace_ends_at` is the renewal deadline for that row's `expires_at`. The
  grace length depends on which registrar the expiry comes from, and is zero
  for a name with no registrar grace, such as a subname
  ([naming dictionary](../api-v1.md#naming-dictionary)). Use the field rather
  than adding a grace period yourself.
- `registration_status`, `owner`, `manager` and `lapsed_registration` describe
  the publication in `meta.as_of`, not the moment you read them
  ([expiry and grace](../api-v1.md#expiry-and-grace)). To place a row in a
  phase, compare its dates with that page's `meta.as_of` timestamp, so the
  phase and the holder come from the same block.

For a `.eth` second-level name:

| Phase, at the page's `meta.as_of` timestamp `T` | What the row shows | Who to notify |
| --- | --- | --- |
| Before expiry: `T < expires_at` | the name is held: `registration_status` is not `released` | `owner` |
| ENSv1 lease in grace: `expires_at <= T <= grace_ends_at` | `registration_status` is unchanged; the lease is not released during its grace, including the second `grace_ends_at` itself, when the registrar still renews it (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L161 @ ens_v1@91c966f) | `owner` |
| ENSv2 registration past its expiry: `expires_at <= T` | `registration_status: released`; `lapsed_registration.release_kind: "expired"`; renewable while `T < grace_ends_at` | `lapsed_registration.owner` |
| ENSv1 lease past its grace: `grace_ends_at < T` | `registration_status: released`; `lapsed_registration.release_kind: "expired"`; `released_at` is the first block whose time is after the grace | `lapsed_registration.owner` |
| Released for another cause | `registration_status: released`, no `lapsed_registration` | nobody the API can name |

The ENSv1 rows compare with the lease's own dates. Before the cutover those are
the row's `expires_at` and `grace_ends_at`. From the cutover a reserved `.eth`
name that ENSv1 still decides serves its ENSv2 reservation's expiry and grace
instead, and the lease's expiry is `ens_v1.expires_at`, with its grace ending
90 days later
(upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L17 @ ens_v1@91c966f). The two grace deadlines usually fall on the same second, but
not always: extending the reservation without renewing the lease splits them
([naming dictionary](../api-v1.md#naming-dictionary), `ens_v1.expires_at`).
For such a name, use `ens_v1.expires_at` in the ENSv1 rows.

A name with no registrar grace, such as a subname, has `grace_ends_at` equal to
`expires_at`, so it has no grace phase.

- `owner` is the token holder, or the registry owner of a name with no token.
  On a wrapped `.eth` name in grace `manager` is omitted, so do not use
  `manager` as the notification address.
- A released row has no `owner` or `manager`. `lapsed_registration.owner` is
  the holder when the registration ended, kept apart from current state so it
  is never read as an owner ([lapsed registration](../api-v1.md#lapsed-registration)).
  Its `released_at` is when the release was recorded and `held_through` the
  contract the registration was held through.
- An ENSv2 registration is released at its expiry
  ([lapsed registration](../api-v1.md#lapsed-registration)): the registry
  treats an entry as expired from the second its expiry is reached
  (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L663-L665 @ ens_v2_sepolia_20260916@366de741)
  but still reports its last owner
  (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L353-L362 @ ens_v2_sepolia_20260916@366de741),
  and the `.eth` registrar renews it in grace while that last owner is set
  (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registrar/ETHRegistrar.sol:L264-L291 @ ens_v2_sepolia_20260916@366de741).
  An explicit unregister burns the token
  (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L224-L235 @ ens_v2_sepolia_20260916@366de741),
  so it cannot be renewed through the `.eth` registrar, whose grace renewal
  needs that last owner
  (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registrar/ETHRegistrar.sol:L264-L291 @ ens_v2_sepolia_20260916@366de741);
  its `expires_at` is `null`, so it is outside every window.
- A released name keeps its old `expires_at` and appears in every window that
  covers it while it stays released. A new registration, or a renewal of an
  ENSv2 registration in its grace, ends that state and removes
  `lapsed_registration`. A sweep that wants only held names filters rows on
  `registration_status`.
- The API serves no field for a premium period after grace or for a price.
  After `grace_ends_at` the API states only that the name is released.

To find names still renewable in grace, ask for `expires_after` at `T` minus
the longest grace you care about, and keep only rows with `expires_at <= T`
that the table puts in a grace phase: ENSv1 leases not yet `released`, and
ENSv2 rows released as `expired` with `T < grace_ends_at`. The window also
holds names that have not expired yet, which are not in grace. For
the names one address last held, use
`GET /v1/addresses/{address}/names?relation=former_owner`
([address names](../api-v1-routes.md#get-v1addressesaddressnames)).

## 4. Freshness

Every page carries `meta.as_of`, keyed by decimal chain ID, with the
`block_number`, `block_hash` and `timestamp` (decimal Unix seconds) of the
publication the page read
([finality and snapshots](../api-v1.md#finality-and-snapshots)). A renewal in a
later block is not on that page.

- A page states the names as they were at its `meta.as_of` timestamp `T`, so
  decide each row's phase against `T`, never against your wall clock.
- Send a notice that says a date has passed ("expired", "in grace", "grace
  ended") only once `T` is past that date. A row whose date is not yet past at
  `T` waits for a page from a later publication.
- If `meta.as_of` lists more than one chain, use the smallest timestamp.
- `T` tells you how fresh a page is. It does not tell you that a walk saw
  every name; section 6 covers that.
- Before a run, `GET /v1/status` gives a readiness gate per chain
  ([`GET /v1/status`](../api-v1-routes.md#get-v1status)): `status` is
  `ready`, `degraded` or `stale`; `lag_blocks` and `lag_seconds` are how far
  the served publication trails the indexer's stored head;
  `ingestion_lag_blocks` and `ingestion_lag_seconds` are how far that stored
  head trails the network head the API last observed, the seconds counted up
  to when it observed that head. `ready` requires both to be within the
  server's thresholds, along with the other conditions that route lists. During a full rebuild of the indexed state, `indexed_block`
  shows the rebuild's progress rather than a served block.
- `lag_blocks` and `lag_seconds` are `null` while an indexer redo (a rebuild
  of indexed state over a block range) is in progress, and the chain reports
  `degraded`, or `stale` if a stronger condition applies. Treat `null` as
  unknown, not zero, and skip the run. During such a redo the listing can
  also refuse pages with `409 stale` (section 5).
- `/v1/status` is a gate only. What a page actually read is its own
  `meta.as_of`.

## 5. Pagination and retries

- `next_cursor` is opaque. It binds the namespace, both bounds, the order and
  the `authority` and `parent` filters, and holds the position of the last row
  returned (its `expires_at`, namespace, name and namehash). It holds no
  publication. Send the next request with the same parameters plus `cursor`; a
  cursor sent with a different namespace, bound, order or filter returns
  `400 invalid_input`
  ([current-state list cursors](../api-v1.md#current-state-list-cursors)).
- A continuation reads whatever is published when it runs and returns the rows
  after the cursor's position. Pages of one walk can therefore read different
  publications, and each page reports its own `meta.as_of`. A name renewed
  between pages can appear twice or not at all, and a name published after
  the first page can appear on a later one.
- `409 stale` on this route is retryable: retry the same request with the same
  cursor, and do not restart the walk for it. A publication that lands while a
  page is being read does not affect that page. The message says which case
  it is:
  - `collection publication changed during the read; retry the request`: a
    new publication landed, or the namespace's set of indexed contracts
    changed, between the request being accepted and the page's first read.
    Retry at once.
  - `collection publication is not available; retry after indexing is ready`
    or `requested snapshot is not available for name`: the API is not serving
    the namespace's indexed state right now, for example during a rebuild or
    redo, or while the indexer trails the chain too far
    ([tier 2 product reads](../api-v1.md#tier-2-product-reads)). Back off and
    retry; the cursor holds no publication, so it stays valid.
- `400 invalid_input` with `cursor must be a valid pagination cursor` means the
  cursor cannot continue, for example after a contract change to the cursor
  layout. Restart the window without a cursor.
- `at` is rejected with `400 invalid_input`: this listing reads current state
  and cannot replay an older publication.
- `page_size` is 1 to 200, default 50.

## 6. Completeness and idempotency

The API gives no guarantee that a walk sees every name: rows can repeat or be
skipped between pages (section 5), and a reserved `.eth` name that ENSv1
decides moves back to its earlier lease date when its reservation stops being
live (section 3). On every run:

- Check freshness with each page's `meta.as_of` (section 4).
- Mark a window covered only after its walk ended with `has_more: false` and
  every notice it produced is durably recorded.
- Rescan back by the longest grace you handle: walk again every window from
  `T` minus that grace onwards, so skipped or moved rows, and rows due a
  later notice such as "grace ends", are seen again.

Make every notification idempotent so that rescans and repeated rows send
nothing twice. Which key to use is your policy; a natural one is
`(namespace, name, expires_at, kind, recipient)`, where `kind` is your
notification type and `recipient` the address you notify. With it, a name
transferred without a renewal still notifies the new holder. The same key
recurs, and you decide whether to notify again, in two cases:

- an ENSv2 registration is unregistered and the name registered again with
  the same expiry
  (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L224-L235 @ ens_v2_sepolia_20260916@366de741)
  (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L452-L494 @ ens_v2_sepolia_20260916@366de741);
- a renewal keeps the same expiry
  (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L240-L255 @ ens_v2_sepolia_20260916@366de741).

`GET /v1/names` rows carry no registration identity; to tell the first case
apart, read `registration_id` from
[`GET /v1/names/{name}`](../api-v1-routes.md#get-v1namesname).
