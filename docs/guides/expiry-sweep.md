# Discover expiry and renewal deadlines

Use `GET /v1/names` to find canonical registration deadlines in a required
namespace. Each response is one current publication; its chain timestamp in
`meta.as_of` determines lifecycle `status`. See the [route contract](../api-v1-routes.md#get-v1names)
and [protocol boundary table](../api-v1.md#expiry-and-grace).

## Choose the date you need

For expiry notices, query `expires_after` (inclusive) and `expires_before`
(exclusive). For renewal-deadline notices, query `grace_ends_after` and
`grace_ends_before` directly. Both accept exact decimal Unix seconds or RFC 3339.
A scalar request needs at least one bound; use two for a bounded notification job.

```text
GET /v1/names?namespace=ens&parent=eth&expires_after=2000000000&expires_before=2000086400
GET /v1/names?namespace=ens&parent=eth&grace_ends_after=2010000000&grace_ends_before=2010086400
```

For several disjoint windows, repeat `expires_window=after..before` or
`grace_ends_window=after..before`, up to 32. Windows require both bounds;
adjacent windows are allowed, overlaps and duplicates are rejected. Input order
sets the corresponding zero-based `expires_window_index` or
`grace_ends_window_index` on every result. Do not mix date families or combine
repeated windows with scalar bounds.

Default sorting follows the selected date family. Explicit `sort=expires_at`
or `sort=grace_ends_at` must match. `order=desc` reverses the deadline order;
namespace/name/namehash ties retain their stable order. `authority` filters the
served authority; `parent=eth` keeps exactly `<label>.eth`. Null or omitted
deadlines never match. Unsupported names keep their documented exclusion.

## Read lifecycle separately from holders

Every row carries `status=active|expired|released|unregistered`, together with
its canonical `expires_at` and `grace_ends_at`. Name/lookup read outcomes use
`read_status`; the outer lookup result still uses its own existing `status`.
There is no `registration_status` compatibility field.

A registration becomes `expired` while within its protocol's renewal grace.
ENSv1/Basenames include the exact grace-end second; ENSv2 releases at that
second. Entries with no registrar grace move directly from active to released,
with emancipated or locked wrapper-only entries retaining the contract's strict
expiry comparison. A plain wrapped name with no registrar lease keeps its holder
past its wrapper expiry, so it stays `active` with a past `expires_at`. Read the protocol boundary table rather than applying a universal
comparison to every row.

Canonical dates remain with the same registration after passive expiry and
release. After the ENSv2 cutover an unmigrated reservation retains its own dates;
there is no backwards switch to its old lease and no need to widen an expiry
window to guess a grace deadline. Renewals, replacements and chain corrections
can genuinely change dates. Explicit unregister immediately releases the instance
but keeps its scheduled dates and `lapsed_registration.release_kind=unregistered`.

A lifecycle status does not promise current control or availability through any
registrar. In particular an ENSv2 registration in grace has already lost its
current control path. Use current `owner`/`manager` or evidence-backed
`lapsed_registration.owner` according to your product's notification policy.
A reservation has no invented owner; a former ENSv1 holder can appear only from
its linked lease history. `lapsed_registration.released_at` records loss of that
held authority and can precede the canonical grace cutoff.

Wrapping is independent: `ens_v1.wrapper_state` distinguishes backed
`wrapped|emancipated|locked` from `lapsed|unwrapped|unknown`. Field presence alone
is not evidence of a live wrapper. Controller-only lease renewal does not imply
wrapper renewal. Use the documented wrapper expiry and restrictions for their
own purposes.

## Walk and resume

Use `page_size` up to 200 and follow the returned cursor unchanged until
`has_more` is false. Counts are intentionally null. A page shares one snapshot;
continuation reads the then-current publication and binds the date family,
windows, namespace, filters and ordering. Old or cross-family cursors return
`stale_cursor`; restart that walk once. Concurrent renewal can move a row across
your boundary, so applications should deduplicate notifications using their own
registration/deadline delivery key.

Lookup detail and feed report the same lightweight status and canonical dates
as detail and names windows for the same publication. A sweep selects candidates;
it does not establish contact consent, a notification recipient, or transaction
success.
