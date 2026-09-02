# GraphQL Compatibility Oracle

The oracle compares bigname offline with one pinned ENS subgraph observation. The fixture was captured from pinned
deployment `QmcE8RpWtsiN5hkJKdfCXGfTDoTgPEjMbQwnjLPfThT7kZ` at block 23000000 on 2026-09-02, with the
pinned-schema verification recorded in its provenance. Refreshes follow the [reviewed refresh
policy](development.md#graphql-compatibility-fixture-refresh). Its `Domain` shape is pinned (upstream:
.refs/ens_subgraph/schema.graphql:L1-L46 @ ens_subgraph@723f1b6).

## Covered surface

The fixture separates three concepts:

1. The **[upstream census](glossary.md#graphql-upstream-census)** is the generated SDL and semantic index of every captured schema member, not a claim of complete implementation.
2. The **[claimed compatibility surface](glossary.md#graphql-claimed-compatibility-surface)** is the exact paths and cases in `coverage.json`; missing or changed claims break compatibility.
3. The **[dispositioned remainder](glossary.md#graphql-dispositioned-remainder)** keeps upstream-only work and extensions visible without requiring whole-schema equality.

Each difference has an exact path or bounded scope, status, GitHub issue/task identifier, and documentation anchor. Notes do
not affect matching; wildcards, conflicts, unknown paths, and stale entries invalidate the fixture.

Upstream-only fields on a shared type require exact field entries; a type-wide entry is valid only when the whole type is
absent locally. The one bounded exception is an upstream-only `Query` entity field: when its return type, after list and
non-null wrappers are unwrapped, is a censused object with an `id` field, the root field inherits that type's owner unless
that return type is claimed. An
upstream-only argument inherits its parent field's disposition, never the parent type's, so a new argument on a claimed field still fails. An
upstream-only enum value inherits the owner of its enum type in `known_upstream_types`; a value whose enum type is absent
from that census still fails.

`coverage.json` is the manually maintained ownership policy. The manifest records the version present at capture time, but later
ownership dispositions do not rewrite the steward's captured deployment manifest.
Offline fixture verification validates that claims and dispositions agree with the captured upstream index. The Rust
fixture tests perform the separate local-introspection comparison and apply the field, argument, Query-entity, and enum
ownership rules above; the command-line capture and verification tool does not duplicate that local census walk.

Broader entity/event fixtures, filter matrix, historical reads, errors, and reports are deferred; only the Domain point and name-equality responses are claimed.

The steward's live introspection observed the Graph Node logging types `LogLevel`, `_LogArgument_`, `_LogMeta_`, and
`_Log_`. They are infrastructure rather than ENS schema, so coverage assigns them to `#670/T0`; they are captured but
not claimed. The pinned Graph Node defines those four types (upstream:
.refs/graph_node/graph/src/schema/logs.graphql:L5-L70 @ graph_node@aefe1737). Graph Node also adds the fixed `_logs`
field to every generated query root (upstream: .refs/graph_node/graph/src/schema/api.rs:L1080-L1107 @
graph_node@aefe1737), with its seven-argument signature (upstream:
.refs/graph_node/graph/src/schema/api.rs:L1313-L1418 @ graph_node@aefe1737); coverage assigns that infrastructure
field to the same owner without claiming it.

## Schema comparison

The index compares roots; type kind/membership; fields, arguments, recursive shapes, defaults, enums, and deprecation.
It ignores order, descriptions, locations, formatting, and deprecation-reason prose.
Directive repeatability is also outside the captured and compared surface because the hosted deployment's resolver
does not satisfy the pinned introspection field contract; the [divergence](upstream.md#known-divergences) is recorded
with the reference-indexer comparisons.

| Difference | Result |
| --- | --- |
| Claimed path absent locally or its signature changed | Fail |
| New or changed shared path without a disposition | Fail |
| Upstream-only path with the field, parent-field, Query-entity, or enum ownership described above | Report |
| Exact documented local extension | Report |
| Unclassified local-only path | Fail |
| Recorded difference no longer present | Fail as stale |
| Unrestricted wildcard disposition | Fail fixture validation |

The census comes from introspection; compressed SDL remains for review. A numeric block is a distinct selector
(upstream: .refs/graph_node/graphql/src/query/ext.rs:L53-L70 @ graph_node@aefe1737).

## Response and block comparison

Responses canonicalize object-key order only. Lists, nullability, scalar strings, nested shape, omission, `_meta`,
nulls, and empty arrays remain exact. GraphQL `errors` fail; expected errors are deferred. Collections fix order,
and the comparator has no value-specific exceptions.

Every answer case supplies a numeric block constraint and requests:

```graphql
_meta(block: $block) {
  block { number hash }
  hasIndexingErrors
}
```

Capture rejects a mismatched metadata block and preserves its hash, including numeric-pin `null` (upstream:
.refs/graph_node/graph/src/schema/meta.graphql:L36-L73 @ graph_node@aefe1737). Bigname's durable [served
head](glossary.md#served-head) equals the fixture block; no historical read is claimed.

Today the response comparison runs against a bigname database seeded from the fixture itself. It therefore proves
response shape — the schema surface plus exact response equality for the claimed cases — against that seeded state,
not data-level equivalence with a live bigname deployment. Once a Mainnet bigname deployment follows chain head, the
fixture is recaptured at that served head under the [reviewed refresh
policy](development.md#graphql-compatibility-fixture-refresh), and live `compare` against that deployment becomes the
operating-path proof.

The captured point response supplies the isolated test name, creation time, and owner, while provenance supplies the
served block. Neither response case can supply a nonmatching row, so `capture` also writes a manifest-digested
`seed.json` descriptor for that distractor instead of leaving it hardcoded in the Rust harness.

## Compatibility-break policy

> A compatibility break is an un-dispositioned schema change on the claimed surface, or an exact response change
> for a covered case, relative to the checked-in pinned upstream observation. A live ENS subgraph change has no
> effect on CI until an operator deliberately regenerates and reviews the fixtures.

Fixture regeneration is a reviewed contract update, not a snapshot rebless. Capture never runs in CI.
