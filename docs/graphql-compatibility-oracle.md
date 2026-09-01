# GraphQL Compatibility Oracle

The oracle compares bigname offline with one pinned ENS subgraph observation. The provisional local-mock fixture
proves only the operator path; a steward must refresh it before merge. Its `Domain` shape is pinned (upstream:
.refs/ens_subgraph/schema.graphql:L1-L43 @ ens_subgraph@723f1b6).

## Covered surface

The fixture separates three concepts:

1. The **[upstream census](glossary.md#graphql-upstream-census)** is the complete generated SDL and semantic index, not a claim of complete implementation.
2. The **[claimed compatibility surface](glossary.md#graphql-claimed-compatibility-surface)** is the exact paths and cases in `coverage.json`; missing or changed claims break compatibility.
3. The **[dispositioned remainder](glossary.md#graphql-dispositioned-remainder)** keeps upstream-only work and extensions visible without requiring whole-schema equality.

Each difference has an exact path or bounded scope, status, issue-shaped owner, and documentation anchor. Notes do
not affect matching; wildcards, conflicts, unknown paths, and stale entries invalidate the fixture.

Broader entity/event fixtures, filter matrix, historical reads, errors, and reports are deferred; only the Domain point and name-equality responses are claimed.

## Schema comparison

The index compares roots; type kind/membership; fields, arguments, recursive shapes, defaults, enums, and deprecation.
It ignores order, descriptions, locations, formatting, and deprecation-reason prose.

| Difference | Result |
| --- | --- |
| Claimed path absent locally or its signature changed | Fail |
| New or changed shared path without a disposition | Fail |
| Upstream-only path in a named deferred type/entity/task | Report |
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

The captured point response supplies the isolated test name, creation time, and owner, while provenance supplies the
served block. Neither response case can supply a nonmatching row, so `capture` also writes a manifest-digested
`seed.json` descriptor for that distractor instead of leaving it hardcoded in the Rust harness.

## Compatibility-break policy

> A compatibility break is an un-dispositioned schema change on the claimed surface, or an exact response change
> for a covered case, relative to the checked-in pinned upstream observation. A live ENS subgraph change has no
> effect on CI until an operator deliberately regenerates and reviews the fixtures.

Fixture regeneration is a reviewed contract update, not a snapshot rebless. Capture never runs in CI.
