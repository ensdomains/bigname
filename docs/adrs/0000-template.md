# ADR NNNN: <Title>

Status: Proposed | Accepted | Superseded by ADR XXXX
Date: YYYY-MM-DD

## Context

What problem is this ADR solving? What constraints or prior decisions shape the solution space? Cite existing docs and ADRs (`docs/architecture.md`, prior ADRs under `docs/adrs/`) where they frame the decision. If the context includes claims about ENSv1, ENSv2, or Basenames behavior, cite the upstream source per `AGENTS.md` § Upstream anchors.

## Decision

What is being decided, stated plainly. Prefer bullet lists of the load-bearing rules when the decision has multiple rules. Name the new terms or identifiers this ADR introduces.

## Upstream anchors

For any rule that mirrors, narrows, widens, or reshapes ENSv1, ENSv2, or Basenames behavior, cite the upstream source:

```
(upstream: .refs/<key>/<path>:L<line> @ <key>@<short-commit>)
```

List each cited upstream file and the rule it anchors. If the decision creates a divergence from upstream, call that out here and add a matching entry to `docs/upstream.md` § Known divergences.

Delete this section only if the ADR genuinely has no upstream dependency.

## Consequences

What changes as a result — both positive and negative. Which docs become load-bearing, which pieces of code must follow the rule, which failure modes the rule eliminates, and which are newly possible.

## Rollout

How the rule ships: is it doc-first, code-concurrent, or retroactive? Which workstreams own the code, fixture, and manifest updates? Reference `docs/internal/workstreams.md` where relevant.

## Alternatives considered

Each alternative gets one paragraph: what it would have looked like and why it was rejected. Keep to the live trade-offs; do not enumerate strawmen.

## References

- Other ADRs and internal docs that anchor this decision.
- External URLs only for context; authoritative upstream claims live in the § Upstream anchors section as `.refs/` citations, not URLs.
