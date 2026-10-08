---
name: linear-issue
description: Create or update a Linear issue in the Tyrell Corporation team (TYR) for bigname work. Fire when filing a ticket, splitting work out of a PR, or recording a decision.
metadata:
  kind: playbook
---

# Linear Issue

The reader is an engineer who knows the ENS smart contracts and what bigname is for, and has not read the code or followed the review. Write for them (see AGENTS.md § Communication).

## Title

Short, sentence case, plain. Say the behaviour or the change, not the component. No prefix. The component goes on a label.

## Labels

Exactly one area label and one type label.

- Area: `api`, `interpret`, `projections`, `storage`, `release`, `ops`, `process`. Each has a description in Linear. Read them before choosing.
- Type: `bug`, `improvement`, `feature`.

Owners in the Ownership Map of `docs/internal/workstreams.md` map to area labels:

- Intake and Adapters, Manifests and Discovery, Upstream Evidence: `interpret`.
- Verified Lookup, Conformance and Fixtures: `api`.
- Agent Process, Platform and DevEx: `process`.
- Projections and API: `api` for route and response work, `projections` for projection and publication work.
- Storage and Domain: `storage`.

## Description

Headings in this order. Edit the description in place when facts change. History and progress go in comments, never appended to the description.

### `## State <date>`

Status and provenance first: where this came from, as the PR number or ticket id it was split from. What is known, with code references as `path:line` at a named commit. State the impact of a fix on the [interpreter content hash](../../../docs/glossary.md#interpreter-content-hash) when it is known. Otherwise say it is unknown until there is a diff. Once a diff exists, the measured result is required.

### Analysis (optional, no fixed heading)

What is wrong or missing and why, as the reader above would understand it.

### `## Decision (<who>, <date>)`

Only once someone has ruled. A decision belongs to a person, so name the person. Never attribute a decision to a review tool or an agent. Cite the PR or ticket where the decision was made. Quote or paraphrase the ruling in one or two sentences.

### `## Do`

Bullets. Each one an action a single implementer can take and finish.

## Rules

- A child ticket takes its parent's priority.
- In Progress when someone takes it.
- An implementation ticket is Done after merge, with the merge sha in a comment.
- Work done outside the repository is Done when a comment records what was done and where.
- No made-up labels for things. Name PRs by number and tickets by id.
- No pointers to scratch files or private notes outside the repo and Linear.
- Short sentences and short paragraphs. Lists for parallel items. No semicolon-joined clauses.
