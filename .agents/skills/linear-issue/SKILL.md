---
name: linear-issue
description: Create or update a Linear issue for bigname work. Fire when filing a ticket, splitting work out of a PR, or recording a decision.
metadata:
  kind: playbook
---

# Linear Issue

The reader is an engineer who knows the ENS smart contracts and what bigname is for, and has not read the code or followed the review. Write for them (see AGENTS.md § Communication).

## Title

Short, sentence case, plain. Say the behaviour or the change, not the component. No prefix. The component goes on a label.

## Labels

Exactly one area label and one type label.

- Area: `api`, `interpret`, `projections`, `storage`, `release`, `ops`. Each has a description in Linear. Read them before choosing.
- Type: `bug`, `improvement`, `feature`.

## Description

Headings in this order. Edit the description in place when facts change. History and progress go in comments, never appended to the description.

### `## State <date>`

Status and provenance first: where this came from, which PR or ticket or review it was split from. What is known, with code references as `path:line` at a named commit. Say whether a fix would rotate the interpreter content hash.

### Analysis (optional, no fixed heading)

What is wrong or missing and why, as the reader above would understand it.

### `## Decision (<who>, <date>)`

Only once someone has ruled. Name who decided. Quote or paraphrase the ruling in one or two sentences.

### `## Do`

Bullets. Each one an action a single implementer can take and finish.

## Rules

- A child ticket takes its parent's priority.
- In Progress when someone takes it. Done after merge, with the merge sha in a comment.
- No made-up labels for things. Name PRs by number and tickets by id.
- No pointers to scratch files or private notes outside the repo and Linear.
- Short sentences and short paragraphs. Lists for parallel items. No semicolon-joined clauses.
