---
name: pr-description
description: Write or fix a pull request title and body for ensdomains/bigname. Fire before opening a PR and after every fix round that changes what the PR does.
metadata:
  kind: playbook
---

# PR Description

The reader is an engineer who knows the ENS smart contracts and what bigname is for, and has not read the code or followed the review. Write for them (see AGENTS.md § Communication).

## Title

Lowercase throughout, except proper names. The component prefix is always lowercase. The component or components, a colon, then a plain description of the behaviour change.

## Body

Exactly three headings, in this order. 260 to 400 words in total.

### What changes

One opening sentence naming the Linear ticket, with a GitHub closing keyword when the PR finishes it and plain prose when it does not. When the PR has no ticket, the opening sentence says so. Then bullets, one per change:

- What a user of the API, or an owner of a name, will observe differently.
- The mechanism behind it, briefly, with the chains and contracts involved.

### Compatibility and rollout

- Response or schema changes a client will notice, and what a client has to do.
- Whether a schema-migration is included.
- Whether the [interpreter content hash](../../../docs/glossary.md#interpreter-content-hash) rotates. Measure it. Build `bigname-content-hash` at the base commit and at the head commit, and compare the `INTERPRETER_CONTENT_HASH` value each build generates. `crates/content-hash/src/compute.rs` defines what is hashed. If it rotates, say so in one sentence with the consequence: adopting the change needs a full re-derivation at a [re-derivation boundary](../../../docs/glossary.md#re-derivation-boundary), so it rides a re-derivation release rather than a patch, and `docs/deployment.md` gets an entry in the same PR.
- Whether the manifest-authority fingerprint changes. The [manifest-authority marker](../../../docs/glossary.md#manifest-authority-marker) records it. It is independent of the interpreter content hash, so an unchanged hash does not settle it. `crates/manifests/src/schema_v2_sync_state.rs` defines what is fingerprinted. If it changes, say so with the consequence:
  - A token-attested full-range Interpret redo, then a stamped Project redo.
  - A stamped Ingest redo first, when the [watch plan](../../../docs/glossary.md#watch-plan--watched-tuple) widened.
  - An entry in `docs/deployment.md` in the same PR.
- What is deliberately not included.

### Validation

Name the tests in this diff, by file or by what they prove. When the diff adds no test source, name the existing checks that cover the change. No test counts, no lint runs, no CI claims, no review history.

## Rules

- No commit shas from this repository, no review or development history, no reviewer or agent names.
- Pinned `.refs` upstream citations keep their shas.
- Rollout ordering belongs in Compatibility and rollout.
- Every sentence must still be true at merge. Re-read the body after each fix round and change what no longer holds.
- Short sentences and short paragraphs. Lists for parallel items. No semicolon-joined clauses.
