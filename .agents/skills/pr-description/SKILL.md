---
name: pr-description
description: Write or fix a pull request title and body for ensdomains/bigname. Use before opening a PR and after every fix round that changes what the PR does.
metadata:
  kind: playbook
---

# PR Description

The reader is an engineer who knows the ENS smart contracts and what bigname is for, and has not read the code or followed the review. Write for them (see AGENTS.md § Communication).

## Title

Lowercase throughout, except proper names and identifiers. The component prefix is always lowercase. The component or components, a colon, then a plain description of the behaviour change.

## Body

Exactly three headings, in this order. At most 400 words.

### What changes

One opening sentence naming the Linear ticket or tickets, with a GitHub closing keyword when the PR finishes it and plain prose when it does not. When the PR has no ticket, the opening sentence says so. Then bullets, one per change:

- What a user of the API, or an owner of a name, will observe differently.
- The mechanism behind it, briefly, with the chains and contracts involved, when any.

### Compatibility and rollout

- Response or schema changes a client will notice, and what a client has to do.
- Whether a schema-migration is included.
- Whether the interpreter content hash rotates (`docs/glossary.md` "Interpreter content hash"). Measure it. Build `bigname-content-hash` at the base commit and at the head commit. Compare the `INTERPRETER_CONTENT_HASH` in the `interpreter_content_hash.rs` file each build writes under its `OUT_DIR`. `crates/content-hash/src/` defines what is hashed, selected `Cargo.lock` entries included. If it rotates, say so with the consequence. Adopting the change needs a full re-derivation at a re-derivation boundary (`docs/glossary.md` "Re-derivation boundary"). `docs/deployment.md` gets an entry in the same PR.
- Whether the manifest-authority fingerprint changes. The manifest-authority marker records it (`docs/glossary.md` "Manifest-authority marker"). It is independent of the interpreter content hash, so an unchanged hash does not settle it. The active manifests, serialized whole except `normalizer_version`, are what is fingerprinted (`crates/manifests/src/schema_v2_sync_state.rs`). The compiled watch plan is part of that payload (`docs/glossary.md` "Compiled watch plan"). If it changes, say so with the consequence for an initialized chain:
  - A token-attested full-range Interpret redo, then a stamped Project redo.
  - A stamped Ingest redo first, when the watch plan widened (`docs/glossary.md` "Watch plan / watched tuple").
  - A full-range Project redo on Base as well, when the Ethereum Mainnet `basenames_execution` authority changed.
  - An entry in `docs/deployment.md` in the same PR.
- What is deliberately not included.

### Validation

Name the evidence for the change:

- When the diff has tests, name them by file or by what they prove.
- When the diff adds no test source, name the existing checks that cover the change.
- When the diff adds no test source and no automated check covers the change, say so plainly.

Leave out:

- Test counts.
- Lint runs.
- CI claims.
- Review history.

## Rules

- No commit shas from this repository. Name an earlier change by its PR number.
- No review or development history.
- No reviewer or agent names.
- Pinned `.refs` upstream citations keep their shas.
- Rollout ordering belongs in Compatibility and rollout.
- Every sentence must still be true at merge. Re-read the body after each fix round and change what no longer holds.
- Short sentences and short paragraphs. Lists for parallel items. No semicolon-joined clauses.
