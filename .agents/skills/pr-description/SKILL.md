---
name: pr-description
description: Write or fix a pull request title and body for ensdomains/bigname. Fire before opening a PR and after every fix round that changes what the PR does.
metadata:
  kind: playbook
---

# PR Description

The reader is an engineer who knows the ENS smart contracts and what bigname is for, and has not read the code or followed the review. Write for them (see AGENTS.md § Communication).

## Title

Lowercase. The component or components, a colon, then a plain description of the behaviour change.

## Body

Exactly three headings, in this order. 260 to 400 words in total.

### What changes

One opening sentence naming the Linear ticket, with a GitHub closing keyword when the PR finishes it and plain prose when it does not. Then bullets, one per change:

- What a user of the API, or an owner of a name, will observe differently.
- The mechanism behind it, briefly, with the chains and contracts involved.

### Compatibility and rollout

- Response or schema changes a client will notice, and what a client has to do.
- Whether a database schema migration is included.
- Whether the interpreter content hash rotates. It rotates when the diff touches any of the hashed paths in `crates/content-hash/src/compute.rs`: the roots `crates/adapters/src`, `crates/manifests/src`, `manifests/`, `crates/project/src`, `crates/interpret/src/write`, the composition files under `crates/storage/src/families`, `Cargo.lock`, and every file in `SEMANTIC_SOURCE_FILES`. If it rotates, say so in one sentence with the consequence: adopting the change needs a full re-derivation, so it rides a re-derivation release rather than a patch, and `docs/deployment.md` gets an entry in the same PR.
- What is deliberately not included.

### Validation

Only tests that exist in this diff, named by file or by what they prove. No test counts, no lint runs, no CI claims, no review history.

## Rules

- No commit shas, no chronology, no reviewer or agent names.
- Every sentence must still be true at merge. Re-read the body after each fix round and change what no longer holds.
- Short sentences and short paragraphs. Lists for parallel items. No semicolon-joined clauses.
