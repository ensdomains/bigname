---
name: manifest-rollout
description: Plan or review bigname manifest and discovery changes. Use whenever a task adds or edits source manifests, rollout status, capability flags, discovery rules, proxy tracking, contract admission, or manifest-driven invalidation behavior.
metadata:
  kind: playbook
---

# Manifest Rollout

Start with `docs/manifests.md`. Read `docs/storage.md`, `docs/execution.md`, and `docs/internal/workstreams.md` only if the task reaches storage ownership, invalidation, or parallel-delivery questions.

## Required manifest shape

Every manifest change should account for:

- `manifest_version`
- `namespace`
- `source_family`
- `chain`
- `deployment_epoch`
- `rollout_status`
- `normalizer_version`
- `capability_flags`
- `roots`
- `contracts`
- `discovery_rules`

## Review checklist

For each manifest or discovery change, state explicitly:

1. why the contract or edge is authoritative
2. whether admission is direct, reachable from a root, or allow-listed for migration
3. which capability flags changed and whether they are `unsupported`, `shadow`, or `supported`
4. which normalized events should result:
   - `SourceManifestUpdated`
   - `ProxyImplementationChanged`
   - `CapabilityChanged`
5. which execution cache entries or projections need invalidation

## Policy constraints

- Capability flags gate behavior, not public-contract existence.
- Unsupported capability must surface explicitly in coverage or as a typed error.
- Shadow capability may write facts or traces without enabling general reads.
- Adapters consume manifest versions as inputs; they must not rely on hidden config.

## Authoritative-source rule

Do not add watched contracts that cannot be explained by an active manifest or an admitted discovery edge.

## Upstream anchor

Every new contract address, role, or discovery-rule admission cites the upstream deployment metadata or Solidity file under `.refs/`:

- ENSv1 → `.refs/ens_v1/`
- ENSv2 → `.refs/ens_v2/`
- Basenames → `.refs/basenames/`

Use the citation format from `AGENTS.md` § Upstream anchors — `(upstream: .refs/<key>/<path>:L<line> @ <key>@<short-commit>)` — either as a comment in the manifest TOML or in the accompanying doc update. Addresses without an upstream citation are unsupported; the existing human-assertion comment style in `manifests/basenames/basenames_base_registry/v1.toml` is the legacy shape and should be replaced on touch.

Keep the output operational: show the manifest change, the admission logic, the upstream citation, and the downstream consequences.
