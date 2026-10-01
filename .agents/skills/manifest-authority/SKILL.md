---
name: manifest-authority
description: Plan or review bigname manifest authority, source-family admission, discovery edges, capability flags, proxy tracking, start-block provenance, watch-plan effects, or manifest-driven invalidation behavior.
metadata:
  kind: playbook
---

# Manifest Authority

Start with `docs/manifests.md`. Read `docs/storage.md`, `docs/execution.md`, or `docs/upstream.md` only when the change reaches storage ownership, invalidation, or upstream authority.

## Check

For each manifest or discovery change, state:

1. why the source, contract, or discovery edge is authoritative
2. whether admission is direct, root-reachable, discovered, or migration allow-listed
3. capability flag changes and whether behavior is `unsupported` or `supported` (`shadow` is a `rollout_status` value, never a capability state)
4. watch-plan and invalidation effects
5. upstream citation or explicit bigname divergence

## Rules

- Capability flags gate behavior; public contract existence alone does not.
- Unsupported capability must surface explicitly in coverage or typed errors.
- `exact_name_profile` and `name_history` are exceptions: no served output reads them. `/v1/namespaces` reports `name_profile` and `name_history` as `full` whenever the namespace has an active manifest, and neither flag gates whether a name is served, which follows the authority decision (`docs/manifests.md` "`capability_flags`" and "Capability policy", and the amendments in `docs/adrs/0007-follow-the-chain-ens-authority.md`).
- Adapters consume manifest decisions; they must not rely on hidden config.
- New addresses, roles, source families, or discovery-rule admissions cite `.refs/` deployment metadata or Solidity.
- Schema or capability-meaning changes require `$contract-impact`.
