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
3. capability flag changes and whether behavior is `unsupported`, `shadow`, or `supported`
4. watch-plan and invalidation effects
5. upstream citation or explicit bigname divergence

## Rules

- Capability flags gate behavior; public contract existence alone does not.
- Unsupported capability must surface explicitly in coverage or typed errors.
- `exact_name_profile` is the one exception: a namespace-level declaration that `/v1/namespaces` reports as `name_profile`; it does not gate whether a name is served, which follows the authority decision (`docs/manifests.md` "Capability policy", and the 2026-09-25 amendment in `docs/adrs/0007-follow-the-chain-ens-authority.md`).
- Adapters consume manifest decisions; they must not rely on hidden config.
- New addresses, roles, source families, or discovery-rule admissions cite `.refs/` deployment metadata or Solidity.
- Schema or capability-meaning changes require `$contract-impact`.
