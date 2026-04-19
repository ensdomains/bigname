# Upstream references

bigname anchors every ENSv1, ENSv2, and Basenames claim to a specific upstream commit pinned under `.refs/`. This doc is the human-readable companion to `.refs/MANIFEST.toml` — the pin table, rotation policy, and the intentional-divergence list.

## Pinned refs

| Key | Repo | Commit | Purpose |
|-----|------|--------|---------|
| `ens_v1` | `ensdomains/ens-contracts` | `91c966fe` | Canonical ENSv1 Solidity |
| `ens_v2` | `ensdomains/contracts-v2` | `725a18d7` | ENSv2 contracts |
| `basenames` | `base-org/basenames` | `1809bbc9` | Canonical Basenames Solidity |
| `ens_subgraph` | `ensdomains/ens-subgraph` | `723f1b6a` | Reference ENSv1 indexer |
| `ensnode` | `namehash/ensnode` | `2017ae62` | Alternative ENS indexer |

Full pin records (including per-ref `authoritative_for` lists) live in `.refs/MANIFEST.toml`. Sync with `scripts/sync-refs`.

## Citation format

```
(upstream: .refs/<key>/<path>:L<line> @ <key>@<short-commit>)
```

Use this exact shape everywhere — docs, ADRs, manifests, code comments, task writeups, agent output. Consistent format lets `verification_reviewer` and `upstream_auditor` verify citations mechanically.

## Rotation policy

- **Bump when**: a cited upstream file changes materially, or we need to adopt a new upstream behavior (new contract, new event, new invariant). Staying behind upstream is fine — being silently wrong is not.
- **Do not bump for**: drive-by upstream refactors, test-only changes, comment edits, rename-only commits.
- **How to bump**:
  1. Update the `commit` field in `.refs/MANIFEST.toml`.
  2. Update the row in the table above.
  3. Run `scripts/sync-refs`.
  4. Re-grep the repo for `@ <key>@<old-short-commit>` citations; update any that point at content that changed across the bump.
  5. Add or edit entries in § Known divergences if the bump surfaced or resolved an intentional deviation.
  6. Commit with a message naming what upstream change motivated the bump, e.g. `chore(refs): bump ens_v1 to <new-sha> — adopt new reverseClaimer event`.
- **Who decides**: whoever owns the surface affected. Ambiguous cases route through `$change-gate`; cross-surface bumps route through `verification_reviewer` after the sync.

## Known divergences

Intentional differences between our docs/manifests and upstream. Every divergence lives here so that citations reading "differently than upstream" are legible instead of looking like bugs. If a divergence is not in this list, it should be treated as drift and closed — either by updating our doc or by adding the entry.

_This list is seeded empty. Add entries as they are discovered or introduced._

Per-entry format:

> **Surface** — one-line description of what differs.
> **Upstream**: `(upstream: .refs/<key>/<path>:L<line> @ <key>@<short-commit>)`
> **Our rule**: `docs/<file>.md` § section.
> **Why**: the constraint that drove the divergence (consumer capability, storage invariant, coverage narrowing, etc.).
> **Since**: commit or date the divergence was introduced.

## Audit loop

`upstream_auditor` (read-only codex agent, `.codex/agents/upstream-auditor.toml`) diffs each pinned commit against its upstream `main`, identifies cited files that changed, and reports pins plausibly worth bumping. It does not bump; bumping stays manual per the rotation policy above.

Run opportunistically when manifests/ADRs change, or on a weekly `$schedule`. Stale pins are not urgent by default — material upstream behavior change is the trigger, not calendar time.
