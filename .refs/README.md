# Upstream references

Pinned checkouts of the canonical ENSv1, ENSv2, and Basenames codebases (plus two reference indexers). Anchors bigname docs, manifests, agent reviews, and task design to real upstream behavior instead of vendored memory.

## Layout

- `MANIFEST.toml` — tracked. Pinned commits and the surfaces each ref is authoritative for.
- `README.md` — tracked. This file.
- `<key>/` — gitignored. Populated by `scripts/sync-refs` at the commit pinned in `MANIFEST.toml`.

## Commands

```
scripts/sync-refs           # clone/fetch/checkout each ref to the pinned commit
scripts/sync-refs --check   # nonzero exit if any ref is missing or off-pin
```

First sync clones each repo shallowly; subsequent runs re-fetch and re-checkout the pinned commit.

## Citation format

Everywhere in the repo — docs, ADRs, manifests, code comments, task writeups, agent outputs — upstream claims are cited as:

```
(upstream: .refs/<key>/<path>:L<line> @ <key>@<short-commit>)
```

Example:

```
(upstream: .refs/basenames/src/L2/Registry.sol:L42 @ basenames@1809bbc)
```

See `AGENTS.md` § Upstream anchors for the governing rules and `docs/upstream.md` for the pin table and rotation policy.

## When pins drift

`scripts/sync-refs --check` and the `upstream_auditor` codex agent both surface drift. Neither bumps pins automatically — bumping is a deliberate, documented change. See `docs/upstream.md` § Rotation policy.
