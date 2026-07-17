# Upstream references

Pinned checkouts of the canonical ENSv1, ENSv2, and Basenames codebases, reference indexers, reference execution clients, and app metadata used by checked-in admissions or comparison rules. Anchors bigname docs, manifests, agent reviews, and task design to real upstream behavior instead of vendored memory.

## Layout

- `MANIFEST.toml` — tracked. Pinned commits and the surfaces each ref is authoritative for.
- `README.md` — tracked. This file.
- `<key>/` — gitignored. Populated by `scripts/sync-refs` at the commit pinned in `MANIFEST.toml`.

## Commands

```
scripts/sync-refs           # clone/fetch/checkout refs and required recursive submodules
scripts/sync-refs --check   # fail for missing/off-pin refs or required submodules
```

First sync clones each repo shallowly and initializes required recursive
submodules (currently Basenames' Forge dependencies). Subsequent runs re-fetch,
re-checkout, and verify both the superproject pins and those gitlinks.

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

`scripts/sync-refs --check` verifies local checkouts and required recursive
submodules match pinned commits. Use `$upstream-evidence` or `evidence_reader`
for claim-to-citation checks, and run deliberate pin-drift checks when
load-bearing citations or manifests change. Nothing bumps pins automatically —
bumping is a deliberate, documented change. See `docs/upstream.md` § Rotation
policy.
