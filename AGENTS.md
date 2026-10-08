# bigname

bigname is a versioned indexing and read API for ENS, ENSv2, and Basenames. The checked-in docs are the source of truth for semantics; agent process stays in this file and the repo-local skills.

## Guardrails

- You must prove the operating path before you can add guardrails, mutation/regression/legacy compatibility protections, or tests.

- Public-contract docs that constrain agent work: `docs/architecture.md`, `docs/api-v1.md` (plus `docs/api-v1-routes.md`), `docs/storage.md`, `docs/manifests.md`, `docs/consumer-capabilities.md`, `docs/adrs/0001-stack.md`, `docs/adrs/0002-surface-resource-identity.md`.
- If a task changes public semantics, shared IDs or enums, coverage meaning, manifest schema, workstream ownership, or replacement meaning, update the relevant docs first or in the same change.
- Prefer cohesive end-to-end slices — a full capability with its tests and wiring, not a commit-sized edge. Do not build disguised legacy API parity or new planning docs unless semantics changed.

## Communication

- In docs, code comments, reviews, task writeups, plans, and agent output, describe the system in language that an engineer familiar with ENS and the project's stated scope can understand without first learning bigname-specific terminology.
- Prefer standard ENS, Ethereum, and indexing terms over project-specific jargon. When a bigname-specific term is necessary, define it in plain language on first use and explain the behavior it represents.
- `docs/glossary.md` is the canonical definition for each necessary bigname-specific term: link it on first use instead of re-defining or assuming the term, and add new coinages there in the same change that introduces them. Qualify the overloaded terms it flags (bare "promotion", "profile", and "migration" are ambiguous — in particular, say "schema-migration" for bigname's own database history and "ENSv1→ENSv2 migration" for the on-chain protocol move).
- Write for an engineer who knows the ENS smart contracts and what bigname is for, but not bigname's code or the agent process. Smart-contract names need no gloss. Bigname phases and tables do, on first use.
- Lead with what an API user or name owner would observe. State a bug as the wrong result, then the cause.
- Keep process vocabulary out of PR bodies, issues, comments and docs: review-request numbers, agent roles, reviewer names as authority, thread-coined labels. Cite docs by path and heading.
- CI and the test files are the evidence. Do not narrate local test counts or lint runs.
- Short sentences and short paragraphs. No semicolons joining clauses, and no paragraph that packs several points into one block. Use a list for parallel items.
- Agent messages and review briefs follow the same rules. IDs and shas are fine as addresses.

## Boundaries

- Schema-v2 interpret writes identity rows, discovery edges, normalized
  events, and append-only operator diagnostics for event logs from undeclared
  emitters skipped after an ABI decode failure or logged before their
  emitter's same-batch discovery admission. Before deleting a redo range,
  Interpret may also preserve finitely retired manifest-declared address ranges
  that keep older observations from reopening retired authority. Those retired
  address ranges and decode-failure diagnostics are coordination or diagnostic
  state, not projection or serving data. Project needs no handoff from
  Interpret: it undoes and replays its own
  [owned key families](docs/glossary.md#owned-key-family).
  At a completed pass boundary, Interpret also owns the
  [`discovery_watch_admissions` coordination snapshot](docs/glossary.md#discovery-watch-admission-snapshot)
  and may atomically install
  required Ingest work through the shared `chain_phase_state` installer when
  newly discovered physical watch coverage overlaps retained intake history.
  The snapshot is not a work queue: `chain_phase_state` remains the sole
  work/redo authority.
  Adapters provide interpretation behavior and do not write projection rows.
  A phase that reads Interpret's tables may add read-only indexes on them
  through its own schema-migration, changing no row, when it lists each index with
  the statement it serves under [table ownership](docs/storage.md#table-ownership).
- API code reads phase projections, normalized events, and request-scoped lookup
  output only, except explicit audit endpoints. API requests never write database
  state; the lookup library's guarded [resolution divergence
  ledger](docs/glossary.md#resolution-divergence-ledger) writer is for non-API callers.
  Provider responses are request-scoped: serving paths never persist them as
  reusable outcomes or as a durable step-by-step record of the calls made.
- Lookup code uses declared topology and manifests, not adapter internals.
- Manifest and discovery code decides what is authoritative.
- Raw facts are immutable. Projections are rebuildable. Canonicality is explicit. Unsupported behavior must be explicit.

## Upstream anchors

The canonical ENSv1, ENSv2, and Basenames codebases are pinned under `.refs/`. Agents read from the pinned checkouts; they do not guess or paraphrase upstream behavior from memory.

- `.refs/ens_v1/` — canonical ENSv1 Solidity
- `.refs/ens_v1_mainnet_1a2ac5c/` — historical deployment ABI evidence for the admitted `0x231b0Ee…` Mainnet PublicResolver generation only
- `.refs/ens_v1_sepolia_8209157/` — historical deployment ABI evidence for the admitted `0x8948458…` Sepolia PublicResolver generation only
- `.refs/ens_v1_sepolia_ac32490/` — historical deployment ABI evidence for the admitted `0x8FADE66…` Sepolia PublicResolver generation only
- `.refs/ens_v1_lll/` — historical evidence for the 2017 LLL registry only
- `.refs/ens_v2/` — pinned protocol source and historical June/July deployment evidence; current Sepolia deployment authority is the separate pin below.
- `.refs/ens_v2_sepolia_20261001/` — official 2026-10-01 Sepolia redeploy artifacts, receipts, compiler inputs and matching source at `07e55a05`; current manifest authority (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/.deployment.json:L4 @ ens_v2_sepolia_20261001@07e55a05)
- `.refs/ens_v2_sepolia_20260916/` — the superseded 2026-09-15 Sepolia deployment at `366de741`, which the manifests drop; ENSv2 Solidity source evidence only where the cited lines are unchanged at the 2026-10-01 pin, and evidence of the dropped deployment's own history; never deployment authority
- `.refs/ens_v2_sepolia_20260629/` — historical Solidity evidence for the admitted 2026-06-29 old-model Sepolia deployment only; it is not authority for future deployments or current-model admission
- `.refs/ens_v2_sepolia_dev/` — historical evidence for deprecated pre-audit `sepolia-dev` manifest versions only
- `.refs/basenames/` — canonical Basenames Solidity
- `.refs/ens_rainbow/` — Graph Protocol ENS rainbow-table tooling, labelhash preimage import table-shape evidence only
- `.refs/ens_subgraph/`, `.refs/ensnode/` — reference indexers for cross-check only
- `.refs/ens_app_v3/` — ENS app known-resolver metadata for first-party app admission rows only
- `.refs/ponder/` — reference indexer for chain-intake cross-check only
- `.refs/graph_node/` — reference indexer for chain-intake cross-check only
- `.refs/reth/` — reference Ethereum execution client for node-level chain-intake cross-check only

Pins live in `.refs/MANIFEST.toml`. Sync with `scripts/sync-refs`; verify with `scripts/sync-refs --check`. Rotation policy and known divergences live in `docs/upstream.md`.

Citation rules:

- Any claim about ENSv1, ENSv2, Basenames, admitted upstream app metadata, reference-indexer comparison, or reference execution-client comparison behavior — in docs, manifests, ADRs, code comments, task writeups, or agent output — must cite the upstream source as `(upstream: .refs/<key>/<path>:L<line> @ <key>@<short-commit>)`.
- "Upstream says X" without a `.refs/` citation is unsupported and should be rejected in review.
- When upstream disagrees with our docs or manifests, the disagreement is a doc-first task. We may intentionally narrow, widen, or reshape upstream semantics; the divergence must be stated explicitly in the doc that carries our rule and listed in `docs/upstream.md` § Known divergences.
- Manifest address changes and new source families cite the upstream deployment metadata or Solidity file rather than relying on external URLs.

## High Conflict

- Keep `crates/domain` narrow.
- Coordinate migrations carefully.
- Treat fixture updates as cross-workstream review points.
- Inspect dirty state before staging. Stage explicit paths only, and never stage unrelated user or agent work.

## Rust File Size

- Hand-written production `.rs` files normally target <=500 LOC.
- The script emits advisory warnings for hand-written production files >500 LOC.
- Hand-written production files >600 LOC require an explicit entry in `scripts/rust-file-size-baseline.toml`; the file is now an oversized-file allowlist, not a full production-file ratchet list.
- Every allowlist entry must match the current file size and include a justification. Entries >900 LOC also require explicit review justification.
- Newly allowlisted hand-written production files may not exceed 1200 LOC. Existing allowlist allowances may not increase over the base allowance.
- Remove allowlist entries once files shrink to <=600 LOC. Omitting a base entry is OK only when the current file is no longer oversized.
- Generated code, bindings, typegen, constants, fixtures, tests, and equivalent non-production files remain excluded from the gate.
- `lib.rs` and `main.rs` are wiring files: target <=300 LOC, with hard review and an allowlist entry required above 500 LOC.
- The CI/script gate lives in `scripts/check-rust-file-size`.

## Core Skills

- `$contract-impact`: classify implementation-only vs doc-first/shared-interface work before coding.
- `$upstream-evidence`: gather pinned `.refs/` citations and divergence notes for upstream behavior claims.
- `$consumer-slice`: scope one end-to-end consumer capability with docs, behavior, tests, and explicit deferrals.
- `$manifest-authority`: plan or review manifests, discovery, admission, capability flags, and watch-plan authority.
- `$replay-safety`: review raw facts, normalized events, canonicality, projection rebuilds, invalidation, and migrations.
- `$verify-loop`: user-invoked reviewer/fix loop that spawns a fresh `verification_reviewer`, confirms real findings with failing tests or checks, fixes them, and repeats until clean.
- `$pr-description`: title and body shape for a pull request, including when to say the interpreter content hash rotates.
- `$linear-issue`: title, labels and description shape for a Tyrell Corporation (TYR) ticket.

## Core Agents

- `evidence_reader`, `contract_editor`, `slice_builder`, and `verification_reviewer` are defined in `.codex/agents/`.
- Use subagents only for bounded work with a clear output contract. Do not run autonomous "keep shipping" loops without a named capability target and review gate.
