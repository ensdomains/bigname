# bigname — Claude Code binding

@AGENTS.md

## Claude Code notes

AGENTS.md above is the canonical process contract; this file is only the thin Claude binding for the same harness.

- **Skills.** The six repo skills are canonical in `.agents/skills/` and exposed to Claude Code via symlinks in `.claude/skills/`. Where AGENTS.md or a skill body writes `$skill-name`, that is the skill named `skill-name`: invoke it as `/skill-name`, or fire it implicitly when its description matches the work. Skill discovery requires a symlink-capable checkout (`core.symlinks`, the POSIX default — deployment, CI, and contributors are Linux); on a Git-for-Windows checkout with `core.symlinks=false` the symlinks materialize as plain text files and these skills will not load.
- **Subagents.** `evidence-reader`, `contract-editor`, `slice-builder`, and `verification-reviewer` are ported to `.claude/agents/*.md`. The `.codex/agents/*.toml` definitions stay canonical — if you change one side, change the other. Where AGENTS.md or a skill body writes a codex-registered underscore name (`evidence_reader`, `contract_editor`, `slice_builder`, `verification_reviewer`), the registered Claude Code agent is the hyphenated form — always spawn `verification-reviewer`, never `verification_reviewer`. Codex-only spawn options like `fork_context=false` have no Claude equivalent; a plain Agent spawn already starts with fresh context.
- **`.refs/` is gitignored and absent on a fresh clone.** Run `scripts/sync-refs` before any work that makes upstream claims; without the pinned checkouts the citation skills are inert and citations cannot be verified.
- **Code style.** Keep comment density low, matching the existing code: comments only for constraints the code cannot show, no narration of what the next line does, no restating the diff. Prefer token-efficient implementations over defensive boilerplate.

## When to fire which skill

Skills are checklists you follow inline (they shape how you act now); subagents are fresh-context workers you delegate a bounded task to. A skill and a subagent can compose (a delegated `slice-builder` can run `$contract-impact` first; `$verify-loop` drives the `verification-reviewer` subagent). Skills are preventive (fire *during* the work); `$verify-loop` is the after-the-fact gate. Skip all of this for trivial mechanical edits, non-bigname files, and throwaway work — they cost doc-reading and are for load-bearing changes only.

`$contract-impact` is the router: hit it first whenever a change might touch anything public; it classifies the work and names which specialized lens and which docs/fixtures/migrations must move in the same change.

| Fire | The moment you're about to… |
| --- | --- |
| `/contract-impact` | change anything possibly public — API, coverage, shared IDs/enums, manifest schema, replay semantics, or a parity/replacement claim |
| `/manifest-authority` | add or edit a manifest, address, role, discovery edge, capability flag, or start block |
| `/upstream-evidence` | write any claim about ENSv1/ENSv2/Basenames on-chain behavior (produces the `.refs` citation ledger; uncited claims are deleted, not softened) |
| `/replay-safety` | touch raw facts, normalized events, projections, canonicality/reorg, migrations, or fixtures |
| `/consumer-slice` | build or claim one end-to-end capability (route → projection → tests), or any "we've replaced X" claim (blocks it without conformance evidence) |
| `/verify-loop` | finish a change and want the pre-commit gate (user-invoked only; spawns a blind reviewer, confirms each finding with a failing test first, loops until clean) |

## Subagents

Delegate a bounded chunk to its own context and tool set — keeps the main thread clean, runs in parallel, restricts tools. They pipeline: `evidence-reader` → `contract-editor` → `slice-builder`, gated by `verification-reviewer`.

- **`evidence-reader`** (read-only) — gather `.refs` citations into a ledger without burning main-thread context.
- **`contract-editor`** (docs tools) — make the doc-first edits a change requires; docs/contract artifacts only.
- **`slice-builder`** (all tools) — implement one coherent slice with its tests.
- **`verification-reviewer`** (read-only + inspection Bash) — review a diff and report risks, never fix; this is what `$verify-loop` spawns.
