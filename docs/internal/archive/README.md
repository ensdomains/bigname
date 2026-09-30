# Historical plans and reviews

These documents record past decisions, audits, and implementation plans. They
are retained for the links and rationale used by current documentation; they
are not an active backlog or instructions for the current runtime. Upstream
pins and descriptions of retired components are historical.

Use [architecture](../../architecture.md), [storage](../../storage.md), the
[ADRs](../../adrs/), and the [runbooks](../../runbooks/) for current contracts.
[Ownership boundaries](../workstreams.md) and the
[e2e coverage ledger](../e2e-testing-plan.md) remain outside this archive.

- [July simplification audit](simplification-audit-20260730.md) — the census
  and product decisions behind the phase-runner rewrite.
- [July replacement build plan](simplification-build-plan-20260730.md) — the
  staged implementation plan for that rewrite.
- [Phase-runner design](a2-phase-runner-design-20260731.md) — its original
  orchestration design, before later changes to storage and publication.
- [Development plan](development-plan.md) — the original bootstrap plan.
- [Production Rust inventory](production-rust-logic-inventory.md) — the
  frozen May 2026 source census.
- [API flattening working sheet](api-surface-flattening-scope-decisions.md) —
  historical input to ADR 0003.
- [June remediation post-mortem](remediation-2026-06-postmortem.md) — the
  closed-out review and delivery record.

References in immutable migration comments or upstream-rotation receipts may
name a document's original location. The filenames are preserved here; Git
history records their original paths.
