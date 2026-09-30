# Same-release restore exercise

This directory retains the PostgreSQL restore exercise delivered in
[PR #882](https://github.com/ensdomains/bigname/pull/882). Its Rust driver is
[`same_release_restore.rs`](../src/bin/same_release_restore.rs); `checkpoint.sql`
and `roles.sql` supply its selected-state checks and disposable database roles.

`operate.sh` and `evidence-contract.md` record the original, explicitly
reserved exercise. The launcher is deliberately pinned to its original base
commit, image, upstream artifacts, and source budget, so it refuses current
main. Moving these files does not refresh or rerun that historical proof.
The contract's coordination and usage instructions are historical, not
current contributor instructions.

This exercise is separate from the regular e2e CI gate. Use the current
[rollback runbook](../../../docs/runbooks/rollback.md) and
[production runbook](../../../docs/runbooks/production-docker.md) for operational
requirements. Retaining these files does not establish current deployment
recovery times or cross-release rollback compatibility.
