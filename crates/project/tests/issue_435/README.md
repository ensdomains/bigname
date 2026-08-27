# Issue 435 measurement

The ignored release-mode test loads a deterministic PostgreSQL 16 corpus, extracts the
[Project phase](../../../../docs/glossary.md#project-phase) SQL, and keeps competing scan types enabled.
```bash
DATABASE_URL='postgresql://...' \
ISSUE_435_SEED=435 \
cargo test -p bigname-project --test issue_435_measurement \
  --release -- --ignored --nocapture
```
Artifacts go to `target/issue-435-evidence/<commit-sha>/`; the test appends 5M rows to the indexed 5M corpus and refuses a seed other than 435.
