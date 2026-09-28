# Installed schema before TYR-36 removal

These four immutable DDL fixtures are copied from `aa06621a9`, the complete pre-removal publication. `apply-check.sh` uses them only for the existing historical schema-migration probes and the exact predecessor of `20260929160000_remove_served_projections.sql`. Other baseline files were unchanged by removal and are shared.

The production initializer reads `schema-v2/baseline` only. The removal check applies the new migration twice and compares the surviving catalog with the current fresh baseline. Do not use these fixtures for new endpoint tests or restore their serving tables to production.
