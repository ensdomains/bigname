# Interpreter fixtures

`raw-events.json` contains raw log inputs copied from existing adapter tests. Each case runs in a
new migrated database so the copied block, transaction, and log positions remain unchanged.

The sources are:

- ENS and Basenames reverse records:
  `crates/adapters/src/ens_v1_reverse_claim/tests.rs`
- wrapped-name preimage observation:
  `crates/adapters/src/block_derived_normalized_events/tests.rs`
- ENSv1 registrar, registry, and resolver observations:
  `crates/adapters/src/ens_v1_unwrapped_authority/tests.rs`

`expected-outputs.json` records every normalized event and every row in the adapter-owned
`name_surfaces`, `surface_bindings`, `resources`, and `token_lineages` tables. An intentional
interpreter change must update that file in the same review.
