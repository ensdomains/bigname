# Interpreter fixtures

`raw-events.json` contains 11 bounded raw-log cases. Each case runs in a new
migrated database so its block, transaction, and log positions remain
unchanged. `expected-outputs.json` records every
[normalized event](../../../../../docs/glossary.md) and every row in the
adapter-owned `name_surfaces`, `surface_bindings`, `resources`,
`token_lineages`, and `discovery_edges` tables.

The original four cases were copied from these adapter tests:

- ENS and Basenames reverse records:
  `crates/adapters/src/ens_v1_reverse_claim/tests.rs`
- wrapped-name preimage observation:
  `crates/adapters/src/block_derived_normalized_events/tests.rs`
- ENSv1 registrar, registry, and resolver observations:
  `crates/adapters/src/ens_v1_unwrapped_authority/tests.rs`

The seven A3 additions exercise:

- ENSv2 registry label registration, resource linking, and subregistry
  discovery
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L464 @ ens_v2@ccaeb58)
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L468 @ ens_v2@ccaeb58)
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L472 @ ens_v2@ccaeb58);
- ENSv2 permission grant and revoke
  (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L267 @ ens_v2@ccaeb58)
  (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L301 @ ens_v2@ccaeb58);
- an ENSv2 text record
  (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L475 @ ens_v2@ccaeb58)
  and registrar registration
  (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@ccaeb58);
- ENSv1 registration followed by renewal against non-empty persisted state,
  and a losing registration branch that is orphaned before a winning branch
  restores canonical state
  (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L116 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L133 @ ens_v1@91c966f);
- the ENSv1 `NameWrapped` → `FusesSet` → `NameUnwrapped` lifecycle
  (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L27 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L35 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L37 @ ens_v1@91c966f).

The fixture metadata carries the full pinned upstream citations. The harness
also asserts the required event kinds, the renewal's non-empty before-state,
the orphaned and restored reorg outputs, and the ordered wrapper transitions
before it permits golden output to be refreshed.

Refresh an intentional semantic change with:

```console
BIGNAME_BLESS_INTERPRETER_FIXTURES=1 \
  cargo test -p bigname-adapters --test interpreter_fixtures --locked
```

The A3 content hash covers decode and mapping semantics. For every
`[[abi.events]]` entry it hashes the entire block, including `fragment`,
`emitter_roles`, and `normalized_events`, together with production adapter
sources, manifest-authority sources used by discovery reconciliation, and
worker projection sources. A change that only expands the watched signature
set is an ingest concern: build-plan amendment A requires fetching the new
signature's historical [raw facts](../../../../../docs/glossary.md) before
the derived rebuild, rather than relying on this hash.

The file-level scan deliberately over-triggers in two cases. Inline
`#[cfg(test)]` modules inside an otherwise production file remain hashed
conservatively, and B-stage internal cuts will bump the hash during the
rewrite. Both are expected.

The deliberate remaining corpus gaps ride the pre-B2 gate: the Basenames
forward path and broader resolver-record coverage. An intentional interpreter
change must update the golden output in the same review.
