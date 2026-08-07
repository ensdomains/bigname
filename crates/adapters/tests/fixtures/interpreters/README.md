# Interpreter fixtures

`raw-events.json` contains 19 bounded raw-log cases. Each case runs in a new
migrated database so its block, transaction, and log positions remain
unchanged. `expected-outputs.json` records every
[normalized event](../../../../../docs/glossary.md) and every row in the
interpret-phase-owned `name_surfaces`, `surface_bindings`, `resources`,
`token_lineages`, and `discovery_edges` tables.

`binding-fk-release.json` is a production-derived, four-batch regression
corpus for the `puy.eth` lease release observed at Ethereum block 16,176,355.
It retains the exact `NameRenewed`, registrar transfers, registry `NewOwner`,
block hashes, timestamps, and expected release-side resource and binding IDs.
The physical-batch harness proves that compacted prior-state restoration and a
live incremental session both materialize the dormant direct-registry resource
before opening its replacement binding.

`binding-closure-dangling.json` is that same corpus with two hand-built logs
added in the release block, sharing one transaction: a registry `Transfer` and
a legacy-controller `NameRegistered`. It pins issue #339. The lapsed lease
settles at a bare block boundary, deriving a surface binding whose provenance
carries no transaction or log index; the reconciler's binding index defaults
that to `(block, 0, 0)`, which is where the added registry log sits, so the
binding is dropped, while the closure's own `(-1, -1)` sentinel spares it —
leaving `except_surface_binding_id` naming a binding the batch no longer
opens. The fixture's `case.synthetic_logs` says which logs are not production
and which ones a real registration would also emit.

The original four cases were copied from these now-deleted legacy adapter
tests:

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
- a non-`.eth` ENSv1 `NameWrapped` → `FusesSet` lifecycle, with
  committed `wrapped`, `emancipated`, `locked`, and owner-controlled-fuse
  outputs. The fixture covers a zero bitmap, `PARENT_CANNOT_CONTROL`,
  `CANNOT_UNWRAP`, and `CANNOT_SET_RESOLVER`; it intentionally adds no
  registrar-controller events
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L358 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/README.md:L32 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/README.md:L34 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L10 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L13 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L18 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L27 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L35 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L37 @ ens_v1@91c966f).

The four B2 discovery-semantics additions exercise:

- an ENSv1 registry `NewOwner` child-history event whose owner remains a leaf
  and creates no discovery edge
  (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L82 @ ens_v1@91c966f);

- an ENSv2 registry instance announced by its constructor's
  `RegistryCreated` event without any parent link
  (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L9 @ ens_v2@ccaeb58)
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L113 @ ens_v2@ccaeb58);
- an ENSv1 `AddrChanged` record selected by the match-all resolver signature
  scope even though no registry resolver pointer or discovery edge names the
  emitting address
  (upstream: .refs/ens_v1/contracts/resolvers/profiles/IAddrResolver.sol:L6 @ ens_v1@91c966f);
- the standard proxy `Upgraded` event as contract-scoped history
  (upstream: .refs/basenames/lib/openzeppelin-contracts/contracts/interfaces/IERC1967.sol:L13 @ basenames@1809bbc).

The four PR #301 reconciliation additions exercise a Basenames registration
over a preceding registry owner, an ENS registration born wrapped, a reverse
claim followed by a resolver name record without a materialized forward
[name surface](../../../../../docs/glossary.md), and the legacy mainnet
controller's `registerWithConfig` ordering. The last case
commits one mid-flow `RecordChanged`, the final `NewOwner`-derived
`SubregistryChanged`, `AuthorityTransferred`, and `PermissionChanged`, and the
five `NameRegistered` lifecycle events. All nine retained events reference the
same materialized surface and registration resource; the temporary-controller
ownership events are absent
(upstream: .refs/ens_subgraph/subgraph.yaml:L145 @ ens_subgraph@723f1b6)
(upstream: .refs/ens_v1/deployments/mainnet/solcInputs/40ce5451dce8f428cafdaca8fb82d91d.json:L158 @ ens_v1@91c966f).

The fixture metadata carries the full pinned upstream citations. The harness
also asserts the required event kinds, the renewal's non-empty before-state,
the orphaned and restored reorg outputs, and the ordered wrapper transitions
before it permits golden output to be refreshed.

Validate the byte-identical corpus through its schema-v2 consumer with:

```console
cargo test -p bigname-adapters --test schema_v2_interpreter_fixtures --locked
```

This harness has no bless mode. Any intentional corpus change requires a
separate semantic review; old-runtime co-deletion must not regenerate the
expectations.

The A3 content hash covers decode and mapping semantics. For every
`[[abi.events]]` entry it hashes the entire block, including `fragment`,
`emitter_roles`, and `normalized_events`, together with production adapter
sources, manifest-authority sources used to persist declarations and select
interpretation inputs, and worker projection sources. A change that only
expands the watched signature set is an ingest concern: build-plan amendment A
requires fetching the new
signature's historical [raw facts](../../../../../docs/glossary.md) before
the derived rebuild, rather than relying on this hash.

The file-level scan deliberately over-triggers in two cases. Inline
`#[cfg(test)]` modules inside an otherwise production file remain hashed
conservatively, and B-stage internal cuts will bump the hash during the
rewrite. Both are expected.

The deliberate remaining corpus gaps ride the pre-B2 gate: the Basenames
forward path and broader resolver-record coverage. An intentional interpreter
change must update the golden output in the same review.
