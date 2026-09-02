# Interpreter fixtures

`raw-events.json` contains 19 bounded raw-log cases. Each case runs in a new
migrated database so its block, transaction, and log positions remain
unchanged. `expected-outputs.json` records every
[normalized event](../../../../../docs/glossary.md) and every row in the
interpret-phase-owned `name_surfaces`, `surface_bindings`, `resources`,
`token_lineages`, and `discovery_edges` tables.

`binding-fk-release.json` is a production-derived, four-batch regression
corpus for the `puy.eth` lease release observed at Ethereum block 16,176,355.
It retains the exact numeric BaseRegistrar renewal before the controller
`NameRenewed`, registrar transfers, registry `NewOwner`,
block hashes, timestamps, and expected release-side resource and binding IDs.
The physical-batch harness proves that compacted prior-state restoration and a
live incremental session both materialize the dormant direct-registry resource
before opening its replacement binding.

`binding-closure-dangling.json` is that same corpus with two hand-built logs
added in the release block, sharing one transaction: a registry `Transfer` and
a legacy-controller `NameRegistered`. It pins issue #339. The lapsed lease
settles at a bare block boundary, deriving a [surface
binding](../../../../../docs/glossary.md#surface-name-surface) whose provenance
carries no transaction or log index. Those missing indexes are not a chain
position and must remain distinct from the added registry log's real `(block,
0, 0)` position. The fixture's `case.synthetic_logs` says which logs are not
production and which ones a real registration would also emit.

`v2-expiry-retirement.json` registers and links `alice.eth`, sets its resolver,
subregistry, and one effective permission grant, ends the first physical batch,
and then supplies an empty block whose timestamp equals the retained expiry. A
later `ExpiryUpdated` log revives the same token and retained resource. The
first block also observes an ownerless reservation whose nonzero expiry equals
that block timestamp; the corpus pins its raw history row followed in the same block immediately
by a state-derived, bindingless release. (upstream:
.refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L452-L465 @
ens_v2@a971bd64) The
corpus pins the block-derived
`SurfaceUnbound`, `RegistrationReleased`, `ResolverChanged`, and
`SubregistryChanged` order and attribution, with no invented transaction or log
position. The harness compares fresh, live incremental, tiny-cache, and
compacted cold-restore execution; repeats the retirement from the same retained
predecessor; and substitutes a same-height, same-timestamp block hash to prove
stable semantic payloads with block-specific identities. The Project fixture
anchors reserve the same identity and event provenance for downstream
incremental-versus-fresh Project convergence coverage; this adapter harness
does not itself run Project. ENSv2 returns no subregistry, resolver, or owner
once the entry expires. (upstream:
.refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L249-L258 @
ens_v2@a971bd64) (upstream:
.refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L343-L354 @
ens_v2@a971bd64) The registry accepts a later expiry update for the same token.
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L212-L227 @
ens_v2@a971bd64)

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
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L467 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L471 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L475 @ ens_v2@a971bd64);
- ENSv2 permission grant and revoke
  (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L267 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L301 @ ens_v2@a971bd64);
- an ENSv2 text record
  (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L475 @ ens_v2_sepolia_20260629@ccaeb58)
  and registrar registration
  (upstream: .refs/ens_v2/contracts/src/registrar/interfaces/IETHRegistrar.sol:L32 @ ens_v2@a971bd64);
- numeric BaseRegistrar registration followed by a controller-free numeric
  renewal against non-empty persisted state, and numeric registrar events plus
  plaintext controller enrichment across a losing registration branch that is
  orphaned before a winning branch restores canonical state
  (upstream: .refs/ens_v1/contracts/ethregistrar/IBaseRegistrar.sol:L15-L20 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L168 @ ens_v1@91c966f)
  (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L108-L139 @ ens_v1@91c966f);
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
  (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L9 @ ens_v2@a971bd64)
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L113 @ ens_v2@a971bd64);
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
controller's `registerWithConfig` ordering. The `.eth` cases place the numeric
BaseRegistrar registration before later registry, resolver, transfer, and
controller logs. The last case commits the ordinary lifecycle from that
BaseRegistrar event, retains the controller event only as plaintext enrichment,
and reconciles the mid-flow record and final registry owner to the same
registration resource. Temporary-controller ownership events are absent
(upstream: .refs/ens_subgraph/subgraph.yaml:L145 @ ens_subgraph@723f1b6)
(upstream: .refs/ens_v1/deployments/mainnet/solcInputs/40ce5451dce8f428cafdaca8fb82d91d.json:L158 @ ens_v1@91c966f).

The fixture metadata carries the full pinned upstream citations. The harness
also asserts the required event kinds, the renewal's non-empty before-state,
the orphaned and restored reorg outputs, and the ordered wrapper transitions
before it permits golden output to be refreshed.

The registrar-authority cases additionally cover a labelhash-only registration
and renewal without controller observations, numeric-plus-controller binding,
conflicting registrar/controller expiry payloads, and resolver setup after the
numeric registration event. ENSv1→ENSv2 correlation, exact Graveyard cleanup,
wrapped renewal routing, and live-versus-cold surface enrichment are exercised
by the schema-v2 unit fixtures that share the same manifest catalog. The dense
same-transaction corpus now contains 1,472 logs and 320 numeric registrar
anchors; `binding-fk-release.json` and `binding-closure-dangling.json` include
the numeric renewal that establishes the lease they later settle.

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
interpretation inputs, and project projection sources. A change that only
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
