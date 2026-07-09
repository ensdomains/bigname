# bigname end-to-end scenario tests

This package tests the whole pipeline against a real local chain running the
**pinned upstream contracts**. Where `tests/conformance` seeds synthetic rows
directly into Postgres, this harness starts from actual contract emissions:
it deploys the ENSv1 stack from the pinned `.refs/ens_v1` deployment
artifacts onto a local anvil node, drives on-chain state transitions
(registrations, transfers, expiry via time-warp), ingests them with the real
`bigname-indexer run` loop, rebuilds projections with the real
`bigname-worker`, and asserts against the real `bigname-api` binary over
HTTP.

The two packages are complementary:

- `tests/conformance` — fast, hermetic checks of route contracts, coverage
  semantics, and replay determinism over hand-authored state.
- `tests/e2e` — checks that our beliefs about upstream contract behavior are
  true, by observing the pipeline ingest events emitted by the exact bytecode
  upstream shipped.

## Prerequisites

- [foundry](https://getfoundry.sh) (`anvil` on `PATH`)
- pinned upstream checkouts: `scripts/sync-refs`
- a test Postgres: run through `scripts/test-db`

```sh
scripts/test-db -- cargo test --manifest-path tests/e2e/Cargo.toml
```

## How a scenario runs

1. **Chain** — `harness::anvil` starts a local node with a fixed genesis
   timestamp, presented to the indexer as `ethereum-mainnet` (chain identity
   is the provider label; nothing verifies the numeric chain id).
2. **Contracts** — `harness::ens_v1` deploys the mainnet ENSv1 topology from
   pinned artifact bytecode (`.refs/ens_v1/deployments/`): the legacy
   registry, the current registry deployed with the legacy registry as its
   constructor argument
   (upstream: .refs/ens_v1/deployments/sepolia/ENSRegistry.json:L414 @ ens_v1@91c966f),
   the `.eth` base registrar, the current registrar controller with its
   commit/reveal flow
   (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L210 @ ens_v1@91c966f),
   the exponential-premium price oracle over upstream's own dummy USD oracle
   (upstream: .refs/ens_v1/contracts/ethregistrar/DummyOracle.sol:L3 @ ens_v1@91c966f),
   reverse registrars, name wrapper, and public resolver. Deploying from
   artifacts rather than re-compiling means the local chain runs byte-exact
   upstream code; when `.refs` pins rotate, the harness re-verifies our
   decoding against the new artifacts.
3. **Manifests** — `harness::manifests` copies **every version file** of
   the shipped `manifests/mainnet/ethereum/ens` family manifests and
   re-points each declared root/role at its locally deployed address and
   real deploy block. Rollout statuses, capability flags, ABI declarations,
   and discovery rules are preserved verbatim, so admission semantics stay
   identical to production — including the active registry v3 manifest with
   its old-registry role. (Mirroring only `v1.toml` once produced a false
   "production doesn't watch the registry" finding; completeness here is
   load-bearing.) Roles a scenario does not deploy get placeholder
   addresses (no code, no logs). Nothing under the checked-in `manifests/`
   tree changes.
4. **Pipeline** — `harness::pipeline` runs the real binaries: an
   `indexer run` live-intake session supervised until the canonical
   checkpoint reaches the scenario head (the live loop, not `backfill`, is
   what promotes checkpoints that snapshot-selected API reads require), then
   `worker replay all-current-projections`, then `bigname-api serve` on a
   local port. An `indexer backfill` entry point is also provided for future
   backfill-vs-live parity scenarios.
5. **Assertions** — each scenario checkpoint asserts at the validation
   layers named in `docs/architecture.md` § Test matrix: persisted raw logs,
   canonical normalized events, execution traces (once an execution-plane
   scenario exists), and public API output over HTTP.

## Scenarios

- `register_eth_name` — walking skeleton. Registers `alice.eth` through the
  controller's commit/reveal flow (time-warped past the minimum commitment
  age) and asserts raw-log persistence, canonical normalized event kinds,
  and the exact-name route's registration/coverage output. Verified
  resolution is out of scope: no execution RPC is configured.
- `registry_driven_reads` — registry-sourced declared state under the
  shipped profile: declared resolver bindings, registry owner,
  record-inventory selectors, and registry-only subnames appearing as
  bracketed labelhash placeholder children with no exact-name surface
  minted.
- `lifecycle::renew_and_transfer_keep_identity` — renewal extends expiry on
  the same backing resource; the two-transaction transfer→reclaim pair
  opens a genuine registry-owner divergence window (transient anchor) and
  converges back to the original registrar resource.
- `lifecycle::expiry_grace_and_reregistration_rotate_identity` — ingests
  the same chain twice: once inside the grace window (registration stays
  `active` with a past expiry; no wire-level grace status) and once after a
  different account re-registers post-premium-decay (new backing resource;
  both leases' history preserved under distinct resources).
- `lifecycle::register_without_resolver_keeps_declared_resolver_empty` —
  registers through the controller with resolver `address(0)` and asserts
  active registration state with a supported null declared-resolver shape.
- `lifecycle::expire_without_reregistration_releases_and_unlists_registration`
  — registers for the upstream minimum duration and warps past expiry plus
  grace without re-registering. Pins both halves of the contract: on a
  quiet chain the release never settles (sync boundaries are driven by
  log-bearing blocks), and the first unrelated post-grace activity lets the
  next sync round derive the release, flip exact-name to `released`, and
  drop the name from the current registrant collection.
- `registry_driven_reads::same_label_under_two_parents_keeps_children_distinct`
  — creates `sub` under two registered parents and asserts separate child
  namehashes/owners with no cross-parent leakage.
- `registry_driven_reads::deep_registry_hierarchy_lists_direct_children_only`
  — creates a registry-only grandchild under a placeholder parent. Registry
  facts derive at any depth, but enumeration stops at unknown surfaces:
  placeholder names are rejected as `invalid_input` and the grandchild
  projects no children row.
- `registry_driven_reads::zero_owner_subname_leaves_default_children_listing`
  — creates and then zeroes a registry-only subname, asserting the tombstoned
  child leaves the default parent children listing.
- `registry_migration::registry_migration_legacy_to_current_admission` —
  exercises the active registry v3 old-registry role end to end: a pure
  legacy 2LD derives subregistry state without minting an exact-name surface,
  a current-registry registration suppresses later legacy resolver/owner
  writes for that node, and a different unmigrated legacy child remains
  admitted after the cutover.
- `resolver_records::resolver_changes_follow_registry_and_zero_releases` —
  registers with the admitted PublicResolver, moves the registry binding to a
  second deployed PublicResolver copy, then sets it to zero; exact-name and
  compact records resolver state follow each transition.
- `resolver_records::records_route_values_and_version_boundaries_follow_current_resolver`
  — writes a non-60 multicoin addr record and contenthash, asserts compact
  cached values and selectors, then checks resolver replacement and
  `clearRecords` move the record-version boundary without leaking prior
  values.
- `resolver_records::unadmitted_custom_resolver_observes_facts_but_keeps_profile_gated`
  — binds a name to an unpatched PublicResolver copy and writes a text
  record on it; declared reads never surface the unadmitted write (no
  record events, no inventory selectors, `not_found` on request,
  enumeration supported-but-empty).
- `resolver_records::shared_resolver_keeps_per_name_records_and_overview_fan_in_unsupported`
  — two names share one resolver while per-node records stay distinct; the
  resolver overview keeps binding fan-in explicitly unsupported.

## Debugging

- `BIGNAME_E2E_KEEP_DB=1` keeps each scenario's database (the URL is
  printed) instead of dropping it.
- The supervised `indexer run` session writes its full log to
  `$TMPDIR/bigname-e2e-indexer-<pid>-<target block>.log`; failures include
  the tail.

## Extending

The scenario matrices, perturbation multipliers, harness roadmap, and
phasing live in [`docs/internal/e2e-testing-plan.md`](../../docs/internal/e2e-testing-plan.md)
— that document is the coverage ledger; update it in the same change that
adds or unblocks a scenario. Scenarios are ordered on-chain action scripts
with named checkpoints; prefer one scenario per lifecycle path over one per
event.

Keep upstream behavior claims cited to pinned `.refs/` sources; uncited
claims get rejected in review (AGENTS.md § Upstream anchors).
