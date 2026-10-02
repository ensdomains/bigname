# Official Sepolia deployment coverage

`manifests/sepolia` selects the official 2026-10-01 Sepolia redeploy at contracts-v2
`07e55a056f5b6a9c90119f501bdd05714e67dddd`. Upstream deployed a fresh ENSv2 set
on 2026-10-01 and pointed the long-lived Universal Resolver proxies at it, so the
2026-09-15 deployment the manifests previously selected no longer decides
resolution. Its contracts are dropped, not kept as retired history: no Sepolia
manifest names them, and Sepolia's ENSv2 history starts with the new set.
(upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/.deployment.json:L4 @ ens_v2_sepolia_20261001@07e55a05)

The following inventory accounts for every entry in the pinned address list.
A helper's presence does not grant indexed ownership, records, or permissions.
Existing capability flags and unsupported responses retain their documented
scope; this change does not add DNS, payment-token, smart-account, or pricing APIs.

| Artifact | Address | Admission or boundary |
|---|---|---|
| `BatchRegistrar` | `0x4a4c8b7cdab6b19dc2cdb417cdb53a2ccbaf5322` | ens_v2_migration_l1 / batch_registrar sender metadata; registry logs remain authoritative. (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/BatchRegistrar.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `BoxedENSURIRenderer` | `0x0f5b101b6fc626b9b210bb5e70f60f3dd9ca0d96` | Token metadata URI renderer; no indexed token metadata. (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/BoxedENSURIRenderer.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `ContractNamer` | `0x606f2453484f4fa85b6e5fdb0e0bf777064bf9f3` | Contract naming dependency; no indexed contract-account names or permissions. (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/ContractNamer.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `DefaultReverseRegistrarAdapter` | `0x36f97328e843e37520cbf530e9402791c2754066` | Default reverse helper; it writes through the ENSv1 `DefaultReverseRegistrar` `0x4F382928805ba0e23B30cFB75fC9E848e82DFD47`, whose `NameForAddrChanged` intake belongs to ens_v1_reverse_l1 / default_reverse_registrar. (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/DefaultReverseRegistrarAdapter.json:L2 @ ens_v2_sepolia_20261001@07e55a05) (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/reverse-registrar/DefaultReverseRegistrarAdapter.sol:L64-L75 @ ens_v2_sepolia_20261001@07e55a05) (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/DefaultReverseRegistrarAdapter.json:L324 @ ens_v2_sepolia_20261001@07e55a05) (upstream: .refs/ens_v1/deployments/sepolia/DefaultReverseRegistrar.json:L2 @ ens_v1@91c966f) |
| `DNSAliasResolver` | `0xe30c9374929de41b71b6fb999b945553f2194c5f` | DNS resolution dependency; no indexed DNS record inventory. (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/DNSAliasResolver.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `DNSSECGatewayProvider` | `0xd542a53982fdd0c456f02c5a54734c06073fa915` | CCIP-read gateway dependency; no indexed gateway policy. (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/DNSSECGatewayProvider.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `DNSTLDResolver` | `0x0c9f5e9ae61165140b49919f0df13c0a6642e80d` | DNS resolution dependency; no indexed DNSSEC verifier inventory. (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/DNSTLDResolver.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `DNSTXTResolver` | `0x00263173b7de91594eb4140ad8dd3a723b8e4eb3` | DNS resolution dependency; no indexed DNS TXT inventory. (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/DNSTXTResolver.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `ENSURIRenderer` | `0x4d7f0349dffb8e7bed9ffba1b9e45f7ecbd4f0f8` | Token metadata URI renderer; no indexed token metadata. (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/ENSURIRenderer.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `ENSV1Resolver` | `0x322b7581ca210a69c6d0e0d7c88a7688d2789cb0` | ens_v2_resolver_l1 / ensv1_mirror_resolver (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/ENSV1Resolver.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `ENSV2Resolver` | `0x1cf3989ed3e5ec3cb1d731fc3777323813b61acf` | Resolver routing dependency; no indexed record inventory. (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/ENSV2Resolver.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `ETHRegistrar` | `0xf633e7fc17e2bbe0d0965d18ec1821dcb754a3d3` | ens_v2_registrar_l1 / registrar (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/ETHRegistrar.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `ETHRegistry` | `0xd4ebcbbdf463c9c45784603db0ddd499bc44a8b4` | ens_v2_registry_l1 / registry (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/ETHRegistry.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `ETHRenewerV1` | `0xf2ece44980778966b8a0fccb3a9e339440f6e045` | ens_v2_migration_l1 / ens_v1_renewal_bridge (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/ETHRenewerV1.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `Graveyard` | `0xb58a90a39d13cce1d0e192b5da5c47640855b04d` | ens_v2_migration_l1 / graveyard (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/Graveyard.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `HCAOwnerAndSessionValidator` | `0x4bf641590ab18e31b9f8789a3417a2620f860466` | Smart-account validation dependency; no indexed session permissions. (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/HCAOwnerAndSessionValidator.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `HCAUpgradeSet` | `0xcde956d6e2949bc25a4273f93e0db6f68a1a6f34` | Smart-account upgrade policy; no indexed session or upgrade-policy inventory. (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/HCAUpgradeSet.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `LabelStore` | `0xed8246ff02203a4d4cb262bd78beaa7408a57cae` | Label storage helper; standalone Label announcements are not indexed as name registrations. (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/LabelStore.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `LockedMigrationController` | `0x6029a063d69b09d23c52a754a90e4fe43adac3a8` | ens_v2_migration_l1 / locked_migration_controller (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/LockedMigrationController.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `ManagedUniversalResolverProxy` | `0x6d80F2172CFdEc5730fE683860C33d26fC42e6F1` | ens_execution / universal_resolver_managed: intermediate execution proxy, `Upgraded` admitted from block `10922008` for the cutover; no independent indexed authority. (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/ManagedUniversalResolverProxy.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `MigrationHelper` | `0xa8f86ee5cdd28703bd876f3a8c10b1de70f36899` | ens_v2_migration_l1 / migration_helper (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/MigrationHelper.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `MockDAI` | `0xf6fac8a58a0be13b9197f27c41b73162fe32572b` | Test payment token; token transfers are outside naming intake. (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/MockDAI.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `MockUSDC` | `0x240b0316df57887dbbe58b586508b19e633a14aa` | Test payment token; token transfers are outside naming intake. (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/MockUSDC.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `PermissionedResolverImpl` | `0x115eb53f0c60696633855f90b138178fb40b2b2c` | resolver implementation admission metadata (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/PermissionedResolverImpl.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `PublicResolverSet` | `0x5b2bd5208dac31905106d8e5a4973ae1cd7414f2` | Migration resolver allowlist; no independently served allowlist inventory. (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/PublicResolverSet.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `PublicResolverV2` | `0xdc4a563d00f5c3012b699794eb9e13a561be386f` | ens_v2_resolver_l1 / public_resolver_v2 (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/PublicResolverV2.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `RegistryUpgradeSet` | `0xf0a6f68c28603bdf2881ca17476ee477c72cd2fe` | Registry upgrade allowlist; actual instance Upgraded observations govern indexing. (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/RegistryUpgradeSet.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `ReverseRegistrarAdapter` | `0x56bce5e727faa9d237341bb5b9e8a03d5919779d` | Delegates reverse writes; intake belongs to the canonical ReverseRegistrar. (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/ReverseRegistrarAdapter.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `RootBatchRegistrar` | `0x4cdedc6b514a6bc9b64854dfe2d6849d5b1369a6` | Batch submission helper; naming effects come from registry logs. (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/RootBatchRegistrar.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `RootRegistry` | `0xb458d6a3a77919449d03e7a6903c26827c1ec43f` | ens_v2_root_l1 / root_registry (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/RootRegistry.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `StandaloneHCAFactory` | `0x6bad0176236e97b346b5dd13bcc8325b931ee8ab` | Smart-account factory; HCA deployment is outside indexed naming ownership. (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/StandaloneHCAFactory.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `StandaloneHCAImplementation` | `0xc940e5c5bf263c0e097054aecf73826769a72cee` | Smart-account implementation; module/session state is outside indexed naming permissions. (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/StandaloneHCAImplementation.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `StandardRentPriceOracle` | `0x8196665d4ca7488b6474a9ec8e7d2719fb42263a` | Registration pricing dependency; no indexed pricing API. (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/StandardRentPriceOracle.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `UniversalHelper` | `0xd453e5bdb62cc3bea84341b1e306319c8ffd7dfe` | Read helper; no independently indexed state. (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/UniversalHelper.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `UniversalResolverV2` | `0x24e1d8e068620b647ca097f961a61055f4f42d72` | Execution implementation behind the declared entrypoint; listed in `ens_execution`'s `universal_resolver_implementations`, so its installation behind the proxies marks the [Universal Resolver cutover](glossary.md#universal-resolver-cutover) (block `11821680`). (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/UniversalResolverV2.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `UnlockedMigrationController` | `0x2a35b94df22cc7354570be2284655e2cdc0e64a2` | ens_v2_migration_l1 / unlocked_migration_controller (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/UnlockedMigrationController.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `UpgradableUniversalResolverProxy` | `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe` | ens_execution / universal_resolver, `Upgraded` admitted from block `8928790` (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/UpgradableUniversalResolverProxy.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `UserRegistryImpl` | `0x9bd8a88719068d09ecee662f36c0e3856708366a` | Registry implementation; instances enter through RegistryCreated, not direct implementation ownership. (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/UserRegistryImpl.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `VerifiableFactory` | `0xda70306c98e97ece36f997a21368e53298572991` | ens_v2_migration_l1 / verifiable_factory (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/VerifiableFactory.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |
| `WrapperRegistryImpl` | `0xbe768b63e5fbbfbb0ae97e9064e0002df8001880` | ens_v2_migration_l1 / wrapper_registry_implementation (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/WrapperRegistryImpl.json:L2 @ ens_v2_sepolia_20261001@07e55a05) |

## Historical intake and deployment replacement

The new migration controllers bind the canonical Sepolia ENSv1 NameWrapper,
and the mirror resolver binds the canonical ENSv1 registry. Preserve the canonical
ENSv1 registry, registrar, wrapper and declared resolver history; the hackathon's
separate ENSv1 addresses are not part of this corpus.
(upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/LockedMigrationController.json:L744 @ ens_v2_sepolia_20261001@07e55a05)
(upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/ENSV1Resolver.json:L749 @ ens_v2_sepolia_20261001@07e55a05)

The canonical ReverseRegistrar is also declared for reverse-claim keys.
(upstream: .refs/ens_v1/deployments/sepolia/ReverseRegistrar.json:L2 @ ens_v1@91c966f)
Start fresh intake at block zero: some retained ENSv1 resolver provenance and the
long-lived Universal Resolver proxy have no exact creation receipt in the
admitted artifact set. Zero is a conservative lower bound, not a deployment date.
New ENSv2 declarations use their individual deployment receipt blocks.

A fresh database follows the same steps: initialize it with the current
migrations and phase schema and run ingestion through verified live follow.
A database that already indexed the 2026-09-15 deployment does not need a
reset. The redeploy ships as version 2 of the six ENSv2 families under a new
deployment epoch, and manifest synchronization retires the dropped addresses
and stamps the Ingest, Interpret and Project redo that re-derives Sepolia
without them; the release entry in
[`deployment.md`](deployment.md#sepolia-ensv2-redeploy-of-2026-10-01) gives
the order and the stamped range. Raw facts the dropped contracts emitted stay
stored, as raw facts always do. Their registries announced themselves with
`RegistryCreated`, so like any self-announced registry they remain indexable,
but they are not reachable from the admitted root and name nothing. On an
upgraded database their writes after the synchronization head are ignored; see
the release entry.

## Resolver and discovery coverage

The canonical Universal Resolver proxy `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe` remains the
request entrypoint. The manifest admits both `ens_v1` and `ens_v2` verified authority arms through
it. The evidence for the ENSv2 arm has two parts of different strength.

What the pinned artifacts prove: the `UniversalResolverV2` implementation at
`0x24e1d8e068620b647ca097f961a61055f4f42d72` was constructed with the new RootRegistry
`0xb458d6a3a77919449d03e7a6903c26827c1ec43f` as its first argument, which the constructor stores
as the immutable `ROOT_REGISTRY` that resolution starts from.
(upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/UniversalResolverV2.json:L2 @ ens_v2_sepolia_20261001@07e55a05)
(upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/UniversalResolverV2.json:L1593 @ ens_v2_sepolia_20261001@07e55a05)
(upstream: .refs/ens_v2_sepolia_20261001/contracts/src/universalResolver/UniversalResolverV2.sol:L20 @ ens_v2_sepolia_20261001@07e55a05)
(upstream: .refs/ens_v2_sepolia_20261001/contracts/src/universalResolver/UniversalResolverV2.sol:L29-L38 @ ens_v2_sepolia_20261001@07e55a05)
(upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/RootRegistry.json:L2 @ ens_v2_sepolia_20261001@07e55a05)
The pinned deployment notes also describe the intended route: on Sepolia the long-lived proxy
already points at the managed proxy `0x6d80F2172CFdEc5730fE683860C33d26fC42e6F1`, and a fresh
deployment's only on-chain change is `upgradeTo(newImplementation)` on that managed proxy.
(upstream: .refs/ens_v2_sepolia_20261001/contracts/docs/universalResolver.md:L16-L17 @ ens_v2_sepolia_20261001@07e55a05)
(upstream: .refs/ens_v2_sepolia_20261001/contracts/docs/universalResolver.md:L24 @ ens_v2_sepolia_20261001@07e55a05)
(upstream: .refs/ens_v2_sepolia_20261001/contracts/script/deploy-constants.ts:L10-L12 @ ens_v2_sepolia_20261001@07e55a05)
(upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/ManagedUniversalResolverProxy.json:L2 @ ens_v2_sepolia_20261001@07e55a05)

What the pinned artifacts do not prove: that the `upgradeTo` was executed. Both proxy artifacts
carry an address and ABI only, with empty constructor arguments and no transaction receipt, so
no checked-in file shows which implementation either proxy currently targets.
(upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/UpgradableUniversalResolverProxy.json:L2 @ ens_v2_sepolia_20261001@07e55a05)

The only evidence for the live route is one read-only on-chain call, which is not reproducible
from the pins: on 2026-10-02 an `eth_call` of `ROOT_REGISTRY()` on the entrypoint proxy
`0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe` on Ethereum Sepolia, at the head (block
`11827257`), returned `0xb458d6a3a77919449d03e7a6903c26827c1ec43f`, the new RootRegistry, and
the managed proxy's ERC-1967 implementation slot held `0x24e1d8e0…`. A head read is not a
fixed-block proof of any other block. It shows the proxy route reached a V2
implementation bound to the declared root at that time, and nothing about later blocks: either
proxy's admin can re-point it. Bigname does not yet check the root binding at request time;
[#906](https://github.com/ensdomains/bigname/issues/906) tracks that missing runtime check. Until
it lands, repeat the `ROOT_REGISTRY()` call during rollout and after any announced Universal
Resolver upgrade, and treat a different result as a reason to stop serving ENSv2 verified reads.

The indexed side now reads the route from chain data: `ens_execution` admits both proxies'
`Upgraded` events and lists `UniversalResolverV2` as the implementation that marks the
[Universal Resolver cutover](glossary.md#universal-resolver-cutover). On chain the long-lived
proxy moved to the managed proxy at block `10928435`, and the managed proxy reached the new
`UniversalResolverV2` at block `11821680` after earlier implementations, one rollback and the
2026-09-15 deployment's `0x5d25c1d6…` (installed at `11710193`, now unlisted); the transactions
are in `docs/upstream.md` ("Sepolia Universal Resolver proxies admitted from chain evidence").
Because only the new implementation is listed, every block before `11821680` reads as not cut
over, including the `11710193`–`11821679` window in which the dropped deployment answered
resolution: with that deployment dropped, no admitted ENSv2 registry existed there. A later
upgrade to an unlisted implementation ends the cutover for the names it affects
(`docs/api-v1.md` § Expiry and grace) until the manifest lists it. This does not replace
the request-time root check above.

Registry instances retain announcement-based admission. Resolver proxies require
the declared PermissionedResolver implementation and canonical upgrade evidence.
The directly declared PublicResolverV2 keeps the existing address/text/contenthash
and version-boundary subset, plus the standard `ABIChanged` event that the ABI
content-type inventory reads and the `NameChanged` event that reverse claims read
through a reverse node's resolver; it does not claim exhaustive DNS, pubkey,
or permission enumeration. The exact ENSV1 mirror declaration uses the canonical
ENSv1 registry's projected records. ABI matching includes argument types and
indexed positions, not just event names.
