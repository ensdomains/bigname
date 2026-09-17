# Official Sepolia deployment coverage

`manifests/sepolia` selects the official 2026-09-15 deployment at contracts-v2
`366de741187e38686c904242753c532b7de70d47`. This replaces both previous runtime
profiles. The previous Sepolia and hackathon manifests are removed.
(upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/.deployment.json:L4 @ ens_v2_sepolia_20260916@366de741)

The following inventory accounts for every entry in the pinned address list.
A helper's presence does not grant indexed ownership, records, or permissions.
Existing capability flags and unsupported responses retain their documented
scope; this change does not add DNS, payment-token, smart-account, or pricing APIs.

| Artifact | Address | Admission or boundary |
|---|---|---|
| `BatchRegistrar` | `0xbe68ff9afc7d5a1864ffef5c82de0a1c13e6b529` | ens_v2_migration_l1 / batch_registrar sender metadata; registry logs remain authoritative. (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/BatchRegistrar.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `ContractNamer` | `0xdab8b3dcb4c2bb181b9215b699b2c3e1fe180ae1` | Contract naming dependency; no indexed contract-account names or permissions. (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/ContractNamer.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `DefaultReverseRegistrarAdapter` | `0x4f32a1c62e202922d4d6307126f43218db9da6f5` | Default reverse helper; standalone/default reverse inventory is not added. (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/DefaultReverseRegistrarAdapter.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `DNSAliasResolver` | `0x6fd64a35388d2c22f1fba96476db59184b1feaa0` | DNS resolution dependency; no indexed DNS record inventory. (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/DNSAliasResolver.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `DNSSECGatewayProvider` | `0x5710ee3945caa32287553574fe995c0e8e3d613e` | CCIP-read gateway dependency; no indexed gateway policy. (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/DNSSECGatewayProvider.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `DNSTLDResolver` | `0xb0c788195697db17543bf22cbc1b0e2b4a04f9b8` | DNS resolution dependency; no indexed DNSSEC verifier inventory. (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/DNSTLDResolver.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `DNSTXTResolver` | `0x1ec6e8d261b1f8fd0d755ea06186bdc1486197f5` | DNS resolution dependency; no indexed DNS TXT inventory. (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/DNSTXTResolver.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `ENSV1Resolver` | `0xb2bf4a9a86d29661ea93223582b9945943931e42` | ens_v2_resolver_l1 / ensv1_mirror_resolver (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/ENSV1Resolver.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `ENSV2Resolver` | `0x9458ec65b4a703ee1be03434879f35fd32b64704` | Resolver routing dependency; no indexed record inventory. (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/ENSV2Resolver.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `ETHRegistrar` | `0xabe76f6c8dfced81aa5a2bb8034202a7136b94ca` | ens_v2_registrar_l1 / registrar (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/ETHRegistrar.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `ETHRegistry` | `0x657ea849311d3d5823348dded7c2aaafb3ede09e` | ens_v2_registry_l1 / registry (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/ETHRegistry.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `ETHRenewerV1` | `0xd06e726e9bd8ac0f33a2a45f4cc28fe10d656a36` | ens_v2_migration_l1 / ens_v1_renewal_bridge (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/ETHRenewerV1.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `Graveyard` | `0x950b93885b33ce4c7e8571be2c88a1aa93d82f49` | ens_v2_migration_l1 / graveyard (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/Graveyard.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `HCAOwnerAndSessionValidator` | `0x6a62af42d4241a02547b096c7db43ca6411af813` | Smart-account validation dependency; no indexed session permissions. (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/HCAOwnerAndSessionValidator.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `HCAUpgradeSet` | `0x2ceedf92fd032167c90936c9e6ca2931bd7ec2c0` | Smart-account upgrade policy; no indexed session or upgrade-policy inventory. (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/HCAUpgradeSet.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `LabelStore` | `0x375c082021e677a40ea2ae094d050602dba90992` | Label storage helper; standalone Label announcements are not indexed as name registrations. (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/LabelStore.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `LockedMigrationController` | `0xab1b57c6ee5e91e6090595c0af14cb9b8bc7773f` | ens_v2_migration_l1 / locked_migration_controller (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/LockedMigrationController.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `ManagedUniversalResolverProxy` | `0x6d80F2172CFdEc5730fE683860C33d26fC42e6F1` | Intermediate execution proxy; no independent indexed authority. (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/ManagedUniversalResolverProxy.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `MigrationHelper` | `0x58d12d60471b98f191856e4c2d56886e9c3ea573` | ens_v2_migration_l1 / migration_helper (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/MigrationHelper.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `MockDAI` | `0x278053acc97888e63ec81c80fec641bf0bf19664` | Test payment token; token transfers are outside naming intake. (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/MockDAI.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `MockUSDC` | `0x16f95d91dba7da3aca778ec053df0ff6c6a8aa8e` | Test payment token; token transfers are outside naming intake. (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/MockUSDC.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `PermissionedResolverImpl` | `0x14f09fd05d4585759e54844dc9b00147131cf243` | resolver implementation admission metadata (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/PermissionedResolverImpl.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `PublicResolverSet` | `0xd12af6ac82648056fe7d6b2a9db97235aa509021` | Migration resolver allowlist; no independently served allowlist inventory. (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/PublicResolverSet.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `PublicResolverV2` | `0xd7e590ad0e92a6ac1d81f4483a9b951d3585a50f` | ens_v2_resolver_l1 / public_resolver_v2 (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/PublicResolverV2.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `RegistryUpgradeSet` | `0xd8a8369477b67f837e2b1054b2d47f3d1956b543` | Registry upgrade allowlist; actual instance Upgraded observations govern indexing. (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/RegistryUpgradeSet.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `ReverseRegistrarAdapter` | `0x39993148caa6a20ae1f08e1b2427966e97f85aab` | Delegates reverse writes; intake belongs to the canonical ReverseRegistrar. (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/ReverseRegistrarAdapter.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `RootBatchRegistrar` | `0xcf5d485a531863856ed9d8a10d61707de7f06c21` | Batch submission helper; naming effects come from registry logs. (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/RootBatchRegistrar.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `RootRegistry` | `0x9703dbd26dab89504490994138cf2c575251a9ce` | ens_v2_root_l1 / root_registry (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/RootRegistry.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `StandaloneHCAFactory` | `0xb7cfeceed32dba66c507b3c002dad510b8399928` | Smart-account factory; HCA deployment is outside indexed naming ownership. (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/StandaloneHCAFactory.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `StandaloneHCAImplementation` | `0xdf4a24c42921810fed9363b07292e9152578d706` | Smart-account implementation; module/session state is outside indexed naming permissions. (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/StandaloneHCAImplementation.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `StandardRentPriceOracle` | `0x9b0b9c65bdaf9794ff7697e4dcfb1f50581072bb` | Registration pricing dependency; no indexed pricing API. (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/StandardRentPriceOracle.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `UniversalHelper` | `0x33f571aa8a160a21b877cf6e0fb8806692b97df5` | Read helper; no independently indexed state. (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/UniversalHelper.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `UniversalResolverV2` | `0x5d25c1d6acbb71b7a28aa7899618a3412a8303e3` | Execution implementation behind the declared entrypoint. (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/UniversalResolverV2.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `UnlockedMigrationController` | `0x7ed171bb143a905f56105e4ea146543ecb122f55` | ens_v2_migration_l1 / unlocked_migration_controller (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/UnlockedMigrationController.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `UpgradableUniversalResolverProxy` | `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe` | ens_execution / universal_resolver (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/UpgradableUniversalResolverProxy.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `UserRegistryImpl` | `0xa80338aaa8d23831cea25e858d1774534abb0263` | Registry implementation; instances enter through RegistryCreated, not direct implementation ownership. (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/UserRegistryImpl.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `VerifiableFactory` | `0x9e726eb570beb6bceb495ab8cda7df517d4e841c` | ens_v2_migration_l1 / verifiable_factory (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/VerifiableFactory.json:L2 @ ens_v2_sepolia_20260916@366de741) |
| `WrapperRegistryImpl` | `0x2741543c3b14640b97bc70a233318032f7e35bac` | ens_v2_migration_l1 / wrapper_registry_implementation (upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/WrapperRegistryImpl.json:L2 @ ens_v2_sepolia_20260916@366de741) |

## Historical intake and deployment replacement

The new migration controllers bind the canonical Sepolia ENSv1 NameWrapper,
and the mirror resolver binds the canonical ENSv1 registry. Preserve the canonical
ENSv1 registry, registrar, wrapper and declared resolver history; the hackathon's
separate ENSv1 addresses are not part of this corpus.
(upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/LockedMigrationController.json:L744 @ ens_v2_sepolia_20260916@366de741)
(upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/ENSV1Resolver.json:L749 @ ens_v2_sepolia_20260916@366de741)

The canonical ReverseRegistrar is also declared for reverse-claim keys.
(upstream: .refs/ens_v1/deployments/sepolia/ReverseRegistrar.json:L2 @ ens_v1@91c966f)
Start fresh intake at block zero: some retained ENSv1 resolver provenance and the
long-lived Universal Resolver proxy have no exact creation receipt in the
admitted artifact set. Zero is a conservative lower bound, not a deployment date.
New ENSv2 declarations use their individual deployment receipt blocks.

Back up and retire the old server corpus, initialize a fresh database with the
current migrations and phase schema, and run ingestion through verified live
follow. Use the same commit for API and runner, reapply the documented API and
verification grants, and do not mix old manifest state into the new database.
This is an explicit corpus replacement, not an in-place address edit or cursor
rewrite. See [deployment replacement](deployment.md#replacing-an-initialized-phase-schema).

## Resolver and discovery coverage

The canonical Universal Resolver proxy `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe` remains the
request entrypoint. The manifest admits both `ens_v1` and `ens_v2` verified authority arms through
it. The evidence for the ENSv2 arm has two parts of different strength.

What the pinned artifacts prove: the `UniversalResolverV2` implementation at
`0x5d25c1d6acbb71b7a28aa7899618a3412a8303e3` was constructed with the new RootRegistry
`0x9703dbd26dab89504490994138cf2c575251a9ce` as its first argument, which the constructor stores
as the immutable `ROOT_REGISTRY` that resolution starts from.
(upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/UniversalResolverV2.json:L2 @ ens_v2_sepolia_20260916@366de741)
(upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/UniversalResolverV2.json:L1593 @ ens_v2_sepolia_20260916@366de741)
(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/UniversalResolverV2.sol:L20 @ ens_v2_sepolia_20260916@366de741)
(upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/UniversalResolverV2.sol:L29-L38 @ ens_v2_sepolia_20260916@366de741)
(upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/RootRegistry.json:L2 @ ens_v2_sepolia_20260916@366de741)
The pinned deployment notes also describe the intended route: on Sepolia the long-lived proxy
already points at the managed proxy `0x6d80F2172CFdEc5730fE683860C33d26fC42e6F1`, and a fresh
deployment's only on-chain change is `upgradeTo(newImplementation)` on that managed proxy.
(upstream: .refs/ens_v2_sepolia_20260916/contracts/docs/universalResolver.md:L16-L17 @ ens_v2_sepolia_20260916@366de741)
(upstream: .refs/ens_v2_sepolia_20260916/contracts/docs/universalResolver.md:L24 @ ens_v2_sepolia_20260916@366de741)
(upstream: .refs/ens_v2_sepolia_20260916/contracts/script/deploy-constants.ts:L10-L12 @ ens_v2_sepolia_20260916@366de741)
(upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/ManagedUniversalResolverProxy.json:L2 @ ens_v2_sepolia_20260916@366de741)

What the pinned artifacts do not prove: that the `upgradeTo` was executed. Both proxy artifacts
carry an address and ABI only, with empty constructor arguments and no transaction receipt, so
no checked-in file shows which implementation either proxy currently targets.
(upstream: .refs/ens_v2_sepolia_20260916/contracts/deployments/sepolia/UpgradableUniversalResolverProxy.json:L2 @ ens_v2_sepolia_20260916@366de741)

The only evidence for the live route is one read-only on-chain call, which is not reproducible
from the pins: on 2026-09-17 an `eth_call` of `ROOT_REGISTRY()` on the entrypoint proxy
`0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe` on Ethereum Sepolia returned
`0x9703DBD26dAB89504490994138cF2c575251a9cE`, the new RootRegistry. The block it ran
against was not recorded, so it is not a fixed-block proof. It shows the proxy route reached a V2
implementation bound to the declared root at that time, and nothing about later blocks: either
proxy's admin can re-point it. Bigname does not yet check the root binding at request time;
[#906](https://github.com/ensdomains/bigname/issues/906) tracks that missing runtime check. Until
it lands, repeat the `ROOT_REGISTRY()` call during rollout and after any announced Universal
Resolver upgrade, and treat a different result as a reason to stop serving ENSv2 verified reads.

Registry instances retain announcement-based admission. Resolver proxies require
the declared PermissionedResolver implementation and canonical upgrade evidence.
The directly declared PublicResolverV2 keeps the existing address/text/contenthash
and version-boundary subset; it does not claim exhaustive DNS, ABI, pubkey, alias,
or permission enumeration. The exact ENSV1 mirror declaration uses the canonical
ENSv1 registry's projected records. ABI matching includes argument types and
indexed positions, not just event names.
