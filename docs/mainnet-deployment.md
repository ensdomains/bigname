# Official mainnet deployment coverage

`manifests/mainnet` selects the official 2026-10-02 mainnet ENSv2 deployment at namechain
`b1f97b91f4b28e28d8a61fa13f09da70fdca6c6a`, as the pinned `deployments/mainnet` folder lists it
(upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/.deployment.json:L4 @ ens_v2_mainnet_20261002@b1f97b91).
Upstream redeployed part of the set on 2026-10-13 and on 2026-10-18. No mainnet manifest names
the contracts those redeploys replaced, and none of them names a name
([replaced contracts](#replaced-contracts)).

The following inventory accounts for every one of the 42 contract artifacts in the pinned
folder. A helper's presence does not grant indexed ownership, records, or permissions. Existing
capability flags and unsupported responses retain their documented scope. This change does not
add DNS, payment-token, smart-account, or pricing APIs. Start blocks are the artifacts' receipt
blocks unless the row says otherwise.

| Artifact | Address | Admission or boundary |
|---|---|---|
| `BatchRegistrar` | `0xf0ccfbf6dba1e01ccb833ced6d551a8f000e9721` | ens_v2_migration_l1 / batch_registrar sender metadata, from block `26105656`. Registry logs remain authoritative. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/BatchRegistrar.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `BoxedENSURIRenderer` | `0x6f412ff4144d525f791777d3384e9a1b170492fb` | Token metadata URI renderer. No indexed token metadata. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/BoxedENSURIRenderer.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `ContractNamer` | `0x5cdaf63d4c2a01218149dde707e5bc239c68730c` | Contract naming dependency, the proxy the `ContractNamer_Proxy` artifact also names. No indexed contract-account names or permissions. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/ContractNamer.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `ContractNamer_Implementation` | `0x87d7cdffafbe9826d978fbdbe3ad45e7a4bf3d1c` | Implementation behind `ContractNamer`. No independently indexed state. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/ContractNamer_Implementation.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `ContractNamer_Proxy` | `0x5cdaf63d4c2a01218149dde707e5bc239c68730c` | The same address as `ContractNamer`. No indexed contract-account names or permissions. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/ContractNamer_Proxy.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `DefaultReverseRegistrarAdapter` | `0xd86e32499ecafe9c181dd7410f93a1a9bb61ef0d` | Default reverse helper. It writes through the ENSv1 `DefaultReverseRegistrar` `0x283F227c4Bd38ecE252C4Ae7ECE650B0e913f1f9`, whose `NameForAddrChanged` intake belongs to ens_v1_reverse_l1 / default_reverse_registrar. DAO proposal call 2 makes it a controller there. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/DefaultReverseRegistrarAdapter.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `DNSAliasResolver` | `0x13285e928c65befd90904f77a68fe4e7d9a43882` | DNS resolution dependency. No indexed DNS record inventory. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/DNSAliasResolver.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `DNSSECGatewayProvider` | `0xa646bcee466d77669a087f3379640cb03aca7c3d` | CCIP-read gateway dependency. No indexed gateway policy. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/DNSSECGatewayProvider.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `DNSTLDResolver` | `0xab48304a2520ce420e636aacd48a8bb3e7e485ee` | DNS resolution dependency. No indexed DNSSEC verifier inventory, so its `VerifierChanged` is not admitted. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/DNSTLDResolver.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `DNSTXTResolver` | `0xbb76e2ae1a61ef91d095ec044abf563e4c464e3e` | DNS resolution dependency. No indexed DNS TXT inventory. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/DNSTXTResolver.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `ENSURIRenderer` | `0x82cac0d801b9a392a4ff217d5e68145801e90a41` | Token metadata URI renderer. No indexed token metadata. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/ENSURIRenderer.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `ENSV1Resolver` | `0x394d8eb4a4fdf599384deab25f31d111d755c398` | ens_v2_resolver_l1 / ensv1_mirror_resolver, from block `26105292`, the first block of the deployment. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/ENSV1Resolver.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `ENSV2Resolver` | `0x3071ad643ce4ed8b3d67b289845b45db82b068a4` | Resolver routing dependency, which DAO proposal call 1 sets as the registrar resolver. No indexed record inventory. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/ENSV2Resolver.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `ETHRegistrar` | `0xd6d8369e55c94ae8afa714e6cf7ab5be54d5cc81` | ens_v2_registrar_l1 / registrar, from block `26183999`. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/ETHRegistrar.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `ETHRegistry` | `0xf60aba43e1f7f3dab1186ff03b561401c9df85e2` | ens_v2_registry_l1 / registry, from block `26105621`. Its `ApprovalForAll` watch starts there too. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/ETHRegistry.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `ETHRenewerV1` | `0x56c447f308f463a01121c41e72b0e7e7557fcb68` | ens_v2_migration_l1 / ens_v1_renewal_bridge, from block `26184001`. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/ETHRenewerV1.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `Graveyard` | `0xda021a2c1c2f99574d861ef8c6fa745851beb7f6` | ens_v2_migration_l1 / graveyard, from block `26105641`. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/Graveyard.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `HCAOwnerAndSessionValidator` | `0xa818428ed2f3f949bdc71b8a15bdd7b5d27ce938` | Smart-account validation dependency. No indexed session permissions. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/HCAOwnerAndSessionValidator.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `HCAUpgradeSet` | `0x85e115f5b43c72e2ba4419a9a95e325db18431c2` | Smart-account upgrade policy. No indexed session or upgrade-policy inventory. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/HCAUpgradeSet.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `LabelStore` | `0x31e5e081fe38d15b3e8214ba5cb08dc4708b70ba` | Label storage helper. Standalone `Label` announcements are not indexed as name registrations, because the registry `LabelRegistered` carries the label. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/LabelStore.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `LockedMigrationController` | `0xc46bf3b6c7ebcffd4ecfc3b012a1726a2463694d` | ens_v2_migration_l1 / locked_migration_controller, from block `26216326`. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/LockedMigrationController.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `ManagedUniversalResolverProxy` | `0xcffd2ae680cd110d28739a70922636b5937b022b` | ens_execution / universal_resolver_managed: intermediate execution proxy, `Upgraded` admitted from its creation block `26105675` for monitoring. It has no independent indexed authority. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/ManagedUniversalResolverProxy.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `MigrationHelper` | `0x0b92006fa187b4c98d0a7dfc9d3b918b6d243485` | ens_v2_migration_l1 / migration_helper, from block `26216328`. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/MigrationHelper.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `PermissionedResolverImpl` | `0x565b0f456a04ac9b48758190e90030623cff57c5` | resolver implementation admission metadata. Its `Upgraded` announcement watch starts at its creation block `26105631`. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/PermissionedResolverImpl.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `PublicResolverSet` | `0xffceda021cbdf20f1a66ebbef32e49d208cabf62` | Migration resolver allowlist. No independently served allowlist inventory. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/PublicResolverSet.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `PublicResolverV2` | `0x66f9f566ff2f26309c1bd9134388478a801da3e1` | ens_v2_resolver_l1 / public_resolver_v2, from block `26216323`. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/PublicResolverV2.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `RegistryUpgradeSet` | `0x573a73c0b3c0c90bb8bd728519434af19fb88935` | Registry upgrade allowlist. Actual instance `Upgraded` observations govern indexing. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/RegistryUpgradeSet.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `ReservedNameResolverUpdater` | `0xccc9c6ef74b8837d998a6e1e659992a178dbe53b` | Batch helper for root resolver updates. Its naming effects are RootRegistry `ResolverUpdated` logs, and it emits only `OwnershipTransferred`. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/ReservedNameResolverUpdater.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `ReverseRegistrarAdapter` | `0x349151735790269a9c14668b58d7c83b5ecb9607` | Delegates reverse writes. Intake belongs to the canonical ReverseRegistrar `0xa58E81fe9b61B5c3fE2AFD33CF304c454AbFc7Cb` under ens_v1_reverse_l1, which DAO proposal call 3 makes it a controller of. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/ReverseRegistrarAdapter.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `RootBatchRegistrar` | `0xe467ae049ddd65a21fb85b5f1de5f7997a8eb164` | Batch submission helper. Its naming effects come from RootRegistry logs, and it emits only `OwnershipTransferred`. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/RootBatchRegistrar.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `RootRegistry` | `0x2553c09be6447beffdb86fa622ad41fb8f77a91e` | ens_v2_root_l1 / root_registry, from block `26183742`. Its admission is the mainnet [Universal Resolver cutover](glossary.md#universal-resolver-cutover). (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/RootRegistry.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `StandaloneHCAFactory` | `0x742bf21897a78e2c98cc5f5c5473a370a081e561` | Smart-account factory. HCA deployment is outside indexed naming ownership. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/StandaloneHCAFactory.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `StandaloneHCAImplementation` | `0x116f462fa078ec0009e9b25dcd60309e70aac4f4` | Smart-account implementation. Module and session state is outside indexed naming permissions. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/StandaloneHCAImplementation.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `StandardRentPriceOracle` | `0x76f29609a363a8a1a2a6abe2e859c0f8916af469` | Registration pricing dependency. No indexed pricing API. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/StandardRentPriceOracle.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `UniversalHelper` | `0x46ae127d14683ac74ef151e46ae2117a1a25c676` | Read helper. No independently indexed state. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/UniversalHelper.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `UniversalResolverV1` | `0x804bf0d289c8353a0bafdaaf9069ab14c92b2099` | Not listed in `universal_resolver_implementations`. On 2026-10-19 neither proxy's implementation slot held it. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/UniversalResolverV1.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `UniversalResolverV2` | `0x06fdd07f007adac667177b46ab2eb9a75132cfd5` | Execution implementation behind the declared entrypoint. It is listed in `ens_execution`'s `universal_resolver_implementations`, so the monitoring reports the proxies against it. It does not decide the [Universal Resolver cutover](glossary.md#universal-resolver-cutover). (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/UniversalResolverV2.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `UnlockedMigrationController` | `0xab821ca676cf2e0790fa273526a165cfa86aeb64` | ens_v2_migration_l1 / unlocked_migration_controller, from block `26105642`. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/UnlockedMigrationController.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `UpgradableUniversalResolverProxy` | `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe` | ens_execution / universal_resolver, `Upgraded` admitted from block `26105292`, the first block of the deployment, not its creation block ([resolver and discovery coverage](#resolver-and-discovery-coverage)). (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/UpgradableUniversalResolverProxy.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `UserRegistryImpl` | `0x75ff9adbcfce2469b097ac68f6ae650eae1351bb` | ens_v2_migration_l1 / user_registry_implementation, from block `26105644`. Instances enter through `RegistryCreated`, not direct implementation ownership. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/UserRegistryImpl.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `VerifiableFactory` | `0x77510ba162dfea3d8989e3f0928a691b345730d2` | ens_v2_migration_l1 / verifiable_factory, from block `26105299`. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/VerifiableFactory.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |
| `WrapperRegistryImpl` | `0xcf3edb3f5501de63b57254d910b6782ee370039b` | ens_v2_migration_l1 / wrapper_registry_implementation, from block `26216324`. (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/WrapperRegistryImpl.json:L2 @ ens_v2_mainnet_20261002@b1f97b91) |

## Replaced contracts

The pin keeps the contracts the 2026-10-13 root redeploy and the 2026-10-18 redeploy replaced in
`deployments/mainnet-root-20261013/` (15 artifacts) and `deployments/mainnet-redeploy-20261018/`
(10 artifacts). Their 25 addresses are disjoint from the current set, and no manifest declares
them. Interpret admits an address only through a declaration or a discovery edge from an active
manifest, so the only path that reaches one is a registry's `RegistryCreated` self-announcement.
The replaced RootRegistry `0x51bbc58b3dae7d45b14fba8cf31b7c8f7904a750` was created at block
`26105295` and announced itself there. The registry family's watch entries, `RegistryCreated`
included, start at the ETHRegistry's block `26105621`, but that start bounds only the stamped
Ingest redo and the first-effect scan of the change. This release rotates the interpreter content
hash, so its Interpret and Project redo run over the full range. Runtime
intake reads `RegistryCreated` from every emitter over the whole redo range, which includes block
`26105295`, so the announcement is fetched and, like any self-announced registry, the address is
indexed. It is an unnamed registry that nothing admitted reaches, so its entries, its own `eth`
entry included, name nothing, as Sepolia's dropped registries do
([Sepolia deployment](sepolia-deployment.md#historical-intake-and-deployment-replacement)). A
read-only `eth_getLogs` on 2026-10-19 found exactly two `RegistryCreated` logs from block
`26105000` to `26105621`: that one (transaction
`0xcc68ce517a4bab4c98003939c6a48de2b0668d12caf2c00dc39f2c5ad085f739`) and the ETHRegistry's
(transaction `0xf69d06286b2688b64fcafa572f862487a5cb5a4607fd60a925c5727b93d1a6a7`).
(upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet-root-20261013/RootRegistry.json:L2 @ ens_v2_mainnet_20261002@b1f97b91)
(upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet-root-20261013/RootRegistry.json:L2995 @ ens_v2_mainnet_20261002@b1f97b91)

The ETHRegistry is a suffix anchor named `eth`, so the reservations premigration wrote from block
`26105656`, while the replaced root was current, are named and keep their expiry. The admitted
RootRegistry's `eth` entry points at the ETHRegistry from block `26183992` (transaction
`0x94f9bc93e125e997f998fdfdbb4e41d3c2bee92e89a8cc600929c713e925857d`, the root's only
`SubregistryUpdated`). Name reads serve only the current publication at the head, so the window
before that block is never served.

## Historical intake and the upgrade

The migration controllers and the renewal bridge bind the canonical mainnet ENSv1 NameWrapper
`0xD4416b13d2b3a9aBae7AcD5D6C2BbDBE25686401`, and the mirror resolver binds the canonical ENSv1
registry `0x00000000000C2E074eC69A0dFb2997BA6C7d2e1e`. The ENSv1 registry, registrar, wrapper,
reverse and resolver families are unchanged.
(upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/UnlockedMigrationController.json:L624 @ ens_v2_mainnet_20261002@b1f97b91)
(upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/LockedMigrationController.json:L744 @ ens_v2_mainnet_20261002@b1f97b91)
(upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/ETHRenewerV1.json:L949 @ ens_v2_mainnet_20261002@b1f97b91)
(upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/ENSV1Resolver.json:L749 @ ens_v2_mainnet_20261002@b1f97b91)

The DAO proposal's controller calls need no ENSv1 manifest change. Its five
`removeRegistrarController` calls and its two `addRegistrarController` calls, for the Graveyard and
the ETHRenewerV1, derive `PermissionChanged` rows from the BaseRegistrar's `ControllerRemoved` and
`ControllerAdded`, which `ens_v1_registrar_l1` already admits. Renewals through the ETHRenewerV1
derive `RegistrationRenewed` on `label.eth` from its declared `ens_v1_renewal_bridge` role.
(upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/dao/proposal.md:L13-L19 @ ens_v2_mainnet_20261002@b1f97b91)

A database that already indexed mainnet under the ENSv1-only profile does not need a reset.
Manifest synchronization stamps the Ingest, Interpret and Project redo that re-derives mainnet
with the ENSv2 families. The deleted shadow `ens_execution` version 1 keeps its `shadow` row,
which nothing watches or interprets. The Ingest redo runs from block `26105292`, the
deployment's first block, to the head. The registry and resolver families' family-wide and
all-emitter events start at each family's first declaration, `26105621` and `26105292`, and every
other new watch entry starts at its declaration. The release entry in
[`deployment.md`](deployment.md#mainnet-ensv2-admission-of-2026-10-02) gives the order and the
stamped range.

Premigration reserves `.eth` names in batches, and ENSv1 registration stays open until the DAO
proposal removes the ENSv1 registrar controllers. A `.eth` name registered through ENSv1 after the
last reservation batch and before that removal has no ENSv2 entry. bigname serves it with
`unresolvable_reason` `no_live_ens_v2_entry` and no resolver or records, while the live Universal
Resolver still answers it through ENSv1. The divergence lasts until a later batch reserves the
name, or until the DAO proposal has removed the controllers and the client-facing proxy reaches
`UniversalResolverV2` ([below](#resolver-and-discovery-coverage)). From then bigname and the live
Universal Resolver agree: the name is reserved and served by both, or unresolved in both
([known divergence](upstream.md#ensv2-resolution-modelled-from-manifest-admission),
[Cutover at ENSv2 admission](deployment.md#cutover-at-ensv2-admission)).

The same window applies to expiry. An ENSv2 reservation carries the expiry written at its batch,
and a later batch renews a reserved name only to a later expiry. A renewal made through ENSv1
after a name's batch and before the controllers are removed does not move that expiry until a
later batch does. bigname takes a premigrated name's expiry from its ENSv2 entry, so it can show
the earlier expiry while the live ENSv1 lease runs longer. Once the controllers are removed and
the client-facing proxy reaches `UniversalResolverV2`, the chain's ENSv2 state holds the same
expiry bigname serves.
(upstream: .refs/ens_v2_mainnet_20261002/contracts/src/registrar/BatchRegistrar.sol:L61-L71 @ ens_v2_mainnet_20261002@b1f97b91)

## Resolver and discovery coverage

The canonical Universal Resolver proxy `0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe` remains the
request entrypoint. The manifest admits both `ens_v1` and `ens_v2` verified authority arms through
it. The `UniversalResolverV2` implementation at `0x06fdd07f007adac667177b46ab2eb9a75132cfd5` was
constructed with the admitted RootRegistry `0x2553c09be6447beffdb86fa622ad41fb8f77a91e` as its first
argument, which the constructor stores as the immutable `ROOT_REGISTRY` that resolution starts from.
(upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/UniversalResolverV2.json:L2 @ ens_v2_mainnet_20261002@b1f97b91)
(upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/UniversalResolverV2.json:L1634 @ ens_v2_mainnet_20261002@b1f97b91)
(upstream: .refs/ens_v2_mainnet_20261002/contracts/src/universalResolver/UniversalResolverV2.sol:L20 @ ens_v2_mainnet_20261002@b1f97b91)
(upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/RootRegistry.json:L2 @ ens_v2_mainnet_20261002@b1f97b91)

The DAO proposal has not executed. Today both proxies forward to the pre-ENSv2 Universal Resolver
implementation `0xED73a03F19e8D849E44a39252d222c6ad5217E1e`. The managed proxy took it as a
constructor argument, and the proxy's constructor sets its first implementation without an event.
Only `upgradeTo` emits `Upgraded`. DAO proposal call 15 calls `upgradeTo` on the client-facing proxy
with the managed proxy `0xcffd2ae680cd110d28739a70922636b5937b022b`. A separate deployment step
upgrades the managed proxy to `UniversalResolverV2`, and the pinned migration record has not run it.
(upstream: .refs/ens_v1/deployments/mainnet/UniversalResolver.json:L2 @ ens_v1@91c966f)
(upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/ManagedUniversalResolverProxy.json:L316 @ ens_v2_mainnet_20261002@b1f97b91)
(upstream: .refs/ens_v2_mainnet_20261002/contracts/src/universalResolver/UpgradableUniversalResolverProxy.sol:L71-L75 @ ens_v2_mainnet_20261002@b1f97b91)
(upstream: .refs/ens_v2_mainnet_20261002/contracts/src/universalResolver/UpgradableUniversalResolverProxy.sol:L163-L165 @ ens_v2_mainnet_20261002@b1f97b91)
(upstream: .refs/ens_v2_mainnet_20261002/contracts/src/universalResolver/UpgradableUniversalResolverProxy.sol:L111-L115 @ ens_v2_mainnet_20261002@b1f97b91)
(upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/dao/proposal.md:L177-L186 @ ens_v2_mainnet_20261002@b1f97b91)
(upstream: .refs/ens_v2_mainnet_20261002/contracts/deploy/universalResolver/05_setup_ManagedUniversalResolverProxyToUniversalResolverImplementation.ts:L26-L46 @ ens_v2_mainnet_20261002@b1f97b91)
(upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/.migrations.json:L1 @ ens_v2_mainnet_20261002@b1f97b91)

The chain agrees. Read-only `eth_getLogs` on 2026-10-19 found one log from the client-facing proxy
in all of its history, an `AdminChanged` at block `23085558`, and no `Upgraded`. The managed
proxy's only log is its constructor `AdminChanged` at `26105675`. Both ERC-1967 implementation
slots held `0xed73a03f…`. The transactions are in `docs/upstream.md` ("Mainnet Universal Resolver
proxies admitted from chain evidence").

The indexed side reads the route from chain data for monitoring. `ens_execution` admits both
proxies' `Upgraded` events and lists `UniversalResolverV2` as the implementation the proxies are
reported against. Unlike Sepolia, the client-facing proxy is declared from block `26105292`, the
deployment's first block, not from its creation block. A creation-block start would read no more,
because the proxy has emitted no `Upgraded` since its creation.

The start rests on the proxy's own code and on a chain scan. The constructor (L71-L75) writes the
implementation slot through `_setImplementation` (L163-L165), whose slot write at L164 emits no
event. `upgradeTo` (L111-L115) is the only `Upgraded` emitter, at L114. As of 2026-10-19, a
full-history, address-filtered log scan of the client-facing proxy found no `Upgraded` at any
block and exactly one `AdminChanged`, at block `23085558`, while the implementation slot held the
constructor-set `0xed73a03f19e8d849e44a39252d222c6ad5217e1e`. The constructor also calls
`_setAdmin` (L74), which emits `AdminChanged` (L170), so that single log is consistent with the
constructor's own log. It is still not provable as the creation block, because the deployment
artifact records no receipt or transaction hash, and `AdminChanged` is not a declared event.
Served names never read the proxies, so no proxy history, read or unread, changes what bigname
serves. Monitoring reads each proxy's latest `Upgraded`, so from the first in-range upgrade (call 15
or the managed proxy's own upgrade) an earlier one changes nothing. Before it the gauges read 0 for
no proxy event. Only an earlier upgrade could have read otherwise, and the full-history scan found
none.
(upstream: .refs/ens_v2_mainnet_20261002/contracts/src/universalResolver/UpgradableUniversalResolverProxy.sol:L71-L75 @ ens_v2_mainnet_20261002@b1f97b91)
(upstream: .refs/ens_v2_mainnet_20261002/contracts/src/universalResolver/UpgradableUniversalResolverProxy.sol:L163-L165 @ ens_v2_mainnet_20261002@b1f97b91)
(upstream: .refs/ens_v2_mainnet_20261002/contracts/src/universalResolver/UpgradableUniversalResolverProxy.sol:L111-L115 @ ens_v2_mainnet_20261002@b1f97b91)
(upstream: .refs/ens_v2_mainnet_20261002/contracts/src/universalResolver/UpgradableUniversalResolverProxy.sol:L170 @ ens_v2_mainnet_20261002@b1f97b91)

A constructor-set implementation is invisible to bigname. Mainnet's first proxy row appears at DAO
proposal call 15 or at the managed proxy's upgrade to `UniversalResolverV2`, whichever comes first.
Until then `phase_runner_universal_resolver_unadmitted` reads 0 because there is no proxy event,
not because the chain resolves through a listed implementation, and
`BignameUniversalResolverUnadmitted` does not page. After call 15 the client-facing proxy hops to
the managed proxy. A hop to a proxy with no `Upgraded` yet reads as not unadmitted, so the chain
reads admitted once the managed proxy's `Upgraded(0x06fd…)` exists, in either order
([deployment](deployment.md#universal-resolver-cutover-gauges-and-alert)).

The route does not decide the
[Universal Resolver cutover](glossary.md#universal-resolver-cutover). Mainnet is cut over because
its profile admits the deployment's root registry, and it reads as cut over for its whole retained
history, including the blocks before the DAO proposal executes, while clients still resolve through
the pre-ENSv2 implementation ([known divergence](upstream.md#ensv2-resolution-modelled-from-manifest-admission)). A premigrated
`.eth` name with a live ENSv1 lease keeps the lease's owner and registration time, takes its expiry
and the 28-day grace from its ENSv2 entry, and is served through the resolver its ENSv1 registry
record names. The `ENSV1Resolver` route reads the same records.

Registry instances retain announcement-based admission. Resolver proxies require the declared
PermissionedResolver implementation and canonical upgrade evidence. The directly declared
PublicResolverV2 and the exact ENSV1 mirror declaration keep the Sepolia profile's record coverage
([Sepolia deployment](sepolia-deployment.md#resolver-and-discovery-coverage)).
