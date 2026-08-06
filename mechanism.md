# ENSv1 → ENSv2 migration mechanism (as implemented at `ens_v2@ccaeb58b`)

Authority: the pinned checkout `.refs/ens_v2` at commit `ccaeb58b` (HEAD of
`origin/post-audit-2`). Namechain was cancelled; the mechanism below is entirely
L1. ENSv1 counterpart behavior cites `.refs/ens_v1` at `ens_v1@91c966f`
(`origin/staging`). Nothing in this document is from memory of the L2-era
design.

Terminology: "v1" = ENSv1 (ENSRegistry + BaseRegistrarImplementation +
NameWrapper), "v2" = ENSv2 (`PermissionedRegistry`-family registries). A
"controller" here is an ENSv2 migration controller contract, not a v1
registrar controller, unless said otherwise.

---

## 0. Big picture

Migration is **transfer-driven and per-name**. There is no global state copy.

1. **Premigration** batch-reserves every existing .eth 2LD in the new ENSv2
   .eth registry as `RESERVED` (owner = 0) with a fallback resolver that
   answers from v1.
2. **Launch** freezes all v1 registration paths and hands v1 registrar
   control to two v2 contracts (Graveyard, ETHRenewerV1).
3. **Migration of one name** = the v1 token holder transfers the v1 token
   (ERC-721 registrar token or ERC-1155 NameWrapper token) into a migration
   controller with an ABI-encoded parameter struct. The controller destroys
   the name's v1 presence (registry entry → Graveyard, resolver cleared,
   token → Graveyard) and claims the v2 `RESERVED` entry, minting the v2
   ERC-1155 and emitting v2 registry events in the same transaction.
4. **Post-migration**, the v1 side of the name is inert: registry owner is the
   Graveyard, resolver is cleared, token sits in the Graveyard forever.
   Unmigrated names keep resolving through the v1 fallback resolver;
   `ETHRenewerV1` keeps them renewable (dual-writing v1 + v2 expiry) until
   they migrate or expire.

The mechanism has exactly these on-chain entrypoints:

| # | Entry | Contract | Token source | Handles |
|---|-------|----------|--------------|---------|
| E1 | `onERC721Received` | UnlockedMigrationController | BaseRegistrar (ERC-721) | unwrapped .eth 2LD |
| E2 | `onERC1155Received` / `onERC1155BatchReceived` | UnlockedMigrationController | NameWrapper | wrapped **unlocked** .eth 2LD |
| E3 | `onERC1155Received` / `onERC1155BatchReceived` | LockedMigrationController | NameWrapper | wrapped **locked** .eth 2LD |
| E4 | `onERC1155Received` / `onERC1155BatchReceived` | WrapperRegistry (per migrated locked ancestor) | NameWrapper | wrapped locked or emancipated (N+1)-LD child |
| E5 | `migrate(...)` | MigrationHelper | both, via operator approval | batch of E1+E2+E3+E4 |
| E6 | `clear(bytes[] names)` | Graveyard | — | permissionless v1 namespace cleanup |
| E7 | `renew` / `syncWrapper` / `getRemainingGracePeriod` | ETHRenewerV1 | — | renewal of premigrated-but-unmigrated names |

---

## 1. Premigration: RESERVED names in the v2 .eth registry

The v2 `PermissionedRegistry` has a three-state label lifecycle with an
explicit ASCII state diagram in source: `AVAILABLE → RESERVED → REGISTERED`,
with `register(owner=0)` + `ROLE_REGISTRAR` producing `RESERVED`,
`register()` + `ROLE_REGISTER_RESERVED` promoting `RESERVED → REGISTERED`,
`renew()` extending either live state, `unregister()` returning to
`AVAILABLE`, and expiry (`block.timestamp >= expiry`) making any entry
`AVAILABLE` (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L36-L57 @ ens_v2@ccaeb58b).
Status is computed, not stored: expired ⇒ `AVAILABLE`; unexpired with no
token owner ⇒ `RESERVED`; else `REGISTERED`
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L651-L659 @ ens_v2@ccaeb58b).

- Reserving: `register(label, owner=0, registry, resolver, roleBitmap=0, expiry)`
  requires `ROLE_REGISTRAR` on the root resource and a nonzero expiry; it
  emits `LabelReserved(tokenId, labelHash, label, expiry, sender)` and mints
  **no** token (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L428-L434,L449-L451,L461-L462 @ ens_v2@ccaeb58b).
- The DAO-operated batch tool is `BatchRegistrar.batchRegister(registry,
  resolver, labels[], expires[])` (onlyOwner): reserves `AVAILABLE` labels
  with `owner=0`, and for already-`RESERVED` labels extends expiry via
  `renew` when the new expiry is later
  (upstream: .refs/ens_v2/contracts/src/registrar/BatchRegistrar.sol:L48-L72 @ ens_v2@ccaeb58b).
- Reservation expiry = v1 registrar expiry + `PREMIGRATION_BONUS_PERIOD`,
  where the bonus is `1 + (GRACE_PERIOD_V1 − GRACE_PERIOD_V2)` =
  `1s + (90d − 28d)`
  (upstream: .refs/ens_v2/contracts/script/deploy-constants.ts:L216-L219 @ ens_v2@ccaeb58b);
  the devnet/e2e premigration helper computes exactly
  `nameExpires(tokenId) + PREMIGRATION_BONUS_PERIOD`
  (upstream: .refs/ens_v2/contracts/test/e2e/migration.test.ts:L64-L75 @ ens_v2@ccaeb58b).
  Effect: a v2 reservation stays alive for the whole v1 lifetime including
  the 90-day v1 grace period (v2 reserved expiry + 28d v2 grace ends exactly
  1 second after v1 grace ends).
- The reservation's resolver field is set to the **ENSV1Resolver fallback**
  so unmigrated names resolve via v1 through the v2 tree
  (upstream: .refs/ens_v2/contracts/test/e2e/migration.test.ts:L67-L74 @ ens_v2@ccaeb58b;
  upstream: .refs/ens_v2/contracts/script/preMigrateDevnetNames.ts:L132-L134 @ ens_v2@ccaeb58b).

Names never registered in v1 are simply never reserved; after launch they are
`AVAILABLE` and register through the normal v2 `ETHRegistrar` commit/reveal
flow — that path is out of migration scope except where noted.

## 2. Launch: freezing v1

The rehearsed activation sequence (`activateV2()`):

1. `NameWrapper.renounceOwnership()` — wrapper becomes ownerless
   (upstream: .refs/ens_v2/contracts/script/setup.ts:L839-L843 @ ens_v2@ccaeb58b).
2. Remove every v1 registration path as BaseRegistrar controller:
   `ETHRegistrarController`, `LegacyETHRegistrarController`, and
   `NameWrapper` itself (which is how wrapped registrations/renewals route)
   (upstream: .refs/ens_v2/contracts/script/setup.ts:L844-L870 @ ens_v2@ccaeb58b).
3. Add `Graveyard` and `ETHRenewerV1` as the only BaseRegistrar controllers
   (upstream: .refs/ens_v2/contracts/script/setup.ts:L871-L879 @ ens_v2@ccaeb58b).
4. Transfer BaseRegistrar ownership to `ETHRenewerV1`
   (upstream: .refs/ens_v2/contracts/script/setup.ts:L889-L893 @ ens_v2@ccaeb58b).

After launch, on the v1 side: no new .eth 2LD registrations (a removed
controller's `register` hits a bare `require`), no wrapped renewals except
through `ETHRenewerV1.syncWrapper`; everything else v1 (registry writes by
current owners, wrapper subname ops, transfers, resolver record writes)
remains live. The freeze order and its observable reverts are exercised in
(upstream: .refs/ens_v2/contracts/test/e2e/phasedMigration.test.ts:L104-L116 @ ens_v2@ccaeb58b)
and (upstream: .refs/ens_v2/contracts/test/e2e/v1RegistrarFreeze.test.ts:L1-L497 @ ens_v2@ccaeb58b).
The v2 `ETHRegistrar` is enabled *after* migration opens by granting it
`ROLE_REGISTRAR | ROLE_RENEW` on the v2 .eth registry; until then v2
registrations revert `EACUnauthorizedAccountRoles`, and premigrated
(`RESERVED`) labels revert `NameNotAvailable` even after enablement
(upstream: .refs/ens_v2/contracts/test/e2e/phasedMigration.test.ts:L138-L167 @ ens_v2@ccaeb58b).

## 3. The migration payload: `LibMigration.Data`

Every transfer-driven entrypoint carries
`Data { string label; address owner; IRegistry subregistry; address resolver }`
ABI-encoded in the transfer `data` (single struct for single transfers, a
`Data[]` for ERC-1155 batches; `MIN_DATA_SIZE = 7*32`)
(upstream: .refs/ens_v2/contracts/src/migration/libraries/LibMigration.sol:L19-L38 @ ens_v2@ccaeb58b).
`subregistry` is ignored by locked migration; `resolver` is ignored when the
name is locked with `CANNOT_SET_RESOLVER`
(upstream: .refs/ens_v2/contracts/src/migration/libraries/LibMigration.sol:L25-L30 @ ens_v2@ccaeb58b).

Lock classification (the routing predicate for the whole mechanism):

- `isLocked(fuses)` ⇔ `CANNOT_UNWRAP` burned (`PARENT_CANNOT_CONTROL` is a
  prerequisite of `CANNOT_UNWRAP`, so one bit suffices)
  (upstream: .refs/ens_v2/contracts/src/migration/libraries/LibMigration.sol:L72-L77 @ ens_v2@ccaeb58b).
- `isEmancipatedChild(fuses)` ⇔ `PARENT_CANNOT_CONTROL` burned and
  `IS_DOT_ETH` not set — i.e. an emancipated non-2LD
  (upstream: .refs/ens_v2/contracts/src/migration/libraries/LibMigration.sol:L84-L89 @ ens_v2@ccaeb58b).
- `notFrozen(fuses)` ⇔ `CANNOT_BURN_FUSES` clear — controls whether v2 admin
  roles are granted (upstream: .refs/ens_v2/contracts/src/migration/libraries/LibMigration.sol:L79-L82 @ ens_v2@ccaeb58b).

v1 fuse constants: `CANNOT_UNWRAP=1, CANNOT_BURN_FUSES=2, CANNOT_TRANSFER=4,
CANNOT_SET_RESOLVER=8, CANNOT_CREATE_SUBDOMAIN=32, CANNOT_APPROVE=64,
PARENT_CANNOT_CONTROL=1<<16, IS_DOT_ETH=1<<17, CAN_EXTEND_EXPIRY=1<<18`
(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L10-L20 @ ens_v1@91c966f).

Because NameWrapper's ERC-1155 acceptance check swallows typed errors and
only re-throws `Error(string)`
(upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L317-L335 @ ens_v1@91c966f),
all receiver-side reverts are wrapped through `WrappedErrorLib` into
`Error(string)` payloads
(upstream: .refs/ens_v2/contracts/src/migration/AbstractWrapperReceiver.sol:L16-L20,L119-L123 @ ens_v2@ccaeb58b).

## 4. E1 — unwrapped 2LD: `UnlockedMigrationController.onERC721Received`

Trigger: `BaseRegistrar.safeTransferFrom(owner, UnlockedMigrationController,
tokenId=labelhash, data)` by the registrant or an approved operator.

Steps (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L92-L121 @ ens_v2@ccaeb58b):

1. `msg.sender` must be the BaseRegistrar (`UnauthorizedCaller`), data ≥
   `MIN_DATA_SIZE` (`InvalidData`), decode one `Data`, and
   `tokenId == keccak256(label)` (`NameDataMismatch`).
2. `BaseRegistrar.reclaim(tokenId, controller)` — points the v1 registry
   .eth subnode at the controller; emits registry
   `NewOwner(eth_node, labelhash, controller)`
   (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L175 @ ens_v1@91c966f).
3. `ENSRegistry.setRecord(namehash(label.eth), GRAVEYARD, 0, 0)` — v1 owner →
   Graveyard, v1 resolver cleared; emits registry `Transfer(node, GRAVEYARD)`,
   `NewResolver(node, 0)`, `NewTTL(node, 0)`
   (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L33-L41 @ ens_v1@91c966f;
   events: .refs/ens_v1/contracts/registry/ENS.sol:L6-L15 @ ens_v1@91c966f).
4. `BaseRegistrar.safeTransferFrom(controller, GRAVEYARD, tokenId)` — the
   ERC-721 goes to the Graveyard.
5. `_inject`: `owner != 0` required (`InvalidOwner`), then
   `ETHRegistry.register(label, owner, subregistry, resolver,
   REGISTRATION_ROLE_BITMAP, expiry=0)`
   (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L152-L166 @ ens_v2@ccaeb58b).

`expiry=0` means "use the RESERVED expiry" — the claim path requires the
label to currently be `RESERVED` and the caller to hold
`ROLE_REGISTER_RESERVED`; the registered entry silently inherits
`v1 expiry + bonus` and additionally gets the sticky `ROLE_WAS_RESERVED`
marker role
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L435-L448 @ ens_v2@ccaeb58b).
If the name was never premigrated, the controller lacks `ROLE_REGISTRAR` and
the whole transfer reverts `EACUnauthorizedAccountRoles`
(upstream: .refs/ens_v2/contracts/test/e2e/migration.test.ts:L378-L386 @ ens_v2@ccaeb58b).

`REGISTRATION_ROLE_BITMAP` = `ROLE_SET_SUBREGISTRY(+ADMIN) |
ROLE_SET_RESOLVER(+ADMIN) | ROLE_CAN_TRANSFER_ADMIN` — deliberately without
renew roles; renewal stays with the .eth registrar contracts
(upstream: .refs/ens_v2/contracts/src/registrar/ETHRegistrar.sol:L17-L23 @ ens_v2@ccaeb58b).

## 5. E2 — wrapped unlocked 2LD: `UnlockedMigrationController._migrateWrapped`

Trigger: `NameWrapper.safeTransferFrom(owner, UnlockedMigrationController,
id=uint256(namehash(label.eth)), 1, data)` (or `safeBatchTransferFrom` with
`Data[]`). The shared `AbstractWrapperReceiver` guards: caller must be the
NameWrapper, data must decode, `ids.length == mds.length`
(upstream: .refs/ens_v2/contracts/src/migration/AbstractWrapperReceiver.sol:L101-L174 @ ens_v2@ccaeb58b).

Per name (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L132-L150 @ ens_v2@ccaeb58b):

1. `getData(id)`; `isLocked(fuses)` ⇒ revert `NameIsLocked` (use E3).
2. `id` must equal `namehash(eth, keccak256(label))` (`NameDataMismatch`) —
   this is also what makes E2 2LD-only: a 3LD id never matches the ETH_NODE
   parent computation.
3. `NameWrapper.setResolver(id, 0)` — clears the v1 resolver (the controller
   is the current wrapped owner, and unlocked names cannot have
   `CANNOT_SET_RESOLVER`; burning any of `CANNOT_TRANSFER |
   CANNOT_SET_RESOLVER | CANNOT_SET_TTL`-class fuses requires `CANNOT_UNWRAP`
   first (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L644-L651 @ ens_v1@91c966f)).
   Emits registry `NewResolver(node, 0)`.
4. `NameWrapper.unwrapETH2LD(labelHash, GRAVEYARD, GRAVEYARD)` — burns the
   ERC-1155 (`TransferSingle` to 0), emits `NameUnwrapped(node, GRAVEYARD)`,
   sets v1 registry owner of the node to Graveyard (`NewOwner`), and
   transfers the underlying registrar ERC-721 to the Graveyard (`Transfer`)
   (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L382-L401 @ ens_v1@91c966f;
   event: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L35 @ ens_v1@91c966f).
5. `_inject` — same v2 claim as E1 step 5.

Note: every wrapped .eth 2LD has `PARENT_CANNOT_CONTROL | IS_DOT_ETH` burned
by construction (`_wrapETH2LD` forces `fuses | PARENT_CANNOT_CONTROL |
IS_DOT_ETH`), so "wrapped unlocked 2LD" = wrapped with `CANNOT_UNWRAP`
clear, regardless of PCC
(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L996-L1013 @ ens_v1@91c966f;
verified by execution, see validation/).

## 6. E3 — wrapped locked 2LD: `LockedMigrationController`

Trigger: `NameWrapper.safeTransferFrom(owner, LockedMigrationController,
id=uint256(namehash(label.eth)), 1, data)`. `LockedMigrationController` is a
`LockedWrapperReceiver` bound to parent node `ETH_NODE` and the v2 .eth
registry (upstream: .refs/ens_v2/contracts/src/migration/LockedMigrationController.sol:L22,L80-L116 @ ens_v2@ccaeb58b).

Shared locked-path logic, per name
(upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L104-L191 @ ens_v2@ccaeb58b):

1. `owner != 0` (`InvalidOwner`); `id == namehash(parentNode,
   keccak256(label))` (`NameDataMismatch`).
2. `getData` → fuses, expiry. **Locked branch** (`CANNOT_UNWRAP` burned):
   - If `CANNOT_APPROVE` burned and an ERC-1155 token approval exists,
     revert `FrozenTokenApproval` — an unclearable approval must not survive
     into v2 (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L128-L133 @ ens_v2@ccaeb58b).
   - Resolver: if `CANNOT_SET_RESOLVER` is clear, clear the v1 resolver and
     use the caller-supplied `md.resolver`. If burned, the caller's resolver
     is **ignored**; the current v1 resolver is carried over, and if it is a
     known v1 `PublicResolver` (membership in the immutable
     `PUBLIC_RESOLVER_SET`) it is swapped for the replacement wrapper-aware
     `PublicResolver`
     (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L135-L142 @ ens_v2@ccaeb58b).
   - `NameWrapper.safeTransferFrom(receiver, GRAVEYARD, id, 1, "")` — the
     name stays wrapped forever; the ERC-1155 parks in the Graveyard
     (`TransferSingle`).
   - Deploy a **`WrapperRegistry`** proxy via
     `VerifiableFactory.deployProxy(WRAPPER_REGISTRY_IMPL, salt=uint256(node),
     initialize(node, parentRegistry, label, subregistryRoles))` — a
     deterministic per-name v2 subregistry
     (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L146-L162 @ ens_v2@ccaeb58b).
     Its `initialize` emits `RegistryCreated` and `ParentUpdated(parent,
     label, virtualOwner)` and grants the fuse-derived root roles to the
     parent registry address as "virtual owner"
     (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L113-L132 @ ens_v2@ccaeb58b).
   - `_inject(label, owner, subregistry=new WrapperRegistry, resolver,
     tokenRolesFromFuses, expiry)` → v2 .eth registry `register(...,
     expiry=0 ⇒ reserved expiry)` for the 2LD case.
3. **Emancipated-but-not-locked branch** (`isEmancipatedChild`, only
   reachable for non-2LD children arriving at a WrapperRegistry, E4): clear
   v1 resolver, `NameWrapper.unwrap(parentNode, labelHash, GRAVEYARD)`
   (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L403-L420 @ ens_v1@91c966f),
   then `_inject` with `REGISTRATION_ROLE_BITMAP`, plus `ROLE_RENEW |
   ROLE_RENEW_ADMIN` when `CAN_EXTEND_EXPIRY` was burned, at the **wrapper
   expiry** (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L176-L186 @ ens_v2@ccaeb58b).
4. Anything else (parent-controlled child, unlocked 2LD at the locked
   controller) reverts `NameNotLocked`.

Fuse → v2 role translation
(upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L209-L241 @ ens_v2@ccaeb58b):

- Subregistry root roles (granted to the virtual owner of the new
  WrapperRegistry): `ROLE_REGISTRAR` unless `CANNOT_CREATE_SUBDOMAIN`;
  always `ROLE_RENEW | ROLE_UPGRADE | ROLE_CAN_NAME`; admin counterparts
  only if `notFrozen` (no `CANNOT_BURN_FUSES`).
- Token roles (granted to the v2 owner of the migrated name):
  `ROLE_RENEW` if `CAN_EXTEND_EXPIRY`; `ROLE_SET_RESOLVER` unless
  `CANNOT_SET_RESOLVER`; admin counterparts only if `notFrozen`;
  `ROLE_CAN_TRANSFER_ADMIN` unless `CANNOT_TRANSFER`.
- Never granted: `ROLE_SET_SUBREGISTRY` (the WrapperRegistry binding is
  permanent) and `ROLE_SET_PARENT` (the subregistry is canonical)
  (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L39-L42 @ ens_v2@ccaeb58b).

Locked 2LD migration claims the reservation with the fuse-translated bitmap
instead of `REGISTRATION_ROLE_BITMAP`, and `expiry=0 ⇒ reserved expiry`
(upstream: .refs/ens_v2/contracts/src/migration/LockedMigrationController.sol:L89-L111 @ ens_v2@ccaeb58b).

## 7. E4 — locked/emancipated children: `WrapperRegistry` as receiver

`WrapperRegistry` is itself a `LockedWrapperReceiver` whose parent node is
its own name, `_getRegistry() = this`, and whose `_inject` calls
`_register(..., checkRoles=false)` — children are added to the parent's own
WrapperRegistry with **no reservation required and a real expiry** (the
wrapper expiry) (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L210-L225,L285-L288 @ ens_v2@ccaeb58b).
The chain composes: migrating `nick.eth` (E3) creates
`WrapperRegistry(nick.eth)`; transferring wrapped `sub.nick.eth` to that
registry migrates it and creates `WrapperRegistry(sub.nick.eth)` if locked,
and so on
(upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L28-L42 @ ens_v2@ccaeb58b).

While a child is emancipated-in-v1 but not yet migrated, the WrapperRegistry
bridges it:

- `getResolver(label)` returns the shared `V1_RESOLVER` fallback for
  "migratable children" (emancipated in v1, never registered in v2, active
  v1 owner) (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L175-L184,L290-L304 @ ens_v2@ccaeb58b).
- `register(label, ...)` for a migratable child reverts
  `NameRequiresMigration` — nobody can clobber an emancipated child's slot
  before it migrates
  (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L155-L173 @ ens_v2@ccaeb58b).
- Non-emancipated (parent-controlled) v1 children get **no** such
  protection: they cannot migrate (`NameNotLocked`) and the v2 parent owner
  may register their label directly in the WrapperRegistry ("clobber"),
  provided the subregistry kept `ROLE_REGISTRAR`
  (upstream: .refs/ens_v2/contracts/test/e2e/migration.test.ts:L759-L788 @ ens_v2@ccaeb58b).
- An expired-and-reregistered v2 entry flips authority to v2 permanently
  (`getExpiry > 0` ⇒ not migratable); an ABANDONED v1 child (null v1 owner)
  is also not migratable so its label is not locked forever
  (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L296-L303 @ ens_v2@ccaeb58b).
- Revival of expired children in a WrapperRegistry (renew-after-expiry) is
  blocked unless the migrated fuses had allowed subdomain creation
  (`_canRevive` requires the initial bitmap to include `ROLE_REGISTRAR`)
  (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L266-L271 @ ens_v2@ccaeb58b).
- Root-resource admin roles can never be granted on a WrapperRegistry
  (`_getSettableRoles` masks the admin half), and the "owner" of the root
  resource is virtual: whoever owns the parent label in the parent registry
  (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L227-L264 @ ens_v2@ccaeb58b).
- WrapperRegistry proxies are UUPS-upgradeable but only to targets in the
  immutable `UPGRADE_SET` allowlist and only by `ROLE_UPGRADE`
  (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L273-L283 @ ens_v2@ccaeb58b).

## 8. E5 — `MigrationHelper.migrate`: approval-based batch

`migrate(unwrapped[], unlockedGroups[][], lockedGroups[][],
lockedChildrenGroups[])` lets any `msg.sender` who is the owner **or an
approved operator** (`isApprovedForAll` on BaseRegistrar / NameWrapper) move
many names in one transaction; it performs the same `safeTransferFrom` /
`safeBatchTransferFrom` calls as E1-E4, so all controller semantics apply
unchanged (upstream: .refs/ens_v2/contracts/src/migration/MigrationHelper.sol:L94-L135,L193-L200 @ ens_v2@ccaeb58b).
Specifics:

- Each wrapped group must share one owner (`WrappedOwnerMismatch`);
  singleton groups use `safeTransferFrom`, larger ones
  `safeBatchTransferFrom`
  (upstream: .refs/ens_v2/contracts/src/migration/MigrationHelper.sol:L156-L191 @ ens_v2@ccaeb58b).
- For 3LD+ groups, the parent's v2 registry is resolved by walking the live
  v2 tree from the root registry (`LibRegistry.findExactRegistry`); if the
  parent has not migrated the batch reverts `ParentNotMigrated`
  (upstream: .refs/ens_v2/contracts/src/migration/MigrationHelper.sol:L122-L134 @ ens_v2@ccaeb58b).
- Missing approval reverts `NotApprovedOperator`
  (upstream: .refs/ens_v2/contracts/src/migration/MigrationHelper.sol:L57-L59,L193-L200 @ ens_v2@ccaeb58b).
- Third-party (non-owner) batch callers need **two** approvals per owner:
  owner→caller (`isApprovedForAll`, checked by the helper per token, and
  checked *before* the same-owner group rule at L173-L178) and owner→helper
  (checked by the token contract during the actual transfer). Validated:
  H-03 shows `NotApprovedOperator` masking `WrappedOwnerMismatch` until the
  owner→caller approval exists.

## 9. E6 — Graveyard

The Graveyard is "the ENSv1 ETHRegistrarController for ENSv2 launch which
becomes the burn address for migrated tokens": it passively holds ERC-721
and ERC-1155 tokens, and exposes one permissionless function, `clear`
(upstream: .refs/ens_v2/contracts/src/migration/Graveyard.sol:L20-L25,L97-L103 @ ens_v2@ccaeb58b).

`clear(names[])` recursively walks each DNS-encoded name from `.eth` down
and scrubs residual v1 registry state under migrated or expired ancestors
(upstream: .refs/ens_v2/contracts/src/migration/Graveyard.sol:L109-L204 @ ens_v2@ccaeb58b):

- Only `.eth` names are clearable (`NameNotClearable` otherwise; clearing
  `eth`/root is a no-op).
- 2LD level: if the v1 registry owner is already the Graveyard (migrated),
  descend. If the 2LD is a locked wrapped name it must be Graveyard-owned in
  the wrapper (migrated), else `NameNotClearable`. If the 2LD **expired**
  (past v1 grace — `nameExpires` must be nonzero), the Graveyard claims it:
  `BaseRegistrar.register(labelhash, graveyard, ~max duration)` (it is a
  registrar controller) and clears the node's resolver. Live unmigrated
  names are untouchable.
- Deeper levels: Graveyard-owned locked wrapped nodes pass; emancipated
  unlocked wrapped nodes must be migrated (wrapper owner 0 + registry owner
  Graveyard); parent-controlled wrapped children are forcibly re-owned via
  `NameWrapper.setSubnodeRecord(parent, label, graveyard, 0, 0, 0, 0)` +
  `unwrap` to the Graveyard; plain registry children are re-owned via
  `ENSRegistry.setSubnodeRecord(parent, labelhash, graveyard, 0, 0)`.
- A modified DNS encoding (`\x00` + 32-byte labelhash) lets unknown labels
  be cleared by hash where the preimage is not required; where a preimage is
  required (wrapped parent-controlled children) it reverts
  `NameRequiresPreimage`
  (upstream: .refs/ens_v2/contracts/src/migration/Graveyard.sol:L114-L135,L186-L200 @ ens_v2@ccaeb58b).

Effect for indexers: **Graveyard clearing produces additional v1 registry
writes (NewOwner/Transfer/NewResolver to Graveyard/zero) long after
migration, from a permissionless caller.** The Graveyard also claims fully
expired 2LDs with a huge duration, emitting v1
`NameRegistered(id, graveyard, expires≈uint64.max)` + registry `NewOwner`
without any human intent — expected upstream behavior, not a squat
(upstream: .refs/ens_v2/contracts/src/migration/Graveyard.sol:L158-L170 @ ens_v2@ccaeb58b;
exercised in .refs/ens_v2/contracts/test/unit/migration/Graveyard.t.sol:L47-L56,L142-L147 @ ens_v2@ccaeb58b).

## 10. E7 — ETHRenewerV1

`ETHRenewerV1` is the post-launch renewal path for **premigrated but not yet
migrated** names. It is an `AbstractETHRegistrar`: `renew(label, duration,
paymentToken, referrer)` charges an ERC-20 via the rent oracle, calls
`ETH_REGISTRY.renew(tokenId, expiry+duration)` (v2 `ExpiryUpdated`), then
`BaseRegistrar.renew(labelhash_as_id, duration)` on v1 (v1 `NameRenewed`),
and emits its own `NameRenewed(tokenId, label, duration, newExpiry,
paymentToken, referrer, amount)`
(upstream: .refs/ens_v2/contracts/src/registrar/AbstractETHRegistrar.sol:L84-L94 @ ens_v2@ccaeb58b;
upstream: .refs/ens_v2/contracts/src/registrar/ETHRenewerV1.sol:L132-L135 @ ens_v2@ccaeb58b).

- Renewable ⇔ v2 status `RESERVED`, or `AVAILABLE` with no prior token owner
  within the v2 grace window after the reservation expired
  (upstream: .refs/ens_v2/contracts/src/registrar/ETHRenewerV1.sol:L137-L149 @ ens_v2@ccaeb58b).
  Once a name is migrated (`REGISTERED`), ETHRenewerV1 refuses it
  (`NameNotRenewable`) — migrated names renew via the v2 `ETHRegistrar`
  renew path instead. The combined window
  (`GRACE_PERIOD = bonusPeriod + gracePeriodV2`) ends exactly when the v1
  90-day grace ends
  (upstream: .refs/ens_v2/contracts/src/registrar/ETHRenewerV1.sol:L67-L84,L114-L126 @ ens_v2@ccaeb58b;
  boundary exercised in .refs/ens_v2/contracts/test/e2e/migration.test.ts:L402-L439 @ ens_v2@ccaeb58b).
- The v2 `renew` on an expired-but-graced reservation is a **revival**: the
  registry allows it because ETHRenewerV1 holds root `ROLE_RENEW`
  (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L214-L230,L597-L608 @ ens_v2@ccaeb58b).
- `syncWrapper(labels[])`: because launch removed NameWrapper as a
  BaseRegistrar controller, wrapper-recorded expiry of wrapped names can no
  longer follow v1 renewals; `syncWrapper` temporarily re-adds NameWrapper
  as controller (ETHRenewerV1 owns the BaseRegistrar), calls the v1 wrapped
  controller's `renew(label, 0)` to refresh wrapper expiry, and removes the
  controller again — emitting v1 `ControllerAdded`/`ControllerRemoved` and
  wrapper `ExpiryExtended` events
  (upstream: .refs/ens_v2/contracts/src/registrar/ETHRenewerV1.sol:L104-L112 @ ens_v2@ccaeb58b).
- It also holds BaseRegistrar ownership and can hand it on or set the
  registrar-node resolver
  (upstream: .refs/ens_v2/contracts/src/registrar/ETHRenewerV1.sol:L90-L102 @ ens_v2@ccaeb58b).

## 11. What migration emits (v2 side)

All v2 registries (ETHRegistry and every WrapperRegistry) are
`PermissionedRegistry`s and share the ENSIP-16-style event set
(upstream: .refs/ens_v2/contracts/src/registry/interfaces/IRegistryEvents.sol:L7-L89 @ ens_v2@ccaeb58b):

- `RegistryCreated()` — constructor / WrapperRegistry initialize.
- `LabelReserved(tokenId indexed, labelHash indexed, label, expiry, sender indexed)` — premigration.
- `LabelRegistered(tokenId indexed, labelHash indexed, label, owner, expiry, sender indexed)` — the migration claim (sender = the controller).
- `LabelUnregistered(tokenId indexed, sender indexed)`.
- `ExpiryUpdated(tokenId indexed, newExpiry indexed, sender indexed)` — renewals.
- `SubregistryUpdated(tokenId indexed, subregistry indexed, sender indexed)` — emitted during `register` when a nonzero subregistry is set (locked migration: the new WrapperRegistry).
- `ResolverUpdated(tokenId indexed, resolver indexed, sender indexed)` — emitted during `register` when a nonzero resolver is set.
- `ParentUpdated(parent indexed, label, sender indexed)` — WrapperRegistry initialize (canonical parent binding).
- `TokenRegenerated(oldTokenId indexed, newTokenId indexed)` — token id version bump on role grant/revoke.
- `TokenResource(tokenId indexed, resource indexed)` — token↔ACL-resource binding on mint (upstream: .refs/ens_v2/contracts/src/registry/interfaces/IPermissionedRegistry.sol:L33-L38 @ ens_v2@ccaeb58b).
- Plus ERC-1155 `TransferSingle`/`TransferBatch` (mint/burn/transfer) and
  EnhancedAccessControl role events on every grant.

Token identity subtlety for indexers: v2 token ids and ACL resource ids are
the labelhash with embedded version counters; `unregister`/re-register bumps
both counters and **role changes regenerate the token id** (burn+mint with
`TokenRegenerated`) while the entry (labelhash) stays the same
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L23-L34,L528-L540 @ ens_v2@ccaeb58b).
`register` over an expired-but-minted entry burns the previous owner's token
first (visible as `TransferSingle` to 0 inside the same tx)
(upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L452-L457 @ ens_v2@ccaeb58b).

## 12. State transitions (text diagram)

Per .eth 2LD, joint (v1 registrar/wrapper state × v2 .eth-registry status):

```
                         BatchRegistrar.batchRegister (DAO premigration)
 (v1: live, any shape)  ─────────────────────────────────────────────────►  v2 RESERVED
        │                                                                    (owner=0, expiry = v1exp+62d+1s,
        │                                                                     resolver = ENSV1Resolver fallback)
        │ v1 renewals pre-launch / ETHRenewerV1.renew post-launch:
        │ v1 expiry += d  AND  v2 reserved expiry += d   (stays RESERVED)
        │
        ├── E1 unwrapped transfer ────────────► v2 REGISTERED (roles = REGISTRATION_ROLE_BITMAP)
        ├── E2 wrapped-unlocked transfer ─────► v2 REGISTERED (roles = REGISTRATION_ROLE_BITMAP)
        ├── E3 wrapped-locked transfer ───────► v2 REGISTERED (roles = fuses→roles,
        │                                        subregistry = new WrapperRegistry(name))
        │        v1 side after E1/E2: registry owner = Graveyard, resolver = 0,
        │        registrar ERC-721 parked in Graveyard.
        │        v1 side after E3: registry owner REMAINS NameWrapper (name
        │        stays wrapped), still-wrapped ERC-1155 parked in Graveyard,
        │        resolver cleared unless CANNOT_SET_RESOLVER (then left set).
        │        [validated: L-01..L-08 terminal-state checks]
        │
        └── nobody migrates, v1 expiry passes:
             t ∈ [v1exp, v1exp+62d+1s)          v2 still RESERVED (bonus window), v1 in grace
             t ∈ [v1exp+62d+1s, v1exp+90d+1s)   v2 AVAILABLE-in-grace: only ETHRenewerV1 revival
             t ≥ v1exp+90d+1s                   v1 past grace AND v2 grace over:
                                                  v2 ETHRegistrar can register fresh (new owner,
                                                  fresh identity), Graveyard.clear can claim the
                                                  v1 husk. Reservation dead.

 v2 REGISTERED ── ETHRegistrar.renew (v2-only; v1 husk expiry NOT extended)
              ── transfer/setResolver/setSubregistry per granted roles
              ── expiry passes → v2 AVAILABLE → ETHRegistrar register = new identity
                                             → WrapperRegistry child revival only if fuses allowed
```

Per wrapped child under a locked, migrated parent (`sub.nick.eth`):

```
 v1 wrapped child                    v2 WrapperRegistry(nick.eth) view
 ──────────────────                  ─────────────────────────────────
 parent-controlled (no PCC)          no entry; resolver = 0; label clobberable by
                                     parent via register() [if ROLE_REGISTRAR];
                                     child transfer reverts NameNotLocked
 emancipated (PCC, no CU)            "migratable child": getResolver = V1_RESOLVER
                                     fallback; register() reverts NameRequiresMigration;
                                     E4 transfer → REGISTERED at wrapper expiry
                                     (+RENEW roles if CAN_EXTEND_EXPIRY)
 locked (PCC|CU)                     same protection; E4 transfer → REGISTERED +
                                     its own WrapperRegistry(sub.nick.eth), token
                                     stays wrapped, parked in Graveyard
 v1-abandoned (owner=0) or           not migratable: register()/revival rules of the
 v2 entry expired                    WrapperRegistry apply (v2 is authority once
                                     getExpiry > 0)
```

## 13. Hard reachability walls (proven upstream, drive the pruning in dimensions.md)

- **Locked + `CANNOT_TRANSFER` cannot migrate.** The NameWrapper transfer
  hook reverts `OperationProhibited` for any live token with
  `CANNOT_TRANSFER` burned, and migration is transfer-driven; there is no
  other entrypoint
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L830-L833 @ ens_v1@91c966f;
  upstream: .refs/ens_v2/contracts/test/e2e/migration.test.ts:L644-L653 @ ens_v2@ccaeb58b).
  The fuse-to-role code still handles the bit defensively
  (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L238-L240 @ ens_v2@ccaeb58b).
- **Expired v1 names cannot migrate.** Unwrapped: `ownerOf` reverts once
  `expiries[id] <= now`, so `safeTransferFrom` is impossible during grace
  (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L71-L76 @ ens_v1@91c966f).
  Wrapped .eth 2LD: `_beforeTransfer` treats 2LDs as expiring at grace
  start and every wrapped 2LD has PCC, so in-grace transfer reverts
  (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L825-L836 @ ens_v1@91c966f).
  Path: renew via ETHRenewerV1 first, then migrate.
- **Un-premigrated names cannot migrate** (`EACUnauthorizedAccountRoles`):
  the controllers hold `ROLE_REGISTER_RESERVED`, not `ROLE_REGISTRAR`, on
  the .eth registry (upstream: .refs/ens_v2/contracts/test/e2e/migration.test.ts:L378-L386 @ ens_v2@ccaeb58b).
- **Migration is all-or-nothing per name**: every v1-side mutation and the
  v2 claim happen in the token-transfer transaction; any failure reverts the
  transfer (the wrapped-error dance exists only to surface reasons through
  NameWrapper's `Error(string)` filter).
- **No unmigration.** Nothing moves state v2→v1; the Graveyard has no
  outbound transfer function.
