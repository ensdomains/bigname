# Migration scenario dimension space (pruned to reachable combinations)

Companion to `mechanism.md` (which carries the primary citations). Every
pruning below is backed either by a cited revert condition or by an executed
run in `validation/` (referenced by scenario ID from `catalog.md`).

## D1 — Pre-migration v1 state of the .eth 2LD

| Value | Reachable? | Notes / proof |
|---|---|---|
| unwrapped | yes | baseline; E1 path |
| wrapped, unlocked (`CANNOT_UNWRAP` clear) | yes | every wrapped .eth 2LD carries `PARENT_CANNOT_CONTROL\|IS_DOT_ETH` by construction, so "unlocked" is solely `CANNOT_UNWRAP` clear (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L996-L1013 @ ens_v1@91c966f); E2 path |
| wrapped, locked (`CANNOT_UNWRAP`) | yes | E3 path |
| locked + `CANNOT_SET_RESOLVER` | yes | resolver-carryover branch; sub-split on current v1 resolver: {zero, recognized PublicResolver (in `PUBLIC_RESOLVER_SET`), custom} — all three reachable (L-02/L-03) |
| locked + `CANNOT_CREATE_SUBDOMAIN` | yes | subregistry loses `ROLE_REGISTRAR`; revival blocked (L-05) |
| locked + `CANNOT_BURN_FUSES` (frozen) | yes | v2 admin roles withheld (L-04) |
| locked + `CANNOT_APPROVE`, no live approval | yes | migrates normally (L-06) |
| locked + `CANNOT_APPROVE`, live ERC-1155 approval | reaches revert | `FrozenTokenApproval` — terminal until approval… is unclearable, so terminal, period (X-L-02) |
| locked + `CANNOT_TRANSFER` | **unmigratable** | transfer hook reverts `OperationProhibited`; mechanism is transfer-only (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L830-L833 @ ens_v1@91c966f; X-L-01) |
| 2LD with `CAN_EXTEND_EXPIRY` | **pruned (unreachable)** | `wrapETH2LD`/`onERC721Received` accept only `uint16 ownerControlledFuses`, which cannot express bit 18; parent-controlled fuses on a 2LD would need the wrapper's `.eth` parent flow, which doesn't exist (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L785-L794 @ ens_v1@91c966f). CEE matters only for children (C-03) |

## D2 — Records & resolver state pre-migration

| Value | Reachable? | Notes |
|---|---|---|
| no resolver | yes | migrated entry gets `md.resolver` (possibly 0) |
| v1 PublicResolver + records (addr/text/contenthash) | yes | record continuity assertions; resolver cleared on v1 during migration |
| custom resolver | yes | carried into `md.resolver` by convention (caller-chosen), or forcibly carried when `CANNOT_SET_RESOLVER` |
| TTL set | yes, ignored | migration zeroes it with `setRecord`; no v2 counterpart |

## D3 — Subname shape beneath the 2LD

| Value | Reachable? | Notes |
|---|---|---|
| none | yes | baseline |
| unwrapped registry subnode (v1 `setSubnodeOwner`) | yes | survives on v1 after parent migrates; Graveyard.clear can scrub (U-06, G-01) |
| wrapped parent-controlled child (no PCC) | yes | cannot migrate (`NameNotLocked`); clobberable in WrapperRegistry (C-06) |
| wrapped emancipated child (PCC, no CU) | yes | "detached": protected + migratable via E4 unwrap branch (C-02, C-05) |
| wrapped locked child (PCC\|CU) | yes | E4 locked branch → nested WrapperRegistry (C-01, C-04) |
| child with CAN_EXTEND_EXPIRY | yes | set by parent via `setSubnodeOwner` fuses / `setChildFuses`; extra v2 renew roles (C-03) |
| abandoned emancipated child (wrapper expiry passed, owner burned) | yes | not migratable nor protected; label re-registrable in WrapperRegistry (C-08) |
| under an **unlocked** (E2-migrated) parent | pruned to trivial | E2/E1 migration does not create a WrapperRegistry; v1 children of such parents have no v2 bridge at all — their subtree is plain "v2 parent, v1 residue" (covered by U-06); no per-child migration path exists (`md.subregistry` is the owner's own choice) |
| deeper than 3LD | yes | same rules recursively (C-04 covers 4LD) |

## D4 — Expiry timing (v1 expiry `E`; v2 reserved expiry `E + 62d + 1s`)

| Window | Migration | Renewal | Proof |
|---|---|---|---|
| active (`t < E`) | yes | ETHRenewerV1 (RESERVED) | R-01 |
| v1 grace, bonus window (`E ≤ t < E+62d+1s`) | **no** — ERC-721 `ownerOf` reverts; wrapped 2LD transfer reverts (PCC + grace-start rule) | yes, still RESERVED | X-U-06, X-W-03, R-02 (renew-then-migrate) |
| v1 grace, v2 grace window (`E+62d+1s ≤ t < E+90d+1s`) | no | yes — revival path (`AVAILABLE`, never-owned, within v2 grace) | mechanism §10; R-02 boundary |
| past v1 grace (`t ≥ E+90d+1s`) | no — reservation dead | no (`NameNotRenewable`); label falls to v2 ETHRegistrar; Graveyard can claim v1 husk | R-03, G-02, P-09 |

## D5 — Ownership / authorization shape

| Value | Reachable? | Notes |
|---|---|---|
| owner EOA, self-migrates | yes | baseline |
| approved operator migrates (ERC-721 approve/setApprovalForAll; ERC-1155 setApprovalForAll) | yes | U-02; also the MigrationHelper predicate (H-01) |
| `md.owner` ≠ v1 owner (give-away migration) | yes | U-03; v2 owner is whatever the payload says — the *transferrer* chooses |
| `md.owner` = contract without `onERC1155Received` | reaches revert | v2 mint fails `ERC1155InvalidReceiver` (X-U-07) |
| `md.owner = 0` | reaches revert | `InvalidOwner` (X-U-02) |
| v1 owner is a contract | yes | only matters that *someone* can trigger its transfer; no extra mechanism branch — pruned to note |

## D6 — Migration path

| Value | Notes |
|---|---|
| E1 direct ERC-721 transfer | U-* |
| E2 direct ERC-1155 single / batch | W-01/W-03 |
| E3 direct ERC-1155 single / batch | L-01/L-08 |
| E4 child → parent WrapperRegistry | C-* |
| E5 MigrationHelper batch (mixed groups) | H-*; per-group single-owner rule, parent-must-be-migrated rule |
| wrong-controller cross products | all revert, each with a distinct error: unwrapped→Locked (ERC-721 non-receiver), unlocked→Locked (`NameNotLocked`), locked→Unlocked (`NameIsLocked`), 2LD→WrapperRegistry (`NameDataMismatch`), 3LD→LockedController (`NameDataMismatch`) (X-U-05, X-W-01, X-W-02, X-L-03, X-L-04) |

## D7 — Post-migration perturbations (the authority-precedence class)

Contrast with the 2020 within-v1 registry migration
(`tests/e2e/src/scenarios/registry_migration.rs`): there, the superseded
registry remained fully writable, so "later write must not stand" is the
core assertion. In v1→v2 migration the superseded v1 node is owned by the
Graveyard, so *for the migrated node itself* late v1 writes are mostly
**unreachable by construction** — the reachable late-write surface is:

| Perturbation | Reachable? | Proof / scenario |
|---|---|---|
| v1 registry write on migrated node by old owner/operator | **no** — `authorised` modifier fails (owner is Graveyard) | P-02, P-03 (validated reverts) |
| v1 wrapper write on E3-migrated node by old owner | **no** — wrapper token owned by Graveyard | P-04 |
| v1 writes by pre-existing **subnode** owners under a migrated 2LD | yes | U-06/P-01: subnode records persist and remain mutable on v1 |
| v1 record writes on a **resolver** for a migrated name (resolver contract not access-gated by registry for already-authorized writers?) | no for PublicResolver (auth checks registry/wrapper owner at write time) — pruned; custom resolvers may allow anything (out of mechanism scope, note for indexer) |
| emancipated sibling child still on v1 (wrapper ops + records) under migrated locked parent | yes | P-05: mixed-authority tree is a *steady state* |
| Graveyard.clear emissions (NewOwner/Transfer/NewResolver→graveyard/0, wrapped-child force-unwrap, expired-2LD claim `NameRegistered` with ≈max expiry) | yes, permissionless | G-01/G-02/G-04 |
| ETHRenewerV1 renewals (unmigrated names): v1 `NameRenewed` + v2 `ExpiryUpdated` dual-write | yes | R-01/R-02 |
| `syncWrapper`: transient `ControllerAdded/ControllerRemoved` + wrapper `ExpiryExtended` | yes | R-05 |
| v1 re-registration of a migrated/expired name | **no** — no public controller remains post-launch; only Graveyard/ETHRenewerV1 are controllers, and Graveyard only self-claims | mechanism §2; phasedMigration.test.ts revert |
| v2 renew of migrated name (ETHRegistrar) → v1 husk expiry diverges | yes | P-06 |
| v2 role grant/revoke → `TokenRegenerated` token-id churn | yes | P-07 |
| v2 ERC-1155 transfer of migrated name | yes | P-08 |
| v2 `unregister` of migrated name | root-held `ROLE_UNREGISTER` only; neither migration bitmap grants it | SPEC-ONLY note (P-11) |
| reservation lapse → fresh v2 registration by third party (identity discontinuity, no `ROLE_WAS_RESERVED`) | yes | P-09 |
| reverse-record continuity: v1 primary name claim + forward verification against post-migration resolver | yes | U-07/P-10 |

## Count

Fully-crossed the space would be ≈ 10 wrap/fuse classes × 4 record states ×
8 subname shapes × 4 expiry windows × 6 auth shapes × 6 paths × 15
perturbations ≈ 10^6. After pruning by the mechanism's routing predicate
(fuses decide the path), the walls above, and collapsing dimensions that
provably do not branch the mechanism (owner contract-ness, TTL, record
*values*), the reachable, behavior-distinct space is the ~60-scenario set in
`catalog.md`: 9 U + 7 X-U + 3 W + 3 X-W + 8 L + 4 X-L + 8 C + 4 H + 5 G +
5 R + 11 P.
