//! Database tests that the [lookahead loader](../../../../docs/glossary.md#lookahead-loader)
//! and the full-state loader produce identical
//! output on Sepolia, whose manifests mix ENSv1 and ENSv2 families: batch by batch over a
//! history with an ENSv1→ENSv2 migration and registration expiries, across a one-block
//! reorg, and across a restart.
use std::{collections::BTreeSet, num::NonZeroU32};

use alloy_primitives::{Address, U256, keccak256};
use alloy_sol_types::SolEvent;
use sqlx::PgPool;

use super::equivalence_tests::{
    FIRST_BLOCK, GRACE, OWNER, SECOND_OWNER, START, Seeder, block_hash, child,
    database_with_manifests, eth_node, seed_lineage, stored_events, token, transaction_hash,
    walk_seeded,
};
use crate::{BatchRequest, Engine, Marker, RunMode, StateLoader};

type TestResult<T = ()> = anyhow::Result<T>;

const CHAIN: &str = "ethereum-sepolia";
const ENS_REGISTRY: &str = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e";
const BASE_REGISTRAR: &str = "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85";
const WRAPPED_CONTROLLER: &str = "0xfed6a969aaa60e4961fcd3ebf1a2e8913ac65b72";
const ETH_REGISTRY: &str = "0xd4ebcbbdf463c9c45784603db0ddd499bc44a8b4";
const PUBLIC_RESOLVER: &str = "0xdc4a563d00f5c3012b699794eb9e13a561be386f";
const UNLOCKED_CONTROLLER: &str = "0x2a35b94df22cc7354570be2284655e2cdc0e64a2";
const LOCKED_CONTROLLER: &str = "0x6029a063d69b09d23c52a754a90e4fe43adac3a8";
const GRAVEYARD: &str = "0xb58a90a39d13cce1d0e192b5da5c47640855b04d";
const VERIFIABLE_FACTORY: &str = "0xda70306c98e97ece36f997a21368e53298572991";
const WRAPPER_REGISTRY_IMPLEMENTATION: &str = "0xbe768b63e5fbbfbb0ae97e9064e0002df8001880";
const ROOT_REGISTRY: &str = "0xb458d6a3a77919449d03e7a6903c26827c1ec43f";
const MIGRATION_REGISTRY: &str = "0x0000000000000000000000000000000000000771";
/// The role bitmap the unlocked controller grants a migrated name's owner.
const MIGRATED_ROLES: &str = "97409655027181761882228017414928043062435250176";
/// `MIGRATED_ROLES` plus `ROLE_UNREGISTER`
/// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/libraries/RegistryRolesLib.sol:L24 @ ens_v2_sepolia_20260916@366de741).
const UNREGISTER_ROLES: &str = "97409655027181761882228017414928043062435254272";
/// `MIGRATED_ROLES` with its lowest role revoked.
const REVOKED_ROLES: &str = "97409655027181761882228017414928043062434201600";

mod v1 {
    alloy_sol_types::sol! {
        event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
        event Transfer(bytes32 indexed node, address owner);
    }

    pub(super) mod registrar {
        alloy_sol_types::sol! {
            event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
            event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
            event NameRenewed(uint256 indexed id, uint256 expires);
        }
    }

    pub(super) mod controller {
        alloy_sol_types::sol! {
            event NameRenewed(string name, bytes32 indexed label, uint256 cost, uint256 expires);
        }
    }
}

mod v2 {
    alloy_sol_types::sol! {
        event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender);
        event LabelUnregistered(uint256 indexed tokenId, address indexed sender);
        event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender);
        event TokenRegenerated(uint256 indexed oldTokenId, uint256 indexed newTokenId);
        event ExpiryUpdated(uint256 indexed tokenId, uint64 indexed newExpiry, address indexed sender);
        event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender);
        event TokenResource(uint256 indexed tokenId, uint256 indexed resource);
        event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value);
        event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap);
        event RegistryCreated();
        event ParentUpdated(address indexed parent, string label, address indexed sender);
        event ProxyDeployed(address indexed sender, address indexed proxyAddress, uint256 salt, address implementation);
        event AddrChanged(bytes32 indexed node, address a);
        event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value);
    }
}

/// Seconds after `START` at which each block is mined.
const OFFSETS: [i64; 9] = [
    0,                   // 0: alice and carol registered in ENSv1; bob, dave and erin in ENSv2
    10,                  // 1: alice renewed, disclosing her label; bob's expiry and address set
    500, // 2: alice moves from ENSv1 to ENSv2; a migration registry claims alice.eth
    1_000 + GRACE - 5, // 3: erin unregistered; sub registered in the migration registry
    1_000 + GRACE + 1, // 4: alice renewed in ENSv1 after her move; carol's ENSv1 registration lapses
    1_000 + GRACE + 100, // 5: quiet; dave's ENSv2 registration has lapsed
    1_000 + GRACE + 200, // 6: alice gets a resolver, a text record, a new token id and a subregistry
    1_000 + GRACE + 300, // 7: bob transferred; a text record; alice's subregistry cleared
    1_000 + GRACE + 400, // 8: the ETH registry's parent set again
];

/// ENSv2 token ids carry a version in their low four bytes
/// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/utils/LibLabel.sol:L15-16 @ ens_v2_sepolia_20260916@366de741)
/// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L685 @ ens_v2_sepolia_20260916@366de741);
/// a fresh label's is zero, because the version only increments on unregister or regeneration
/// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L34 @ ens_v2_sepolia_20260916@366de741).
fn v2_token(label: &str) -> U256 {
    let mut versioned = keccak256(label.as_bytes()).0;
    versioned[28..].fill(0);
    U256::from_be_bytes(versioned)
}

fn v2_token_version(label: &str, version: u32) -> U256 {
    v2_token(label) | U256::from(version)
}

fn seeder(pool: &PgPool) -> Seeder<'_> {
    Seeder {
        pool,
        chain: CHAIN,
        block: 0,
        hash: String::new(),
        transaction: String::new(),
        log_index: 0,
    }
}

impl Seeder<'_> {
    async fn register_v1(&mut self, label: &str, expires: i64) -> TestResult {
        let owner: Address = OWNER.parse()?;
        let subnode = v1::NewOwner {
            node: eth_node(),
            label: keccak256(label.as_bytes()),
            owner,
        };
        self.log(ENS_REGISTRY, subnode.encode_log_data()).await?;
        let minted = v1::registrar::Transfer {
            from: Address::ZERO,
            to: owner,
            tokenId: token(label),
        };
        self.log(BASE_REGISTRAR, minted.encode_log_data()).await?;
        let registered = v1::registrar::NameRegistered {
            id: token(label),
            owner,
            expires: U256::from(expires),
        };
        self.log(BASE_REGISTRAR, registered.encode_log_data()).await
    }

    /// The ETHRegistry logs of one registration: label, mint, resource link and owner roles.
    async fn register_v2(
        &mut self,
        label: &str,
        expiry: i64,
        sender: &str,
        roles: &str,
    ) -> TestResult {
        self.register_v2_in(ETH_REGISTRY, label, expiry, sender, roles)
            .await
    }

    async fn register_v2_in(
        &mut self,
        registry: &str,
        label: &str,
        expiry: i64,
        sender: &str,
        roles: &str,
    ) -> TestResult {
        let owner: Address = OWNER.parse()?;
        let sender: Address = sender.parse()?;
        let registered = v2::LabelRegistered {
            tokenId: v2_token(label),
            labelHash: keccak256(label.as_bytes()),
            label: label.to_owned(),
            owner,
            expiry: u64::try_from(expiry)?,
            sender,
        };
        self.log(registry, registered.encode_log_data()).await?;
        let minted = v2::TransferSingle {
            operator: sender,
            from: Address::ZERO,
            to: owner,
            id: v2_token(label),
            value: U256::from(1_u64),
        };
        self.log(registry, minted.encode_log_data()).await?;
        let resource = v2::TokenResource {
            tokenId: v2_token(label),
            resource: v2_token(label),
        };
        self.log(registry, resource.encode_log_data()).await?;
        let granted = v2::EACRolesChanged {
            resource: v2_token(label),
            account: owner,
            oldRoleBitmap: U256::ZERO,
            newRoleBitmap: roles.parse()?,
        };
        self.log(registry, granted.encode_log_data()).await
    }

    async fn transfer_v2(&mut self, id: U256, from: Address, to: Address) -> TestResult {
        let transferred = v2::TransferSingle {
            operator: OWNER.parse()?,
            from,
            to,
            id,
            value: U256::from(1_u64),
        };
        self.log(ETH_REGISTRY, transferred.encode_log_data()).await
    }

    async fn set_v2_resolver(&mut self, label: &str) -> TestResult {
        let updated = v2::ResolverUpdated {
            tokenId: v2_token(label),
            resolver: PUBLIC_RESOLVER.parse()?,
            sender: OWNER.parse()?,
        };
        self.log(ETH_REGISTRY, updated.encode_log_data()).await
    }

    async fn text(&mut self, label: &str, value: &str) -> TestResult {
        let record = v2::TextChanged {
            node: child(eth_node(), label),
            indexedKey: keccak256(b"url"),
            key: "url".to_owned(),
            value: value.to_owned(),
        };
        self.log(PUBLIC_RESOLVER, record.encode_log_data()).await
    }
}

/// Gives every pooled connection the interpreter content hash, as the runner does: the
/// migration writer records it beside each ENSv1→ENSv2 migration it correlates.
async fn stamp_interpreter_hash(pool: &PgPool) -> TestResult {
    let mut connections = Vec::new();
    for _ in 0..pool.options().get_max_connections() {
        let mut connection = pool.acquire().await?;
        sqlx::query("SELECT set_config('bigname.interpreter_content_hash', $1, false)")
            .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
            .execute(&mut *connection)
            .await?;
        connections.push(connection);
    }
    Ok(())
}

/// The raw logs of the whole history, and canonical lineage for the blocks `lineage` times.
async fn seed_history(pool: &PgPool, lineage: &[i64]) -> TestResult {
    seed_lineage(pool, CHAIN, lineage).await?;
    let owner: Address = OWNER.parse()?;
    let controller: Address = UNLOCKED_CONTROLLER.parse()?;
    let graveyard: Address = GRAVEYARD.parse()?;
    let mut seed = seeder(pool);

    seed.block(FIRST_BLOCK).await?;
    // The deployment points the ETH registry at the root registry
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/deploy/01_ETHRegistry.ts:L59-L69 @ ens_v2_sepolia_20261001@07e55a05).
    let parent = v2::ParentUpdated {
        parent: ROOT_REGISTRY.parse()?,
        label: "eth".to_owned(),
        sender: owner,
    };
    seed.log(ETH_REGISTRY, parent.encode_log_data()).await?;
    seed.register_v1("alice", START + 10 * GRACE).await?;
    seed.register_v1("carol", START + 1_000).await?;
    seed.register_v2("bob", START + 2_000, OWNER, MIGRATED_ROLES)
        .await?;
    seed.set_v2_resolver("bob").await?;
    seed.register_v2("dave", START + 1_000 + GRACE + 50, OWNER, MIGRATED_ROLES)
        .await?;
    seed.register_v2("erin", START + 10 * GRACE, OWNER, UNREGISTER_ROLES)
        .await?;

    seed.block(FIRST_BLOCK + 1).await?;
    let renewed = v1::registrar::NameRenewed {
        id: token("alice"),
        expires: U256::from(START + 20 * GRACE),
    };
    seed.log(BASE_REGISTRAR, renewed.encode_log_data()).await?;
    let disclosed = v1::controller::NameRenewed {
        name: "alice".to_owned(),
        label: keccak256(b"alice"),
        cost: U256::from(1),
        expires: U256::from(START + 20 * GRACE),
    };
    seed.log(WRAPPED_CONTROLLER, disclosed.encode_log_data())
        .await?;
    let extended = v2::ExpiryUpdated {
        tokenId: v2_token("bob"),
        newExpiry: u64::try_from(START + 3 * GRACE)?,
        sender: owner,
    };
    seed.log(ETH_REGISTRY, extended.encode_log_data()).await?;
    let address = v2::AddrChanged {
        node: child(eth_node(), "bob"),
        a: owner,
    };
    seed.log(PUBLIC_RESOLVER, address.encode_log_data()).await?;

    // The unlocked controller's `.eth` ENSv1→ENSv2 migration: registrar transfer to the controller,
    // registry reclaim, registry transfer and registrar transfer to the Graveyard, then the
    // ENSv2 registration, with a migration registry deployed in the same transaction.
    // (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L111-L119 @ ens_v2@a971bd64)
    let migration = FIRST_BLOCK + 2;
    seed.block_sent_to(
        migration,
        &block_hash(migration),
        &transaction_hash(migration),
        UNLOCKED_CONTROLLER,
    )
    .await?;
    let to_controller = v1::registrar::Transfer {
        from: owner,
        to: controller,
        tokenId: token("alice"),
    };
    seed.log(BASE_REGISTRAR, to_controller.encode_log_data())
        .await?;
    let reclaimed = v1::NewOwner {
        node: eth_node(),
        label: keccak256(b"alice"),
        owner: controller,
    };
    seed.log(ENS_REGISTRY, reclaimed.encode_log_data()).await?;
    let buried = v1::Transfer {
        node: child(eth_node(), "alice"),
        owner: graveyard,
    };
    seed.log(ENS_REGISTRY, buried.encode_log_data()).await?;
    let to_graveyard = v1::registrar::Transfer {
        from: controller,
        to: graveyard,
        tokenId: token("alice"),
    };
    seed.log(BASE_REGISTRAR, to_graveyard.encode_log_data())
        .await?;
    seed.register_v2(
        "alice",
        START + 10 * GRACE,
        UNLOCKED_CONTROLLER,
        MIGRATED_ROLES,
    )
    .await?;
    seed.log(MIGRATION_REGISTRY, v2::RegistryCreated {}.encode_log_data())
        .await?;
    let deployed = v2::ProxyDeployed {
        sender: LOCKED_CONTROLLER.parse()?,
        proxyAddress: MIGRATION_REGISTRY.parse()?,
        salt: U256::from_be_bytes(keccak256(b"lookahead-registry").0),
        implementation: WRAPPER_REGISTRY_IMPLEMENTATION.parse()?,
    };
    seed.log(VERIFIABLE_FACTORY, deployed.encode_log_data())
        .await?;
    // The migration registry claims `alice.eth` before alice's token points at it, so its suffix
    // walk still ends at that token and nothing is renamed.
    let claimed = v2::ParentUpdated {
        parent: ETH_REGISTRY.parse()?,
        label: "alice".to_owned(),
        sender: owner,
    };
    seed.log(MIGRATION_REGISTRY, claimed.encode_log_data())
        .await?;

    // Unregistering a live token needs ROLE_UNREGISTER and burns the token
    // (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L222-L234 @ ens_v2_sepolia_20260916@366de741)
    // (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L648-L665 @ ens_v2_sepolia_20260916@366de741).
    seed.block(FIRST_BLOCK + 3).await?;
    // A token no later log touches: alice's subregistry update at block 6 names it
    // sub.alice.eth and the clear at block 7 takes the name away.
    seed.register_v2_in(
        MIGRATION_REGISTRY,
        "sub",
        START + 10 * GRACE,
        OWNER,
        MIGRATED_ROLES,
    )
    .await?;
    let unregistered = v2::LabelUnregistered {
        tokenId: v2_token("erin"),
        sender: owner,
    };
    seed.log(ETH_REGISTRY, unregistered.encode_log_data())
        .await?;
    seed.transfer_v2(v2_token("erin"), owner, Address::ZERO)
        .await?;

    // Anyone may renew an ENSv1 name, so alice's registration renews after she moved to ENSv2
    // (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L352-L367 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L157-L167 @ ens_v1@91c966f).
    // Her ENSv2 token keeps alice.eth's current resource through the renewal, as a full refresh
    // of every name elects.
    seed.block(FIRST_BLOCK + 4).await?;
    let renewed_again = v1::registrar::NameRenewed {
        id: token("alice"),
        expires: U256::from(START + 30 * GRACE),
    };
    seed.log(BASE_REGISTRAR, renewed_again.encode_log_data())
        .await?;

    seed.block(FIRST_BLOCK + 6).await?;
    seed.set_v2_resolver("alice").await?;
    seed.text("alice", "0x06").await?;
    // Revoking a role regenerates the token: burn, TokenRegenerated, mint under the next version
    // (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/access-control/EnhancedAccessControl.sol:L318-L320 @ ens_v2_sepolia_20260916@366de741)
    // (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L568-L580 @ ens_v2_sepolia_20260916@366de741).
    let revoked = v2::EACRolesChanged {
        resource: v2_token("alice"),
        account: owner,
        oldRoleBitmap: MIGRATED_ROLES.parse()?,
        newRoleBitmap: REVOKED_ROLES.parse()?,
    };
    seed.log(ETH_REGISTRY, revoked.encode_log_data()).await?;
    seed.transfer_v2(v2_token("alice"), owner, Address::ZERO)
        .await?;
    let regenerated = v2::TokenRegenerated {
        oldTokenId: v2_token("alice"),
        newTokenId: v2_token_version("alice", 1),
    };
    seed.log(ETH_REGISTRY, regenerated.encode_log_data())
        .await?;
    seed.transfer_v2(v2_token_version("alice", 1), Address::ZERO, owner)
        .await?;
    let subregistry = v2::SubregistryUpdated {
        tokenId: v2_token_version("alice", 1),
        subregistry: MIGRATION_REGISTRY.parse()?,
        sender: owner,
    };
    seed.log(ETH_REGISTRY, subregistry.encode_log_data())
        .await?;

    seed_last_block(&mut seed, &block_hash(FIRST_BLOCK + 7), "0x07").await?;
    if lineage.len() > 8 {
        seed_reparent(&mut seed).await?;
    }
    Ok(())
}

/// Block 8: the ETH registry's parent set again, which renames every token in it, so a later
/// batch requests the registry whole.
async fn seed_reparent(seed: &mut Seeder<'_>) -> TestResult {
    seed.block(FIRST_BLOCK + 8).await?;
    let parent = v2::ParentUpdated {
        parent: ROOT_REGISTRY.parse()?,
        label: "eth".to_owned(),
        sender: OWNER.parse()?,
    };
    seed.log(ETH_REGISTRY, parent.encode_log_data()).await
}

/// Block 7. A reorg replaces it with a block of the same shape but another text value.
async fn seed_last_block(seed: &mut Seeder<'_>, hash: &str, text: &str) -> TestResult {
    let number = FIRST_BLOCK + 7;
    seed.block_with_hash(
        number,
        hash,
        &format!("0x{:064x}", keccak256(hash.as_bytes())),
    )
    .await?;
    let transferred = v2::TransferSingle {
        operator: OWNER.parse()?,
        from: OWNER.parse()?,
        to: SECOND_OWNER.parse()?,
        id: v2_token("bob"),
        value: U256::from(1_u64),
    };
    seed.log(ETH_REGISTRY, transferred.encode_log_data())
        .await?;
    seed.text("bob", text).await?;
    // `setSubregistry` stores whatever registry it is given, so a zero-address update clears the
    // pointer
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L147-L152 @ ens_v2_sepolia_20261001@07e55a05);
    // it is retained beside the token's latest ordinary subregistry event.
    let cleared = v2::SubregistryUpdated {
        tokenId: v2_token_version("alice", 1),
        subregistry: Address::ZERO,
        sender: OWNER.parse()?,
    };
    seed.log(ETH_REGISTRY, cleared.encode_log_data()).await
}

fn name(label: &str) -> String {
    format!("ens:{:#x}", child(eth_node(), label))
}

/// Every batch length interprets each batch identically through both loaders, the walk
/// checks batch by batch, and stores exactly what the forced full-state loader stores.
#[tokio::test]
async fn ensv2_lookahead_matches_full_state_for_every_batch() -> TestResult {
    let mut grids = Vec::new();
    for (blocks_per_batch, force_full_state) in
        [(1, false), (2, false), (3, false), (500, false), (3, true)]
    {
        let database = database_with_manifests("interpret_lookahead_ensv2", "sepolia").await?;
        seed_history(database.pool(), &OFFSETS).await?;
        stamp_interpreter_hash(database.pool()).await?;
        super::RETRIES.set(0);
        super::WHOLE_REGISTRY_BATCHES.take();
        let walk = walk_seeded(
            database.pool(),
            CHAIN,
            &OFFSETS,
            blocks_per_batch,
            force_full_state,
        )
        .await?;
        if blocks_per_batch == 500 {
            assert_sql_files_v2_events_like_the_adapter(database.pool()).await?;
        }
        database.cleanup().await?;
        // The walk runs the lookahead loader beside the engine's choice. This history reads
        // names no log or stored event mentions, so it must exercise the retry.
        // Only a batch whose registry's suffix walk changes reads the whole registry: alice's
        // subregistry update and its clear move the migration registry's suffix, and the first
        // batch has no earlier refresh to compare with. The migration registry's claim at block
        // 2 and the anchored ETH registry's second ParentUpdated rename nothing.
        let span = i64::from(blocks_per_batch);
        let batch_of = |block: i64| FIRST_BLOCK + (block - FIRST_BLOCK) / span * span;
        let migration = format!("{MIGRATION_REGISTRY}:*");
        assert_eq!(
            super::WHOLE_REGISTRY_BATCHES.take(),
            BTreeSet::from([
                (FIRST_BLOCK, format!("{ETH_REGISTRY}:*")),
                (batch_of(FIRST_BLOCK + 6), migration.clone()),
                (batch_of(FIRST_BLOCK + 7), migration),
            ]),
            "batches that loaded a whole ENSv2 registry at {blocks_per_batch} blocks per batch"
        );
        assert!(
            super::RETRIES.get() > 0,
            "no lookahead attempt was retried for {blocks_per_batch} blocks per batch"
        );
        assert!(
            walk.released.contains(&name("carol")),
            "carol's lapse is released: {:?}",
            walk.released
        );
        grids.push((blocks_per_batch, walk.stored));
    }
    let (_, full_state) = grids.pop().expect("forced full-state run");
    let rows: Vec<serde_json::Value> = full_state
        .iter()
        .map(|row| serde_json::from_str(row))
        .collect::<Result<_, _>>()?;
    let has = |family: &str, kind: &str| {
        rows.iter()
            .any(|row| row["source_family"] == family && row["event_kind"] == kind)
    };
    for (family, kind) in [
        ("ens_v1_registrar_l1", "RegistrationGranted"),
        ("ens_v1_registrar_l1", "RegistrationReleased"),
        ("ens_v2_registry_l1", "RegistrationGranted"),
        ("ens_v2_registry_l1", "ExpiryChanged"),
        ("ens_v2_registry_l1", "ResolverChanged"),
        ("ens_v2_registry_l1", "SubregistryChanged"),
        ("ens_v2_registry_l1", "TokenRegenerated"),
        ("ens_v2_registry_l1", "RegistrationReleased"),
        (
            "ens_v2_registry_l1",
            bigname_adapters::schema_v2::seam::TOKEN_CONTROL_TRANSFERRED_EVENT_KIND,
        ),
        ("ens_v2_resolver_l1", "RecordChanged"),
        ("ens_v2_migration_l1", "MigrationApplied"),
    ] {
        assert!(
            has(family, kind),
            "the history must exercise {family} {kind}"
        );
    }
    assert!(
        rows.iter().any(|row| row["block_number"] == FIRST_BLOCK + 3
            && row["event_kind"] == "RegistrationReleased"
            && row["after_state"]["source_event"] == "LabelUnregistered"),
        "erin's unregister releases her registration"
    );
    let sub = format!("ens:{:#x}", child(child(eth_node(), "alice"), "sub"));
    let renamed = |block: i64, kind: &str| {
        rows.iter().any(|row| {
            row["block_number"] == block
                && row["event_kind"] == kind
                && row["logical_name_id"] == sub.as_str()
        })
    };
    assert!(
        renamed(FIRST_BLOCK + 6, "RegistrationGranted")
            && renamed(FIRST_BLOCK + 7, "RegistrationReleased"),
        "the migration registry's suffix moves name sub.alice.eth and take it away"
    );
    for (blocks_per_batch, stored) in grids {
        assert_eq!(
            stored, full_state,
            "stored events differ for {blocks_per_batch} blocks per batch"
        );
    }
    Ok(())
}

/// The loader finds ENSv2 events by the keys `v2_keys.sql` files them under, while the adapter's
/// loaded-keys check and its scoped-restore tests use `v2_event_keys`: both must file every
/// stored event under the same keys.
async fn assert_sql_files_v2_events_like_the_adapter(pool: &PgPool) -> TestResult {
    use bigname_adapters::schema_v2::{PriorEventInput, seam::STATE_SCOPE_KEY, v2_event_keys};
    let keys = super::super::lookahead_query::V2_KEYS
        .trim_end()
        .replace("{state_scope}", STATE_SCOPE_KEY);
    let rows: Vec<(String, Option<String>, serde_json::Value, Vec<String>)> =
        sqlx::query_as(&format!(
            "SELECT event.source_family, event.raw_fact_ref ->> '{STATE_SCOPE_KEY}',
                    event.after_state, {keys}
             FROM normalized_events event WHERE event.source_family LIKE 'ens\\_v2\\_%'"
        ))
        .fetch_all(pool)
        .await?;
    assert!(rows.len() > 10, "only {} ENSv2 events stored", rows.len());
    for (source_family, state_scope, after_state, filed) in rows {
        let input = PriorEventInput {
            retained_state_key: String::new(),
            chain_id: CHAIN.to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: None,
            resource_id: None,
            event_kind: String::new(),
            source_family,
            manifest_version: 1,
            source_manifest_id: None,
            emitting_address: None,
            state_scope,
            block_timestamp: None,
            write_position: None,
            after_state,
        };
        let filed = filed.into_iter().collect::<BTreeSet<_>>();
        let expected = v2_event_keys(&input).into_iter().collect::<BTreeSet<_>>();
        assert_eq!(filed, expected, "{input:?}");
    }
    Ok(())
}

async fn run(
    engine: &Engine,
    current: Option<Marker>,
    to: i64,
    mode: RunMode,
) -> TestResult<Marker> {
    let from = if matches!(mode, RunMode::Redo) {
        to
    } else {
        FIRST_BLOCK
    };
    Ok(engine
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            from_block: from,
            to_block: to,
            resume_current: current,
            mode,
        })
        .await?
        .current)
}

/// Interprets blocks 0 to 7 three per batch on one engine, orphans block 7 and redoes its
/// replacement, then follows a new block 8 on a second engine, standing in for a restart.
/// Returns the stored events and the loader each engine chose.
async fn reorg_and_restart(force_full_state: bool) -> TestResult<(Vec<String>, Vec<StateLoader>)> {
    let database = database_with_manifests("interpret_lookahead_ensv2_reorg", "sepolia").await?;
    let pool = database.pool();
    stamp_interpreter_hash(pool).await?;
    seed_history(pool, &OFFSETS[..8]).await?;
    let engine = || {
        Engine::new(pool.clone())
            .with_blocks_per_batch(NonZeroU32::new(3).expect("positive batch"))
            .with_full_state_loader_forced(force_full_state)
    };
    let first = engine();
    let orphaned = FIRST_BLOCK + 7;
    let mut current = None;
    while current
        .as_ref()
        .is_none_or(|marker: &Marker| marker.number < orphaned)
    {
        current = Some(run(&first, current, orphaned, RunMode::Normal).await?);
    }

    let replacement = format!("0x{:064x}", orphaned + 1_000_000);
    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND block_number = $2",
    )
    .bind(CHAIN)
    .bind(orphaned)
    .execute(pool)
    .await?;
    for (number, hash, parent) in [
        (orphaned, replacement.clone(), block_hash(orphaned - 1)),
        (orphaned + 1, block_hash(orphaned + 1), replacement.clone()),
    ] {
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, $4, to_timestamp($5), 'canonical')",
        )
        .bind(CHAIN)
        .bind(&hash)
        .bind(&parent)
        .bind(number)
        .bind(START + OFFSETS[usize::try_from(number - FIRST_BLOCK)?])
        .execute(pool)
        .await?;
    }
    let mut seed = seeder(pool);
    seed_last_block(&mut seed, &replacement, "0x0b").await?;
    seed_reparent(&mut seed).await?;

    run(&first, None, orphaned, RunMode::Redo).await?;
    let redone = Marker {
        number: orphaned,
        hash: replacement,
    };
    let second = engine();
    let current = run(&second, Some(redone), orphaned + 1, RunMode::Normal).await?;
    assert_eq!(current.number, orphaned + 1);
    let choices = vec![
        first.chosen_loader(CHAIN)?.expect("first engine chose"),
        second.chosen_loader(CHAIN)?.expect("second engine chose"),
    ];
    let stored = stored_events(pool, CHAIN).await?;
    database.cleanup().await?;
    Ok((stored, choices))
}

/// A reorg costs the lookahead loader one redo batch read from the database, and a restart
/// costs nothing; both must store exactly what the full-state loader stores, which restores
/// the chain's history again after each.
#[tokio::test]
async fn ensv2_reorg_and_restart_match_full_state() -> TestResult {
    let (lookahead, choices) = reorg_and_restart(false).await?;
    assert_eq!(
        choices,
        vec![StateLoader::Lookahead, StateLoader::Lookahead]
    );
    let (full_state, _) = reorg_and_restart(true).await?;
    assert_eq!(lookahead, full_state);
    let texts: BTreeSet<_> = lookahead
        .iter()
        .filter(|row| row.contains("\"url\"") && row.contains(&name("bob")[4..]))
        .collect();
    assert!(
        !texts.is_empty() && texts.iter().all(|row| row.contains("0x0b")),
        "only the replacement block's text record is stored for bob: {texts:?}"
    );
    Ok(())
}
