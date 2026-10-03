//! Database tests that the lookahead loader and the full-state loader produce identical
//! output on Base, whose manifests are all Basenames Base families: batch by batch over a
//! history that crosses registration expiries, across a one-block reorg, and across a restart.
use std::{collections::BTreeSet, num::NonZeroU32};

use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::SolEvent;
use sqlx::PgPool;

use super::equivalence_tests::{
    FIRST_BLOCK, GRACE, OWNER, SECOND_OWNER, START, Seeder, block_hash, child, database,
    seed_lineage, stored_events, token, walk_seeded,
};
use crate::{BatchRequest, Engine, Marker, RunMode, StateLoader};

type TestResult<T = ()> = anyhow::Result<T>;

pub(super) const CHAIN: &str = "base-mainnet";
const REGISTRY: &str = "0xb94704422c2a1e396835a571837aa5ae53285a95";
const REGISTRAR: &str = "0x03c4738ee98ae44591e1a4a4f3cab6641d95dd9a";
const CONTROLLER: &str = "0x4ccb0bb02fcaba27e82a56646e81d8c5bc4119a5";
const UPGRADEABLE_CONTROLLER: &str = "0xa7d2607c6bd39ae9521e514026cbb078405ab322";
const RESOLVER: &str = "0xc6d566a56a1aff6508b41f6c90ff131615583bcd";
const REVERSE_REGISTRAR: &str = "0x0000000000d8e504002cc26e3ec46d81971c1664";

mod registry {
    alloy_sol_types::sol! {
        event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
        event NewResolver(bytes32 indexed node, address resolver);
    }
}

mod registrar {
    alloy_sol_types::sol! {
        event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
        event Approval(address indexed owner, address indexed approved, uint256 indexed tokenId);
        event ApprovalForAll(address indexed owner, address indexed operator, bool approved);
    }
}

mod controller {
    alloy_sol_types::sol! {
        event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires);
        event NameRenewed(string name, bytes32 indexed label, uint256 expires);
        event Upgraded(address indexed implementation);
    }
}

mod resolver {
    alloy_sol_types::sol! {
        event AddrChanged(bytes32 indexed node, address a);
        event NameChanged(bytes32 indexed node, string name);
        event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value);
        event NameForAddrChanged(address indexed addr, string name);
    }
}

/// Seconds after `START` at which each block is mined. Expiry plus grace falls strictly
/// between two blocks for alice and exactly on one for carol.
pub(super) const OFFSETS: [i64; 10] = [
    0,   // 0: alice, bob and carol registered
    10,  // 1: bob renewed far ahead; carol's token transferred and approved
    500, // 2: alice gets a subname, a resolver, an address and primary names;
    //                      the upgradeable controller is upgraded
    1_000 + GRACE - 5,   // 3: quiet; nothing has lapsed
    1_000 + GRACE + 1,   // 4: quiet; alice (expiry 1000) lapses
    1_500 + GRACE,       // 5: quiet; carol (expiry 1500) is still live at exact equality
    1_500 + GRACE + 1,   // 6: quiet; carol lapses
    1_500 + GRACE + 100, // 7: alice registered again by a new owner
    1_500 + GRACE + 200, // 8: alice's subname gets a resolver; bob transferred; a text record
    1_500 + GRACE + 300, // 9: quiet
];

fn base_eth() -> B256 {
    let eth = keccak256([B256::ZERO.as_slice(), keccak256(b"eth").as_slice()].concat());
    child(eth, "base")
}

fn name(label: &str) -> String {
    format!("basenames:{:#x}", child(base_eth(), label))
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
    /// The token mint, registry `NewOwner` and controller `NameRegistered` of one Basenames
    /// controller registration, in the order the contracts emit them
    /// (upstream: .refs/basenames/src/L2/RegistrarController.sol:L536 @ basenames@1809bbc)
    /// (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L272-L274 @ basenames@1809bbc)
    /// (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L445 @ basenames@1809bbc)
    /// (upstream: .refs/basenames/src/L2/Registry.sol:L90 @ basenames@1809bbc)
    /// (upstream: .refs/basenames/src/L2/Registry.sol:L122 @ basenames@1809bbc)
    /// (upstream: .refs/basenames/src/L2/RegistrarController.sol:L548 @ basenames@1809bbc).
    /// The registry's resolver and TTL logs and the registrar's `NameRegisteredWithRecord`
    /// between them are left out.
    async fn register_basename(&mut self, label: &str, owner: &str, expires: i64) -> TestResult {
        let owner: Address = owner.parse()?;
        let minted = registrar::Transfer {
            from: Address::ZERO,
            to: owner,
            tokenId: token(label),
        };
        self.log(REGISTRAR, minted.encode_log_data()).await?;
        let subnode = registry::NewOwner {
            node: base_eth(),
            label: keccak256(label.as_bytes()),
            owner,
        };
        self.log(REGISTRY, subnode.encode_log_data()).await?;
        let registered = controller::NameRegistered {
            name: label.to_owned(),
            label: keccak256(label.as_bytes()),
            owner,
            expires: U256::from(expires),
        };
        self.log(CONTROLLER, registered.encode_log_data()).await
    }
}

/// The raw logs of the whole history, and canonical lineage for the blocks `lineage` times.
pub(super) async fn seed_history(pool: &PgPool, lineage: &[i64]) -> TestResult {
    seed_lineage(pool, CHAIN, lineage).await?;
    let owner: Address = OWNER.parse()?;
    let second_owner: Address = SECOND_OWNER.parse()?;
    let alice = child(base_eth(), "alice");
    let mut seed = seeder(pool);

    seed.block(FIRST_BLOCK).await?;
    seed.register_basename("alice", OWNER, START + 1_000)
        .await?;
    seed.register_basename("bob", OWNER, START + 2_000).await?;
    seed.register_basename("carol", OWNER, START + 1_500)
        .await?;

    seed.block(FIRST_BLOCK + 1).await?;
    let renewed = controller::NameRenewed {
        name: "bob".to_owned(),
        label: keccak256(b"bob"),
        expires: U256::from(START + GRACE + 50_000),
    };
    seed.log(CONTROLLER, renewed.encode_log_data()).await?;
    let carol_transfer = registrar::Transfer {
        from: owner,
        to: second_owner,
        tokenId: token("carol"),
    };
    seed.log(REGISTRAR, carol_transfer.encode_log_data())
        .await?;
    let approval = registrar::Approval {
        owner: second_owner,
        approved: owner,
        tokenId: token("carol"),
    };
    seed.log(REGISTRAR, approval.encode_log_data()).await?;
    let operator = registrar::ApprovalForAll {
        owner: second_owner,
        operator: owner,
        approved: true,
    };
    seed.log(REGISTRAR, operator.encode_log_data()).await?;

    seed.block(FIRST_BLOCK + 2).await?;
    let subname = registry::NewOwner {
        node: alice,
        label: keccak256(b"sub"),
        owner,
    };
    seed.log(REGISTRY, subname.encode_log_data()).await?;
    let alice_resolver = registry::NewResolver {
        node: alice,
        resolver: RESOLVER.parse()?,
    };
    seed.log(REGISTRY, alice_resolver.encode_log_data()).await?;
    let address = resolver::AddrChanged {
        node: alice,
        a: owner,
    };
    seed.log(RESOLVER, address.encode_log_data()).await?;
    let primary = resolver::NameForAddrChanged {
        addr: owner,
        name: "alice.base.eth".to_owned(),
    };
    seed.log(REVERSE_REGISTRAR, primary.encode_log_data())
        .await?;
    let reverse = child(
        child(child(B256::ZERO, "reverse"), "80002105"),
        &alloy_primitives::hex::encode(owner),
    );
    let reverse_name = resolver::NameChanged {
        node: reverse,
        name: "alice.base.eth".to_owned(),
    };
    seed.log(RESOLVER, reverse_name.encode_log_data()).await?;
    let upgraded = controller::Upgraded {
        implementation: SECOND_OWNER.parse()?,
    };
    seed.log(UPGRADEABLE_CONTROLLER, upgraded.encode_log_data())
        .await?;

    seed.block(FIRST_BLOCK + 7).await?;
    seed.register_basename("alice", SECOND_OWNER, START + 3 * GRACE)
        .await?;

    seed_last_block(&mut seed, &block_hash(FIRST_BLOCK + 8), "0x08").await
}

/// Block 8. A reorg replaces it with a block of the same shape but another text value.
async fn seed_last_block(seed: &mut Seeder<'_>, hash: &str, text: &str) -> TestResult {
    let number = FIRST_BLOCK + 8;
    seed.block_with_hash(
        number,
        hash,
        &format!("0x{:064x}", keccak256(hash.as_bytes())),
    )
    .await?;
    let alice = child(base_eth(), "alice");
    let sub_resolver = registry::NewResolver {
        node: child(alice, "sub"),
        resolver: RESOLVER.parse()?,
    };
    seed.log(REGISTRY, sub_resolver.encode_log_data()).await?;
    let bob_transfer = registrar::Transfer {
        from: OWNER.parse()?,
        to: SECOND_OWNER.parse()?,
        tokenId: token("bob"),
    };
    seed.log(REGISTRAR, bob_transfer.encode_log_data()).await?;
    let record = resolver::TextChanged {
        node: alice,
        indexedKey: keccak256(b"url"),
        key: "url".to_owned(),
        value: text.to_owned(),
    };
    seed.log(RESOLVER, record.encode_log_data()).await
}

fn lapsed() -> BTreeSet<String> {
    BTreeSet::from([name("alice"), name("carol")])
}

/// Every batch length interprets each batch identically through both loaders, the walk
/// checks batch by batch, and stores exactly what the forced full-state loader stores.
#[tokio::test]
async fn basenames_lookahead_matches_full_state_for_every_batch() -> TestResult {
    let mut grids = Vec::new();
    for (blocks_per_batch, force_full_state) in
        [(1, false), (2, false), (3, false), (500, false), (3, true)]
    {
        let database = database("interpret_lookahead_basenames").await?;
        seed_history(database.pool(), &OFFSETS).await?;
        let walk = walk_seeded(
            database.pool(),
            CHAIN,
            &OFFSETS,
            blocks_per_batch,
            force_full_state,
        )
        .await?;
        database.cleanup().await?;
        assert_eq!(walk.released, lapsed());
        if blocks_per_batch == 1 {
            assert_eq!(walk.quiet_releases, 2, "both lapses are found as due names");
        }
        grids.push((blocks_per_batch, walk.stored));
    }
    let (_, full_state) = grids.pop().expect("forced full-state run");
    let rows: Vec<serde_json::Value> = full_state
        .iter()
        .map(|row| serde_json::from_str(row))
        .collect::<Result<_, _>>()?;
    let has = |kind: &str| rows.iter().any(|row| row["event_kind"] == kind);
    for kind in [
        "RegistrationGranted",
        "RegistrationRenewed",
        "RegistrationReleased",
        bigname_adapters::schema_v2::seam::TOKEN_CONTROL_TRANSFERRED_EVENT_KIND,
        "ResolverChanged",
        "RecordChanged",
        "ReverseChanged",
        "Upgraded",
    ] {
        assert!(has(kind), "the history must exercise {kind}");
    }
    for (blocks_per_batch, stored) in grids {
        assert_eq!(
            stored, full_state,
            "stored events differ for {blocks_per_batch} blocks per batch"
        );
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

/// Interprets blocks 0 to 8 three per batch on one engine, orphans block 8 and redoes its
/// replacement, then follows a new block 9 on a second engine, standing in for a restart.
/// Returns the stored events and the loader each engine chose.
async fn reorg_and_restart(force_full_state: bool) -> TestResult<(Vec<String>, Vec<StateLoader>)> {
    let database = database("interpret_lookahead_basenames_reorg").await?;
    let pool = database.pool();
    seed_history(pool, &OFFSETS[..9]).await?;
    let engine = || {
        Engine::new(pool.clone())
            .with_blocks_per_batch(NonZeroU32::new(3).expect("positive batch"))
            .with_full_state_loader_forced(force_full_state)
    };
    let first = engine();
    let orphaned = FIRST_BLOCK + 8;
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
    seed_last_block(&mut seeder(pool), &replacement, "0x0b").await?;

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
async fn basenames_reorg_and_restart_match_full_state() -> TestResult {
    let (lookahead, choices) = reorg_and_restart(false).await?;
    assert_eq!(
        choices,
        vec![StateLoader::Lookahead, StateLoader::Lookahead]
    );
    let (full_state, _) = reorg_and_restart(true).await?;
    assert_eq!(lookahead, full_state);
    let texts: Vec<_> = lookahead
        .iter()
        .filter(|row| row.contains("\"url\""))
        .collect();
    assert!(
        !texts.is_empty() && texts.iter().all(|row| row.contains("0x0b")),
        "only the replacement block's text record is stored: {texts:?}"
    );
    Ok(())
}
