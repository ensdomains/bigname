//! Database tests that the lookahead loader and the full-state loader produce identical
//! output, batch by batch, over a history that crosses registration expiries.
use std::{collections::BTreeSet, num::NonZeroU32};

use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use bigname_adapters::{
    SchemaV2BatchOutput, StateCacheCapacity, prepare_schema_v2_batch_incremental_with_provenance,
    schema_v2::prepare_schema_v2_batch_lookahead,
};
use bigname_manifests::{load_repository, sync_schema_v2_repository};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::PgPool;

use crate::{BatchRequest, Engine, FullStateReason, Marker, RunMode, StateLoader, load};

type TestResult<T = ()> = anyhow::Result<T>;

const CHAIN: &str = "ethereum-mainnet";
const FIRST_BLOCK: i64 = 17_000_000;
const START: i64 = 1_700_000_000;
const GRACE: i64 = 90 * 24 * 60 * 60;
const ENS_REGISTRY: &str = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e";
const BASE_REGISTRAR: &str = "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85";
const LEGACY_CONTROLLER: &str = "0x283af0b28c62c092c9727f1ee09c02ca627eb7f5";
const OWNER: &str = "0x0000000000000000000000000000000000000051";
const SECOND_OWNER: &str = "0x0000000000000000000000000000000000000052";
const RESOLVER: &str = "0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41";
const CAPACITY: StateCacheCapacity = StateCacheCapacity::Entries(65_536);
/// An ENSv2 family lookahead does not cover, under which the drift tests retain history.
const UNCOVERED_FAMILY: &str = "ens_v2_registry_l1";

mod registry {
    alloy_sol_types::sol! {
        event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
        event NewResolver(bytes32 indexed node, address resolver);
    }
}

mod registrar {
    alloy_sol_types::sol! {
        event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
        event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
        event NameRenewed(uint256 indexed id, uint256 expires);
    }
}

sol! {
    event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 cost, uint256 expires);
    event NameRenewed(string name, bytes32 indexed label, uint256 cost, uint256 expires);
}

/// Seconds after `START` at which each block of the history is mined. The gaps put each
/// expiry-plus-grace instant strictly between two blocks, and the batch lengths the tests use
/// put those blocks first in a batch, last in a batch, and in the middle of one.
const BLOCK_OFFSETS: [i64; 12] = [
    0,                   // 0: alice, bob, carol and erin registered
    10,                  // 1: bob renewed far ahead; carol's token transferred; erin named
    500,                 // 2: alice gets a subname and a resolver
    1_000 + GRACE - 5,   // 3: quiet; nothing has lapsed
    1_000 + GRACE + 1,   // 4: quiet; alice (expiry 1000) lapses
    1_500 + GRACE,       // 5: quiet; carol (expiry 1500) is still live at exact equality
    1_500 + GRACE + 1,   // 6: quiet; carol lapses
    1_700 + GRACE + 1,   // 7: quiet; erin (expiry 1700) lapses
    1_700 + GRACE + 100, // 8: alice registered again by a new owner
    1_700 + GRACE + 200, // 9: alice's old subname gets a resolver; bob transferred
    1_700 + GRACE + 300, // 10: quiet
    1_700 + GRACE + 400, // 11: quiet
];

/// The smallest history in which a registration is recorded with an expiry that lapsed,
/// grace period included, before its own block, followed by a block with no logs.
const LAPSED_AT_BIRTH_OFFSETS: [i64; 2] = [0, 12];

/// Three blocks, twelve seconds apart, for the exact-equality cases.
const EXPIRY_BOUNDARY_OFFSETS: [i64; 3] = [0, 12, 24];

#[derive(Clone, Copy)]
enum History {
    /// Registrations, renewals, transfers, a subname, lapses, and a re-registration.
    Lifecycle,
    /// One registration whose expiry had already lapsed when it was recorded.
    LapsedAtBirth,
    /// A chain of subnames twenty labels deep, then a resolver set on the deepest one.
    DeepSubname,
    /// Two registrations whose expiry plus grace period equals a block timestamp exactly.
    ExpiryBoundaries,
}

impl History {
    fn offsets(self) -> &'static [i64] {
        match self {
            Self::Lifecycle => &BLOCK_OFFSETS,
            Self::LapsedAtBirth | Self::DeepSubname => &LAPSED_AT_BIRTH_OFFSETS,
            Self::ExpiryBoundaries => &EXPIRY_BOUNDARY_OFFSETS,
        }
    }

    fn last_block(self) -> i64 {
        FIRST_BLOCK + self.offsets().len() as i64 - 1
    }
}

fn block_hash(number: i64) -> String {
    format!("0x{:064x}", number + 1)
}

fn transaction_hash(number: i64) -> String {
    format!("0x{:064x}", number + 10_000)
}

fn eth_node() -> B256 {
    keccak256([B256::ZERO.as_slice(), keccak256(b"eth").as_slice()].concat())
}

fn child(parent: B256, label: &str) -> B256 {
    keccak256([parent.as_slice(), keccak256(label.as_bytes()).as_slice()].concat())
}

fn name(label: &str) -> String {
    format!("ens:{:#x}", child(eth_node(), label))
}

fn token(label: &str) -> U256 {
    U256::from_be_bytes(keccak256(label.as_bytes()).0)
}

async fn database(prefix: &str) -> TestResult<TestDatabase> {
    let database = TestDatabase::create(TestDatabaseConfig::new(prefix)).await?;
    for statement in [
        include_str!("../../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../../schema-v2/baseline/05_normalized_events.sql"),
        include_str!("../../../../schema-v2/baseline/06_projections.sql"),
        include_str!("../../../../schema-v2/baseline/07_labels.sql"),
        include_str!("../../../../schema-v2/baseline/08_heartbeats.sql"),
        include_str!("../../../../schema-v2/baseline/09_divergence.sql"),
        include_str!("../../../../schema-v2/baseline/10_phase_state.sql"),
        include_str!("../../../../schema-v2/baseline/11_manifest_authority_attestations.sql"),
        include_str!("../../../../schema-v2/baseline/12_project_generation_failures.sql"),
        include_str!("../../../../schema-v2/baseline/13_interpret_decode_skips.sql"),
        include_str!("../../../../schema-v2/baseline/14_discovery_watch_admissions.sql"),
    ] {
        sqlx::raw_sql(statement).execute(database.pool()).await?;
    }
    let manifest_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/mainnet");
    sync_schema_v2_repository(database.pool(), &load_repository(manifest_root)?).await?;
    Ok(database)
}

struct Seeder<'a> {
    pool: &'a PgPool,
    block: i64,
    log_index: i64,
}

impl Seeder<'_> {
    async fn block(&mut self, number: i64) -> TestResult {
        self.block = number;
        self.log_index = 0;
        sqlx::query(
            "INSERT INTO raw_transactions (
                 chain_id, block_hash, block_number, transaction_hash,
                 transaction_index, from_address, to_address
             ) VALUES ($1, $2, $3, $4, 0, $5, $6)",
        )
        .bind(CHAIN)
        .bind(block_hash(number))
        .bind(number)
        .bind(transaction_hash(number))
        .bind(OWNER)
        .bind(LEGACY_CONTROLLER)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    async fn log(&mut self, emitter: &str, encoded: alloy_primitives::LogData) -> TestResult {
        sqlx::query(
            "INSERT INTO raw_logs (
                 chain_id, block_hash, block_number, transaction_hash,
                 transaction_index, log_index, emitting_address, topics, data
             ) VALUES ($1, $2, $3, $4, 0, $5, $6, $7, $8)",
        )
        .bind(CHAIN)
        .bind(block_hash(self.block))
        .bind(self.block)
        .bind(transaction_hash(self.block))
        .bind(self.log_index)
        .bind(emitter)
        .bind(
            encoded
                .topics()
                .iter()
                .map(|topic| format!("{topic:#x}"))
                .collect::<Vec<_>>(),
        )
        .bind(encoded.data.to_vec())
        .execute(self.pool)
        .await?;
        self.log_index += 1;
        Ok(())
    }

    /// The logs one controller registration emits, in the order the contracts emit them.
    async fn register(&mut self, label: &str, owner: &str, expires: i64) -> TestResult {
        let owner: Address = owner.parse()?;
        let registry_event = registry::NewOwner {
            node: eth_node(),
            label: keccak256(label.as_bytes()),
            owner,
        };
        self.log(ENS_REGISTRY, registry_event.encode_log_data())
            .await?;
        let transfer = registrar::Transfer {
            from: Address::ZERO,
            to: owner,
            tokenId: token(label),
        };
        self.log(BASE_REGISTRAR, transfer.encode_log_data()).await?;
        let registered = registrar::NameRegistered {
            id: token(label),
            owner,
            expires: U256::from(expires),
        };
        self.log(BASE_REGISTRAR, registered.encode_log_data())
            .await?;
        let named = NameRegistered {
            name: label.to_owned(),
            label: keccak256(label.as_bytes()),
            owner,
            cost: U256::from(1),
            expires: U256::from(expires),
        };
        self.log(LEGACY_CONTROLLER, named.encode_log_data()).await
    }
}

async fn seed_history(pool: &PgPool, history: History) -> TestResult {
    for (offset, seconds) in history.offsets().iter().enumerate() {
        let number = FIRST_BLOCK + i64::try_from(offset)?;
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, $4, to_timestamp($5), 'canonical')",
        )
        .bind(CHAIN)
        .bind(block_hash(number))
        .bind((offset > 0).then(|| block_hash(number - 1)))
        .bind(number)
        .bind(START + seconds)
        .execute(pool)
        .await?;
    }
    let owner: Address = OWNER.parse()?;
    let second_owner: Address = SECOND_OWNER.parse()?;
    let mut seed = Seeder {
        pool,
        block: 0,
        log_index: 0,
    };
    if matches!(history, History::DeepSubname) {
        seed.block(FIRST_BLOCK).await?;
        seed.register("deep", OWNER, START + 3 * GRACE).await?;
        let mut node = child(eth_node(), "deep");
        for depth in 0..20 {
            let label = format!("level{depth}");
            let subname = registry::NewOwner {
                node,
                label: keccak256(label.as_bytes()),
                owner,
            };
            seed.log(ENS_REGISTRY, subname.encode_log_data()).await?;
            node = child(node, &label);
        }
        seed.block(FIRST_BLOCK + 1).await?;
        let resolver = registry::NewResolver {
            node,
            resolver: RESOLVER.parse()?,
        };
        return seed.log(ENS_REGISTRY, resolver.encode_log_data()).await;
    }
    if matches!(history, History::ExpiryBoundaries) {
        seed.block(FIRST_BLOCK).await?;
        // Ivy's expiry plus grace equals the timestamp of her own block. With one block per
        // batch that block is the last of the previous batch, so she sits exactly on the
        // line between the two branches of the due-names query.
        seed.register("ivy", OWNER, START - GRACE).await?;
        // June's expiry plus grace equals the timestamp of the next block, the first block of
        // the next batch. A registration is still live at equality, so she is released one
        // block later.
        return seed.register("june", OWNER, START + 12 - GRACE).await;
    }
    if matches!(history, History::LapsedAtBirth) {
        // The deployed registrar cannot record this (its `register` sets the expiry to the
        // block time plus a positive duration), but the adapter accepts it.
        seed.block(FIRST_BLOCK).await?;
        return seed.register("gina", OWNER, START - GRACE - 10).await;
    }

    seed.block(FIRST_BLOCK).await?;
    seed.register("alice", OWNER, START + 1_000).await?;
    seed.register("bob", OWNER, START + 2_000).await?;
    seed.register("carol", OWNER, START + 1_500).await?;
    // Erin is registered on the registrar alone, so her label text is not yet known.
    let erin_transfer = registrar::Transfer {
        from: Address::ZERO,
        to: owner,
        tokenId: token("erin"),
    };
    seed.log(BASE_REGISTRAR, erin_transfer.encode_log_data())
        .await?;
    let erin_registered = registrar::NameRegistered {
        id: token("erin"),
        owner,
        expires: U256::from(START + 1_600),
    };
    seed.log(BASE_REGISTRAR, erin_registered.encode_log_data())
        .await?;

    seed.block(FIRST_BLOCK + 1).await?;
    let far = START + GRACE + 50_000;
    let bob_renewed = registrar::NameRenewed {
        id: token("bob"),
        expires: U256::from(far),
    };
    seed.log(BASE_REGISTRAR, bob_renewed.encode_log_data())
        .await?;
    let bob_named = NameRenewed {
        name: "bob".to_owned(),
        label: keccak256(b"bob"),
        cost: U256::from(1),
        expires: U256::from(far),
    };
    seed.log(LEGACY_CONTROLLER, bob_named.encode_log_data())
        .await?;
    let carol_transfer = registrar::Transfer {
        from: owner,
        to: second_owner,
        tokenId: token("carol"),
    };
    seed.log(BASE_REGISTRAR, carol_transfer.encode_log_data())
        .await?;
    // The controller renewal is the first log to carry erin's label text.
    let erin_renewed = registrar::NameRenewed {
        id: token("erin"),
        expires: U256::from(START + 1_700),
    };
    seed.log(BASE_REGISTRAR, erin_renewed.encode_log_data())
        .await?;
    let erin_named = NameRenewed {
        name: "erin".to_owned(),
        label: keccak256(b"erin"),
        cost: U256::from(1),
        expires: U256::from(START + 1_700),
    };
    seed.log(LEGACY_CONTROLLER, erin_named.encode_log_data())
        .await?;

    seed.block(FIRST_BLOCK + 2).await?;
    let alice = child(eth_node(), "alice");
    let subname = registry::NewOwner {
        node: alice,
        label: keccak256(b"sub"),
        owner,
    };
    seed.log(ENS_REGISTRY, subname.encode_log_data()).await?;
    let alice_resolver = registry::NewResolver {
        node: alice,
        resolver: RESOLVER.parse()?,
    };
    seed.log(ENS_REGISTRY, alice_resolver.encode_log_data())
        .await?;

    seed.block(FIRST_BLOCK + 8).await?;
    seed.register("alice", SECOND_OWNER, START + 3 * GRACE)
        .await?;

    seed.block(FIRST_BLOCK + 9).await?;
    let sub_resolver = registry::NewResolver {
        node: child(alice, "sub"),
        resolver: RESOLVER.parse()?,
    };
    seed.log(ENS_REGISTRY, sub_resolver.encode_log_data())
        .await?;
    let bob_transfer = registrar::Transfer {
        from: owner,
        to: second_owner,
        tokenId: token("bob"),
    };
    seed.log(BASE_REGISTRAR, bob_transfer.encode_log_data())
        .await?;
    Ok(())
}

async fn interpret(
    pool: &PgPool,
    from: i64,
    loaded: load::LoadedBatch,
) -> TestResult<(SchemaV2BatchOutput, load::CachedPrior)> {
    let session = loaded
        .adapter_session
        .expect("both loaders restore a session");
    let prepared = match &loaded.lookahead_nodes {
        Some(nodes) => prepare_schema_v2_batch_lookahead(
            loaded.input,
            loaded.provenance_manifests,
            session,
            nodes,
            CAPACITY,
        )?,
        None => prepare_schema_v2_batch_incremental_with_provenance(
            loaded.input,
            loaded.provenance_manifests,
            Some(session),
            CAPACITY,
        )?,
    };
    let values =
        load::prior_state_values(pool, CHAIN, from, prepared.state_value_requests()).await?;
    let (output, adapter_session) = prepared.finish(values)?;
    let cache = load::fold_prior_cache(loaded.prior_cache, &output.normalized_events);
    Ok((
        output,
        load::CachedPrior {
            cache,
            adapter_session,
        },
    ))
}

fn released(output: &SchemaV2BatchOutput) -> BTreeSet<String> {
    output
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "RegistrationReleased")
        .map(|event| {
            format!(
                "{}:{}",
                event.namespace,
                event.after_state["namehash"]
                    .as_str()
                    .expect("released namehash")
            )
        })
        .collect()
}

struct Walk {
    /// Every name a `RegistrationReleased` event was emitted for.
    released: BTreeSet<String>,
    /// Releases of names no log in their batch mentioned: only the due-names query loads them.
    quiet_releases: usize,
    /// Every stored normalized event, without its surrogate id and insertion time.
    stored: Vec<String>,
}

async fn stored_events(pool: &PgPool) -> TestResult<Vec<String>> {
    Ok(sqlx::query_scalar(
        "SELECT (to_jsonb(event) - 'normalized_event_id' - 'observed_at')::text
         FROM normalized_events event
         WHERE chain_id = $1
         ORDER BY block_number, transaction_index, log_index, event_identity",
    )
    .bind(CHAIN)
    .fetch_all(pool)
    .await?)
}

/// Walks the history in batches of `blocks_per_batch`. Before the engine publishes each
/// batch, both loaders interpret it from the same database state and their complete
/// `BatchOutput` values are compared; the names the full-state loader releases must be among
/// the due names the lookahead loader asks for.
async fn walk(history: History, blocks_per_batch: u32, force_full_state: bool) -> TestResult<Walk> {
    let database = database("interpret_lookahead_equivalence").await?;
    let pool = database.pool();
    seed_history(pool, history).await?;
    let engine = Engine::new(pool.clone())
        .with_blocks_per_batch(NonZeroU32::new(blocks_per_batch).expect("positive batch"))
        .with_full_state_loader_forced(force_full_state);
    let mut current: Option<Marker> = None;
    let mut carried: Option<load::CachedPrior> = None;
    let mut all_released = BTreeSet::new();
    let mut quiet_releases = 0;
    let mut from = FIRST_BLOCK;
    let last_block = history.last_block();
    while from <= last_block {
        let to = (from + i64::from(blocks_per_batch) - 1).min(last_block);
        let lookahead =
            match super::batch_input(pool, CHAIN, from, to, None, CAPACITY, None).await? {
                super::Attempt::Loaded(loaded) => *loaded,
                super::Attempt::FullStateRequired(choice) => {
                    anyhow::bail!("mainnet manifests must choose lookahead, got {choice:?}")
                }
            };
        // Names the batch's own logs mention are loaded whether or not they are due.
        let mentioned: BTreeSet<String> =
            bigname_adapters::schema_v2::collect_v1_batch_dependencies(
                &lookahead.input,
                &lookahead.provenance_manifests,
            )?
            .nodes
            .into_iter()
            .map(|request| format!("{}:{}", request.namespace, request.node))
            .collect();
        let (lookahead_output, _) = interpret(pool, from, lookahead).await?;
        let cold = load::batch_input(pool, CHAIN, from, to, None, None, CAPACITY).await?;
        let (cold_output, cold_session) = interpret(pool, from, cold).await?;
        assert_eq!(
            lookahead_output, cold_output,
            "lookahead differs from a cold full-state restore for blocks {from}..={to}"
        );
        if let Some(carried) = carried.take() {
            let carried =
                load::batch_input(pool, CHAIN, from, to, None, Some(carried), CAPACITY).await?;
            assert_eq!(
                carried.restored_event_count, 0,
                "the session must be reused"
            );
            let (carried_output, _) = interpret(pool, from, carried).await?;
            assert_eq!(
                lookahead_output, carried_output,
                "lookahead differs from the carried full-state session for blocks {from}..={to}"
            );
        }
        carried = Some(cold_session);

        let mut connection = pool.acquire().await?;
        let predecessor = load::resume::predecessor_timestamp(&mut connection, CHAIN, from).await?;
        let last = time::OffsetDateTime::from_unix_timestamp(
            START + history.offsets()[usize::try_from(to - FIRST_BLOCK)?],
        )?;
        let due: BTreeSet<String> =
            load::lookahead_query::due_names(&mut connection, CHAIN, from, predecessor, last)
                .await?
                .into_iter()
                .collect();
        drop(connection);
        let batch_released = released(&cold_output);
        let quiet_released: BTreeSet<_> = batch_released.difference(&mentioned).cloned().collect();
        assert!(
            quiet_released.is_subset(&due),
            "full-state released {quiet_released:?} in blocks {from}..={to} without a log \
             naming them, but the due names were {due:?}"
        );
        quiet_releases += quiet_released.len();
        all_released.extend(batch_released);

        let outcome = engine
            .run_batch(BatchRequest {
                chain_id: CHAIN.to_owned(),
                from_block: FIRST_BLOCK,
                to_block: last_block,
                resume_current: current.clone(),
                mode: RunMode::Normal,
            })
            .await?;
        assert_eq!(
            outcome.current.number, to,
            "the engine must use the configured batch length"
        );
        current = Some(outcome.current);
        from = to + 1;
    }
    let expected_choice = if force_full_state {
        "full-state"
    } else {
        "lookahead"
    };
    assert_eq!(
        engine
            .chosen_loader(CHAIN)?
            .map(|choice| choice.to_string()),
        Some(expected_choice.to_owned())
    );
    if !force_full_state {
        assert_eq!(engine.chosen_loader(CHAIN)?, Some(StateLoader::Lookahead));
    }
    let stored = stored_events(pool).await?;
    database.cleanup().await?;
    Ok(Walk {
        released: all_released,
        quiet_releases,
        stored,
    })
}

fn lapsed_names() -> BTreeSet<String> {
    BTreeSet::from([name("alice"), name("carol"), name("erin")])
}

/// Every name the full-state loader releases without a log naming it must be one the
/// due-names query returns: the walk asserts that for each batch. One-block batches make
/// every release such a quiet one, covering a plain grant (alice), a grant followed by a token
/// transfer (carol), a grant rebuilt from registrar evidence and then renewed (erin), and a
/// renewal that moves a name out of the window (bob, never released).
#[tokio::test]
async fn due_names_cover_every_full_state_release() -> TestResult {
    let walk = walk(History::Lifecycle, 1, false).await?;
    assert_eq!(walk.released, lapsed_names());
    assert_eq!(walk.quiet_releases, 3);
    Ok(())
}

/// Each stored `NewOwner` links a name to its parent, and the loader follows such links one
/// round at a time. A name many labels deep must still load, not stop Interpret.
#[tokio::test]
async fn deeply_nested_subname_loads_through_lookahead() -> TestResult {
    let walk = walk(History::DeepSubname, 1, false).await?;
    assert!(walk.released.is_empty());
    assert!(walk.stored.iter().any(|row| row.contains("NewResolver")));
    Ok(())
}

/// A registration is released at the first block whose timestamp is strictly greater than
/// its expiry plus the grace period. These two names put that instant exactly on a block
/// timestamp: the last block of the previous batch (ivy) and the first block of the batch
/// (june). Every batch length must release each of them in the same block as the full-state
/// loader, which the walk checks batch by batch.
#[tokio::test]
async fn expiry_exactly_on_a_block_timestamp_matches_full_state() -> TestResult {
    let mut stored = Vec::new();
    for blocks_per_batch in [1, 2, 3] {
        let walk = walk(History::ExpiryBoundaries, blocks_per_batch, false).await?;
        assert_eq!(walk.released, BTreeSet::from([name("ivy"), name("june")]));
        stored.push(walk.stored);
    }
    let full_state = walk(History::ExpiryBoundaries, 1, true).await?.stored;
    let released_at = |label: &str| {
        full_state
            .iter()
            .map(|row| serde_json::from_str::<serde_json::Value>(row).expect("stored row"))
            .find(|row| {
                row["event_kind"] == "RegistrationReleased"
                    && row["after_state"]["namehash"] == name(label)[4..]
            })
            .map(|row| row["block_number"].as_i64().expect("block number") - FIRST_BLOCK)
    };
    assert_eq!(released_at("ivy"), Some(1), "strictly after her own block");
    assert_eq!(
        released_at("june"),
        Some(2),
        "still live at exact equality in block 1"
    );
    for stored in stored {
        assert_eq!(stored, full_state);
    }
    Ok(())
}

/// A registration recorded with an expiry that had already lapsed is released by the
/// full-state loader at the next block boundary. When that block opens a new batch, the name
/// is in no expiry window, so the due-names query must find it by where its event sits.
#[tokio::test]
async fn registration_recorded_already_lapsed_is_released_by_both_loaders() -> TestResult {
    let walk = walk(History::LapsedAtBirth, 1, false).await?;
    assert_eq!(walk.released, BTreeSet::from([name("gina")]));
    assert_eq!(walk.quiet_releases, 1);
    Ok(())
}

#[tokio::test]
async fn lookahead_matches_full_state_for_every_batch() -> TestResult {
    let mut grids = Vec::new();
    // One block per batch puts every lapse first in its batch; two and three move the
    // lapses to the end and the middle; 500 is the default single batch.
    for blocks_per_batch in [1, 2, 3, 5, 500] {
        let walk = walk(History::Lifecycle, blocks_per_batch, false).await?;
        assert_eq!(
            walk.released,
            lapsed_names(),
            "the history must release exactly the three lapsed names"
        );
        grids.push((blocks_per_batch, walk.stored));
    }
    // What the engine stored through lookahead is identical for every batch length, and
    // identical to what the forced full-state loader stores.
    let full_state = walk(History::Lifecycle, 3, true).await?.stored;
    assert!(!full_state.is_empty());
    // The history exercises each kind of event the due-names query reads.
    let rows: Vec<serde_json::Value> = full_state
        .iter()
        .map(|row| serde_json::from_str(row))
        .collect::<Result<_, _>>()?;
    let has = |kind: &str, predicate: &dyn Fn(&serde_json::Value) -> bool| {
        rows.iter()
            .any(|row| row["event_kind"] == kind && predicate(&row["after_state"]))
    };
    assert!(has("RegistrationRenewed", &|state| state["namehash"]
        == name("bob")[4..]));
    assert!(has(
        bigname_adapters::schema_v2::seam::TOKEN_CONTROL_TRANSFERRED_EVENT_KIND,
        &|state| state["namehash"] == name("carol")[4..]
    ));
    // Erin's label first appears in a renewal, so her grant is rebuilt from registrar evidence.
    assert!(has("RegistrationGranted", &|state| {
        state["namehash"] == name("erin")[4..] && state.get("registrar_surface_evidence").is_some()
    }));
    for (blocks_per_batch, stored) in grids {
        assert_eq!(
            stored, full_state,
            "stored events differ for {blocks_per_batch} blocks per batch"
        );
    }
    Ok(())
}

async fn run_batch(
    engine: &Engine,
    current: Option<Marker>,
    last_block: i64,
) -> TestResult<Marker> {
    let outcome = engine
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            from_block: FIRST_BLOCK,
            to_block: last_block,
            resume_current: current,
            mode: RunMode::Normal,
        })
        .await?;
    Ok(outcome.current)
}

/// Adds a manifest of `source_family` to the chain in `rollout_status`, as a manifest file
/// moved to that state would be synced.
async fn add_manifest(pool: &PgPool, source_family: &str, rollout_status: &str) -> TestResult<i64> {
    Ok(sqlx::query_scalar(
        "INSERT INTO manifest_versions (
             manifest_version, namespace, source_family, chain_id, deployment_label,
             rollout_status, normalizer_version, file_path, manifest_payload
         ) VALUES (1, 'ens', $1, $2, 'test', $3, 'test', 'test/' || $1, '{}')
         RETURNING manifest_id",
    )
    .bind(source_family)
    .bind(CHAIN)
    .bind(rollout_status)
    .fetch_one(pool)
    .await?)
}

/// Retains one event of `UNCOVERED_FAMILY` at the chain's second block, attributed to a
/// manifest that was active when the event was written, and then moves that manifest to
/// `rollout_status`. The event carries no state scope, so restoring it changes no ENSv1
/// state: the two loaders differ only in whether they read it.
async fn retain_uncovered_family(pool: &PgPool, rollout_status: &str) -> TestResult {
    let manifest_id = add_manifest(pool, UNCOVERED_FAMILY, "active").await?;
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, event_kind, source_family, manifest_version,
             source_manifest_id, chain_id, block_number, block_hash, transaction_hash,
             transaction_index, log_index, raw_fact_ref, derivation_kind,
             canonicality_state, after_state
         ) VALUES ($1, 'ens', 'SubregistryChanged', $2, 1, $3, $4, $5, $6, $7, 0, 99, '{}',
                   'ens_v2_registry_resource_surface', 'canonical', '{}')",
    )
    .bind(format!("test:{UNCOVERED_FAMILY}:retained"))
    .bind(UNCOVERED_FAMILY)
    .bind(manifest_id)
    .bind(CHAIN)
    .bind(FIRST_BLOCK + 1)
    .bind(block_hash(FIRST_BLOCK + 1))
    .bind(transaction_hash(FIRST_BLOCK + 1))
    .execute(pool)
    .await?;
    sqlx::query("UPDATE manifest_versions SET rollout_status = $1 WHERE manifest_id = $2")
        .bind(rollout_status)
        .bind(manifest_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Interprets the Lifecycle history three blocks per batch. After the first batch an
/// uncovered manifest with no history joins the chain, which must not change the loader;
/// then history of `UNCOVERED_FAMILY` is retained under a manifest moved to
/// `rollout_status`, which must. The second engine stands in for a restart. Returns the
/// stored events and each engine's final loader choice.
async fn walk_with_retained_uncovered_family(
    rollout_status: &'static str,
    force_full_state: bool,
) -> TestResult<(Vec<String>, Vec<Option<StateLoader>>)> {
    let database = database("interpret_lookahead_retained_family").await?;
    let pool = database.pool();
    seed_history(pool, History::Lifecycle).await?;
    let last_block = History::Lifecycle.last_block();
    let engine = || {
        Engine::new(pool.clone())
            .with_blocks_per_batch(NonZeroU32::new(3).expect("positive batch"))
            .with_full_state_loader_forced(force_full_state)
    };
    let first = engine();
    let mut current = run_batch(&first, None, last_block).await?;
    let (from, to) = (current.number + 1, current.number + 3);
    add_manifest(pool, "ens_v2_root_l1", rollout_status).await?;
    if !force_full_state {
        match super::batch_input(pool, CHAIN, from, to, None, CAPACITY, None).await? {
            super::Attempt::Loaded(_) => {}
            super::Attempt::FullStateRequired(choice) => {
                anyhow::bail!(
                    "a {rollout_status} manifest with no retained history chose {choice:?}"
                )
            }
        }
    }
    retain_uncovered_family(pool, rollout_status).await?;
    if !force_full_state {
        match super::batch_input(pool, CHAIN, from, to, None, CAPACITY, None).await? {
            super::Attempt::Loaded(_) => anyhow::bail!(
                "lookahead was chosen although {UNCOVERED_FAMILY} history is retained under a \
                 {rollout_status} manifest"
            ),
            super::Attempt::FullStateRequired(choice) => assert_eq!(
                choice,
                StateLoader::FullState {
                    reason: FullStateReason::UnsupportedSourceFamily {
                        source_family: UNCOVERED_FAMILY.to_owned(),
                        rollout_status,
                    },
                }
            ),
        }
    }
    current = run_batch(&first, Some(current), last_block).await?;
    let second = engine();
    while current.number < last_block {
        current = run_batch(&second, Some(current), last_block).await?;
    }
    let choices = vec![first.chosen_loader(CHAIN)?, second.chosen_loader(CHAIN)?];
    let stored = stored_events(pool).await?;
    database.cleanup().await?;
    Ok((stored, choices))
}

/// History retained from a family whose manifest has since left the `active` and
/// `deprecated` states is restored by the full-state loader and read by no lookahead query.
/// Its presence must therefore choose the full-state loader, on the engine that saw the
/// change and on one started afterwards, and what is stored must equal a forced full-state
/// run. A manifest in such a state with no retained history changes nothing.
#[tokio::test]
async fn retained_history_of_an_uncovered_family_requires_full_state() -> TestResult {
    for rollout_status in ["draft", "shadow"] {
        let (stored, choices) = walk_with_retained_uncovered_family(rollout_status, false).await?;
        let full_state = StateLoader::FullState {
            reason: FullStateReason::UnsupportedSourceFamily {
                source_family: UNCOVERED_FAMILY.to_owned(),
                rollout_status,
            },
        };
        assert_eq!(
            choices,
            vec![Some(full_state.clone()), Some(full_state)],
            "{rollout_status}: both engines must end on the full-state loader"
        );
        let (forced, forced_choices) =
            walk_with_retained_uncovered_family(rollout_status, true).await?;
        assert!(forced_choices.iter().all(|choice| {
            matches!(
                choice,
                Some(StateLoader::FullState {
                    reason: FullStateReason::OperatorOverride
                })
            )
        }));
        assert_eq!(stored, forced, "{rollout_status}: stored events differ");
        assert!(stored.iter().any(|row| row.contains(UNCOVERED_FAMILY)));
    }
    Ok(())
}
