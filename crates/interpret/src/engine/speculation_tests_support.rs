use std::{collections::BTreeMap, num::NonZeroU32};

use alloy_primitives::{Address, B256, LogData, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use bigname_manifests::{load_repository, sync_schema_v2_repository};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder};

use crate::{BatchRequest, Engine, Marker, RunMode};

pub(super) type TestResult<T = ()> = anyhow::Result<T>;
pub(super) const CHAIN: &str = "ethereum-mainnet";
pub(super) const FIRST: i64 = 17_000_000;
pub(super) const START: i64 = 1_700_000_000;
pub(super) const GRACE: i64 = 90 * 24 * 60 * 60;
pub(super) const REGISTRY: &str = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e";
pub(super) const REGISTRAR: &str = "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85";
pub(super) const CONTROLLER: &str = "0x283af0b28c62c092c9727f1ee09c02ca627eb7f5";
pub(super) const RESOLVER: &str = "0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41";
pub(super) const DISCOVERED: &str = "0x0000000000000000000000000000000000000778";
pub(super) const OWNER: &str = "0x0000000000000000000000000000000000000051";

sol! {
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
    event NewResolver(bytes32 indexed node, address resolver);
    event NameChanged(bytes32 indexed node, string name);
    event AddrChanged(bytes32 indexed node, address a);
    event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value);
    event ApprovalForAll(address indexed owner, address indexed operator, bool approved);
}

mod registrar {
    alloy_sol_types::sol! {
        event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
        event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
    }
}

mod controller {
    alloy_sol_types::sol! {
        event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 cost, uint256 expires);
    }
}

pub(super) async fn database(prefix: &str, network: &str) -> TestResult<TestDatabase> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new(prefix).pool_max_connections(12)).await?;
    database.create_phase_schema().await?;
    for statement in [
        include_str!("../../../storage/schema/baseline/01_chain.sql"),
        include_str!("../../../storage/schema/baseline/02_raw_facts.sql"),
        include_str!("../../../storage/schema/baseline/03_identity.sql"),
        include_str!("../../../storage/schema/baseline/04_manifests.sql"),
        include_str!("../../../storage/schema/baseline/05_normalized_events.sql"),
        include_str!("../../../storage/schema/baseline/06_projections.sql"),
        include_str!("../../../storage/schema/baseline/07_labels.sql"),
        include_str!("../../../storage/schema/baseline/08_heartbeats.sql"),
        include_str!("../../../storage/schema/baseline/09_divergence.sql"),
        include_str!("../../../storage/schema/baseline/10_phase_state.sql"),
        include_str!("../../../storage/schema/baseline/11_manifest_authority_attestations.sql"),
        include_str!("../../../storage/schema/baseline/12_project_generation_failures.sql"),
        include_str!("../../../storage/schema/baseline/13_interpret_decode_skips.sql"),
        include_str!("../../../storage/schema/baseline/14_discovery_watch_admissions.sql"),
    ] {
        sqlx::raw_sql(statement).execute(database.pool()).await?;
    }
    let manifest_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../manifests")
        .join(network);
    sync_schema_v2_repository(database.pool(), &load_repository(manifest_root)?).await?;
    let pool = database.pool();
    pool.set_connect_options(pool.connect_options().as_ref().clone().options([
        ("search_path", "bigname_phase,public"),
        ("bigname.interpreter_content_hash", "speculation-test"),
    ]));
    let mut connections = Vec::new();
    for _ in 0..pool.options().get_max_connections() {
        let mut connection = pool.acquire().await?;
        sqlx::query(
            "SELECT set_config('bigname.interpreter_content_hash', 'speculation-test', false)",
        )
        .execute(&mut *connection)
        .await?;
        connections.push(connection);
    }
    drop(connections);
    Ok(database)
}

pub(super) fn block_hash(number: i64) -> String {
    format!("0x{:064x}", number + 1)
}

pub(super) fn transaction_hash(number: i64) -> String {
    format!("0x{:064x}", number + 10_000)
}

pub(super) fn eth_node() -> B256 {
    keccak256([B256::ZERO.as_slice(), keccak256(b"eth").as_slice()].concat())
}

pub(super) fn node(label: &str) -> B256 {
    keccak256(
        [
            eth_node().as_slice(),
            keccak256(label.as_bytes()).as_slice(),
        ]
        .concat(),
    )
}

pub(super) fn text(node: B256, value: &str) -> LogData {
    TextChanged {
        node,
        indexedKey: keccak256(b"description"),
        key: "description".to_owned(),
        value: value.to_owned(),
    }
    .encode_log_data()
}

pub(super) fn registration(label: &str, expiry: i64) -> TestResult<Vec<(&'static str, LogData)>> {
    let owner = OWNER.parse::<Address>()?;
    let labelhash = keccak256(label.as_bytes());
    let token = U256::from_be_bytes(labelhash.0);
    Ok(vec![
        (
            REGISTRY,
            NewOwner {
                node: eth_node(),
                label: labelhash,
                owner,
            }
            .encode_log_data(),
        ),
        (
            REGISTRAR,
            registrar::Transfer {
                from: Address::ZERO,
                to: owner,
                tokenId: token,
            }
            .encode_log_data(),
        ),
        (
            REGISTRAR,
            registrar::NameRegistered {
                id: token,
                owner,
                expires: U256::from(expiry),
            }
            .encode_log_data(),
        ),
        (
            CONTROLLER,
            controller::NameRegistered {
                name: label.to_owned(),
                label: labelhash,
                owner,
                cost: U256::from(1),
                expires: U256::from(expiry),
            }
            .encode_log_data(),
        ),
    ])
}

pub(super) async fn seed_block(
    pool: &PgPool,
    chain: &str,
    offset: i64,
    seconds: i64,
    logs: Vec<(&str, LogData)>,
) -> TestResult {
    let number = FIRST + offset;
    let hash = block_hash(number);
    seed_block_with_hash(pool, chain, offset, seconds, &hash, logs).await
}

pub(super) async fn seed_block_with_hash(
    pool: &PgPool,
    chain: &str,
    offset: i64,
    seconds: i64,
    hash: &str,
    logs: Vec<(&str, LogData)>,
) -> TestResult {
    let number = FIRST + offset;
    let transaction = transaction_hash(number);
    sqlx::query(
        "INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state)
         VALUES ($1, $2, $3, $4, to_timestamp($5), 'canonical')",
    )
    .bind(chain)
    .bind(hash)
    .bind((offset > 0).then(|| block_hash(number - 1)))
    .bind(number)
    .bind(START + seconds)
    .execute(pool)
    .await?;
    if logs.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "INSERT INTO raw_transactions (chain_id, block_hash, block_number,
             transaction_hash, transaction_index, from_address, to_address)
         VALUES ($1, $2, $3, $4, 0, $5, $6)",
    )
    .bind(chain)
    .bind(hash)
    .bind(number)
    .bind(&transaction)
    .bind(OWNER)
    .bind(CONTROLLER)
    .execute(pool)
    .await?;
    for (chunk_index, chunk) in logs.chunks(1_000).enumerate() {
        let mut query = QueryBuilder::<Postgres>::new(
            "INSERT INTO raw_logs (chain_id, block_hash, block_number, transaction_hash,
                 transaction_index, log_index, emitting_address, topics, data) ",
        );
        query.push_values(
            chunk.iter().enumerate(),
            |mut row, (index, (emitter, data))| {
                row.push_bind(chain)
                    .push_bind(hash)
                    .push_bind(number)
                    .push_bind(&transaction)
                    .push_bind(0_i64)
                    .push_bind((chunk_index * 1_000 + index) as i64)
                    .push_bind(*emitter)
                    .push_bind(
                        data.topics()
                            .iter()
                            .map(|topic| format!("{topic:#x}"))
                            .collect::<Vec<_>>(),
                    )
                    .push_bind(data.data.to_vec());
            },
        );
        query.build().execute(pool).await?;
    }
    Ok(())
}

pub(super) async fn seed_records(
    pool: &PgPool,
    blocks: i64,
    names: usize,
    hot: bool,
) -> TestResult {
    for offset in 0..blocks {
        let mut logs = Vec::with_capacity(names * 3);
        for index in 0..names {
            let identity = if hot {
                index
            } else {
                offset as usize * names + index
            };
            let label = format!("specimen{identity}");
            let node = node(&label);
            logs.push((
                RESOLVER,
                NameChanged {
                    node,
                    name: format!("{label}.eth"),
                }
                .encode_log_data(),
            ));
            logs.push((
                RESOLVER,
                AddrChanged {
                    node,
                    a: OWNER.parse()?,
                }
                .encode_log_data(),
            ));
            logs.push((
                RESOLVER,
                text(
                    node,
                    &format!("Revision {offset}: {}", "ens records ".repeat(16)),
                ),
            ));
        }
        seed_block(pool, CHAIN, offset, offset * 12, logs).await?;
    }
    Ok(())
}

pub(super) fn engine(pool: &PgPool, workers: u32, batch_blocks: u32) -> Engine {
    Engine::new(pool.clone())
        .with_blocks_per_batch(NonZeroU32::new(batch_blocks).unwrap())
        .with_speculative_workers(NonZeroU32::new(workers).unwrap())
}

pub(super) fn request(
    chain: &str,
    from: i64,
    to: i64,
    current: Option<Marker>,
    mode: RunMode,
) -> BatchRequest {
    BatchRequest {
        chain_id: chain.to_owned(),
        from_block: from,
        to_block: to,
        resume_current: current,
        mode,
    }
}

pub(super) async fn complete(
    engine: &Engine,
    chain: &str,
    from: i64,
    to: i64,
    mode: RunMode,
) -> TestResult {
    let mut current = None;
    loop {
        let outcome = engine
            .run_batch(request(chain, from, to, current, mode))
            .await?;
        if outcome.complete {
            return Ok(());
        }
        current = Some(outcome.current);
    }
}

pub(super) type Snapshot = BTreeMap<String, Vec<String>>;

/// Compare all semantic columns. Database-generated row ids and observation times differ
/// across fresh databases. Search postings are joined to their stable name identity.
/// Discovery retirement has a wall-clock timestamp: compare its presence and retain the
/// exact active-from/active-to block columns. Name deactivation timestamps remain exact.
pub(super) async fn snapshot(pool: &PgPool, chain: &str) -> TestResult<Snapshot> {
    let mut result = BTreeMap::new();
    for (table, removed) in [
        (
            "normalized_events",
            vec!["normalized_event_id", "observed_at"],
        ),
        ("name_surfaces", vec!["observed_at", "inserted_at"]),
        ("surface_bindings", vec!["observed_at", "inserted_at"]),
        ("resources", vec!["observed_at", "inserted_at"]),
        ("token_lineages", vec!["observed_at", "inserted_at"]),
        ("contract_instances", vec!["inserted_at"]),
        (
            "contract_instance_addresses",
            vec![
                "contract_instance_address_id",
                "admitted_at",
                "deactivated_at",
            ],
        ),
        (
            "discovery_edges",
            vec!["discovery_edge_id", "admitted_at", "deactivated_at"],
        ),
        ("discovery_watch_admissions", vec!["acknowledged_at"]),
        ("interpret_decode_skips", vec!["detected_at"]),
        ("migration_event_associations", vec!["observed_at"]),
        ("migration_discovery_associations", vec!["observed_at"]),
        ("migration_candidate_identity_effects", vec!["observed_at"]),
        ("migration_candidate_discovery_effects", vec!["observed_at"]),
        ("name_search_documents", vec!["search_id"]),
    ] {
        let retirement = if matches!(table, "contract_instance_addresses" | "discovery_edges") {
            " || jsonb_build_object('deactivated', row.deactivated_at IS NOT NULL)"
        } else {
            ""
        };
        let statement = format!(
            "SELECT ((to_jsonb(row) - $2::text[]){retirement})::text FROM {table} row WHERE chain_id = $1 ORDER BY 1"
        );
        let rows = sqlx::query_scalar(&statement)
            .bind(chain)
            .bind(removed)
            .fetch_all(pool)
            .await?;
        result.insert(table.to_owned(), rows);
    }
    result.insert("label_preimages".to_owned(), sqlx::query_scalar(
        "SELECT (to_jsonb(row) - 'observed_at' - 'inserted_at')::text FROM label_preimages row ORDER BY 1"
    ).fetch_all(pool).await?);
    result.insert("name_search_postings".to_owned(), sqlx::query_scalar(
        "SELECT ((to_jsonb(posting) - 'search_id') || jsonb_build_object('logical_name_id', document.logical_name_id))::text
         FROM name_search_postings posting JOIN name_search_documents document USING (search_id)
         WHERE document.chain_id = $1 ORDER BY 1"
    ).bind(chain).fetch_all(pool).await?);
    Ok(result)
}

pub(super) fn events(snapshot: &Snapshot) -> Vec<Value> {
    snapshot["normalized_events"]
        .iter()
        .map(|row| serde_json::from_str(row).unwrap())
        .collect()
}

pub(super) fn assert_snapshots(actual: &Snapshot, expected: &Snapshot) {
    assert_eq!(
        actual.keys().collect::<Vec<_>>(),
        expected.keys().collect::<Vec<_>>()
    );
    for (table, expected) in expected {
        assert_eq!(
            &actual[table], expected,
            "speculative and serial rows differ in {table}"
        );
    }
}
