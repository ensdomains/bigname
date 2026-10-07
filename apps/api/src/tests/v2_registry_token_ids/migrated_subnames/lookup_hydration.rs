//! Admitted legacy text writes are hydrated by Project Follow and served from its publication.
use super::*;
use alloy_primitives::Bytes;
use alloy_sol_types::SolValue;
use axum::{Json, Router, extract::State, routing::post};
use std::sync::{Arc, Mutex};

const LEGACY: &str = "0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41";
const CHAIN: &str = "ethereum-mainnet";
const CHILD: &str = "hydrate259.eth";
sol! {
    // This deployed ABI's TextChanged has no value; setText writes one for text() to return.
    // (upstream: .refs/ens_subgraph/subgraph.yaml:L109-L110 @ ens_subgraph@723f1b6)
    // (upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L71-L87 @ ens_app_v3@7175858)
    event TextChanged(bytes32 indexed node, string indexed indexedKey, string key);
    // (upstream: .refs/ens_v1/deployments/archive/ETHRegistrarController_mainnet_9380471.sol/ETHRegistrarController_mainnet_9380471.json:L35-L50 @ ens_v1@91c966f)
}
mod controller {
    use super::*;
    sol! { event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 cost, uint256 expires); }
}

#[derive(Clone, Default)]
struct RpcState {
    value: Arc<Mutex<Option<String>>>,
    calls: Arc<Mutex<Vec<(String, String)>>>,
}

async fn response(State(state): State<RpcState>, Json(request): Json<Value>) -> Json<Value> {
    let data = alloy_primitives::hex::decode(request["params"][0]["data"].as_str().unwrap())
        .expect("aggregate3 calldata");
    let (calls,) =
        <(Vec<(Address, bool, Bytes)>,)>::abi_decode_params(&data[4..]).expect("aggregate3 calls");
    let value = state.value.lock().unwrap().clone();
    let results: Vec<(bool, Bytes)> = calls
        .iter()
        .map(|(target, _, _)| {
            if format!("{target:#x}") == LEGACY {
                state.calls.lock().unwrap().push((
                    request["params"][1]["blockHash"]
                        .as_str()
                        .unwrap()
                        .to_owned(),
                    format!("{target:#x}"),
                ));
                value.as_ref().map_or((false, Bytes::new()), |value| {
                    (true, Bytes::from((value.clone(),).abi_encode_params()))
                })
            } else {
                // The block availability probe targets Multicall3 itself.
                (false, Bytes::new())
            }
        })
        .collect();
    Json(json!({"jsonrpc":"2.0","id":request["id"],
        "result":format!("0x{}",alloy_primitives::hex::encode((results,).abi_encode_params()))}))
}

async fn apply_hydrated(database: &TestDatabase, block: i64, endpoint: &str) -> Result<()> {
    use bigname_project::families::{self, FamilyMode, FamilyOptions};
    let token = families::input_token(&database.pool, CHAIN).await?;
    let options = FamilyOptions::new(bigname_content_hash::INTERPRETER_CONTENT_HASH)
        .with_hydration(bigname_lookup::ChainRpcUrls::from_entries(&[format!(
            "{CHAIN}={endpoint}"
        )])?);
    let outcome = families::apply(
        &database.pool,
        CHAIN,
        &bigname_project::Marker {
            number: BASE + block,
            hash: format!("0x{:064x}", BASE + block),
        },
        FamilyMode::Normal,
        &token,
        &options,
    )
    .await?;
    assert_eq!(
        outcome.marker.as_ref().map(|m| m.number),
        Some(BASE + block)
    );
    database.seed_snapshot_selector_chain_positions(&json!({CHAIN:{"chain_id":CHAIN,
        "block_number":BASE+block,"block_hash":format!("0x{:064x}",BASE+block),
        "timestamp":bigname_storage::UnixSeconds::from(timestamp(1_700_000_000+BASE+block)).internal_string()}})).await?;
    Ok(())
}

async fn text(database: &TestDatabase, block: i64) -> Result<()> {
    let logs = transaction(
        block,
        0,
        vec![(
            LEGACY.parse()?,
            TextChanged {
                node: bigname_lookup::ens_namehash_hex(CHILD)?.parse()?,
                indexedKey: keccak256("url"),
                key: "url".into(),
            }
            .encode_log_data(),
        )],
    );
    prepare(database, &logs, block).await
}

async fn prepare(database: &TestDatabase, logs: &[RawLogInput], block: i64) -> Result<()> {
    let number = BASE + block;
    let hash = format!("0x{number:064x}");
    upsert_phase_raw_blocks(
        &database.pool,
        &[raw_block(
            CHAIN,
            &hash,
            None,
            number,
            1_700_000_000 + number,
        )],
    )
    .await?;
    seed_schema_v2_lookup_head(
        &database.pool,
        CHAIN,
        number,
        &hash,
        &bigname_storage::UnixSeconds::from(timestamp(1_700_000_000 + number)).internal_string(),
    )
    .await?;
    for source in logs {
        let mut log = source.clone();
        log.chain_id = CHAIN.into();
        log.block_hash = hash.clone();
        sqlx::query("INSERT INTO raw_transactions(chain_id,block_hash,block_number,transaction_hash,transaction_index,from_address,to_address) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING")
            .bind(CHAIN).bind(&hash).bind(number).bind(&log.transaction_hash)
            .bind(log.transaction_index).bind(GRANTEE).bind(if block==122 {"0x283af0b28c62c092c9727f1ee09c02ca627eb7f5"} else {LEGACY}).execute(&database.pool).await?;
        sqlx::query("INSERT INTO raw_logs(chain_id,block_hash,block_number,transaction_hash,transaction_index,log_index,emitting_address,topics,data) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)")
            .bind(CHAIN).bind(&hash).bind(number).bind(&log.transaction_hash)
            .bind(log.transaction_index).bind(log.log_index).bind(&log.emitting_address)
            .bind(&log.topics).bind(&log.data).execute(&database.pool).await?;
    }
    bigname_interpret::Engine::new(database.pool.clone())
        .run_batch(bigname_interpret::BatchRequest {
            chain_id: CHAIN.into(),
            from_block: number,
            to_block: number,
            resume_current: None,
            mode: bigname_interpret::RunMode::Normal,
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn lookup_precomputation_publishes_successful_failed_and_undone_text_hydration() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    stamp(&database).await?;
    let repository = bigname_manifests::load_repository(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/mainnet"),
    )?;
    bigname_manifests::sync_schema_v2_repository(&database.pool, &repository).await?;
    let label = keccak256("hydrate259");
    let node = bigname_lookup::ens_namehash_hex(CHILD)?.parse()?;
    let owner = GRANTEE.parse()?;
    let registry = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".parse()?;
    let registrar = "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85".parse()?;
    let logs = transaction(
        122,
        0,
        vec![
            (
                registrar,
                NameRegistered {
                    id: U256::from_be_bytes(*label),
                    owner,
                    expires: U256::from(2_000_000_000u64),
                }
                .encode_log_data(),
            ),
            (
                registrar,
                Transfer {
                    from: Address::ZERO,
                    to: owner,
                    tokenId: U256::from_be_bytes(*label),
                }
                .encode_log_data(),
            ),
            (
                registry,
                NewOwner {
                    node: bigname_lookup::ens_namehash_hex("eth")?.parse()?,
                    label,
                    owner,
                }
                .encode_log_data(),
            ),
            (
                registry,
                NewResolver {
                    node,
                    resolver: LEGACY.parse()?,
                }
                .encode_log_data(),
            ),
            (
                LEGACY.parse()?,
                NameChanged {
                    node,
                    name: CHILD.into(),
                }
                .encode_log_data(),
            ),
            (
                "0x283af0b28c62c092c9727f1ee09c02ca627eb7f5".parse()?,
                controller::NameRegistered {
                    name: "hydrate259".into(),
                    label,
                    owner,
                    cost: U256::from(1),
                    expires: U256::from(2_000_000_000u64),
                }
                .encode_log_data(),
            ),
        ],
    );
    prepare(&database, &logs, 122).await?;
    publish_test_families_on(&database.pool, CHAIN, BASE + 122).await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}/", listener.local_addr()?);
    let state = RpcState::default();
    *state.value.lock().unwrap() = Some("https://hydrated.example/one".into());
    let app = Router::new()
        .route("/", post(response))
        .with_state(state.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    text(&database, 123).await?;
    // Interpret the admitted raw write, then let the actual Follow path prepare and publish
    // its hydration and lookup components together. No derived value is seeded.
    apply_hydrated(&database, 123, &endpoint).await?;
    let hydrated = parity(&database).await?;
    assert_eq!(
        hydrated["records"]["texts"]["url"],
        "https://hydrated.example/one"
    );
    assert_eq!(
        state.calls.lock().unwrap().as_slice(),
        &[(format!("0x{:064x}", BASE + 123), LEGACY.into())]
    );
    let hydrated_components = lookup_publication::components(&database).await?;
    let baseline:Value=sqlx::query_scalar("SELECT to_jsonb(v) FROM project_node_record_value v WHERE resolver_address=$1 AND record_key='text:url'")
        .bind(LEGACY).fetch_one(&database.pool).await?;
    assert!(baseline["value"].is_null());
    assert_eq!(baseline["hydrated_at_block"], BASE + 123);

    text(&database, 124).await?;
    *state.value.lock().unwrap() = None;
    let (result, work) =
        lookup_publication::observed(apply_hydrated(&database, 124, &endpoint)).await;
    result?;
    lookup_publication::assert_key_work(&work, 1, 1);
    let failed = parity(&database).await?;
    assert!(failed["records"]["texts"]["url"].is_null(), "{failed:#}");
    let failed_components = lookup_publication::components(&database).await?;
    assert_ne!(failed_components, hydrated_components);
    families_undo(&database, 123).await?;
    assert_eq!(
        lookup_publication::components(&database).await?,
        hydrated_components
    );
    // Exact component restoration is checked before replay updates the API snapshot metadata.
    apply_hydrated(&database, 124, &endpoint).await?;
    assert_eq!(
        lookup_publication::components(&database).await?,
        failed_components
    );
    let journal:Vec<String>=sqlx::query_scalar("SELECT family FROM project_family_undo WHERE chain_id=$1 AND block_number=$2 AND family LIKE 'project_lookup_%'")
        .bind(CHAIN).bind(BASE+124).fetch_all(&database.pool).await?;
    assert!(!journal.is_empty());
    assert!(
        journal
            .iter()
            .all(|family| family == "project_lookup_record")
    );
    server.abort();
    database.cleanup().await
}

async fn families_undo(database: &TestDatabase, offset: i64) -> Result<()> {
    assert_eq!(
        bigname_project::families::undo_to(&database.pool, CHAIN, BASE + offset).await?,
        1
    );
    Ok(())
}

async fn parity(database: &TestDatabase) -> Result<Value> {
    let detail = path_get(database, &format!("/v1/names/{CHILD}")).await?;
    let lookup = path_lookup(
        database,
        json!({"profile":"detail","inputs":[{"name":CHILD}]}),
    )
    .await?;
    assert_eq!(lookup["data"][0]["record"], detail["data"]);
    let id = format!("ens:{}", bigname_lookup::ens_namehash_hex(CHILD)?);
    let fresh = bigname_storage::families::name::load_family_name(&database.pool, &id)
        .await?
        .context("mainnet name")?;
    let resource = fresh
        .record_serving_resource_id()
        .context("serving resource")?;
    let inventory = bigname_storage::families::records::load_family_record_inventory_detail(
        &database.pool,
        CHAIN,
        resource,
        bigname_storage::families::records::FamilyAttribution::Omit,
    )
    .await?
    .context("mainnet inventory")?;
    let mut stored =
        bigname_storage::load_phase_identity_records_by_ids(&database.pool, &[id]).await?;
    let actual = stored
        .pop()
        .context("prepared name")?
        .record_inventory_current
        .context("prepared inventory")?;
    assert_eq!(actual.entries, inventory.row.entries);
    assert_eq!(actual.selectors, inventory.row.selectors);
    assert_eq!(actual.provenance, inventory.row.provenance);
    Ok(detail["data"].clone())
}

pub(super) async fn stamp(database: &TestDatabase) -> Result<()> {
    let hash = bigname_content_hash::INTERPRETER_CONTENT_HASH;
    database.pool.set_connect_options(
        database
            .pool
            .connect_options()
            .as_ref()
            .clone()
            .options([("bigname.interpreter_content_hash", hash)]),
    );
    let mut connections = Vec::new();
    for _ in 0..database.pool.options().get_max_connections() {
        let mut conn = database.pool.acquire().await?;
        sqlx::query("SELECT set_config('bigname.interpreter_content_hash',$1,false)")
            .bind(hash)
            .execute(&mut *conn)
            .await?;
        connections.push(conn);
    }
    Ok(())
}
