//! Full Sepolia callback receipt through Engine, Project and the public HTTP history route.
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289-L304 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L382-L395 @ ens_v1@91c966f)
use super::v2_sepolia_redeploy::{CHAIN, HEAD, checked_in_profile, complete_phases, get};
use super::*;
use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};

const ENS_REGISTRY: &str = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e";
const NAME_WRAPPER: &str = "0x0635513f179d50a207757e05759cbd106d7dfce8";
const BASE_REGISTRAR: &str = "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85";
const WRAPPED_CONTROLLER: &str = "0xfed6a969aaa60e4961fcd3ebf1a2e8913ac65b72";
const OWNER: &str = "0x0000000000000000000000000000000000000051";
const CONTROLLER: &str = "0x00000000000000000000000000000000000000a1";
const REGISTRANT: &str = "0x00000000000000000000000000000000000000b2";
const REGISTRAR_EXPIRY: u64 = 1_900_000_000;
const GRACE_PERIOD: u64 = 90 * 86400;
const DOT_ETH_FUSES: u32 = 0x30000;
sol! {
    event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value);
    event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry);
    event NameUnwrapped(bytes32 indexed node, address owner);
}
mod base_registrar {
    alloy_sol_types::sol! { event Transfer(address indexed from, address indexed to, uint256 indexed tokenId); }
}
mod ens_registry {
    alloy_sol_types::sol! {
        event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
        event Transfer(bytes32 indexed node, address owner);
    }
}
mod renewal_events {
    alloy_sol_types::sol! { event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires); }
}
fn eth_node() -> B256 {
    keccak256([B256::ZERO.as_slice(), keccak256(b"eth").as_slice()].concat())
}
fn eth_namehash(label: B256) -> B256 {
    keccak256([eth_node().as_slice(), label.as_slice()].concat())
}

#[tokio::test]
async fn callback_mint_holder_has_http_and_prepared_owner_history() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let pool = &database.pool;
    bigname_manifests::sync_schema_v2_repository(
        pool,
        &bigname_manifests::load_repository(checked_in_profile())?,
    )
    .await?;
    sqlx::query("INSERT INTO chain_lineage(chain_id,block_hash,parent_hash,block_number,block_timestamp,canonicality_state)
        VALUES($1,$2,NULL,$3,to_timestamp($3),'canonical')")
        .bind(CHAIN).bind(format!("{CHAIN}-block-{HEAD}")).bind(HEAD).execute(pool).await?;
    let label = b"callbackunwrapped";
    let labelhash = keccak256(label);
    let namehash = eth_namehash(labelhash);
    let receiver = OWNER.parse::<Address>()?;
    let name_wrapper = NAME_WRAPPER.parse::<Address>()?;
    let token = U256::from_be_bytes(labelhash.0);
    let mut dns_name = vec![u8::try_from(label.len())?];
    dns_name.extend_from_slice(label);
    dns_name.extend_from_slice(b"\x03eth\0");

    let logs = [
        (
            BASE_REGISTRAR,
            base_registrar::Transfer {
                from: Address::ZERO,
                to: name_wrapper,
                tokenId: token,
            }
            .encode_log_data(),
        ),
        (
            ENS_REGISTRY,
            ens_registry::NewOwner {
                node: eth_node(),
                label: labelhash,
                owner: name_wrapper,
            }
            .encode_log_data(),
        ),
        (
            BASE_REGISTRAR,
            renewal_events::NameRegistered {
                id: token,
                owner: name_wrapper,
                expires: U256::from(REGISTRAR_EXPIRY),
            }
            .encode_log_data(),
        ),
        (
            NAME_WRAPPER,
            TransferSingle {
                operator: name_wrapper,
                from: Address::ZERO,
                to: receiver,
                id: U256::from_be_bytes(namehash.0),
                value: U256::from(1_u64),
            }
            .encode_log_data(),
        ),
        (
            NAME_WRAPPER,
            TransferSingle {
                operator: receiver,
                from: receiver,
                to: Address::ZERO,
                id: U256::from_be_bytes(namehash.0),
                value: U256::from(1_u64),
            }
            .encode_log_data(),
        ),
        (
            ENS_REGISTRY,
            ens_registry::Transfer {
                node: namehash,
                owner: CONTROLLER.parse()?,
            }
            .encode_log_data(),
        ),
        (
            NAME_WRAPPER,
            NameUnwrapped {
                node: namehash,
                owner: CONTROLLER.parse()?,
            }
            .encode_log_data(),
        ),
        (
            BASE_REGISTRAR,
            base_registrar::Transfer {
                from: name_wrapper,
                to: REGISTRANT.parse()?,
                tokenId: token,
            }
            .encode_log_data(),
        ),
        (
            NAME_WRAPPER,
            NameWrapped {
                node: namehash,
                name: dns_name.into(),
                owner: receiver,
                fuses: DOT_ETH_FUSES,
                expiry: REGISTRAR_EXPIRY + GRACE_PERIOD,
            }
            .encode_log_data(),
        ),
    ];
    let block_hash = format!("{CHAIN}-block-{HEAD}");
    let transaction = format!("{CHAIN}-transaction-{HEAD}");
    sqlx::query("INSERT INTO raw_transactions(chain_id,block_hash,block_number,transaction_hash,transaction_index,from_address,to_address)
        VALUES($1,$2,$3,$4,0,$5,$6)")
        .bind(CHAIN).bind(&block_hash).bind(HEAD).bind(&transaction).bind(OWNER).bind(WRAPPED_CONTROLLER).execute(pool).await?;
    for (index, (emitter, data)) in logs.into_iter().enumerate() {
        let topics: Vec<String> = data
            .topics()
            .iter()
            .map(|topic| format!("{topic:#x}"))
            .collect();
        sqlx::query("INSERT INTO raw_logs(chain_id,block_hash,block_number,transaction_hash,transaction_index,log_index,emitting_address,topics,data)
            VALUES($1,$2,$3,$4,0,$5,$6,$7,$8)")
            .bind(CHAIN).bind(&block_hash).bind(HEAD).bind(&transaction).bind(index as i64).bind(emitter).bind(topics).bind(data.data.as_ref()).execute(pool).await?;
    }
    bigname_interpret::Engine::new(pool.clone())
        .run_batch(bigname_interpret::BatchRequest {
            chain_id: CHAIN.into(),
            from_block: HEAD,
            to_block: HEAD,
            resume_current: None,
            mode: bigname_interpret::RunMode::Normal,
        })
        .await?;
    sqlx::query(
        "INSERT INTO chain_heads(chain_id,latest_block_hash,latest_block_number) VALUES($1,$2,$3)",
    )
    .bind(CHAIN)
    .bind(&block_hash)
    .bind(HEAD)
    .execute(pool)
    .await?;
    complete_phases(pool).await?;
    let token = bigname_project::families::input_token(pool, CHAIN).await?;
    let publication = bigname_project::families::apply(
        pool,
        CHAIN,
        &bigname_project::Marker {
            number: HEAD,
            hash: block_hash,
        },
        bigname_project::families::FamilyMode::Rebuild,
        &token,
        &bigname_project::families::FamilyOptions::new(
            bigname_content_hash::INTERPRETER_CONTENT_HASH,
        ),
    )
    .await?;
    assert_eq!(
        publication.marker.as_ref().map(|marker| marker.number),
        Some(HEAD),
        "{publication:?}"
    );
    let logical = format!("ens:{namehash:#x}");
    let mint:Vec<(i64,Value)>=sqlx::query_as("SELECT log_index,after_state FROM normalized_events
        WHERE chain_id=$1 AND logical_name_id=$2 AND event_kind='TokenControlTransferred' AND after_state->>'wrapper_mint'='true'")
        .bind(CHAIN).bind(&logical).fetch_all(pool).await?;
    assert_eq!(mint.len(), 1, "one actual wrapper mint");
    assert_eq!(mint[0].0, 3, "mint cites the real TransferSingle position");
    assert_eq!(mint[0].1["to"], OWNER);
    assert_eq!(mint[0].1["source_event"], "TransferSingle");
    assert_eq!(mint[0].1["matched_wrapper_completion"]["log_index"], 8);
    for address in [OWNER, REGISTRANT, NAME_WRAPPER] {
        let (status,body)=get(&database,&format!("/v1/addresses/{address}/history?namespace=ens&relation=owner&include=data,raw&order=asc&page_size=200")).await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        let rows = body["data"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("history payload: {body}"))?;
        assert_eq!(
            rows.iter()
                .any(|row| row["name"] == "callbackunwrapped.eth"),
            address != NAME_WRAPPER,
            "history for {address}: {body}"
        );
    }
    let (status, history) = get(&database, &format!("/v1/addresses/{OWNER}/history?namespace=ens&relation=owner&include=data,raw&order=asc&page_size=200")).await?;
    assert_eq!(status, StatusCode::OK, "{history}");
    let wraps = history["data"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["data"]["action"] == "name_wrapped")
        .collect::<Vec<_>>();
    assert_eq!(
        wraps.len(),
        1,
        "temporary receiver keeps the historical wrap: {history}"
    );
    assert_eq!(wraps[0]["log_index"], 3);
    assert_eq!(wraps[0]["data"]["owner"], OWNER);
    assert_eq!(wraps[0]["data"]["fuses"], DOT_ETH_FUSES);
    assert_eq!(
        wraps[0]["data"]["expires_at"],
        (REGISTRAR_EXPIRY + GRACE_PERIOD).to_string()
    );
    let (status, detail) = get(&database, "/v1/names/callbackunwrapped.eth?namespace=ens").await?;
    assert_eq!(status, StatusCode::OK, "{detail}");
    let registrar_resource: Uuid = sqlx::query_scalar("SELECT resource_id FROM normalized_events
        WHERE chain_id=$1 AND event_kind='RegistrationGranted' AND source_family='ens_v1_registrar_l1'
          AND after_state->>'source_event'='NameRegistered' AND log_index=2")
        .bind(CHAIN).fetch_one(pool).await?;
    assert_eq!(
        detail["data"]["registration_id"],
        registrar_resource.to_string(),
        "legitimate registrar lease: {detail}"
    );
    assert_eq!(detail["data"]["owner"], REGISTRANT, "{detail}");
    assert_eq!(detail["data"]["manager"], CONTROLLER, "{detail}");
    assert_eq!(detail["data"]["expires_at"], REGISTRAR_EXPIRY.to_string());
    assert_eq!(
        detail["data"]["grace_ends_at"],
        (REGISTRAR_EXPIRY + GRACE_PERIOD).to_string()
    );
    assert_eq!(detail["data"]["registered_at"], HEAD.to_string());
    let (status, current) = get(
        &database,
        &format!("/v1/addresses/{OWNER}/names?namespace=ens&relation=owner,manager"),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{current}");
    assert!(
        current["data"].as_array().is_some_and(Vec::is_empty),
        "{current}"
    );
    let anchors: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM project_address_history_anchor WHERE chain_id=$1 AND address=$2 AND anchor_kind=0 AND anchor_id=$3 AND historical_mask<>0",
    )
    .bind(CHAIN)
    .bind(OWNER)
    .bind(&logical)
    .fetch_one(pool)
    .await?;
    assert!(
        anchors > 0,
        "temporary receiver has prepared history membership"
    );
    database.cleanup().await
}
