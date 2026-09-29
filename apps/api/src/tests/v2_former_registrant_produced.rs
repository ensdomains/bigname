//! The former relation over releases emitted by the real registry decoder and published by Project.
use super::*;
use alloy_primitives::{U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use bigname_adapters::schema_v2::{
    AddressAdmissionInput, BatchInput, RawBlockInput, RawLogInput, StateCacheCapacity,
    prepare_schema_v2_batch_incremental,
};

const CHAIN: &str = "ethereum-mainnet";
const REGISTRY: &str = "0x657ea849311d3d5823348dded7c2aaafb3ede09e";
const HOLDER: &str = "0x0000000000000000000000000000000000000063";

sol! {
    event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender);
    event TokenResource(uint256 indexed tokenId, uint256 indexed resource);
    event LabelUnregistered(uint256 indexed tokenId, address indexed sender);
    event TokenRegenerated(uint256 indexed oldTokenId, uint256 indexed newTokenId);
}

fn token(label: &str) -> U256 {
    let mut bytes = *keccak256(label.as_bytes());
    bytes[28..].copy_from_slice(&1_u32.to_be_bytes());
    U256::from_be_bytes(bytes)
}

fn raw(data: alloy_primitives::LogData, block: i64, index: i64) -> RawLogInput {
    RawLogInput {
        chain_id: CHAIN.into(),
        block_hash: format!("0xhistory{block}"),
        block_number: block,
        block_timestamp: timestamp(1_700_000_000 + block),
        canonicality_state: "canonical".into(),
        transaction_hash: format!("0x{block:064x}"),
        transaction_index: 0,
        log_index: index,
        emitting_address: REGISTRY.into(),
        topics: data.topics().iter().map(|t| format!("{t:#x}")).collect(),
        data: data.data.to_vec(),
    }
}

fn registered(label: &str, block: i64) -> Result<Vec<RawLogInput>> {
    Ok(vec![
        raw(
            LabelRegistered {
                tokenId: token(label),
                labelHash: keccak256(label.as_bytes()),
                label: label.into(),
                owner: HOLDER.parse()?,
                sender: HOLDER.parse()?,
                expiry: 1_900_000_000,
            }
            .encode_log_data(),
            block,
            0,
        ),
        raw(
            TokenResource {
                tokenId: token(label),
                resource: U256::from(block as u64),
            }
            .encode_log_data(),
            block,
            1,
        ),
    ])
}

async fn publish_release(database: &TestDatabase, unregister: bool) -> Result<()> {
    let (manifest, rules) = v2_history_bounded_regeneration::manifest_and_rules();
    let mut logs = registered("departed", 120)?;
    if unregister {
        logs.push(raw(
            LabelUnregistered {
                tokenId: token("departed"),
                sender: HOLDER.parse()?,
            }
            .encode_log_data(),
            122,
            0,
        ));
    } else {
        // Exercise the decoder's retained-token collision branch, also covered by its
        // regeneration_collision_reasserts_a_displaced_names_surviving_coholder test.
        logs.extend(registered("survivor", 121)?);
        logs.push(raw(
            TokenRegenerated {
                oldTokenId: token("survivor"),
                newTokenId: token("departed"),
            }
            .encode_log_data(),
            122,
            0,
        ));
    }
    let (output, _) = prepare_schema_v2_batch_incremental(
        BatchInput {
            chain_id: CHAIN.into(),
            manifests: vec![manifest.clone()],
            discovery_rules: rules,
            admissions: vec![AddressAdmissionInput {
                address: REGISTRY.into(),
                contract_instance_id: Uuid::from_u128(manifest.manifest_id as u128),
                source_manifest_id: Some(manifest.manifest_id),
                role: Some("registry".into()),
                discovery_edge_kind: None,
                discovery_from_contract_instance_id: None,
                discovery_observation_key: None,
                active_from_block: Some(0),
                active_to_block: None,
            }],
            prior_events: vec![],
            blocks: (120..=122)
                .map(|block| RawBlockInput {
                    chain_id: CHAIN.into(),
                    block_hash: format!("0xhistory{block}"),
                    block_number: block,
                    block_timestamp: timestamp(1_700_000_000 + block),
                    canonicality_state: "canonical".into(),
                })
                .collect(),
            raw_logs: logs,
        },
        None,
        StateCacheCapacity::Unlimited,
    )?
    .finish(vec![])?;
    assert!(output.decode_skips.is_empty(), "{:?}", output.decode_skips);
    let release = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationReleased" && event.block_number == Some(122))
        .context("real producer release")?;
    assert_eq!(
        release.after_state["source_event"],
        if unregister {
            "LabelUnregistered"
        } else {
            "TokenRegenerated"
        }
    );
    assert!(
        release.after_state.get("released_at").is_none(),
        "timestamp must come from the canonical block"
    );
    seed_v2_history_blocks(database, 120..=123).await?;
    v2_history_bounded_rebinding::persist_with_manifests(&database.pool, &[manifest], &output)
        .await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({CHAIN: {
            "chain_id": CHAIN, "block_number": 123, "block_hash": "0xhistory123",
            "timestamp": "1700000123"
        }}))
        .await?;
    publish_test_families_on(&database.pool, CHAIN, 123).await
}

#[tokio::test]
async fn v2_former_registrant_real_unregister_uses_canonical_release_time() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    publish_release(&database, true).await?;
    let profile = v2_names_payload(&database, "/v1/names/departed.eth").await?;
    assert_eq!(
        profile["data"]["registration_status"], "released",
        "{profile}"
    );
    assert_eq!(
        profile["data"]["lapsed_registration"],
        json!({
            "registrant": HOLDER, "held_through": "registry", "release_kind": "unregistered",
            "released_at": "1700000122"
        })
    );
    let (status, former) = read_family_response(
        &database,
        &format!("/v1/addresses/{HOLDER}/names?relation=former_registrant&namespace=ens"),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{former}");
    assert_eq!(v2_names_listed(&former), ["departed.eth"]);
    assert_eq!(
        former["data"][0]["lapsed_registration"],
        profile["data"]["lapsed_registration"]
    );
    database.cleanup().await
}

#[tokio::test]
async fn v2_former_registrant_real_displacement_is_not_an_unregister() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    publish_release(&database, false).await?;
    let profile = v2_names_payload(&database, "/v1/names/departed.eth").await?;
    assert_eq!(
        profile["data"]["registration_status"], "released",
        "{profile}"
    );
    assert!(
        profile["data"].get("lapsed_registration").is_none(),
        "{profile}"
    );
    let (status, former) = read_family_response(
        &database,
        &format!("/v1/addresses/{HOLDER}/names?relation=former_registrant&namespace=ens"),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{former}");
    assert!(v2_names_listed(&former).is_empty(), "{former}");
    database.cleanup().await
}
