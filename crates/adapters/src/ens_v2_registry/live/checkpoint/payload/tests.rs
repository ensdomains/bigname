use super::*;
use anyhow::{Context, Result};
use bigname_storage::{CanonicalityState, SurfaceBindingKind};
use serde_json::Value;
use sqlx::types::{Uuid, time::OffsetDateTime};

use crate::ens_v2_registry::{
    NameMetadata, ObservationRef, RegistryNameState, RegistryResourceLink,
};

#[test]
fn live_checkpoint_payload_round_trips_every_replay_collection() -> Result<()> {
    let chain = "ethereum-sepolia";
    let registry = "0x00000000000000000000000000000000000000aa";
    let old_token = "1";
    let new_token = "2";
    let first_ref = observation_ref(chain, 10, 1, CanonicalityState::Canonical)?;
    let current_ref = observation_ref(chain, 11, 2, CanonicalityState::Safe)?;
    let linked_ref = observation_ref(chain, 12, 3, CanonicalityState::Finalized)?;
    let mut replay_state = RegistryReplayState::default();
    replay_state
        .registry_suffix_by_address
        .insert(registry.to_owned(), String::new());
    replay_state
        .registry_contract_by_address
        .insert(registry.to_owned(), Uuid::from_u128(1));
    replay_state.states_by_registry_token.insert(
        (registry.to_owned(), old_token.to_owned()),
        RegistryNameState {
            token_id: new_token.to_owned(),
            labelhash: "0xlabel".to_owned(),
            label: "t\0ést".to_owned(),
            full_name: "tést.eth".to_owned(),
            name: NameMetadata {
                namespace: "ens_v2".to_owned(),
                logical_name_id: "ens_v2:tést.eth".to_owned(),
                input_name: "tést.eth".to_owned(),
                canonical_display_name: "tést.eth".to_owned(),
                normalized_name: "tést.eth".to_owned(),
                dns_encoded_name: vec![5, b't', 0, 255],
                namehash: "0xname".to_owned(),
                labelhashes: vec!["0xeth".to_owned(), "0xlabel".to_owned()],
                normalizer_version: "ensip15-v1".to_owned(),
            },
            owner: Some("0xowner".to_owned()),
            expiry: Some(u64::MAX),
            status: "registered",
            first_ref,
            current_ref,
            registry_address: registry.to_owned(),
            registry_contract_instance_id: Uuid::from_u128(1),
            source_manifest_id: 7,
            source_family: "ens_v2_registry_l1".to_owned(),
            manifest_version: 3,
            resource: Some(RegistryResourceLink {
                upstream_resource: "77".to_owned(),
                observed_token_id: new_token.to_owned(),
                observed_expiry: Some(u64::MAX - 1),
                resource_id: Uuid::from_u128(2),
                token_lineage_id: Uuid::from_u128(3),
                surface_binding_id: Uuid::from_u128(4),
                linked_ref,
            }),
            resolver: Some("0xresolver".to_owned()),
            subregistry: Some("0xsubregistry".to_owned()),
            binding_kind: SurfaceBindingKind::LinkedSubregistryPath,
        },
    );
    replay_state.token_aliases.insert(
        (registry.to_owned(), new_token.to_owned()),
        (registry.to_owned(), old_token.to_owned()),
    );
    rebuild_registry_state_indexes(&mut replay_state)?;
    let snapshot = CachedLiveRegistryReplayState {
        through_block_number: 12,
        through_block_hash: "0xblock12".to_owned(),
        raw_log_input_revision: 9,
        raw_log_retention_generation: 2,
        discovery_admission_epoch: 4,
        replay_state,
    };

    let (metadata, items, counts) = encode_snapshot(&snapshot)?;
    let decoded_metadata = decode_metadata(metadata)?;
    assert_eq!(
        decoded_metadata.through_block_hash,
        snapshot.through_block_hash
    );
    assert_eq!(decoded_metadata.discovery_admission_epoch, 4);
    let rows = items
        .into_iter()
        .map(|item| (item.item_kind.to_owned(), item.item_key, item.item_payload))
        .collect();
    let decoded = decode_replay_state(chain, rows, counts)?;
    assert_eq!(decoded, snapshot.replay_state);
    Ok(())
}

#[test]
fn live_checkpoint_payload_rejects_unknown_version() -> Result<()> {
    let snapshot = CachedLiveRegistryReplayState {
        through_block_number: 1,
        through_block_hash: "0xblock".to_owned(),
        raw_log_input_revision: 1,
        raw_log_retention_generation: 0,
        discovery_admission_epoch: 0,
        replay_state: RegistryReplayState::default(),
    };
    let (mut metadata, _, _) = encode_snapshot(&snapshot)?;
    metadata["payload_version"] = Value::from(2);
    let error = decode_metadata(metadata)
        .err()
        .context("unknown checkpoint payload version must fail")?;
    assert!(format!("{error:#}").contains("unsupported ENSv2 live checkpoint payload version 2"));
    Ok(())
}

#[test]
fn live_checkpoint_jsonb_encoding_fixture_and_contexts_remain_stable() -> Result<()> {
    let payload = serde_json::json!({"label": "before\0after"});
    let encoded = encode_value(&payload)?;
    assert_eq!(
        encoded,
        serde_json::json!({
            "label": {
                "__bigname_live_checkpoint_utf8_hex_v1":
                    "6265666f7265006166746572"
            }
        })
    );
    assert_eq!(decode_value::<Value>(encoded)?, payload);

    let codec_error = decode_value::<Value>(serde_json::json!({
        "__bigname_live_checkpoint_utf8_hex_v1": 1
    }))
    .expect_err("invalid live checkpoint envelope must fail");
    assert_eq!(
        format!("{codec_error:#}"),
        "failed to decode ENSv2 live checkpoint JSONB encoding: checkpoint escaped string payload is not a string"
    );

    let payload_error = decode_value::<u64>(Value::String("not a number".to_owned()))
        .expect_err("invalid live checkpoint payload must fail");
    assert!(
        format!("{payload_error:#}")
            .starts_with("failed to decode ENSv2 live checkpoint payload: invalid type")
    );
    Ok(())
}

#[test]
fn live_checkpoint_payload_preserves_per_kind_corruption_detection() -> Result<()> {
    let suffix = RegistrySuffixPayload {
        address: "0x00000000000000000000000000000000000000aa".to_owned(),
        suffix: "eth".to_owned(),
    };
    let item = encoded_item(
        ITEM_KIND_REGISTRY_SUFFIX,
        single_key(&suffix.address)?,
        &suffix,
    )?;
    let expected = SnapshotItemCounts {
        registry_suffixes: 0,
        registry_contracts: 1,
        registry_name_states: 0,
        token_aliases: 0,
    };

    let error = decode_replay_state(
        "ethereum-sepolia",
        vec![(item.item_kind.to_owned(), item.item_key, item.item_payload)],
        expected,
    )
    .expect_err("same-total per-kind corruption must fail");

    assert_eq!(
        error.to_string(),
        "ENSv2 live checkpoint per-kind counts do not match metadata"
    );
    Ok(())
}

fn observation_ref(
    chain: &str,
    block_number: i64,
    nanosecond: u32,
    canonicality_state: CanonicalityState,
) -> Result<ObservationRef> {
    Ok(ObservationRef {
        chain_id: chain.to_owned(),
        block_hash: format!("0xblock{block_number}"),
        block_number,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_000 + block_number)?
            .replace_nanosecond(nanosecond)?,
        transaction_hash: format!("0xtx{block_number}"),
        transaction_index: 1,
        log_index: 2,
        emitting_address: "0x00000000000000000000000000000000000000aa".to_owned(),
        emitting_contract_instance_id: Uuid::from_u128(1),
        canonicality_state,
        namespace: "ens_v2".to_owned(),
        source_manifest_id: 7,
        source_family: "ens_v2_registry_l1".to_owned(),
        manifest_version: 3,
    })
}
