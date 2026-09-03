use std::collections::BTreeMap;

use alloy_primitives::{Address, U256, keccak256};
use alloy_sol_types::SolEvent;
use serde_json::json;

use super::*;
use crate::schema_v2::{
    catalog::Selected,
    manifest::{ManifestEvent, ManifestSource},
    state::State,
};

const REGISTRY: &str = "0x0000000000000000000000000000000000000758";

#[test]
fn named_expiry_update_rejects_missing_registration_renewed_declaration() -> anyhow::Result<()> {
    let (selected, raw, mut state) = named_expiry_fixture(&["ExpiryChanged"])?;

    let error = registry(&selected, &raw, &mut state)
        .expect_err("an under-declaring named renewal must not return partial output");
    assert!(
        error
            .to_string()
            .contains("does not declare required normalized event RegistrationRenewed"),
        "unexpected declaration error: {error:#}"
    );
    Ok(())
}

#[test]
fn named_expiry_update_accepts_declared_registration_renewed() -> anyhow::Result<()> {
    let (selected, raw, mut state) =
        named_expiry_fixture(&["ExpiryChanged", "RegistrationRenewed"])?;

    let output = registry(&selected, &raw, &mut state)?;
    assert_eq!(
        output
            .events
            .iter()
            .map(|event| event.event_kind.as_str())
            .collect::<Vec<_>>(),
        ["ExpiryChanged", "RegistrationRenewed"]
    );
    Ok(())
}

fn named_expiry_fixture(
    normalized_events: &[&str],
) -> anyhow::Result<(Selected, RawLogInput, State)> {
    let token = U256::from_be_bytes(keccak256(b"named-renewal").0);
    let token_id = u256_word_hex(token);
    let now = 1_700_000_758;
    let mut state = State::new(
        Vec::new(),
        vec![(
            REGISTRY.to_owned(),
            "ens".to_owned(),
            vec!["eth".to_owned()],
        )],
    );
    state.replace_v2_registration(
        REGISTRY,
        &token_id,
        uuid::Uuid::from_u128(758),
        "ens",
        b"named-renewal",
        1_800_000_000,
        Some(json!({
            "source_event": "LabelRegistered",
            "labelhash": format!("{:#x}", keccak256(b"named-renewal")),
            "status": "registered",
        })),
    );
    state.refresh_dirty_v2_names(now);

    let encoded = ExpiryUpdated {
        tokenId: token,
        newExpiry: 1_900_000_000,
        sender: Address::from([0x75; 20]),
    }
    .encode_log_data();
    let event = ManifestEvent {
        name: "ExpiryUpdated".to_owned(),
        signature: "ExpiryUpdated(uint256,uint64,address)".to_owned(),
        topic0: format!("{:#x}", ExpiryUpdated::SIGNATURE_HASH),
        emitter_roles: vec!["registry".to_owned()],
        normalized_events: normalized_events
            .iter()
            .map(|event| (*event).to_owned())
            .collect(),
    };
    let source = ManifestSource {
        manifest_id: 758,
        manifest_version: 1,
        namespace: "ens".to_owned(),
        source_family: "ens_v2_registry_l1".to_owned(),
        chain_id: "ethereum-sepolia".to_owned(),
        deployment_label: "unit-test".to_owned(),
        correlation_addresses: BTreeMap::new(),
        events: vec![event.clone()],
    };
    let selected = Selected {
        source,
        event,
        contract_instance_id: uuid::Uuid::from_u128(758),
        emitter_role: Some("registry".to_owned()),
        match_all: false,
        manifest_declared_emitter: true,
    };
    let raw = RawLogInput {
        chain_id: "ethereum-sepolia".to_owned(),
        block_hash: format!("0x{:064x}", 758_u64),
        block_number: 758,
        block_timestamp: time::OffsetDateTime::from_unix_timestamp(now)?,
        canonicality_state: "canonical".to_owned(),
        transaction_hash: format!("0x{:064x}", 7_580_u64),
        transaction_index: 0,
        log_index: 0,
        emitting_address: REGISTRY.to_owned(),
        topics: encoded
            .topics()
            .iter()
            .map(|topic| format!("{topic:#x}"))
            .collect(),
        data: encoded.data.to_vec(),
    };
    Ok((selected, raw, state))
}
