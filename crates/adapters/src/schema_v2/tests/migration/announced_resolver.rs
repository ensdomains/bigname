//! An ENSv2 resolver proxy admitted by an `Upgraded` in an earlier batch, which emits a role
//! change and a text record change and then announces the same implementation again inside the
//! batch under test. The later `Upgraded` replaces the proxy's admission with one starting at
//! its own block, so a re-run of the batch must start from the admissions the batch began with
//! to keep the proxy's earlier logs.

use super::*;

const RESOLVER_MANIFEST_ID: i64 = 105;
const PROXY: &str = "0x00000000000000000000000000000000000000c7";
const IMPLEMENTATION: &str = "0x00000000000000000000000000000000000000c8";
const ROLE_ACCOUNT: &str = "0x00000000000000000000000000000000000000c9";
const ANNOUNCED_AT: i64 = 225;
const ROLES_CHANGED_AT: i64 = 231;
const REANNOUNCED_AT: i64 = 232;

sol! {
    event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value);
}

fn resolver_manifest() -> ManifestInput {
    let mut manifest = manifest_with_events(
        RESOLVER_MANIFEST_ID,
        "ens",
        "ens_v2_resolver_l1",
        &[
            (
                "Upgraded",
                "event Upgraded(address indexed implementation)",
                &[],
                &["Upgraded"],
            ),
            (
                "EACRolesChanged",
                "event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap)",
                &[],
                &["PermissionChanged"],
            ),
            (
                "TextChanged",
                "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)",
                &[],
                &["RecordChanged"],
            ),
        ],
    );
    let mut payload: Value = serde_json::from_str(&manifest.payload_json).unwrap();
    payload["resolver_implementations"] =
        json!([{"role": "permissioned_resolver", "address": IMPLEMENTATION}]);
    manifest.payload_json = payload.to_string();
    manifest
}

fn upgraded(block: i64) -> RawLogInput {
    raw_at_transaction(
        super::super::v2_registry::Upgraded {
            implementation: IMPLEMENTATION.parse().unwrap(),
        }
        .encode_log_data(),
        block,
        0,
        0,
        PROXY,
    )
}

fn roles_changed(block: i64) -> RawLogInput {
    raw_at_transaction(
        super::super::v2_resolver::EACRolesChanged {
            resource: U256::ZERO,
            account: ROLE_ACCOUNT.parse().unwrap(),
            oldRoleBitmap: U256::ZERO,
            newRoleBitmap: U256::from(1),
        }
        .encode_log_data(),
        block,
        0,
        0,
        PROXY,
    )
}

fn text_changed(block: i64) -> RawLogInput {
    raw_at_transaction(
        TextChanged {
            node: keccak256(b"box"),
            indexedKey: keccak256(b"url"),
            key: "url".to_owned(),
            value: "https://example.test".to_owned(),
        }
        .encode_log_data(),
        block,
        0,
        1,
        PROXY,
    )
}

fn block_of(raw: &RawLogInput) -> RawBlockInput {
    RawBlockInput {
        chain_id: raw.chain_id.clone(),
        block_hash: raw.block_hash.clone(),
        block_number: raw.block_number,
        block_timestamp: raw.block_timestamp,
        canonicality_state: raw.canonicality_state.clone(),
    }
}

/// Adds the proxy to `input`: an earlier batch admits it through `Upgraded`, and the loader
/// carries that admission and the batch's events forward, the way
/// `crates/interpret/src/load.rs` re-admits discovered contracts from their discovery edges.
/// `input` then gets the role change, the text change and the second announcement.
pub(super) fn with_announced_resolver(mut input: BatchInput) -> anyhow::Result<BatchInput> {
    input.manifests.push(resolver_manifest());
    let announcement = upgraded(ANNOUNCED_AT);
    let mut head_input = input.clone();
    head_input.prior_events = Vec::new();
    head_input.blocks = if input.blocks.is_empty() {
        Vec::new()
    } else {
        vec![block_of(&announcement)]
    };
    head_input.raw_logs = vec![announcement];
    let head = interpret_test_batch(head_input)?;
    let edge = head
        .discovery_edges
        .iter()
        .find(|edge| edge.edge_kind == "resolver")
        .expect("the earlier Upgraded admits the proxy");
    input.admissions.push(AddressAdmissionInput {
        address: PROXY.to_owned(),
        contract_instance_id: edge.to_contract_instance_id,
        source_manifest_id: Some(edge.source_manifest_id),
        role: None,
        discovery_edge_kind: Some(edge.edge_kind.clone()),
        discovery_from_contract_instance_id: Some(edge.from_contract_instance_id),
        discovery_observation_key: Some(edge.observation_key.clone()),
        active_from_block: Some(edge.active_from_block_number),
        active_to_block: None,
    });
    input
        .prior_events
        .extend(head.normalized_events.iter().map(prior_event));
    let added = [
        roles_changed(ROLES_CHANGED_AT),
        text_changed(ROLES_CHANGED_AT),
        upgraded(REANNOUNCED_AT),
    ];
    if !input.blocks.is_empty() {
        input.blocks.extend(added.iter().map(block_of));
        input.blocks.sort_by_key(|block| block.block_number);
        input.blocks.dedup_by_key(|block| block.block_number);
    }
    input.raw_logs.extend(added);
    input
        .raw_logs
        .sort_by_key(|raw| (raw.block_number, raw.transaction_index, raw.log_index));
    Ok(input)
}

/// Whether the proxy's text change before its second announcement was interpreted.
pub(super) fn kept_record_change(output: &BatchOutput) -> bool {
    output.normalized_events.iter().any(|event| {
        event.source_family == "ens_v2_resolver_l1"
            && event.event_kind == "RecordChanged"
            && event.block_number == Some(ROLES_CHANGED_AT)
    })
}

/// Whether the proxy's role change before its second announcement was interpreted.
pub(super) fn kept_roles_change(output: &BatchOutput) -> bool {
    output.normalized_events.iter().any(|event| {
        event.source_family == "ens_v2_resolver_l1"
            && event.event_kind == "PermissionChanged"
            && event.block_number == Some(ROLES_CHANGED_AT)
    })
}
