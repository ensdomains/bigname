use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::types::Uuid;

use super::super::{CachedLiveRegistryReplayState, RegistryReplayState};

mod codecs;
mod encoding;
use encoding::{decode_value, encode_value};
#[cfg(test)]
mod tests;

pub(super) const ITEM_KIND_REGISTRY_SUFFIX: &str = "registry_suffix";
pub(super) const ITEM_KIND_REGISTRY_CONTRACT: &str = "registry_contract";
pub(super) const ITEM_KIND_REGISTRY_NAME_STATE: &str = "registry_name_state";
pub(super) const ITEM_KIND_TOKEN_ALIAS: &str = "token_alias";
const PAYLOAD_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SnapshotItemCounts {
    pub(super) registry_suffixes: usize,
    pub(super) registry_contracts: usize,
    pub(super) registry_name_states: usize,
    pub(super) token_aliases: usize,
}

impl SnapshotItemCounts {
    pub(super) fn total(self) -> Result<usize> {
        self.registry_suffixes
            .checked_add(self.registry_contracts)
            .and_then(|count| count.checked_add(self.registry_name_states))
            .and_then(|count| count.checked_add(self.token_aliases))
            .context("ENSv2 live checkpoint item count overflow")
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SnapshotMetadata {
    payload_version: u32,
    pub(super) through_block_hash: String,
    pub(super) discovery_admission_epoch: i64,
    pub(super) item_counts: SnapshotItemCounts,
}

pub(super) struct EncodedCheckpointItem {
    pub(super) item_kind: &'static str,
    pub(super) item_key: String,
    pub(super) item_payload: Value,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegistrySuffixPayload {
    address: String,
    suffix: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegistryContractPayload {
    address: String,
    contract_instance_id: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TokenAliasPayload {
    registry_address: String,
    token_id: String,
    target_registry_address: String,
    target_token_id: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegistryNameStateItemPayload {
    registry_address: String,
    token_key: String,
    state: RegistryNameStatePayload,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegistryNameStatePayload {
    token_id: String,
    labelhash: String,
    label: String,
    full_name: String,
    name: NameMetadataPayload,
    owner: Option<String>,
    expiry: Option<u64>,
    status: String,
    first_ref: ObservationRefPayload,
    current_ref: ObservationRefPayload,
    registry_address: String,
    registry_contract_instance_id: String,
    source_manifest_id: i64,
    source_family: String,
    manifest_version: i64,
    resource: Option<RegistryResourceLinkPayload>,
    resolver: Option<String>,
    subregistry: Option<String>,
    binding_kind: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegistryResourceLinkPayload {
    upstream_resource: String,
    observed_token_id: String,
    observed_expiry: Option<u64>,
    resource_id: String,
    token_lineage_id: String,
    surface_binding_id: String,
    linked_ref: ObservationRefPayload,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NameMetadataPayload {
    namespace: String,
    logical_name_id: String,
    input_name: String,
    canonical_display_name: String,
    normalized_name: String,
    dns_encoded_name: Vec<u8>,
    namehash: String,
    labelhashes: Vec<String>,
    normalizer_version: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObservationRefPayload {
    chain_id: String,
    block_hash: String,
    block_number: i64,
    block_timestamp_seconds: i64,
    block_timestamp_nanoseconds: u32,
    transaction_hash: String,
    transaction_index: i64,
    log_index: i64,
    emitting_address: String,
    emitting_contract_instance_id: String,
    canonicality_state: String,
    namespace: String,
    source_manifest_id: i64,
    source_family: String,
    manifest_version: i64,
}

pub(super) fn encode_snapshot(
    snapshot: &CachedLiveRegistryReplayState,
) -> Result<(Value, Vec<EncodedCheckpointItem>, SnapshotItemCounts)> {
    let state = &snapshot.replay_state;
    let counts = SnapshotItemCounts {
        registry_suffixes: state.registry_suffix_by_address.len(),
        registry_contracts: state.registry_contract_by_address.len(),
        registry_name_states: state.states_by_registry_token.len(),
        token_aliases: state.token_aliases.len(),
    };
    let metadata = SnapshotMetadata {
        payload_version: PAYLOAD_VERSION,
        through_block_hash: snapshot.through_block_hash.clone(),
        discovery_admission_epoch: snapshot.discovery_admission_epoch,
        item_counts: counts,
    };
    let mut items = Vec::with_capacity(counts.total()?);
    for (address, suffix) in &state.registry_suffix_by_address {
        items.push(encoded_item(
            ITEM_KIND_REGISTRY_SUFFIX,
            single_key(address)?,
            &RegistrySuffixPayload {
                address: address.clone(),
                suffix: suffix.clone(),
            },
        )?);
    }
    for (address, contract_instance_id) in &state.registry_contract_by_address {
        items.push(encoded_item(
            ITEM_KIND_REGISTRY_CONTRACT,
            single_key(address)?,
            &RegistryContractPayload {
                address: address.clone(),
                contract_instance_id: contract_instance_id.to_string(),
            },
        )?);
    }
    for ((registry_address, token_key), state) in &state.states_by_registry_token {
        items.push(encoded_item(
            ITEM_KIND_REGISTRY_NAME_STATE,
            pair_key(registry_address, token_key)?,
            &RegistryNameStateItemPayload {
                registry_address: registry_address.clone(),
                token_key: token_key.clone(),
                state: RegistryNameStatePayload::from_state(state),
            },
        )?);
    }
    for ((registry_address, token_id), (target_registry_address, target_token_id)) in
        &state.token_aliases
    {
        items.push(encoded_item(
            ITEM_KIND_TOKEN_ALIAS,
            pair_key(registry_address, token_id)?,
            &TokenAliasPayload {
                registry_address: registry_address.clone(),
                token_id: token_id.clone(),
                target_registry_address: target_registry_address.clone(),
                target_token_id: target_token_id.clone(),
            },
        )?);
    }
    items.sort_by(|left, right| {
        (left.item_kind, left.item_key.as_str()).cmp(&(right.item_kind, right.item_key.as_str()))
    });
    Ok((encode_value(&metadata)?, items, counts))
}

pub(super) fn decode_metadata(value: Value) -> Result<SnapshotMetadata> {
    let metadata: SnapshotMetadata = decode_value(value)?;
    ensure!(
        metadata.payload_version == PAYLOAD_VERSION,
        "unsupported ENSv2 live checkpoint payload version {}",
        metadata.payload_version
    );
    ensure!(
        !metadata.through_block_hash.is_empty(),
        "ENSv2 live checkpoint anchor hash is empty"
    );
    ensure!(
        metadata.discovery_admission_epoch >= 0,
        "ENSv2 live checkpoint discovery epoch is negative"
    );
    metadata.item_counts.total()?;
    Ok(metadata)
}

pub(super) fn decode_replay_state(
    chain: &str,
    rows: Vec<(String, String, Value)>,
    expected: SnapshotItemCounts,
) -> Result<RegistryReplayState> {
    ensure!(
        rows.len() == expected.total()?,
        "ENSv2 live checkpoint item count mismatch: expected {}, observed {}",
        expected.total()?,
        rows.len()
    );
    let mut state = RegistryReplayState::default();
    let mut observed = SnapshotItemCounts {
        registry_suffixes: 0,
        registry_contracts: 0,
        registry_name_states: 0,
        token_aliases: 0,
    };
    for (item_kind, item_key, item_payload) in rows {
        match item_kind.as_str() {
            ITEM_KIND_REGISTRY_SUFFIX => {
                let payload: RegistrySuffixPayload = decode_value(item_payload)?;
                ensure!(
                    item_key == single_key(&payload.address)?,
                    "ENSv2 live checkpoint suffix key mismatch"
                );
                ensure!(
                    state
                        .registry_suffix_by_address
                        .insert(payload.address, payload.suffix)
                        .is_none(),
                    "duplicate ENSv2 live checkpoint suffix"
                );
                observed.registry_suffixes += 1;
            }
            ITEM_KIND_REGISTRY_CONTRACT => {
                let payload: RegistryContractPayload = decode_value(item_payload)?;
                ensure!(
                    item_key == single_key(&payload.address)?,
                    "ENSv2 live checkpoint contract key mismatch"
                );
                let contract_id = parse_uuid(&payload.contract_instance_id, "registry contract")?;
                ensure!(
                    state
                        .registry_contract_by_address
                        .insert(payload.address, contract_id)
                        .is_none(),
                    "duplicate ENSv2 live checkpoint contract"
                );
                observed.registry_contracts += 1;
            }
            ITEM_KIND_REGISTRY_NAME_STATE => {
                let payload: RegistryNameStateItemPayload = decode_value(item_payload)?;
                ensure!(
                    item_key == pair_key(&payload.registry_address, &payload.token_key)?,
                    "ENSv2 live checkpoint registry-token key mismatch"
                );
                ensure!(
                    payload.registry_address == payload.state.registry_address,
                    "ENSv2 live checkpoint state registry does not match its map key"
                );
                let key = (payload.registry_address, payload.token_key);
                let value = payload.state.into_state(chain)?;
                ensure!(
                    state.states_by_registry_token.insert(key, value).is_none(),
                    "duplicate ENSv2 live checkpoint registry-token state"
                );
                observed.registry_name_states += 1;
            }
            ITEM_KIND_TOKEN_ALIAS => {
                let payload: TokenAliasPayload = decode_value(item_payload)?;
                ensure!(
                    item_key == pair_key(&payload.registry_address, &payload.token_id)?,
                    "ENSv2 live checkpoint token-alias key mismatch"
                );
                let key = (payload.registry_address, payload.token_id);
                let target = (payload.target_registry_address, payload.target_token_id);
                ensure!(
                    state.token_aliases.insert(key, target).is_none(),
                    "duplicate ENSv2 live checkpoint token alias"
                );
                observed.token_aliases += 1;
            }
            _ => bail!("unknown ENSv2 live checkpoint item kind {item_kind}"),
        }
    }
    ensure!(
        observed == expected,
        "ENSv2 live checkpoint per-kind counts do not match metadata"
    );
    for target in state.token_aliases.values() {
        ensure!(
            state.states_by_registry_token.contains_key(target),
            "ENSv2 live checkpoint token alias target is absent"
        );
    }
    Ok(state)
}

fn parse_uuid(value: &str, field: &str) -> Result<Uuid> {
    Uuid::parse_str(value).with_context(|| format!("invalid ENSv2 live checkpoint {field} UUID"))
}

fn single_key(value: &str) -> Result<String> {
    serde_json::to_string(value).context("failed to encode ENSv2 live checkpoint item key")
}

fn pair_key(left: &str, right: &str) -> Result<String> {
    serde_json::to_string(&(left, right)).context("failed to encode ENSv2 live checkpoint pair key")
}

fn encoded_item<T: Serialize>(
    item_kind: &'static str,
    item_key: String,
    payload: &T,
) -> Result<EncodedCheckpointItem> {
    Ok(EncodedCheckpointItem {
        item_kind,
        item_key,
        item_payload: encode_value(payload)?,
    })
}
