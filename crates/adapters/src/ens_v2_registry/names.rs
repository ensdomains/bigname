use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    time::Duration,
};

use anyhow::{Result, ensure};
use bigname_domain::normalization::{ENS_NORMALIZER_VERSION, normalize_name};
use bigname_manifests::WatchedContractSource;
use bigname_storage::SurfaceBinding;
use serde_json::json;
use sqlx::types::Uuid;

use super::{
    constants::*,
    live::RegistryReplayState,
    types::{ActiveEmitter, NameMetadata, ObservationRef, RegistryNameState},
    util::{dns_encode, event_position_timestamp, hex_string, keccak256_bytes, namehash_bytes},
};

pub(super) type RegistryTokenKey = (String, String);
pub(super) type RegistryNameKey = (String, String);

pub(super) fn initial_registry_suffixes(emitters: &[ActiveEmitter]) -> HashMap<String, String> {
    let mut suffixes = HashMap::new();
    for emitter in emitters {
        if emitter.source_family == SOURCE_FAMILY_ENS_V2_ROOT_L1 {
            suffixes.insert(emitter.address.clone(), String::new());
        } else if emitter.source_family == SOURCE_FAMILY_ENS_V2_REGISTRY_L1
            && emitter.source != WatchedContractSource::DiscoveryEdge
        {
            suffixes.insert(emitter.address.clone(), "eth".to_owned());
        }
    }
    suffixes
}

pub(super) fn name_under_registry(
    registry_address: &str,
    label: &str,
    registry_suffix_by_address: &HashMap<String, String>,
) -> Option<String> {
    if label.is_empty() || label.contains('.') {
        return None;
    }
    let suffix = registry_suffix_by_address.get(registry_address)?;
    if suffix.is_empty() {
        Some(label.to_owned())
    } else {
        Some(format!("{label}.{suffix}"))
    }
}

pub(super) fn observe_name(
    namespace: &str,
    full_name: &str,
    _reference: &ObservationRef,
    _label: &str,
) -> Result<NameMetadata> {
    let normalized = normalize_name(full_name)?;
    let labels = normalized
        .normalized_labels
        .iter()
        .map(|label| label.as_bytes().to_vec())
        .collect::<Vec<_>>();
    let dns_encoded_name = dns_encode(&labels)?;
    let labelhashes = labels
        .iter()
        .map(|label| format!("0x{}", hex_string(keccak256_bytes(label))))
        .collect::<Vec<_>>();
    Ok(NameMetadata {
        namespace: namespace.to_owned(),
        logical_name_id: format!("{namespace}:{}", normalized.normalized_name),
        input_name: normalized.input_name,
        canonical_display_name: normalized.canonical_display_name,
        normalized_name: normalized.normalized_name,
        dns_encoded_name,
        namehash: format!("0x{}", hex_string(namehash_bytes(&labels))),
        labelhashes,
        normalizer_version: ENS_NORMALIZER_VERSION.to_owned(),
    })
}

pub(super) fn state_for_token_mut<'a>(
    states: &'a mut BTreeMap<RegistryTokenKey, RegistryNameState>,
    aliases: &HashMap<RegistryTokenKey, RegistryTokenKey>,
    registry: &str,
    token_id: &str,
) -> Option<&'a mut RegistryNameState> {
    let key = resolve_token_key(aliases, registry, token_id)
        .unwrap_or_else(|| (registry.to_owned(), token_id.to_owned()));
    states.get_mut(&key)
}

pub(super) fn resolve_token_key(
    aliases: &HashMap<RegistryTokenKey, RegistryTokenKey>,
    registry: &str,
    token_id: &str,
) -> Option<RegistryTokenKey> {
    aliases
        .get(&(registry.to_owned(), token_id.to_owned()))
        .cloned()
}

pub(super) fn take_state_for_unregister(
    states: &mut BTreeMap<RegistryTokenKey, RegistryNameState>,
    aliases: &mut HashMap<RegistryTokenKey, RegistryTokenKey>,
    state_keys_by_registry_namehash: &mut HashMap<RegistryNameKey, BTreeSet<RegistryTokenKey>>,
    current_token_alias_by_canonical_key: &mut HashMap<RegistryTokenKey, RegistryTokenKey>,
    registry: &str,
    token_id: &str,
) -> Option<RegistryNameState> {
    let canonical_key = resolve_token_key(aliases, registry, token_id)
        .unwrap_or_else(|| (registry.to_owned(), token_id.to_owned()));
    let state = states.remove(&canonical_key)?;
    remove_state_key_from_name_index(state_keys_by_registry_namehash, &canonical_key, &state);
    remove_token_alias(
        aliases,
        current_token_alias_by_canonical_key,
        &canonical_key,
    );
    Some(state)
}

pub(super) fn take_states_for_name(
    states: &mut BTreeMap<RegistryTokenKey, RegistryNameState>,
    aliases: &mut HashMap<RegistryTokenKey, RegistryTokenKey>,
    state_keys_by_registry_namehash: &mut HashMap<RegistryNameKey, BTreeSet<RegistryTokenKey>>,
    current_token_alias_by_canonical_key: &mut HashMap<RegistryTokenKey, RegistryTokenKey>,
    registry: &str,
    namehash: &str,
) -> Vec<RegistryNameState> {
    state_keys_by_registry_namehash
        .remove(&(registry.to_owned(), namehash.to_owned()))
        .into_iter()
        .flatten()
        .filter_map(|key| {
            remove_token_alias(aliases, current_token_alias_by_canonical_key, &key);
            states.remove(&key)
        })
        .collect()
}

pub(super) fn replace_token_alias(
    aliases: &mut HashMap<RegistryTokenKey, RegistryTokenKey>,
    current_token_alias_by_canonical_key: &mut HashMap<RegistryTokenKey, RegistryTokenKey>,
    registry: &str,
    token_id: &str,
    canonical_key: &RegistryTokenKey,
) {
    let current_alias = (registry.to_owned(), token_id.to_owned());
    if let Some(previous_alias) =
        current_token_alias_by_canonical_key.insert(canonical_key.clone(), current_alias.clone())
        && previous_alias != current_alias
    {
        aliases.remove(&previous_alias);
    }
    aliases.insert(current_alias, canonical_key.clone());
}

pub(super) fn insert_registry_name_state(
    states: &mut BTreeMap<RegistryTokenKey, RegistryNameState>,
    state_keys_by_registry_namehash: &mut HashMap<RegistryNameKey, BTreeSet<RegistryTokenKey>>,
    key: RegistryTokenKey,
    state: RegistryNameState,
) {
    let name_key = (state.registry_address.clone(), state.name.namehash.clone());
    if let Some(previous) = states.insert(key.clone(), state) {
        remove_state_key_from_name_index(state_keys_by_registry_namehash, &key, &previous);
    }
    state_keys_by_registry_namehash
        .entry(name_key)
        .or_default()
        .insert(key);
}

pub(super) fn rebuild_registry_state_indexes(state: &mut RegistryReplayState) -> Result<()> {
    state.state_keys_by_registry_namehash.clear();
    state.current_token_alias_by_canonical_key.clear();
    for (key, value) in &state.states_by_registry_token {
        ensure!(
            key.0 == value.registry_address,
            "ENSv2 registry-state key address does not match its state"
        );
        state
            .state_keys_by_registry_namehash
            .entry((value.registry_address.clone(), value.name.namehash.clone()))
            .or_default()
            .insert(key.clone());
    }
    for (alias, canonical_key) in &state.token_aliases {
        ensure!(
            state.states_by_registry_token.contains_key(canonical_key),
            "ENSv2 token alias target is absent from registry state"
        );
        ensure!(
            state
                .current_token_alias_by_canonical_key
                .insert(canonical_key.clone(), alias.clone())
                .is_none(),
            "ENSv2 registry state has multiple current aliases for one token"
        );
    }
    Ok(())
}

pub(super) fn discovery_observation_key(registry: &str, token_id: &str) -> String {
    format!("{registry}:{}", versionless_token_id(token_id))
}

pub(super) fn versionless_token_id(token_id: &str) -> String {
    token_id
        .strip_prefix("0x")
        .filter(|digits| digits.len() == 64 && digits.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .map(|digits| format!("0x{}00000000", &digits[..56]))
        .unwrap_or_else(|| token_id.to_ascii_lowercase())
}

fn remove_token_alias(
    aliases: &mut HashMap<RegistryTokenKey, RegistryTokenKey>,
    current_token_alias_by_canonical_key: &mut HashMap<RegistryTokenKey, RegistryTokenKey>,
    canonical_key: &RegistryTokenKey,
) {
    if let Some(alias) = current_token_alias_by_canonical_key.remove(canonical_key) {
        aliases.remove(&alias);
    }
}

fn remove_state_key_from_name_index(
    index: &mut HashMap<RegistryNameKey, BTreeSet<RegistryTokenKey>>,
    key: &RegistryTokenKey,
    state: &RegistryNameState,
) {
    let name_key = (state.registry_address.clone(), state.name.namehash.clone());
    if let Some(keys) = index.get_mut(&name_key) {
        keys.remove(key);
        if keys.is_empty() {
            index.remove(&name_key);
        }
    }
}

pub(super) fn remember_linked_resource_state(
    linked_resource_states: &mut BTreeMap<Uuid, RegistryNameState>,
    state: &RegistryNameState,
) {
    if let Some(link) = state.resource.as_ref() {
        linked_resource_states.insert(link.resource_id, state.clone());
    }
}

pub(super) fn closed_surface_binding_for_terminal(
    state: &RegistryNameState,
    reference: &ObservationRef,
) -> Option<SurfaceBinding> {
    let link = state.resource.as_ref()?;
    let active_from = event_position_timestamp(&link.linked_ref);
    let observed_close = event_position_timestamp(reference);
    let active_to = observed_close.max(active_from + Duration::from_micros(1));
    Some(SurfaceBinding {
        surface_binding_id: link.surface_binding_id,
        logical_name_id: state.name.logical_name_id.clone(),
        resource_id: link.resource_id,
        binding_kind: state.binding_kind,
        active_from,
        active_to: Some(active_to),
        chain_id: link.linked_ref.chain_id.clone(),
        block_hash: link.linked_ref.block_hash.clone(),
        block_number: link.linked_ref.block_number,
        provenance: json!({
            "adapter": DERIVATION_KIND_ENS_V2_REGISTRY_RESOURCE_SURFACE,
            "binding_kind": state.binding_kind.as_str(),
            "logical_name_id": state.name.logical_name_id,
            "upstream_resource": link.upstream_resource,
            "token_id": link.observed_token_id,
            "current_token_id": link.observed_token_id,
        }),
        canonicality_state: reference.canonicality_state,
    })
}

pub(super) fn deactivate_registry_suffix(
    registry_suffix_by_address: &mut HashMap<String, String>,
    registry_address: Option<&str>,
    expected_suffix: &str,
) {
    let Some(registry_address) = registry_address else {
        return;
    };
    if registry_address == ZERO_ADDRESS {
        return;
    }
    if registry_suffix_by_address
        .get(registry_address)
        .is_some_and(|suffix| suffix == expected_suffix)
    {
        registry_suffix_by_address.remove(registry_address);
    }
}
