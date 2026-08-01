use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    time::Duration,
};

use anyhow::Result;
use bigname_domain::normalization::{ENS_NORMALIZER_VERSION, normalize_name};
use bigname_manifests::WatchedContractSource;
use bigname_storage::SurfaceBinding;
use serde_json::json;
use sqlx::types::Uuid;

use super::{
    constants::*,
    types::{
        ActiveEmitter, CurrentParentClaim, CurrentSubregistryLink, NameMetadata, ObservationRef,
        RegistryNameState,
    },
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
    root_registry_addresses: &HashSet<String>,
    current_subregistry_by_parent_label: &HashMap<(String, String), CurrentSubregistryLink>,
    current_parent_claim_by_registry: &HashMap<String, CurrentParentClaim>,
    reference: &ObservationRef,
) -> Option<String> {
    let timestamp = u64::try_from(reference.block_timestamp.unix_timestamp()).ok()?;
    // Top-down lookup follows `getSubregistry(label)` at every ancestor, and
    // each call returns zero once that label expires. (upstream: .refs/ens_v2/contracts/src/universalResolver/libraries/LibRegistry.sol:L126 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/universalResolver/libraries/LibRegistry.sol:L129 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L251 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L253 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L625 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L626 @ ens_v2@ccaeb58)
    if !registry_suffix_is_reachable_at(
        registry_address,
        timestamp,
        registry_suffix_by_address,
        root_registry_addresses,
        current_subregistry_by_parent_label,
        current_parent_claim_by_registry,
        &mut HashSet::new(),
    ) {
        return None;
    }
    let suffix = registry_suffix_by_address.get(registry_address)?;
    name_with_suffix(label, suffix)
}

pub(super) fn name_with_suffix(label: &str, suffix: &str) -> Option<String> {
    if label.is_empty() || label.contains('.') {
        return None;
    }
    if suffix.is_empty() {
        Some(label.to_owned())
    } else {
        Some(format!("{label}.{suffix}"))
    }
}

pub(super) fn normalized_label(label: &str) -> Option<String> {
    if label.is_empty() || label.contains('.') {
        return None;
    }
    normalize_name(label).ok().map(|name| name.normalized_name)
}

fn normalized_name_with_suffix(label: &str, suffix: &str) -> Option<String> {
    let full_name = name_with_suffix(label, suffix)?;
    normalize_name(&full_name)
        .ok()
        .map(|name| name.normalized_name)
}

fn registry_suffix_is_reachable_at(
    registry_address: &str,
    timestamp: u64,
    registry_suffix_by_address: &HashMap<String, String>,
    root_registry_addresses: &HashSet<String>,
    current_subregistry_by_parent_label: &HashMap<(String, String), CurrentSubregistryLink>,
    current_parent_claim_by_registry: &HashMap<String, CurrentParentClaim>,
    visiting: &mut HashSet<String>,
) -> bool {
    let Some(registry_suffix) = registry_suffix_by_address.get(registry_address) else {
        return false;
    };
    if root_registry_addresses.contains(registry_address) {
        return true;
    }
    if !visiting.insert(registry_address.to_owned()) {
        return false;
    }
    let reachable = current_parent_claim_by_registry
        .get(registry_address)
        .and_then(|claim| {
            current_subregistry_by_parent_label
                .get(&(claim.parent.clone(), claim.label.clone()))
                .map(|link| (claim, link))
        })
        .is_some_and(|(claim, link)| {
            if link.subregistry != registry_address
                || link.expiry.is_none_or(|expiry| timestamp >= expiry)
            {
                return false;
            }
            let Some(parent_suffix) = registry_suffix_by_address.get(&claim.parent) else {
                return false;
            };
            if normalized_name_with_suffix(&claim.label, parent_suffix).as_deref()
                != Some(registry_suffix.as_str())
            {
                return false;
            }
            registry_suffix_is_reachable_at(
                &claim.parent,
                timestamp,
                registry_suffix_by_address,
                root_registry_addresses,
                current_subregistry_by_parent_label,
                current_parent_claim_by_registry,
                visiting,
            )
        });
    visiting.remove(registry_address);
    reachable
}

pub(super) fn recompute_registry_suffixes(
    registry_suffix_by_address: &mut HashMap<String, String>,
    root_registry_addresses: &HashSet<String>,
    current_subregistry_by_parent_label: &HashMap<(String, String), CurrentSubregistryLink>,
    current_parent_claim_by_registry: &HashMap<String, CurrentParentClaim>,
    reference: &ObservationRef,
) {
    let root_suffixes = registry_suffix_by_address
        .iter()
        .filter(|(address, _)| root_registry_addresses.contains(*address))
        .map(|(address, suffix)| (address.clone(), suffix.clone()))
        .collect::<HashMap<_, _>>();
    *registry_suffix_by_address = root_suffixes;
    let Ok(timestamp) = u64::try_from(reference.block_timestamp.unix_timestamp()) else {
        return;
    };

    loop {
        let next = current_parent_claim_by_registry
            .iter()
            .filter(|(registry, _)| !registry_suffix_by_address.contains_key(*registry))
            .filter_map(|(registry, claim)| {
                // `setParent` replaces the child's current parent and label claim. (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L171 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L175 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L176 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L177 @ ens_v2@ccaeb58)
                // Canonical naming also requires the claimed parent's CURRENT
                // subregistry pointer to lead back to this child. (upstream: .refs/ens_v2/contracts/src/universalResolver/libraries/LibRegistry.sol:L82 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/universalResolver/libraries/LibRegistry.sol:L86 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/universalResolver/libraries/LibRegistry.sol:L87 @ ens_v2@ccaeb58) (upstream: .refs/ens_v2/contracts/src/universalResolver/libraries/LibRegistry.sol:L88 @ ens_v2@ccaeb58)
                let link = current_subregistry_by_parent_label
                    .get(&(claim.parent.clone(), claim.label.clone()))?;
                if link.subregistry != *registry
                    || link.expiry.is_none_or(|expiry| timestamp >= expiry)
                {
                    return None;
                }
                let parent_suffix = registry_suffix_by_address.get(&claim.parent)?;
                let suffix = normalized_name_with_suffix(&claim.label, parent_suffix)?;
                Some((registry.clone(), suffix))
            })
            .collect::<Vec<_>>();
        if next.is_empty() {
            break;
        }
        for (registry, suffix) in next {
            registry_suffix_by_address.insert(registry, suffix);
        }
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

pub(super) fn take_states_for_replacement(
    states: &mut BTreeMap<RegistryTokenKey, RegistryNameState>,
    aliases: &mut HashMap<RegistryTokenKey, RegistryTokenKey>,
    state_keys_by_registry_namehash: &mut HashMap<RegistryNameKey, BTreeSet<RegistryTokenKey>>,
    current_token_alias_by_canonical_key: &mut HashMap<RegistryTokenKey, RegistryTokenKey>,
    registry: &str,
    label: &str,
    namehash: Option<&str>,
) -> Vec<RegistryNameState> {
    let normalized = normalized_label(label);
    let keys = states
        .iter()
        .filter(|(_, state)| {
            state.registry_address == registry
                && (state.label == label
                    || normalized.as_ref().is_some_and(|normalized| {
                        normalized_label(&state.label).as_ref() == Some(normalized)
                    })
                    || namehash.is_some_and(|namehash| state.name.namehash == namehash))
        })
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    keys.into_iter()
        .filter_map(|key| {
            remove_token_alias(aliases, current_token_alias_by_canonical_key, &key);
            let state = states.remove(&key)?;
            remove_state_key_from_name_index(state_keys_by_registry_namehash, &key, &state);
            Some(state)
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

pub(super) fn unindex_registry_name_state(
    index: &mut HashMap<RegistryNameKey, BTreeSet<RegistryTokenKey>>,
    key: &RegistryTokenKey,
    registry_address: &str,
    namehash: &str,
) {
    let name_key = (registry_address.to_owned(), namehash.to_owned());
    if let Some(keys) = index.get_mut(&name_key) {
        keys.remove(key);
        if keys.is_empty() {
            index.remove(&name_key);
        }
    }
}

pub(super) fn index_registry_name_state(
    index: &mut HashMap<RegistryNameKey, BTreeSet<RegistryTokenKey>>,
    key: &RegistryTokenKey,
    registry_address: &str,
    namehash: &str,
) {
    index
        .entry((registry_address.to_owned(), namehash.to_owned()))
        .or_default()
        .insert(key.clone());
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
    let active_from = event_position_timestamp(&link.binding_ref);
    let observed_close = event_position_timestamp(reference);
    let active_to = observed_close.max(active_from + Duration::from_micros(1));
    Some(SurfaceBinding {
        surface_binding_id: link.surface_binding_id,
        logical_name_id: state.name.logical_name_id.clone(),
        resource_id: link.resource_id,
        binding_kind: state.binding_kind,
        active_from,
        active_to: Some(active_to),
        chain_id: link.binding_ref.chain_id.clone(),
        block_hash: link.binding_ref.block_hash.clone(),
        block_number: link.binding_ref.block_number,
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
