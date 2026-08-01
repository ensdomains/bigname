use super::super::*;

mod history;
mod pending;
mod preload;

use history::{
    apply_authority_observation_for_history_key, learn_record_raw_name_preimage,
    should_defer_preloaded_namehash_observation,
};
use pending::observation_raw_log_position;
pub(super) use preload::{name_intro_positions_for_raw_logs, preload_name_metadata_for_raw_logs};

pub(super) fn resolver_profile_fact_nodes(
    raw_logs: &[AuthorityRawLogRow],
    event_topics: &AuthorityEventTopics,
) -> Result<Vec<String>> {
    let mut nodes = BTreeSet::<String>::new();
    for raw_log in raw_logs {
        let Some(topic0) = raw_log.topics.first() else {
            continue;
        };
        if resolver_fact_families_for_topic0(&raw_log.source_family, topic0, event_topics)?
            .is_empty()
        {
            continue;
        }
        let Some(node) = raw_log.topics.get(1) else {
            continue;
        };
        nodes.insert(normalize_hex_32(node)?);
    }
    Ok(nodes.into_iter().collect())
}

pub(super) fn apply_authority_raw_log(
    raw_log: &AuthorityRawLogRow,
    histories: &mut BTreeMap<String, NameHistory>,
    reverse_histories: &mut BTreeMap<String, ReverseClaimSourceHistory>,
    registry_child_events: &mut Vec<NormalizedEvent>,
    known_names_by_namehash: &mut HashMap<String, NameMetadata>,
    known_name_refs_by_namehash: &mut HashMap<String, ObservationRef>,
    namehash_to_labelhash: &mut HashMap<String, String>,
    pending_namehash_observations: &mut HashMap<String, Vec<AuthorityObservation>>,
    same_tx_name_intro_positions: &HashMap<String, Vec<RawLogPosition>>,
    migrated_registry_nodes: &mut MigratedRegistryNodes,
    reverse_claim_sources: &HashMap<String, ReverseClaimSource>,
    resolver_profile_gate: &ResolverProfileGate,
    block_index: &CanonicalBlockIndex,
    event_topics: &AuthorityEventTopics,
) -> Result<bool> {
    let migration_guard = registry_migration_guard_action(raw_log, event_topics)?;
    if migration_guard.suppressed_by(migrated_registry_nodes) {
        return Ok(false);
    }

    let resolver_fact_rejected =
        resolver_profile_gate.rejects_resolver_local_fact(raw_log, event_topics)?;
    let retain_reverse_name_observation = resolver_fact_rejected
        && is_ens_v1_reverse_name_observation(raw_log, reverse_claim_sources, event_topics)?;
    if resolver_fact_rejected && !retain_reverse_name_observation {
        if let Some(node) = migration_guard.mark_migrated_node() {
            migrated_registry_nodes.insert(node.to_owned());
        }
        return Ok(false);
    }
    let resolver_fact_profile_status =
        resolver_profile_gate.resolver_local_fact_profile_status(raw_log, event_topics)?;
    let observations = build_authority_observations(raw_log, event_topics)?;
    if let Some(node) = migration_guard.mark_migrated_node() {
        migrated_registry_nodes.insert(node.to_owned());
    }
    if observations.is_empty() {
        return Ok(false);
    }

    for observation in observations {
        if let AuthorityObservation::RegistryOwnerChanged(event) = &observation
            && event.parent_node.is_some()
        {
            registry_child_events.push(build_registry_child_changed_event(event)?);
        }
        apply_authority_observation(
            observation,
            histories,
            reverse_histories,
            known_names_by_namehash,
            known_name_refs_by_namehash,
            namehash_to_labelhash,
            pending_namehash_observations,
            same_tx_name_intro_positions,
            reverse_claim_sources,
            resolver_fact_profile_status,
            block_index,
        )?;
    }
    Ok(true)
}

fn is_ens_v1_reverse_name_observation(
    raw_log: &AuthorityRawLogRow,
    reverse_claim_sources: &HashMap<String, ReverseClaimSource>,
    event_topics: &AuthorityEventTopics,
) -> Result<bool> {
    if raw_log.source_family != SOURCE_FAMILY_ENS_V1_RESOLVER_L1 {
        return Ok(false);
    }
    let (Some(topic0), Some(node)) = (raw_log.topics.first(), raw_log.topics.get(1)) else {
        return Ok(false);
    };
    if !resolver_fact_families_for_topic0(&raw_log.source_family, topic0, event_topics)?
        .contains(&"resolver_record:name")
    {
        return Ok(false);
    }

    Ok(reverse_claim_sources.contains_key(&normalize_hex_32(node)?))
}

fn apply_authority_observation(
    observation: AuthorityObservation,
    histories: &mut BTreeMap<String, NameHistory>,
    reverse_histories: &mut BTreeMap<String, ReverseClaimSourceHistory>,
    known_names_by_namehash: &mut HashMap<String, NameMetadata>,
    known_name_refs_by_namehash: &mut HashMap<String, ObservationRef>,
    namehash_to_labelhash: &mut HashMap<String, String>,
    pending_namehash_observations: &mut HashMap<String, Vec<AuthorityObservation>>,
    same_tx_name_intro_positions: &HashMap<String, Vec<RawLogPosition>>,
    reverse_claim_sources: &HashMap<String, ReverseClaimSource>,
    resolver_fact_profile_status: Option<ResolverFactProfileStatus>,
    block_index: &CanonicalBlockIndex,
) -> Result<()> {
    if let Some(name) = learn_record_raw_name_preimage(
        &observation,
        reverse_claim_sources,
        known_names_by_namehash,
        known_name_refs_by_namehash,
        namehash_to_labelhash,
    ) && let Some(pending) = pending_namehash_observations.remove(&name.namehash)
    {
        let labelhash = name
            .labelhashes
            .first()
            .cloned()
            .context("learned name preimage is missing a first labelhash")?;
        let name_ref = known_name_refs_by_namehash.get(&name.namehash).cloned();
        for pending_observation in pending {
            apply_authority_observation_for_history_key(
                pending_observation,
                &name.namehash,
                &labelhash,
                Some(name.clone()),
                name_ref.clone(),
                histories,
                known_names_by_namehash,
                known_name_refs_by_namehash,
                namehash_to_labelhash,
                pending_namehash_observations,
                same_tx_name_intro_positions,
                block_index,
            )?;
        }
    }

    if let Some(namehash) = observation_namehash(&observation) {
        let normalized_namehash = namehash.to_ascii_lowercase();
        let defer_to_same_tx_intro = should_defer_preloaded_namehash_observation(
            &observation,
            same_tx_name_intro_positions,
            histories,
            namehash_to_labelhash,
        );
        if !defer_to_same_tx_intro
            && let Some(labelhash) = namehash_to_labelhash.get(&normalized_namehash).cloned()
        {
            let known_name = known_names_by_namehash.get(&normalized_namehash).cloned();
            let known_name_ref = known_name_refs_by_namehash
                .get(&normalized_namehash)
                .cloned();
            return apply_authority_observation_for_history_key(
                observation,
                &normalized_namehash,
                &labelhash,
                known_name,
                known_name_ref,
                histories,
                known_names_by_namehash,
                known_name_refs_by_namehash,
                namehash_to_labelhash,
                pending_namehash_observations,
                same_tx_name_intro_positions,
                block_index,
            );
        } else if !defer_to_same_tx_intro
            && let Some(claim_source) = reverse_claim_sources.get(&normalized_namehash).cloned()
        {
            let history = reverse_histories
                .entry(normalized_namehash.clone())
                .or_insert_with(|| ReverseClaimSourceHistory {
                    claim_source,
                    current_resolver: None,
                    current_record_version: None,
                    events: Vec::new(),
                });
            apply_reverse_claim_source_observation(
                history,
                observation,
                resolver_fact_profile_status,
            )?;
            return Ok(());
        } else {
            pending_namehash_observations
                .entry(normalized_namehash.clone())
                .or_default()
                .push(observation);
            return Ok(());
        }
    } else {
        let LabelhashObservationTarget {
            history_key,
            labelhash,
            known_name,
            known_name_ref,
        } = labelhash_observation_target(&observation, known_names_by_namehash)?;
        return apply_authority_observation_for_history_key(
            observation,
            &history_key,
            &labelhash,
            known_name,
            known_name_ref,
            histories,
            known_names_by_namehash,
            known_name_refs_by_namehash,
            namehash_to_labelhash,
            pending_namehash_observations,
            same_tx_name_intro_positions,
            block_index,
        );
    }
}

struct LabelhashObservationTarget {
    history_key: String,
    labelhash: String,
    known_name: Option<NameMetadata>,
    known_name_ref: Option<ObservationRef>,
}

fn labelhash_observation_target(
    observation: &AuthorityObservation,
    known_names_by_namehash: &HashMap<String, NameMetadata>,
) -> Result<LabelhashObservationTarget> {
    match observation {
        AuthorityObservation::RegistrationGranted(value) => {
            let name = observe_registrar_name_with_reference(
                &value.label,
                &value.reference,
                ENS_NORMALIZER_VERSION,
            )?;
            Ok(LabelhashObservationTarget {
                history_key: name.namehash.clone(),
                labelhash: value.labelhash.clone(),
                known_name: Some(name),
                known_name_ref: Some(value.reference.clone()),
            })
        }
        AuthorityObservation::RegistrationRenewed(value) => {
            let name = observe_registrar_name_with_reference(
                &value.label,
                &value.reference,
                ENS_NORMALIZER_VERSION,
            )?;
            Ok(LabelhashObservationTarget {
                history_key: name.namehash.clone(),
                labelhash: value.labelhash.clone(),
                known_name: Some(name),
                known_name_ref: Some(value.reference.clone()),
            })
        }
        AuthorityObservation::TokenTransferred(value) => {
            let history_key = registrar_child_namehash(&value.reference, &value.labelhash)?;
            Ok(LabelhashObservationTarget {
                known_name: known_names_by_namehash.get(&history_key).cloned(),
                known_name_ref: None,
                history_key,
                labelhash: value.labelhash.clone(),
            })
        }
        AuthorityObservation::RegistryOwnerChanged(value) => {
            let history_key = if let Some(namehash) = value.namehash.clone() {
                namehash
            } else {
                registrar_child_namehash(&value.reference, &value.labelhash)?
            };
            Ok(LabelhashObservationTarget {
                known_name: known_names_by_namehash.get(&history_key).cloned(),
                known_name_ref: None,
                history_key,
                labelhash: value.labelhash.clone(),
            })
        }
        AuthorityObservation::WrapperNameWrapped(value) => {
            let labelhash = value
                .name
                .labelhashes
                .first()
                .cloned()
                .context("wrapper name observation must include a first labelhash")?;
            Ok(LabelhashObservationTarget {
                history_key: value.name.namehash.clone(),
                labelhash,
                known_name: Some(value.name.clone()),
                known_name_ref: Some(value.reference.clone()),
            })
        }
        AuthorityObservation::ResolverChanged(_)
        | AuthorityObservation::RecordChanged(_)
        | AuthorityObservation::RecordVersionChanged(_)
        | AuthorityObservation::WrapperNameUnwrapped(_)
        | AuthorityObservation::WrapperFusesSet(_)
        | AuthorityObservation::WrapperExpiryExtended(_)
        | AuthorityObservation::WrapperTokenTransferred(_) => {
            unreachable!("namehash observations must be resolved before use")
        }
    }
}

fn registrar_child_namehash(reference: &ObservationRef, labelhash: &str) -> Result<String> {
    let profile =
        authority_profile_for_source_family(&reference.source_family).with_context(|| {
            format!(
                "unsupported authority source family {}",
                reference.source_family
            )
        })?;
    child_namehash_hex(&profile.root_node(), labelhash)
}
