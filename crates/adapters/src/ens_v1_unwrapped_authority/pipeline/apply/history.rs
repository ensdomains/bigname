use super::pending::{
    PendingObservationFlush, drain_pending_namehash_observations, is_name_intro_observation,
    observation_raw_log_position, remember_known_name,
    should_clear_stale_wrapper_before_registration_grant,
};
use super::*;

pub(super) fn apply_authority_observation_for_history_key(
    observation: AuthorityObservation,
    history_key: &str,
    labelhash: &str,
    known_name: Option<NameMetadata>,
    known_name_ref: Option<ObservationRef>,
    histories: &mut BTreeMap<String, NameHistory>,
    known_names_by_namehash: &mut HashMap<String, NameMetadata>,
    known_name_refs_by_namehash: &mut HashMap<String, ObservationRef>,
    namehash_to_labelhash: &mut HashMap<String, String>,
    pending_namehash_observations: &mut HashMap<String, Vec<AuthorityObservation>>,
    same_tx_name_intro_positions: &HashMap<String, Vec<RawLogPosition>>,
    block_index: &CanonicalBlockIndex,
    mut checkpoint_delta: Option<&mut UnwrappedAuthorityReplayCheckpointDelta>,
) -> Result<()> {
    if let Some(delta) = checkpoint_delta.as_deref_mut() {
        delta.mark_history(history_key);
    }
    let introduces_name = is_name_intro_observation(&observation);
    {
        let history = histories
            .entry(history_key.to_owned())
            .or_insert_with(|| NameHistory {
                name: known_name.clone(),
                namehash: history_key.to_owned(),
                labelhash: labelhash.to_owned(),
                first_name_ref: known_name_ref.clone(),
                current_registration: None,
                superseded_registration: None,
                current_wrapper_key: None,
                wrapper_authorities: BTreeMap::new(),
                current_registry_owner: None,
                current_resolver: None,
                current_record_version: None,
                open_binding: None,
                bindings: Vec::new(),
                events: Vec::new(),
                registry_resource_anchor: None,
                latest_registry_owner_ref: None,
                latest_registry_owner_before_registration: None,
            });
        if history.name.is_none() {
            history.name = known_name;
            if let Some(reference) = known_name_ref.clone() {
                history.first_name_ref.get_or_insert(reference);
            }
        }
        if history.namehash.is_empty() {
            history.namehash = history_key.to_owned();
        }
    }

    if introduces_name
        && let Some(name) = histories
            .get(history_key)
            .and_then(|history| history.name.clone())
    {
        remember_known_name(
            &name,
            labelhash,
            known_name_ref.as_ref(),
            known_names_by_namehash,
            known_name_refs_by_namehash,
            namehash_to_labelhash,
            checkpoint_delta.as_deref_mut(),
        );
        flush_pending_namehash_observations(
            &name,
            labelhash,
            PendingObservationFlush::BeforeNameIntro(&observation),
            histories,
            known_names_by_namehash,
            known_name_refs_by_namehash,
            namehash_to_labelhash,
            pending_namehash_observations,
            same_tx_name_intro_positions,
            block_index,
            checkpoint_delta.as_deref_mut(),
        )?;
    }

    let learned_name = {
        let history = histories
            .get_mut(history_key)
            .with_context(|| format!("missing ENSv1 authority history {history_key}"))?;
        settle_due_registration_release(
            history,
            &observation_reference(&observation).as_boundary_ref(),
        )?;
        if should_clear_stale_wrapper_before_registration_grant(
            history,
            &observation,
            same_tx_name_intro_positions,
        )? {
            clear_stale_wrapper_authority_for_registration_grant(
                history,
                observation_reference(&observation),
            )?;
        }
        apply_observation(history, observation, block_index)?;
        history.name.clone()
    };
    if introduces_name && let Some(name) = learned_name {
        remember_known_name(
            &name,
            labelhash,
            known_name_ref.as_ref(),
            known_names_by_namehash,
            known_name_refs_by_namehash,
            namehash_to_labelhash,
            checkpoint_delta.as_deref_mut(),
        );
        flush_pending_namehash_observations(
            &name,
            labelhash,
            PendingObservationFlush::AfterNameIntro,
            histories,
            known_names_by_namehash,
            known_name_refs_by_namehash,
            namehash_to_labelhash,
            pending_namehash_observations,
            same_tx_name_intro_positions,
            block_index,
            checkpoint_delta.as_deref_mut(),
        )?;
    }
    Ok(())
}

fn flush_pending_namehash_observations(
    name: &NameMetadata,
    labelhash: &str,
    mode: PendingObservationFlush<'_>,
    histories: &mut BTreeMap<String, NameHistory>,
    known_names_by_namehash: &mut HashMap<String, NameMetadata>,
    known_name_refs_by_namehash: &mut HashMap<String, ObservationRef>,
    namehash_to_labelhash: &mut HashMap<String, String>,
    pending_namehash_observations: &mut HashMap<String, Vec<AuthorityObservation>>,
    same_tx_name_intro_positions: &HashMap<String, Vec<RawLogPosition>>,
    block_index: &CanonicalBlockIndex,
    mut checkpoint_delta: Option<&mut UnwrappedAuthorityReplayCheckpointDelta>,
) -> Result<()> {
    let selected = drain_pending_namehash_observations(
        &name.namehash,
        mode,
        pending_namehash_observations,
        checkpoint_delta.as_deref_mut(),
    );

    let name_ref = known_name_refs_by_namehash.get(&name.namehash).cloned();
    for pending_observation in selected {
        apply_authority_observation_for_history_key(
            pending_observation,
            &name.namehash,
            labelhash,
            Some(name.clone()),
            name_ref.clone(),
            histories,
            known_names_by_namehash,
            known_name_refs_by_namehash,
            namehash_to_labelhash,
            pending_namehash_observations,
            same_tx_name_intro_positions,
            block_index,
            checkpoint_delta.as_deref_mut(),
        )?;
    }
    Ok(())
}

pub(super) fn learn_record_raw_name_preimage(
    observation: &AuthorityObservation,
    reverse_claim_sources: &HashMap<String, ReverseClaimSource>,
    known_names_by_namehash: &mut HashMap<String, NameMetadata>,
    known_name_refs_by_namehash: &mut HashMap<String, ObservationRef>,
    namehash_to_labelhash: &mut HashMap<String, String>,
    mut checkpoint_delta: Option<&mut UnwrappedAuthorityReplayCheckpointDelta>,
) -> Option<NameMetadata> {
    let AuthorityObservation::RecordChanged(event) = observation else {
        return None;
    };
    if event.selector.record_key != "name" {
        return None;
    }
    if !reverse_claim_sources.contains_key(&event.namehash) {
        return None;
    }
    let raw_name = event.raw_name.as_deref()?;
    let name = observe_text_name_with_reference(raw_name, &event.reference, ENS_NORMALIZER_VERSION)
        .ok()?;
    let labelhash = name.labelhashes.first()?.clone();
    namehash_to_labelhash
        .entry(name.namehash.clone())
        .or_insert(labelhash);
    known_name_refs_by_namehash
        .entry(name.namehash.clone())
        .or_insert_with(|| event.reference.clone());
    known_names_by_namehash
        .entry(name.namehash.clone())
        .or_insert_with(|| name.clone());
    if let Some(delta) = checkpoint_delta.as_deref_mut() {
        delta.mark_namehash_labelhash(name.namehash.clone());
        delta.mark_known_name_ref(name.namehash.clone());
        delta.mark_known_name(name.namehash.clone());
    }
    Some(name)
}

pub(super) fn should_defer_preloaded_namehash_observation(
    observation: &AuthorityObservation,
    same_tx_name_intro_positions: &HashMap<String, Vec<RawLogPosition>>,
    histories: &BTreeMap<String, NameHistory>,
    namehash_to_labelhash: &HashMap<String, String>,
) -> bool {
    let Some(namehash) = observation_namehash(observation) else {
        return false;
    };
    let Some(position) = observation_raw_log_position(observation) else {
        return false;
    };
    let normalized_namehash = namehash.to_ascii_lowercase();
    let Some(intro_positions) = same_tx_name_intro_positions.get(&normalized_namehash) else {
        return false;
    };
    let is_later_same_tx_intro = |intro: &RawLogPosition| {
        intro.block_hash == position.block_hash
            && intro.transaction_hash == position.transaction_hash
            && position.log_index < intro.log_index
    };
    let has_later_same_tx_intro = intro_positions
        .iter()
        .any(|intro| is_later_same_tx_intro(intro));
    if !has_later_same_tx_intro {
        return false;
    }
    // Full replay holds the controller's registry-owner and resolver setup
    // until the later registration grant, even when an older registry
    // authority exists. Restricted replay must do the same after preloading
    // that authority. Record writes, renewals, and wraps retain their
    // event-time authority.
    if matches!(
        observation,
        AuthorityObservation::RegistryOwnerChanged(_) | AuthorityObservation::ResolverChanged(_)
    ) && intro_positions
        .iter()
        .any(|intro| is_later_same_tx_intro(intro) && intro.is_registration_granted)
    {
        return true;
    }
    if namehash_to_labelhash.contains_key(&normalized_namehash)
        && let Some(history) = histories.get(&normalized_namehash)
        && history_has_authority_at_observation(history, observation)
    {
        return false;
    }
    true
}

fn history_has_authority_at_observation(
    history: &NameHistory,
    observation: &AuthorityObservation,
) -> bool {
    let reference = observation_reference(observation);
    if history.current_wrapper_key.is_some() {
        return active_anchor_for_history(history, &reference.chain_id).is_some();
    }
    if let Some(registration) = history.current_registration.as_ref() {
        if registration
            .release_ref
            .as_ref()
            .is_some_and(|release_ref| release_ref.block_timestamp <= reference.block_timestamp)
        {
            return registry_anchor_for_history(history, &reference.chain_id, &history.labelhash)
                .is_some();
        }
        return true;
    }
    if registry_anchor_for_history(history, &reference.chain_id, &history.labelhash).is_none() {
        return false;
    }
    registry_authority_started_before_observation(history, reference)
}

fn registry_authority_started_before_observation(
    history: &NameHistory,
    reference: &ObservationRef,
) -> bool {
    history
        .registry_resource_anchor
        .as_ref()
        .is_some_and(|anchor| anchor.block_number < reference.block_number)
        || nonzero_address(history.current_resolver.as_deref()).is_some()
}
