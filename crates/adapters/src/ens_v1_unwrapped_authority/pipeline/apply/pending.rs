use super::*;

pub(super) enum PendingObservationFlush<'a> {
    BeforeNameIntro(&'a AuthorityObservation),
    AfterNameIntro,
}

pub(super) fn is_name_intro_observation(observation: &AuthorityObservation) -> bool {
    matches!(
        observation,
        AuthorityObservation::RegistrationGranted(_)
            | AuthorityObservation::RegistrationRenewed(_)
            | AuthorityObservation::WrapperNameWrapped(_)
    )
}

pub(super) fn remember_known_name(
    name: &NameMetadata,
    labelhash: &str,
    known_name_ref: Option<&ObservationRef>,
    known_names_by_namehash: &mut HashMap<String, NameMetadata>,
    known_name_refs_by_namehash: &mut HashMap<String, ObservationRef>,
    namehash_to_labelhash: &mut HashMap<String, String>,
    mut checkpoint_delta: Option<&mut UnwrappedAuthorityReplayCheckpointDelta>,
) {
    namehash_to_labelhash.insert(name.namehash.clone(), labelhash.to_owned());
    known_names_by_namehash
        .entry(name.namehash.clone())
        .or_insert_with(|| name.clone());
    if let Some(reference) = known_name_ref {
        known_name_refs_by_namehash
            .entry(name.namehash.clone())
            .or_insert_with(|| reference.clone());
    }
    if let Some(delta) = checkpoint_delta.as_deref_mut() {
        delta.mark_namehash_labelhash(name.namehash.clone());
        delta.mark_known_name(name.namehash.clone());
        if known_name_ref.is_some() {
            delta.mark_known_name_ref(name.namehash.clone());
        }
    }
}

pub(super) fn drain_pending_namehash_observations(
    namehash: &str,
    mode: PendingObservationFlush<'_>,
    pending_namehash_observations: &mut HashMap<String, Vec<AuthorityObservation>>,
    mut checkpoint_delta: Option<&mut UnwrappedAuthorityReplayCheckpointDelta>,
) -> Vec<AuthorityObservation> {
    let Some(pending) = pending_namehash_observations.remove(namehash) else {
        return Vec::new();
    };
    if let Some(delta) = checkpoint_delta.as_deref_mut() {
        delta.mark_pending_observations(namehash.to_owned());
    }

    let last_same_tx_registry_owner_index = last_same_tx_registry_owner_index(&pending, &mode);
    let mut selected = Vec::new();
    let mut remaining = Vec::new();
    for (index, pending_observation) in pending.into_iter().enumerate() {
        if should_flush_pending_observation(
            &pending_observation,
            &mode,
            index,
            last_same_tx_registry_owner_index,
        ) {
            selected.push(pending_observation);
        } else if should_keep_pending_observation(
            &pending_observation,
            &mode,
            index,
            last_same_tx_registry_owner_index,
        ) {
            remaining.push(pending_observation);
        } else {
            continue;
        }
    }
    if !remaining.is_empty() {
        pending_namehash_observations.insert(namehash.to_owned(), remaining);
    }
    selected
}

fn should_flush_pending_observation(
    observation: &AuthorityObservation,
    mode: &PendingObservationFlush<'_>,
    _index: usize,
    _last_same_tx_registry_owner_index: Option<usize>,
) -> bool {
    match mode {
        PendingObservationFlush::AfterNameIntro => true,
        PendingObservationFlush::BeforeNameIntro(intro) => {
            if !is_same_transaction_before(observation, intro) {
                return true;
            }
            false
        }
    }
}

fn should_keep_pending_observation(
    observation: &AuthorityObservation,
    mode: &PendingObservationFlush<'_>,
    index: usize,
    last_same_tx_registry_owner_index: Option<usize>,
) -> bool {
    match mode {
        PendingObservationFlush::AfterNameIntro => false,
        PendingObservationFlush::BeforeNameIntro(intro) => {
            if !is_same_transaction_before(observation, intro) {
                return false;
            }
            if last_same_tx_registry_owner_index.is_some_and(|last_index| {
                index < last_index
                    && matches!(observation, AuthorityObservation::RegistryOwnerChanged(_))
            }) {
                return false;
            }
            true
        }
    }
}

fn last_same_tx_registry_owner_index(
    pending: &[AuthorityObservation],
    mode: &PendingObservationFlush<'_>,
) -> Option<usize> {
    let PendingObservationFlush::BeforeNameIntro(intro) = mode else {
        return None;
    };
    pending.iter().rposition(|observation| {
        matches!(observation, AuthorityObservation::RegistryOwnerChanged(_))
            && is_same_transaction_before(observation, intro)
    })
}

fn is_same_transaction_before(
    observation: &AuthorityObservation,
    intro: &AuthorityObservation,
) -> bool {
    let Some(observation_position) = observation_raw_log_position(observation) else {
        return false;
    };
    let Some(intro_position) = observation_raw_log_position(intro) else {
        return false;
    };
    observation_position.block_hash == intro_position.block_hash
        && observation_position.transaction_hash == intro_position.transaction_hash
        && observation_position.log_index < intro_position.log_index
}

pub(super) fn observation_raw_log_position(
    observation: &AuthorityObservation,
) -> Option<RawLogPosition> {
    let reference = observation_reference(observation);
    Some(RawLogPosition {
        block_hash: reference.block_hash.clone(),
        transaction_hash: reference.transaction_hash.clone()?,
        log_index: reference.log_index?,
    })
}

fn observation_reference(observation: &AuthorityObservation) -> &ObservationRef {
    match observation {
        AuthorityObservation::RegistrationGranted(value) => &value.reference,
        AuthorityObservation::RegistrationRenewed(value) => &value.reference,
        AuthorityObservation::TokenTransferred(value) => &value.reference,
        AuthorityObservation::RegistryOwnerChanged(value) => &value.reference,
        AuthorityObservation::ResolverChanged(value) => &value.reference,
        AuthorityObservation::RecordChanged(value) => &value.reference,
        AuthorityObservation::RecordVersionChanged(value) => &value.reference,
        AuthorityObservation::WrapperNameWrapped(value) => &value.reference,
        AuthorityObservation::WrapperNameUnwrapped(value) => &value.reference,
        AuthorityObservation::WrapperFusesSet(value) => &value.reference,
        AuthorityObservation::WrapperExpiryExtended(value) => &value.reference,
        AuthorityObservation::WrapperTokenTransferred(value) => &value.reference,
    }
}
