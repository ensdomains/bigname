use crate::phase::VerificationLevel;

pub(crate) const fn weakest_level(
    retained: VerificationLevel,
    available: VerificationLevel,
) -> VerificationLevel {
    match (retained, available) {
        (VerificationLevel::QuickSynced, _) | (_, VerificationLevel::QuickSynced) => {
            VerificationLevel::QuickSynced
        }
        (VerificationLevel::CrossChecked, _) | (_, VerificationLevel::CrossChecked) => {
            VerificationLevel::CrossChecked
        }
        (VerificationLevel::NodeChecked, VerificationLevel::NodeChecked) => {
            VerificationLevel::NodeChecked
        }
    }
}

pub(crate) fn warn_if_downgraded(
    chain_id: &str,
    retained: VerificationLevel,
    persisted: VerificationLevel,
) {
    if weakest_level(retained, persisted) != persisted || retained == persisted {
        return;
    }
    tracing::warn!(
        event = "verification_level_downgraded",
        chain_id,
        old_verification_level = retained.as_str(),
        new_verification_level = persisted.as_str(),
        cause = "current_source_role_configuration",
        "retained verification level downgraded"
    );
}

pub(crate) fn warn_optional_downgrade(
    chain_id: &str,
    retained: Option<VerificationLevel>,
    persisted: Option<VerificationLevel>,
) {
    if let (Some(retained), Some(persisted)) = (retained, persisted) {
        warn_if_downgraded(chain_id, retained, persisted);
    }
}

pub(crate) fn warn_persisted_downgrade(
    chain_id: &str,
    retained: Option<&str>,
    persisted: Option<VerificationLevel>,
) {
    let retained = match retained {
        Some("quick_synced") => Some(VerificationLevel::QuickSynced),
        Some("cross_checked") => Some(VerificationLevel::CrossChecked),
        Some("node_checked") => Some(VerificationLevel::NodeChecked),
        _ => None,
    };
    warn_optional_downgrade(chain_id, retained, persisted);
}
