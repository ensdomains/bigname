//! Fill ENSv2 token IDs without substituting the labelhash or EAC resource for a token.
use std::collections::{BTreeMap, BTreeSet};

use bigname_storage::SelectedSnapshot;
use sqlx::types::Uuid;

use crate::v2::{Authority, RegistrationStatus, V2Error, V2Result};

#[cfg(test)]
#[path = "token_read_test_hooks.rs"]
pub(crate) mod test_hooks;

pub(crate) fn token_registration(
    authority: Option<Authority>,
    status: Option<RegistrationStatus>,
    registration_id: Option<&str>,
) -> Option<Uuid> {
    (authority == Some(Authority::EnsV2) && status == Some(RegistrationStatus::Registered))
        .then(|| registration_id.and_then(|id| id.parse().ok()))
        .flatten()
}

pub(crate) async fn load(
    db: impl Into<bigname_storage::ReadDb<'_>>,
    registrations: impl IntoIterator<Item = Uuid>,
    snapshot: &SelectedSnapshot,
) -> V2Result<BTreeMap<Uuid, String>> {
    let registrations = registrations.into_iter().collect::<BTreeSet<_>>();
    #[cfg(test)]
    if !registrations.is_empty() {
        test_hooks::before_read().await;
    }
    let bounds = snapshot
        .chain_positions
        .as_map()
        .values()
        .map(|position| (position.chain_id.clone(), position.block_number))
        .collect();
    bigname_storage::load_ens_v2_token_ids(
        db,
        &registrations.into_iter().collect::<Vec<_>>(),
        &bounds,
    )
    .await
    .map_err(|_| V2Error::internal_error("failed to load ENSv2 token IDs"))
}

/// Enrich a returned NameRecord page once on the caller's publication snapshot.
pub(crate) async fn apply_records(
    db: impl Into<bigname_storage::ReadDb<'_>>,
    records: &mut [super::NameRecord],
    snapshot: &SelectedSnapshot,
) -> V2Result<()> {
    let targets = records
        .iter_mut()
        .filter_map(|record| {
            token_registration(
                record.authority,
                record.registration_status,
                record.registration_id.as_deref(),
            )
            .map(|registration| (registration, record))
        })
        .collect::<Vec<_>>();
    let tokens = load(db, targets.iter().map(|(id, _)| *id), snapshot).await?;
    for (registration, record) in targets {
        record.token_id = tokens.get(&registration).cloned();
    }
    Ok(())
}
