use std::collections::{BTreeMap, HashSet};

use anyhow::{Context, Result};
use bigname_manifests::{load_watched_contracts, load_watched_contracts_scoped_with_progress};
use sqlx::{PgPool, types::Uuid};

use crate::adapter_manifest::{
    active_manifest_for_watched_contract, ensure_watched_contract_manifest_chain,
    load_active_manifest_metadata, required_source_manifest_id, source_rank,
};
use crate::{
    checkpoint_context::{StartupAdapterProgress, record_startup_adapter_progress},
    startup_progress::{STARTUP_ADAPTER_PROGRESS_PAGE_ROWS, StartupManifestProgress},
};

use super::SOURCE_FAMILY_ENS_V2_REGISTRAR_L1;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct ActiveEmitter {
    pub(super) address: String,
    pub(super) source_rank: i32,
    pub(super) source_manifest_id: i64,
    pub(super) contract_instance_id: Uuid,
    pub(super) active_from_block_number: Option<i64>,
    pub(super) active_to_block_number: Option<i64>,
    pub(super) namespace: String,
    pub(super) source_family: String,
    pub(super) manifest_version: i64,
}

pub(super) async fn load_active_emitters(
    pool: &PgPool,
    chain: &str,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<Vec<ActiveEmitter>> {
    let watched_contracts = if let Some(progress) = progress.as_deref_mut() {
        let mut manifest_progress = StartupManifestProgress::new(progress);
        load_watched_contracts_scoped_with_progress(
            pool,
            Some(chain),
            &[SOURCE_FAMILY_ENS_V2_REGISTRAR_L1.to_owned()],
            &mut manifest_progress,
        )
        .await
        .context("failed to load watched contracts for ENSv2 registrar adapter")?
    } else {
        load_watched_contracts(pool)
            .await
            .context("failed to load watched contracts for ENSv2 registrar adapter")?
    };
    let watched_contracts = watched_contracts
        .into_iter()
        .filter(|contract| contract.chain == chain)
        .collect::<Vec<_>>();
    if watched_contracts.is_empty() {
        return Ok(Vec::new());
    }

    let mut manifest_ids = HashSet::new();
    for (index, contract) in watched_contracts.iter().enumerate() {
        manifest_ids.insert(required_source_manifest_id(contract)?);
        record_emitter_progress(pool, progress, index + 1, watched_contracts.len()).await?;
    }
    let manifest_ids = manifest_ids.into_iter().collect::<Vec<_>>();
    let active_manifests =
        load_active_manifest_metadata(pool, &manifest_ids, "ENSv2 registrar emitters").await?;

    record_startup_adapter_progress(pool, progress).await?;
    let watched_contract_count = watched_contracts.len();
    let mut emitters_by_address = BTreeMap::<String, ActiveEmitter>::new();
    for (index, watched_contract) in watched_contracts.into_iter().enumerate() {
        let (source_manifest_id, manifest) =
            active_manifest_for_watched_contract(&active_manifests, &watched_contract)?;
        if manifest.source_family != SOURCE_FAMILY_ENS_V2_REGISTRAR_L1 {
            continue;
        }
        ensure_watched_contract_manifest_chain(&watched_contract, manifest, source_manifest_id)?;

        let candidate = ActiveEmitter {
            address: watched_contract.address.clone(),
            source_rank: source_rank(watched_contract.source),
            source_manifest_id,
            contract_instance_id: watched_contract.contract_instance_id,
            active_from_block_number: watched_contract.active_from_block_number,
            active_to_block_number: watched_contract.active_to_block_number,
            namespace: manifest.namespace.clone(),
            source_family: manifest.source_family.clone(),
            manifest_version: manifest.manifest_version,
        };

        insert_preferred_emitter(&mut emitters_by_address, candidate);
        record_emitter_progress(pool, progress, index + 1, watched_contract_count).await?;
    }

    Ok(emitters_by_address.into_values().collect())
}

async fn record_emitter_progress(
    pool: &PgPool,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
    completed: usize,
    total: usize,
) -> Result<()> {
    if completed == total || completed.is_multiple_of(STARTUP_ADAPTER_PROGRESS_PAGE_ROWS) {
        record_startup_adapter_progress(pool, progress).await?;
    }
    Ok(())
}

fn insert_preferred_emitter(
    emitters_by_address: &mut BTreeMap<String, ActiveEmitter>,
    candidate: ActiveEmitter,
) {
    match emitters_by_address.get(&candidate.address) {
        Some(current) if !candidate_precedes(&candidate, current) => {}
        _ => {
            emitters_by_address.insert(candidate.address.clone(), candidate);
        }
    }
}

fn candidate_precedes(candidate: &ActiveEmitter, current: &ActiveEmitter) -> bool {
    candidate < current
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tied_emitter(contract_instance_id: u128) -> ActiveEmitter {
        ActiveEmitter {
            address: "0xregistrar".to_owned(),
            source_rank: 1,
            source_manifest_id: 7,
            contract_instance_id: Uuid::from_u128(contract_instance_id),
            active_from_block_number: Some(100),
            active_to_block_number: None,
            namespace: "ens".to_owned(),
            source_family: SOURCE_FAMILY_ENS_V2_REGISTRAR_L1.to_owned(),
            manifest_version: 1,
        }
    }

    fn winner(candidates: impl IntoIterator<Item = ActiveEmitter>) -> ActiveEmitter {
        let mut emitters = BTreeMap::new();
        for candidate in candidates {
            insert_preferred_emitter(&mut emitters, candidate);
        }
        emitters.into_values().next().expect("one address winner")
    }

    #[test]
    fn exact_rank_tie_has_the_same_winner_for_both_loader_orders() {
        let lower_identity = tied_emitter(1);
        let higher_identity = tied_emitter(2);

        let non_progress = winner([lower_identity.clone(), higher_identity.clone()]);
        let progress = winner([higher_identity, lower_identity.clone()]);

        assert_eq!(progress, non_progress);
        assert_eq!(progress, lower_identity);
    }
}
