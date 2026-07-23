use std::collections::{BTreeMap, HashSet};

use anyhow::{Context, Result};
use bigname_manifests::{load_watched_contracts, load_watched_contracts_scoped_with_progress};
use sqlx::PgPool;

use crate::adapter_manifest::{
    active_manifest_for_watched_contract, ensure_watched_contract_manifest_chain,
    load_active_manifest_metadata, required_source_manifest_id, source_rank,
};
use crate::{
    checkpoint_context::{StartupAdapterProgress, record_startup_adapter_progress},
    startup_progress::{STARTUP_ADAPTER_PROGRESS_PAGE_ROWS, StartupManifestProgress},
};

use super::SOURCE_FAMILY_ENS_V2_REGISTRAR_L1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ActiveEmitter {
    pub(super) address: String,
    pub(super) source_manifest_id: i64,
    pub(super) namespace: String,
    pub(super) source_family: String,
    pub(super) manifest_version: i64,
    pub(super) source_rank: i32,
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
            source_manifest_id,
            namespace: manifest.namespace.clone(),
            source_family: manifest.source_family.clone(),
            manifest_version: manifest.manifest_version,
            source_rank: source_rank(watched_contract.source),
        };

        match emitters_by_address.get(&candidate.address) {
            Some(current) if !candidate_precedes(&candidate, current) => {}
            _ => {
                emitters_by_address.insert(candidate.address.clone(), candidate);
            }
        }
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

fn candidate_precedes(candidate: &ActiveEmitter, current: &ActiveEmitter) -> bool {
    (candidate.source_rank, candidate.source_manifest_id)
        < (current.source_rank, current.source_manifest_id)
}
