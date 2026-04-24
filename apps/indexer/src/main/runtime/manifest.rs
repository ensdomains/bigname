use std::path::Path;

use anyhow::{Context, Result, bail};
use bigname_adapters::ManifestNormalizedEventSyncSummary;
use bigname_manifests::{
    DiscoveryAdmissionState, ManifestLoadStatus, ManifestLoadSummary, ManifestRepository,
    ManifestSyncStatus, ManifestSyncSummary, WatchedChainPlan, WatchedContractSummary,
    load_watched_chain_plan, load_watched_contract_summary,
};

pub(crate) fn load_manifest_repository(manifests_root: &Path) -> Result<ManifestRepository> {
    bigname_manifests::load_repository(manifests_root).with_context(|| {
        format!(
            "failed to load repository manifests from {}",
            manifests_root.display()
        )
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DiscoveryAdmissionSnapshot {
    pub(crate) active_manifest_count: usize,
    pub(crate) active_root_count: usize,
    pub(crate) active_contract_count: usize,
    pub(crate) active_rule_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManifestRuntimeState {
    pub(crate) manifest_summary: ManifestLoadSummary,
    pub(crate) sync_summary: ManifestSyncSummary,
    pub(crate) discovery_admission: DiscoveryAdmissionSnapshot,
    pub(crate) manifest_normalized_event_summary: ManifestNormalizedEventSyncSummary,
    pub(crate) watched_contract_summary: WatchedContractSummary,
    pub(crate) watched_chain_plan: Vec<WatchedChainPlan>,
}

pub(crate) async fn build_manifest_runtime_state(
    pool: &sqlx::PgPool,
    manifest_repository: &ManifestRepository,
) -> Result<ManifestRuntimeState> {
    let manifest_summary = manifest_repository.summary().clone();
    let sync_summary = bigname_manifests::sync_repository(pool, manifest_repository).await?;
    let admission_state = bigname_manifests::load_discovery_admission_state(pool).await?;
    verify_stored_manifest_state(&sync_summary, &admission_state)?;
    let manifest_normalized_event_summary =
        bigname_adapters::sync_manifest_normalized_events(pool).await?;
    let watched_contract_summary = load_watched_contract_summary(pool).await?;
    let watched_chain_plan = load_watched_chain_plan(pool).await?;

    Ok(ManifestRuntimeState {
        manifest_summary,
        sync_summary,
        discovery_admission: discovery_admission_snapshot(&admission_state),
        manifest_normalized_event_summary,
        watched_contract_summary,
        watched_chain_plan,
    })
}

pub(crate) fn discovery_admission_snapshot(
    state: &DiscoveryAdmissionState,
) -> DiscoveryAdmissionSnapshot {
    DiscoveryAdmissionSnapshot {
        active_manifest_count: state.active_manifest_count,
        active_root_count: state.active_root_count,
        active_contract_count: state.active_contract_count,
        active_rule_count: state.active_rule_count,
    }
}

pub(crate) fn ensure_manifest_root_ready(summary: &ManifestLoadSummary) -> Result<()> {
    match summary.status {
        ManifestLoadStatus::Loaded | ManifestLoadStatus::Empty => Ok(()),
        ManifestLoadStatus::MissingRoot => bail!(
            "manifests root {} does not exist; refusing to boot on stale stored manifest state",
            summary.root.display()
        ),
        ManifestLoadStatus::InvalidRoot => bail!(
            "manifests root {} is not a directory; refusing to boot on stale stored manifest state",
            summary.root.display()
        ),
    }
}

pub(crate) fn verify_stored_manifest_state(
    sync_summary: &ManifestSyncSummary,
    admission_state: &DiscoveryAdmissionState,
) -> Result<()> {
    if sync_summary.status == ManifestSyncStatus::Synced
        && sync_summary.active_manifest_count != admission_state.active_manifest_count
    {
        bail!(
            "stored active manifest count {} does not match the synced active manifest count {}",
            admission_state.active_manifest_count,
            sync_summary.active_manifest_count
        );
    }

    Ok(())
}

pub(crate) fn manifest_normalized_event_kind_count(
    summary: &ManifestNormalizedEventSyncSummary,
    event_kind: &str,
) -> usize {
    summary
        .by_kind
        .get(event_kind)
        .map(|kind_summary| kind_summary.synced_count)
        .unwrap_or(0)
}
