use std::collections::BTreeMap;

use anyhow::Result;
use bigname_manifests::{
    WatchedChainPlan, load_discovery_admission_epochs, load_watched_chain_plan,
    load_watched_contract_summary_and_chain_plan,
};

use super::adapter_sync::sync_adapter_owned_raw_log_state;
use super::intake::{IntakeChainTask, sync_intake_chain_tasks};
use super::manifest::ManifestRuntimeState;

pub(crate) async fn refresh_manifest_normalized_events_from_storage(
    pool: &sqlx::PgPool,
    manifest_runtime_state: &ManifestRuntimeState,
) -> Result<Option<ManifestRuntimeState>> {
    let next_summary = bigname_adapters::sync_manifest_normalized_events(pool).await?;
    if next_summary.total_inserted_count == 0 {
        return Ok(None);
    }

    let mut next_manifest_runtime_state = manifest_runtime_state.clone();
    next_manifest_runtime_state.manifest_normalized_event_summary = next_summary;
    Ok(Some(next_manifest_runtime_state))
}

#[allow(dead_code)]
pub(crate) async fn refresh_watched_chain_plan(
    pool: &sqlx::PgPool,
    current_plan: &[WatchedChainPlan],
) -> Result<Option<Vec<WatchedChainPlan>>> {
    let next_plan = load_watched_chain_plan(pool).await?;
    if next_plan == current_plan {
        Ok(None)
    } else {
        Ok(Some(next_plan))
    }
}

pub(crate) async fn refresh_intake_chain_tasks(
    pool: &sqlx::PgPool,
    current_tasks: &[IntakeChainTask],
    watched_chain_plan: &[WatchedChainPlan],
) -> Result<Option<Vec<IntakeChainTask>>> {
    let next_tasks = sync_intake_chain_tasks(pool, watched_chain_plan).await?;
    if next_tasks == current_tasks {
        Ok(None)
    } else {
        Ok(Some(next_tasks))
    }
}

/// Re-derives discovery edges from the whole stored raw-log corpus before the plan reload. Live
/// poll already writes discovery edges per block, so the tailer only needs
/// [`refresh_runtime_state_from_stored_discovery`]; the full re-derivation stays opt-in for broad
/// runtime refresh.
#[allow(dead_code)]
pub(crate) async fn refresh_runtime_state_from_storage_discovery(
    pool: &sqlx::PgPool,
    manifest_runtime_state: &ManifestRuntimeState,
) -> Result<Option<(ManifestRuntimeState, Vec<IntakeChainTask>)>> {
    sync_adapter_owned_raw_log_state(pool, &manifest_runtime_state.watched_chain_plan).await?;

    refresh_runtime_state_from_stored_discovery(pool, manifest_runtime_state).await
}

/// Reload the active watch plan after another owned path has already
/// reconciled discovery. Startup bootstrap and live polling use this form so
/// they can carry newly admitted targets into intake without running a second
/// broad adapter sync.
pub(crate) async fn refresh_runtime_state_from_stored_discovery(
    pool: &sqlx::PgPool,
    manifest_runtime_state: &ManifestRuntimeState,
) -> Result<Option<(ManifestRuntimeState, Vec<IntakeChainTask>)>> {
    let (next_watched_contract_summary, next_watched_chain_plan) =
        load_watched_contract_summary_and_chain_plan(pool).await?;
    if next_watched_chain_plan == manifest_runtime_state.watched_chain_plan {
        return Ok(None);
    }
    let next_intake_chain_tasks = sync_intake_chain_tasks(pool, &next_watched_chain_plan).await?;
    let mut next_manifest_runtime_state = manifest_runtime_state.clone();
    next_manifest_runtime_state.watched_contract_summary = next_watched_contract_summary;
    next_manifest_runtime_state.watched_chain_plan = next_watched_chain_plan;

    Ok(Some((next_manifest_runtime_state, next_intake_chain_tasks)))
}

/// Change-detection sentinel wrapper around
/// [`refresh_runtime_state_from_stored_discovery`]: the full watch-plan reload
/// scans the whole watched surface (tens of millions of discovery edges at
/// Base scale, minutes per pass), so the live tailer must not run it on every
/// poll tick. Every transaction that mutates the watched surface bumps the
/// owning chain's `discovery_admission_epochs` row (the ratified promotion
/// invariant), so an unchanged epoch map proves the stored plan has not moved
/// and the reload is skipped for the price of one read of a tiny table.
///
/// `last_admission_epochs` is the caller-held sentinel state. It is only
/// replaced after a successful reload, so a failed reload retries on the next
/// tick.
pub(crate) async fn refresh_runtime_state_from_stored_discovery_when_epochs_move(
    pool: &sqlx::PgPool,
    manifest_runtime_state: &ManifestRuntimeState,
    last_admission_epochs: &mut Option<BTreeMap<String, i64>>,
) -> Result<Option<(ManifestRuntimeState, Vec<IntakeChainTask>)>> {
    let current_admission_epochs = load_discovery_admission_epochs(pool).await?;
    if last_admission_epochs.as_ref() == Some(&current_admission_epochs) {
        return Ok(None);
    }
    let refreshed = refresh_runtime_state_from_stored_discovery(pool, manifest_runtime_state).await;
    if refreshed.is_ok() {
        *last_admission_epochs = Some(current_admission_epochs);
    }
    refreshed
}

/// Widens a bootstrap-scoped runtime state to the live watch scope by reloading the stored plan.
/// This deliberately avoids `build_manifest_runtime_state_with_watch_scope`: the widen needs no
/// manifest re-sync, and re-running one here would race the normalized-replay catch-up task, which
/// reconciles the same `contract_instance_addresses` rows as it admits discovery edges.
pub(crate) async fn widen_runtime_state_to_live_watch_scope(
    pool: &sqlx::PgPool,
    manifest_runtime_state: &ManifestRuntimeState,
) -> Result<ManifestRuntimeState> {
    let mut live_manifest_runtime_state = manifest_runtime_state.clone();
    let (watched_contract_summary, watched_chain_plan) =
        load_watched_contract_summary_and_chain_plan(pool).await?;
    live_manifest_runtime_state.watched_contract_summary = watched_contract_summary;
    live_manifest_runtime_state.watched_chain_plan = watched_chain_plan;
    Ok(live_manifest_runtime_state)
}
