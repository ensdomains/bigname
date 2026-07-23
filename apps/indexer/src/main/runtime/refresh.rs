use std::collections::BTreeMap;

use anyhow::Result;
use bigname_manifests::{
    ManifestRuntimeProgress, WatchedChainPlan, load_discovery_admission_epochs,
    load_watched_chain_plan, load_watched_contract_summary_and_chain_plan,
    load_watched_contract_summary_and_chain_plan_with_progress,
};

use super::adapter_sync::sync_adapter_owned_raw_log_state;
use super::intake::{
    IntakeChainTask, clone_watched_chain_plan_with_progress,
    intake_chain_tasks_equal_with_progress, sync_intake_chain_tasks,
    sync_intake_chain_tasks_with_progress, watched_chain_plans_equal_with_progress,
};
use super::manifest::ManifestRuntimeState;

pub(crate) struct AdmissionEpochGatedRefresh {
    pub(crate) admission_epochs: BTreeMap<String, i64>,
    pub(crate) refreshed_state: Option<(ManifestRuntimeState, Vec<IntakeChainTask>)>,
}

#[cfg(test)]
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

pub(crate) async fn refresh_manifest_normalized_events_from_storage_with_progress(
    pool: &sqlx::PgPool,
    manifest_runtime_state: &ManifestRuntimeState,
    progress: &mut dyn ManifestRuntimeProgress,
) -> Result<Option<ManifestRuntimeState>> {
    let next_summary =
        bigname_adapters::sync_manifest_normalized_events_with_progress(pool, progress).await?;
    if next_summary.total_inserted_count == 0 {
        return Ok(None);
    }

    let watched_chain_plan = clone_watched_chain_plan_with_progress(
        pool,
        &manifest_runtime_state.watched_chain_plan,
        progress,
    )
    .await?;
    Ok(Some(ManifestRuntimeState {
        manifest_normalized_event_summary: next_summary,
        watched_chain_plan,
        ..manifest_runtime_state_without_watch_plan(manifest_runtime_state)
    }))
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

#[cfg(test)]
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

pub(crate) async fn refresh_intake_chain_tasks_with_progress(
    pool: &sqlx::PgPool,
    current_tasks: &[IntakeChainTask],
    watched_chain_plan: &[WatchedChainPlan],
    progress: &mut dyn ManifestRuntimeProgress,
) -> Result<Option<Vec<IntakeChainTask>>> {
    let next_tasks =
        sync_intake_chain_tasks_with_progress(pool, watched_chain_plan, progress).await?;
    if intake_chain_tasks_equal_with_progress(pool, &next_tasks, current_tasks, progress).await? {
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
    if next_watched_contract_summary == manifest_runtime_state.watched_contract_summary
        && next_watched_chain_plan == manifest_runtime_state.watched_chain_plan
    {
        return Ok(None);
    }
    let next_intake_chain_tasks = sync_intake_chain_tasks(pool, &next_watched_chain_plan).await?;
    let next_manifest_runtime_state = ManifestRuntimeState {
        watched_contract_summary: next_watched_contract_summary,
        watched_chain_plan: next_watched_chain_plan,
        ..manifest_runtime_state_without_watch_plan(manifest_runtime_state)
    };

    Ok(Some((next_manifest_runtime_state, next_intake_chain_tasks)))
}

pub(crate) async fn refresh_runtime_state_from_stored_discovery_with_progress(
    pool: &sqlx::PgPool,
    manifest_runtime_state: &ManifestRuntimeState,
    progress: &mut dyn ManifestRuntimeProgress,
) -> Result<Option<(ManifestRuntimeState, Vec<IntakeChainTask>)>> {
    let (next_watched_contract_summary, next_watched_chain_plan) =
        load_watched_contract_summary_and_chain_plan_with_progress(pool, progress).await?;
    let plan_unchanged = watched_chain_plans_equal_with_progress(
        pool,
        &next_watched_chain_plan,
        &manifest_runtime_state.watched_chain_plan,
        progress,
    )
    .await?;
    if next_watched_contract_summary == manifest_runtime_state.watched_contract_summary
        && plan_unchanged
    {
        return Ok(None);
    }
    let next_intake_chain_tasks =
        sync_intake_chain_tasks_with_progress(pool, &next_watched_chain_plan, progress).await?;
    let next_manifest_runtime_state = ManifestRuntimeState {
        watched_contract_summary: next_watched_contract_summary,
        watched_chain_plan: next_watched_chain_plan,
        ..manifest_runtime_state_without_watch_plan(manifest_runtime_state)
    };

    Ok(Some((next_manifest_runtime_state, next_intake_chain_tasks)))
}

/// Change-detection sentinel wrapper around
/// [`refresh_runtime_state_from_stored_discovery`]: the full watch-plan reload
/// scans the whole watched surface (tens of millions of discovery edges at
/// Base scale, minutes per pass), so the live tailer must not run it on every
/// poll tick. Every transaction that mutates the watched surface bumps the
/// owning chain's `discovery_admission_epochs` row (the ratified watched-surface
/// admission invariant), so an unchanged epoch map proves the stored plan has not moved
/// and the reload is skipped for the price of one read of a tiny table.
///
/// The caller owns the loaded-plan sentinel and commits the returned epoch map
/// only after it has accepted the refreshed state. Keeping that commit outside
/// this loader prevents a later convergence failure from pairing stale runtime
/// tasks with an already-advanced sentinel.
pub(crate) async fn refresh_runtime_state_from_stored_discovery_when_epochs_move(
    pool: &sqlx::PgPool,
    manifest_runtime_state: &ManifestRuntimeState,
    last_admission_epochs: Option<&BTreeMap<String, i64>>,
) -> Result<Option<AdmissionEpochGatedRefresh>> {
    let current_admission_epochs = load_discovery_admission_epochs(pool).await?;
    if last_admission_epochs == Some(&current_admission_epochs) {
        return Ok(None);
    }
    let refreshed_state =
        refresh_runtime_state_from_stored_discovery(pool, manifest_runtime_state).await?;
    Ok(Some(AdmissionEpochGatedRefresh {
        admission_epochs: current_admission_epochs,
        refreshed_state,
    }))
}

pub(crate) async fn refresh_runtime_state_from_stored_discovery_when_epochs_move_with_progress(
    pool: &sqlx::PgPool,
    manifest_runtime_state: &ManifestRuntimeState,
    last_admission_epochs: Option<&BTreeMap<String, i64>>,
    progress: &mut dyn ManifestRuntimeProgress,
) -> Result<Option<AdmissionEpochGatedRefresh>> {
    let current_admission_epochs = load_discovery_admission_epochs(pool).await?;
    if last_admission_epochs == Some(&current_admission_epochs) {
        return Ok(None);
    }
    let refreshed_state = refresh_runtime_state_from_stored_discovery_with_progress(
        pool,
        manifest_runtime_state,
        progress,
    )
    .await?;
    Ok(Some(AdmissionEpochGatedRefresh {
        admission_epochs: current_admission_epochs,
        refreshed_state,
    }))
}

/// Widens a bootstrap-scoped runtime state to the live watch scope by reloading the stored plan.
/// This deliberately avoids `build_manifest_runtime_state_with_watch_scope`: the widen needs no
/// manifest re-sync, and re-running one here would race the normalized-replay catch-up task, which
/// reconciles the same `contract_instance_addresses` rows as it admits discovery edges.
#[allow(dead_code, reason = "retained for focused runtime refresh tests")]
pub(crate) async fn widen_runtime_state_to_live_watch_scope(
    pool: &sqlx::PgPool,
    manifest_runtime_state: &ManifestRuntimeState,
) -> Result<ManifestRuntimeState> {
    Ok(
        widen_runtime_state_to_live_watch_scope_with_admission_epochs(pool, manifest_runtime_state)
            .await?
            .0,
    )
}

/// Load the admission epochs before the live plan so a concurrent mutation is
/// safe in the redundant-refresh direction: the plan is never paired with an
/// epoch newer than the surface it contains.
pub(crate) async fn widen_runtime_state_to_live_watch_scope_with_admission_epochs(
    pool: &sqlx::PgPool,
    manifest_runtime_state: &ManifestRuntimeState,
) -> Result<(ManifestRuntimeState, BTreeMap<String, i64>)> {
    let admission_epochs = load_discovery_admission_epochs(pool).await?;
    let (watched_contract_summary, watched_chain_plan) =
        load_watched_contract_summary_and_chain_plan(pool).await?;
    let live_manifest_runtime_state = ManifestRuntimeState {
        watched_contract_summary,
        watched_chain_plan,
        ..manifest_runtime_state_without_watch_plan(manifest_runtime_state)
    };
    Ok((live_manifest_runtime_state, admission_epochs))
}

pub(crate) async fn widen_runtime_state_to_live_watch_scope_with_admission_epochs_and_progress(
    pool: &sqlx::PgPool,
    manifest_runtime_state: &ManifestRuntimeState,
    progress: &mut dyn ManifestRuntimeProgress,
) -> Result<(ManifestRuntimeState, BTreeMap<String, i64>)> {
    let admission_epochs = load_discovery_admission_epochs(pool).await?;
    let (watched_contract_summary, watched_chain_plan) =
        load_watched_contract_summary_and_chain_plan_with_progress(pool, progress).await?;
    let live_manifest_runtime_state = ManifestRuntimeState {
        watched_contract_summary,
        watched_chain_plan,
        ..manifest_runtime_state_without_watch_plan(manifest_runtime_state)
    };
    Ok((live_manifest_runtime_state, admission_epochs))
}

fn manifest_runtime_state_without_watch_plan(state: &ManifestRuntimeState) -> ManifestRuntimeState {
    ManifestRuntimeState {
        manifest_repository: state.manifest_repository.clone(),
        manifest_summary: state.manifest_summary.clone(),
        sync_summary: state.sync_summary.clone(),
        discovery_admission: state.discovery_admission.clone(),
        manifest_normalized_event_summary: state.manifest_normalized_event_summary.clone(),
        watched_contract_summary: state.watched_contract_summary.clone(),
        watched_chain_plan: Vec::new(),
    }
}
