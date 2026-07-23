use std::future::Future;

use anyhow::{Context, Result};
use sqlx::PgPool;
use tracing::info;

use crate::{
    address_names, children, name_current, permissions, primary_name,
    primary_name::rebuild_heartbeat::LoopHeartbeat, record_inventory, resolver,
};

use super::{
    ALL_CURRENT_PROJECTION_ORDER, AllCurrentProjectionsReplaySummary,
    CurrentProjectionReplayStepSummary,
    progress::{
        clear_projection_replay_completed, mark_projection_replay_completed,
        projection_replay_completed,
    },
};

pub async fn rebuild_all_current_projections(
    pool: &PgPool,
    text_hydration_config: Option<&record_inventory::RecordInventoryTextHydrationConfig>,
    primary_hydration_config: Option<&primary_name::PrimaryNameLegacyReverseHydrationConfig>,
) -> Result<AllCurrentProjectionsReplaySummary> {
    rebuild_all_current_projections_inner(
        pool,
        None,
        false,
        text_hydration_config,
        primary_hydration_config,
        None,
    )
    .await
}

pub async fn rebuild_pending_all_current_projections(
    pool: &PgPool,
    normalized_target_block: Option<i64>,
    text_hydration_config: Option<&record_inventory::RecordInventoryTextHydrationConfig>,
    primary_hydration_config: Option<&primary_name::PrimaryNameLegacyReverseHydrationConfig>,
) -> Result<AllCurrentProjectionsReplaySummary> {
    rebuild_all_current_projections_inner(
        pool,
        normalized_target_block,
        true,
        text_hydration_config,
        primary_hydration_config,
        None,
    )
    .await
}

pub async fn rebuild_pending_all_current_projections_with_heartbeat(
    pool: &PgPool,
    normalized_target_block: Option<i64>,
    text_hydration_config: Option<&record_inventory::RecordInventoryTextHydrationConfig>,
    primary_hydration_config: Option<&primary_name::PrimaryNameLegacyReverseHydrationConfig>,
    loop_heartbeat: &mut LoopHeartbeat,
) -> Result<AllCurrentProjectionsReplaySummary> {
    rebuild_all_current_projections_inner(
        pool,
        normalized_target_block,
        true,
        text_hydration_config,
        primary_hydration_config,
        Some(loop_heartbeat),
    )
    .await
}

async fn rebuild_all_current_projections_inner(
    pool: &PgPool,
    normalized_target_block: Option<i64>,
    skip_completed: bool,
    text_hydration_config: Option<&record_inventory::RecordInventoryTextHydrationConfig>,
    primary_hydration_config: Option<&primary_name::PrimaryNameLegacyReverseHydrationConfig>,
    mut loop_heartbeat: Option<&mut LoopHeartbeat>,
) -> Result<AllCurrentProjectionsReplaySummary> {
    let mut steps = Vec::with_capacity(ALL_CURRENT_PROJECTION_ORDER.len());

    macro_rules! replay_step {
        ($projection:literal, $future:expr, $requested_field:ident) => {
            replay_projection_step(
                pool,
                $projection,
                normalized_target_block,
                skip_completed,
                async {
                    let summary = $future
                        .await
                        .context(concat!("failed to replay ", $projection))?;
                    Ok(CurrentProjectionReplayStepSummary {
                        projection: $projection,
                        requested_key_count: summary.$requested_field,
                        upserted_row_count: summary.upserted_row_count,
                        deleted_row_count: summary.deleted_row_count,
                    })
                },
            )
            .await?
        };
    }

    steps.push(replay_step!(
        "name_current",
        rebuild_name_current(pool, normalized_target_block, &mut loop_heartbeat),
        requested_name_count
    ));
    record_loop_progress(pool, &mut loop_heartbeat).await;
    steps.push(replay_step!(
        "children_current",
        rebuild_children_current(pool, normalized_target_block, &mut loop_heartbeat),
        requested_parent_count
    ));
    record_loop_progress(pool, &mut loop_heartbeat).await;
    steps.push(replay_step!(
        "permissions_current",
        rebuild_permissions_current(pool, normalized_target_block, &mut loop_heartbeat),
        requested_resource_count
    ));
    record_loop_progress(pool, &mut loop_heartbeat).await;
    steps.push(
        replay_projection_step(
            pool,
            "record_inventory_current",
            normalized_target_block,
            skip_completed,
            async {
                let summary = rebuild_record_inventory_current(
                    pool,
                    normalized_target_block,
                    &mut loop_heartbeat,
                )
                .await
                .context("failed to replay record_inventory_current")?;
                if let Some(config) = text_hydration_config {
                    let hydration_summary = hydrate_record_inventory_text_values(
                        pool,
                        config.clone(),
                        &mut loop_heartbeat,
                    )
                    .await
                    .context("failed to hydrate record_inventory_current text values")?;
                    record_inventory::log_text_hydration_summary(None, &hydration_summary);
                }
                Ok(CurrentProjectionReplayStepSummary {
                    projection: "record_inventory_current",
                    requested_key_count: summary.requested_resource_count,
                    upserted_row_count: summary.upserted_row_count,
                    deleted_row_count: summary.deleted_row_count,
                })
            },
        )
        .await?,
    );
    record_loop_progress(pool, &mut loop_heartbeat).await;
    steps.push(replay_step!(
        "resolver_current",
        rebuild_resolver_current(pool, normalized_target_block, &mut loop_heartbeat),
        requested_resolver_count
    ));
    record_loop_progress(pool, &mut loop_heartbeat).await;
    steps.push(replay_step!(
        "address_names_current",
        rebuild_address_names_current(pool, normalized_target_block, &mut loop_heartbeat),
        requested_address_count
    ));
    record_loop_progress(pool, &mut loop_heartbeat).await;
    steps.push(
        replay_projection_step(
            pool,
            "primary_names_current",
            normalized_target_block,
            skip_completed,
            async {
                let summary = rebuild_primary_names_current(
                    pool,
                    normalized_target_block,
                    &mut loop_heartbeat,
                )
                .await
                .context("failed to replay primary_names_current")?;
                if let Some(config) = primary_hydration_config {
                    let hydration_summary = hydrate_primary_names_current(
                        pool,
                        config.clone(),
                        &mut loop_heartbeat,
                    )
                    .await
                    .context(
                        "failed to hydrate primary_names_current legacy reverse-resolver claims",
                    )?;
                    if hydration_summary.candidate_tuple_count > 0
                        || hydration_summary.failed_lookup_count > 0
                    {
                        primary_name::log_legacy_reverse_hydration_summary(&hydration_summary);
                    }
                }
                Ok(CurrentProjectionReplayStepSummary {
                    projection: "primary_names_current",
                    requested_key_count: summary.requested_tuple_count,
                    upserted_row_count: summary.upserted_row_count,
                    deleted_row_count: summary.deleted_row_count,
                })
            },
        )
        .await?,
    );
    record_loop_progress(pool, &mut loop_heartbeat).await;

    debug_assert_eq!(
        steps.iter().map(|step| step.projection).collect::<Vec<_>>(),
        ALL_CURRENT_PROJECTION_ORDER
    );

    Ok(AllCurrentProjectionsReplaySummary { steps })
}

async fn record_loop_progress(pool: &PgPool, loop_heartbeat: &mut Option<&mut LoopHeartbeat>) {
    if let Some(loop_heartbeat) = loop_heartbeat.as_deref_mut() {
        loop_heartbeat.record_if_due(pool).await;
    }
}

async fn rebuild_name_current(
    pool: &PgPool,
    normalized_target_block: Option<i64>,
    loop_heartbeat: &mut Option<&mut LoopHeartbeat>,
) -> Result<name_current::NameCurrentRebuildSummary> {
    name_current::rebuild_name_current_for_replay(
        pool,
        normalized_target_block,
        loop_heartbeat.as_deref_mut(),
    )
    .await
}

async fn rebuild_children_current(
    pool: &PgPool,
    normalized_target_block: Option<i64>,
    loop_heartbeat: &mut Option<&mut LoopHeartbeat>,
) -> Result<children::ChildrenCurrentRebuildSummary> {
    children::rebuild_children_current_for_replay(
        pool,
        normalized_target_block,
        loop_heartbeat.as_deref_mut(),
    )
    .await
}

async fn rebuild_permissions_current(
    pool: &PgPool,
    normalized_target_block: Option<i64>,
    loop_heartbeat: &mut Option<&mut LoopHeartbeat>,
) -> Result<permissions::PermissionsCurrentRebuildSummary> {
    permissions::rebuild_permissions_current_for_replay(
        pool,
        normalized_target_block,
        loop_heartbeat.as_deref_mut(),
    )
    .await
}

async fn rebuild_record_inventory_current(
    pool: &PgPool,
    normalized_target_block: Option<i64>,
    loop_heartbeat: &mut Option<&mut LoopHeartbeat>,
) -> Result<record_inventory::RecordInventoryCurrentRebuildSummary> {
    record_inventory::rebuild_record_inventory_current_for_replay(
        pool,
        normalized_target_block,
        loop_heartbeat.as_deref_mut(),
    )
    .await
}

async fn hydrate_record_inventory_text_values(
    pool: &PgPool,
    config: record_inventory::RecordInventoryTextHydrationConfig,
    loop_heartbeat: &mut Option<&mut LoopHeartbeat>,
) -> Result<record_inventory::RecordInventoryTextHydrationSummary> {
    if let Some(loop_heartbeat) = loop_heartbeat.as_deref_mut() {
        record_inventory::hydrate_record_inventory_text_values_with_heartbeat(
            pool,
            None,
            config,
            loop_heartbeat,
        )
        .await
    } else {
        record_inventory::hydrate_record_inventory_text_values(pool, None, config).await
    }
}

async fn rebuild_resolver_current(
    pool: &PgPool,
    normalized_target_block: Option<i64>,
    loop_heartbeat: &mut Option<&mut LoopHeartbeat>,
) -> Result<resolver::ResolverCurrentRebuildSummary> {
    resolver::rebuild_resolver_current_for_replay(
        pool,
        normalized_target_block,
        loop_heartbeat.as_deref_mut(),
    )
    .await
}

async fn rebuild_address_names_current(
    pool: &PgPool,
    normalized_target_block: Option<i64>,
    loop_heartbeat: &mut Option<&mut LoopHeartbeat>,
) -> Result<address_names::AddressNamesCurrentRebuildSummary> {
    address_names::rebuild_address_names_current_for_replay(
        pool,
        normalized_target_block,
        loop_heartbeat.as_deref_mut(),
    )
    .await
}

async fn rebuild_primary_names_current(
    pool: &PgPool,
    normalized_target_block: Option<i64>,
    loop_heartbeat: &mut Option<&mut LoopHeartbeat>,
) -> Result<primary_name::PrimaryNamesCurrentRebuildSummary> {
    primary_name::rebuild_primary_names_current_for_replay(
        pool,
        normalized_target_block,
        loop_heartbeat.as_deref_mut(),
    )
    .await
}

async fn hydrate_primary_names_current(
    pool: &PgPool,
    config: primary_name::PrimaryNameLegacyReverseHydrationConfig,
    loop_heartbeat: &mut Option<&mut LoopHeartbeat>,
) -> Result<primary_name::PrimaryNameLegacyReverseHydrationSummary> {
    if let Some(loop_heartbeat) = loop_heartbeat.as_deref_mut() {
        primary_name::hydrate_legacy_reverse_resolver_primary_names_with_heartbeat(
            pool,
            config,
            loop_heartbeat,
        )
        .await
    } else {
        primary_name::hydrate_legacy_reverse_resolver_primary_names(pool, config).await
    }
}

async fn replay_projection_step<Fut>(
    pool: &PgPool,
    projection: &'static str,
    normalized_target_block: Option<i64>,
    skip_completed: bool,
    rebuild: Fut,
) -> Result<CurrentProjectionReplayStepSummary>
where
    Fut: Future<Output = Result<CurrentProjectionReplayStepSummary>>,
{
    if projection_should_replay(pool, projection, skip_completed, normalized_target_block).await? {
        let step = rebuild.await?;
        mark_projection_replay_completed(pool, &step, normalized_target_block).await?;
        Ok(step)
    } else {
        super::staging::cleanup_projection_checkpoint(pool, projection).await?;
        Ok(skipped_step(projection))
    }
}

async fn projection_should_replay(
    pool: &PgPool,
    projection: &'static str,
    skip_completed: bool,
    normalized_target_block: Option<i64>,
) -> Result<bool> {
    if skip_completed
        && projection_replay_completed(pool, projection, normalized_target_block).await?
    {
        info!(
            service = "worker",
            replay = "all_current_projections",
            projection,
            normalized_target_block,
            "all-current projection replay skipped because durable completion marker exists"
        );
        return Ok(false);
    }
    clear_projection_replay_completed(pool, projection).await?;
    Ok(true)
}

fn skipped_step(projection: &'static str) -> CurrentProjectionReplayStepSummary {
    CurrentProjectionReplayStepSummary {
        projection,
        requested_key_count: 0,
        upserted_row_count: 0,
        deleted_row_count: 0,
    }
}
