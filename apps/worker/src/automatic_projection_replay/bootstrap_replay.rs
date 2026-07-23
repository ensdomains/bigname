use super::*;

#[cfg(test)]
pub(super) async fn replay_all_current_projections_when_ready(
    pool: &PgPool,
    text_hydration_config: Option<&record_inventory::RecordInventoryTextHydrationConfig>,
    primary_hydration_config: Option<&primary_name::PrimaryNameLegacyReverseHydrationConfig>,
) -> Result<bool> {
    replay_all_current_projections_when_ready_inner(
        pool,
        text_hydration_config,
        primary_hydration_config,
        None,
    )
    .await
}

pub(super) async fn replay_all_current_projections_when_ready_with_heartbeat(
    pool: &PgPool,
    text_hydration_config: Option<&record_inventory::RecordInventoryTextHydrationConfig>,
    primary_hydration_config: Option<&primary_name::PrimaryNameLegacyReverseHydrationConfig>,
    loop_heartbeat: &mut heartbeat::LoopHeartbeat,
) -> Result<bool> {
    replay_all_current_projections_when_ready_inner(
        pool,
        text_hydration_config,
        primary_hydration_config,
        Some(loop_heartbeat),
    )
    .await
}

async fn replay_all_current_projections_when_ready_inner(
    pool: &PgPool,
    text_hydration_config: Option<&record_inventory::RecordInventoryTextHydrationConfig>,
    primary_hydration_config: Option<&primary_name::PrimaryNameLegacyReverseHydrationConfig>,
    mut loop_heartbeat: Option<&mut heartbeat::LoopHeartbeat>,
) -> Result<bool> {
    let readiness = load_projection_replay_readiness(pool).await?;
    if !readiness.is_ready() {
        debug!(
            service = "worker",
            replay = "all_current_projections",
            normalized_replay_cursor_count = readiness.normalized_replay_cursor_count,
            incomplete_normalized_replay_cursor_count =
                readiness.incomplete_normalized_replay_cursor_count,
            failed_normalized_replay_cursor_count = readiness.failed_normalized_replay_cursor_count,
            active_index_build_count = readiness.active_index_build_count,
            missing_projection_index_count = readiness.missing_projection_index_count,
            "automatic all-current projection replay is waiting for normalized replay readiness"
        );
        return Ok(false);
    }

    let Some(mut replay_lock) = try_acquire_replay_lock(pool).await? else {
        debug!(
            service = "worker",
            replay = "all_current_projections",
            "automatic all-current projection replay skipped because another worker holds the replay lock"
        );
        return Ok(false);
    };

    let readiness = load_projection_replay_readiness(pool).await?;
    if !readiness.is_ready() {
        release_replay_lock(&mut replay_lock).await?;
        return Ok(false);
    }

    let replay_result: Result<_> = async {
        let cursor_exists = projection_apply::normalized_event_cursor_exists(pool).await?;
        let chain_checkpoint_max_block =
            projection_apply::load_chain_checkpoint_max_block(pool).await?;
        let (attempt, resumed_attempt) =
            if let Some(attempt) = bootstrap_attempt::load_projection_replay_attempt(pool).await? {
                (attempt, true)
            } else {
                let captured_watermark =
                    projection_apply::load_normalized_event_change_watermark(pool).await?;
                let candidate_target_block = projection_bootstrap_replay_target_block(
                    readiness.normalized_replay_max_target_block,
                    chain_checkpoint_max_block,
                );
                (
                    bootstrap_attempt::start_projection_replay_attempt(
                        pool,
                        candidate_target_block,
                        captured_watermark,
                    )
                    .await?,
                    false,
                )
            };
        let replay_target_block = attempt.normalized_target_block;
        let reusable_marker_count =
            load_current_projection_replay_marker_count(pool, replay_target_block).await?;
        let cursor_seed =
            projection_bootstrap_apply_cursor_seed(cursor_exists, attempt.apply_baseline_change_id);

        info!(
            service = "worker",
            replay = "all_current_projections",
            normalized_replay_cursor_count = readiness.normalized_replay_cursor_count,
            normalized_replay_max_target_block = readiness.normalized_replay_max_target_block,
            chain_checkpoint_max_block,
            projection_replay_target_block = replay_target_block,
            replay_input_revision = attempt.full_replay_input_revision,
            resumed_attempt,
            apply_baseline_change_id = attempt.apply_baseline_change_id,
            reusable_marker_count,
            cursor_seed_change_id = cursor_seed.map(|cursor| cursor.change_id),
            "automatic all-current projection replay started"
        );
        let summary = if let Some(loop_heartbeat) = loop_heartbeat.as_deref_mut() {
            replay::rebuild_pending_all_current_projections_with_heartbeat(
                pool,
                replay_target_block,
                text_hydration_config,
                primary_hydration_config,
                loop_heartbeat,
            )
            .await
        } else {
            replay::rebuild_pending_all_current_projections(
                pool,
                replay_target_block,
                text_hydration_config,
                primary_hydration_config,
            )
            .await
        }
        .context("failed to automatically replay all current projections")?;
        bootstrap_attempt::finalize_projection_replay_attempt(pool, attempt, cursor_seed).await?;
        Ok(summary)
    }
    .await;
    release_replay_lock(&mut replay_lock).await?;

    let summary = replay_result?;
    info!(
        service = "worker",
        replay = "all_current_projections",
        projection_order = ?summary.projection_order(),
        projection_count = summary.steps.len(),
        total_requested_key_count = summary.total_requested_key_count(),
        total_upserted_row_count = summary.total_upserted_row_count(),
        total_deleted_row_count = summary.total_deleted_row_count(),
        "automatic all-current projection replay completed"
    );

    Ok(true)
}
