use anyhow::bail;

use super::*;

pub(crate) async fn replay_all_current_projections_manually(
    pool: &PgPool,
    text_hydration_config: Option<&record_inventory::RecordInventoryTextHydrationConfig>,
    primary_hydration_config: Option<&primary_name::PrimaryNameLegacyReverseHydrationConfig>,
) -> Result<replay::AllCurrentProjectionsReplaySummary> {
    let Some(mut replay_lock) = try_acquire_replay_lock(pool).await? else {
        bail!(
            "manual all-current projection replay cannot start because automatic replay owns the cross-process replay lock"
        );
    };
    let replay_result = async {
        let attempt = resolve_manual_projection_replay_attempt(pool).await?;
        let normalized_target_block = attempt.normalized_target_block.context(
            "manual all-current projection replay requires a persisted normalized target block",
        )?;
        info!(
            service = "worker",
            replay = "all_current_projections",
            projection_replay_target_block = normalized_target_block,
            replay_input_revision = attempt.full_replay_input_revision,
            apply_baseline_change_id = attempt.apply_baseline_change_id,
            "manual all-current projection replay joined the durable bootstrap attempt"
        );
        replay::rebuild_pending_all_current_projections(
            pool,
            Some(normalized_target_block),
            text_hydration_config,
            primary_hydration_config,
        )
        .await
    }
    .await;
    release_replay_lock(&mut replay_lock).await?;
    replay_result
}

pub(super) async fn resolve_manual_projection_replay_attempt(
    pool: &PgPool,
) -> Result<bootstrap_attempt::ProjectionReplayAttempt> {
    if let Some(attempt) = bootstrap_attempt::load_projection_replay_attempt(pool).await? {
        if attempt.normalized_target_block.is_some() {
            return Ok(attempt);
        }
        bootstrap_attempt::clear_projection_replay_attempt(pool).await?;
    }

    let readiness = load_projection_replay_readiness(pool).await?;
    let chain_checkpoint_max_block =
        projection_apply::load_chain_checkpoint_max_block(pool).await?;
    let current_target = projection_bootstrap_replay_target_block(
        readiness.normalized_replay_max_target_block,
        chain_checkpoint_max_block,
    )
    .context(
        "manual all-current projection replay cannot resolve a current normalized replay or chain-checkpoint head",
    )?;
    let captured_watermark = projection_apply::load_normalized_event_change_watermark(pool).await?;
    let attempt = bootstrap_attempt::start_projection_replay_attempt(
        pool,
        Some(current_target),
        captured_watermark,
    )
    .await?;
    anyhow::ensure!(
        attempt.normalized_target_block.is_some(),
        "manual all-current projection replay attempt did not retain a real target block"
    );
    Ok(attempt)
}
