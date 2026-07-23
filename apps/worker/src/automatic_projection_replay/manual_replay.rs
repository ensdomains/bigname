use anyhow::bail;

use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ManualProjectionReplayResolution {
    pub(super) normalized_target_block: Option<i64>,
    persisted_attempt: Option<bootstrap_attempt::ProjectionReplayAttempt>,
}

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
        let resolution = resolve_manual_projection_replay_attempt(pool).await?;
        if let Some(attempt) = resolution.persisted_attempt {
            info!(
                service = "worker",
                replay = "all_current_projections",
                projection_replay_target_block = attempt.normalized_target_block,
                replay_input_revision = attempt.full_replay_input_revision,
                apply_baseline_change_id = attempt.apply_baseline_change_id,
                "manual all-current projection replay joined the durable bootstrap attempt"
            );
        } else {
            info!(
                service = "worker",
                replay = "all_current_projections",
                "manual all-current projection replay started without a current replay target"
            );
        }
        replay::rebuild_pending_all_current_projections(
            pool,
            resolution.normalized_target_block,
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
) -> Result<ManualProjectionReplayResolution> {
    if let Some(attempt) = bootstrap_attempt::load_projection_replay_attempt(pool).await? {
        return Ok(ManualProjectionReplayResolution {
            normalized_target_block: attempt.normalized_target_block,
            persisted_attempt: Some(attempt),
        });
    }

    let readiness = load_projection_replay_readiness(pool).await?;
    let chain_checkpoint_max_block =
        projection_apply::load_chain_checkpoint_max_block(pool).await?;
    let Some(current_target) = projection_bootstrap_replay_target_block(
        readiness.normalized_replay_max_target_block,
        chain_checkpoint_max_block,
    ) else {
        return Ok(ManualProjectionReplayResolution {
            normalized_target_block: None,
            persisted_attempt: None,
        });
    };
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
    Ok(ManualProjectionReplayResolution {
        normalized_target_block: attempt.normalized_target_block,
        persisted_attempt: Some(attempt),
    })
}
