use anyhow::{Context, Result};
use sqlx::PgPool;
use tracing::info;

use crate::{
    address_names, children, name_current, permissions, primary_name, record_inventory, resolver,
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
) -> Result<AllCurrentProjectionsReplaySummary> {
    rebuild_all_current_projections_inner(pool, None, false).await
}

pub async fn rebuild_pending_all_current_projections(
    pool: &PgPool,
    normalized_target_block: Option<i64>,
) -> Result<AllCurrentProjectionsReplaySummary> {
    rebuild_all_current_projections_inner(pool, normalized_target_block, true).await
}

async fn rebuild_all_current_projections_inner(
    pool: &PgPool,
    normalized_target_block: Option<i64>,
    skip_completed: bool,
) -> Result<AllCurrentProjectionsReplaySummary> {
    let mut steps = Vec::with_capacity(ALL_CURRENT_PROJECTION_ORDER.len());

    if projection_should_replay(
        pool,
        "name_current",
        skip_completed,
        normalized_target_block,
    )
    .await?
    {
        let summary = name_current::rebuild_name_current(pool, None)
            .await
            .context("failed to replay name_current")?;
        let step = CurrentProjectionReplayStepSummary {
            projection: "name_current",
            requested_key_count: summary.requested_name_count,
            upserted_row_count: summary.upserted_row_count,
            deleted_row_count: summary.deleted_row_count,
        };
        mark_projection_replay_completed(pool, &step, normalized_target_block).await?;
        steps.push(step);
    } else {
        steps.push(skipped_step("name_current"));
    }

    if projection_should_replay(
        pool,
        "children_current",
        skip_completed,
        normalized_target_block,
    )
    .await?
    {
        let summary = children::rebuild_children_current(pool, None)
            .await
            .context("failed to replay children_current")?;
        let step = CurrentProjectionReplayStepSummary {
            projection: "children_current",
            requested_key_count: summary.requested_parent_count,
            upserted_row_count: summary.upserted_row_count,
            deleted_row_count: summary.deleted_row_count,
        };
        mark_projection_replay_completed(pool, &step, normalized_target_block).await?;
        steps.push(step);
    } else {
        steps.push(skipped_step("children_current"));
    }

    if projection_should_replay(
        pool,
        "permissions_current",
        skip_completed,
        normalized_target_block,
    )
    .await?
    {
        let summary = permissions::rebuild_permissions_current(pool, None)
            .await
            .context("failed to replay permissions_current")?;
        let step = CurrentProjectionReplayStepSummary {
            projection: "permissions_current",
            requested_key_count: summary.requested_resource_count,
            upserted_row_count: summary.upserted_row_count,
            deleted_row_count: summary.deleted_row_count,
        };
        mark_projection_replay_completed(pool, &step, normalized_target_block).await?;
        steps.push(step);
    } else {
        steps.push(skipped_step("permissions_current"));
    }

    if projection_should_replay(
        pool,
        "record_inventory_current",
        skip_completed,
        normalized_target_block,
    )
    .await?
    {
        let summary = record_inventory::rebuild_record_inventory_current(pool, None)
            .await
            .context("failed to replay record_inventory_current")?;
        let step = CurrentProjectionReplayStepSummary {
            projection: "record_inventory_current",
            requested_key_count: summary.requested_resource_count,
            upserted_row_count: summary.upserted_row_count,
            deleted_row_count: summary.deleted_row_count,
        };
        mark_projection_replay_completed(pool, &step, normalized_target_block).await?;
        steps.push(step);
    } else {
        steps.push(skipped_step("record_inventory_current"));
    }

    if projection_should_replay(
        pool,
        "resolver_current",
        skip_completed,
        normalized_target_block,
    )
    .await?
    {
        let summary = resolver::rebuild_resolver_current(pool, None, None)
            .await
            .context("failed to replay resolver_current")?;
        let step = CurrentProjectionReplayStepSummary {
            projection: "resolver_current",
            requested_key_count: summary.requested_resolver_count,
            upserted_row_count: summary.upserted_row_count,
            deleted_row_count: summary.deleted_row_count,
        };
        mark_projection_replay_completed(pool, &step, normalized_target_block).await?;
        steps.push(step);
    } else {
        steps.push(skipped_step("resolver_current"));
    }

    if projection_should_replay(
        pool,
        "address_names_current",
        skip_completed,
        normalized_target_block,
    )
    .await?
    {
        let summary = address_names::rebuild_address_names_current(pool, None)
            .await
            .context("failed to replay address_names_current")?;
        let step = CurrentProjectionReplayStepSummary {
            projection: "address_names_current",
            requested_key_count: summary.requested_address_count,
            upserted_row_count: summary.upserted_row_count,
            deleted_row_count: summary.deleted_row_count,
        };
        mark_projection_replay_completed(pool, &step, normalized_target_block).await?;
        steps.push(step);
    } else {
        steps.push(skipped_step("address_names_current"));
    }

    if projection_should_replay(
        pool,
        "primary_names_current",
        skip_completed,
        normalized_target_block,
    )
    .await?
    {
        let summary = primary_name::rebuild_primary_names_current(pool, None, None, None)
            .await
            .context("failed to replay primary_names_current")?;
        let step = CurrentProjectionReplayStepSummary {
            projection: "primary_names_current",
            requested_key_count: summary.requested_tuple_count,
            upserted_row_count: summary.upserted_row_count,
            deleted_row_count: summary.deleted_row_count,
        };
        mark_projection_replay_completed(pool, &step, normalized_target_block).await?;
        steps.push(step);
    } else {
        steps.push(skipped_step("primary_names_current"));
    }

    debug_assert_eq!(
        steps.iter().map(|step| step.projection).collect::<Vec<_>>(),
        ALL_CURRENT_PROJECTION_ORDER
    );

    Ok(AllCurrentProjectionsReplaySummary { steps })
}

async fn projection_should_replay(
    pool: &PgPool,
    projection: &'static str,
    skip_completed: bool,
    normalized_target_block: Option<i64>,
) -> Result<bool> {
    if skip_completed && projection_replay_completed(pool, projection).await? {
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
