use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::PgPool;
use tokio::time::timeout;
use uuid::Uuid;

use crate::{
    address_names, name_current, primary_name::rebuild_heartbeat::LoopHeartbeat, record_inventory,
};

use super::{
    apply_locks::{
        acquire_invalidation_apply_locks, ensure_invalidation_apply_locks_alive,
        release_invalidation_apply_locks,
    },
    dead_letters::dead_letter_invalidation,
};

mod claim;
mod dispatch;
#[cfg(test)]
use claim::refresh_claimed_invalidation_claim;
use claim::{claim_pending_invalidations, spawn_claim_heartbeats, stop_claim_heartbeats};
use dispatch::apply_one;

const NAME_CURRENT_SINGLE_APPLY_TIMEOUT: Duration = Duration::from_secs(120);
const MAX_PROJECTION_INVALIDATION_ATTEMPTS: i64 = 5;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct ProjectionInvalidationApplySummary {
    pub(super) claimed_invalidation_count: usize,
    pub(super) applied_invalidation_count: usize,
    pub(super) failed_invalidation_count: usize,
}

#[derive(Clone, Debug)]
pub(super) struct ClaimedInvalidation {
    pub(super) projection: String,
    pub(super) projection_key: String,
    pub(super) key_payload: Value,
    pub(super) generation: i64,
    pub(super) claim_token: Uuid,
    pub(super) attempt_count: i64,
}

#[cfg(test)]
pub(super) async fn apply_pending_invalidations(
    pool: &PgPool,
    batch_limit: i64,
    text_hydration_config: Option<&record_inventory::RecordInventoryTextHydrationConfig>,
) -> Result<ProjectionInvalidationApplySummary> {
    apply_pending_invalidations_inner(pool, batch_limit, text_hydration_config, None).await
}

pub(super) async fn apply_pending_invalidations_with_heartbeat(
    pool: &PgPool,
    batch_limit: i64,
    text_hydration_config: Option<&record_inventory::RecordInventoryTextHydrationConfig>,
    loop_heartbeat: &mut LoopHeartbeat,
) -> Result<ProjectionInvalidationApplySummary> {
    apply_pending_invalidations_inner(
        pool,
        batch_limit,
        text_hydration_config,
        Some(loop_heartbeat),
    )
    .await
}

async fn apply_pending_invalidations_inner(
    pool: &PgPool,
    batch_limit: i64,
    text_hydration_config: Option<&record_inventory::RecordInventoryTextHydrationConfig>,
    mut loop_heartbeat: Option<&mut LoopHeartbeat>,
) -> Result<ProjectionInvalidationApplySummary> {
    if batch_limit <= 0 {
        bail!("projection apply batch limit must be positive, got {batch_limit}");
    }

    let claim_token = Uuid::new_v4();
    let mut invalidations = claim_pending_invalidations(pool, batch_limit, claim_token).await?;
    sort_claimed_invalidations_for_apply(&mut invalidations);
    let heartbeats = spawn_claim_heartbeats(pool, &invalidations);
    let result = async {
        let mut summary = ProjectionInvalidationApplySummary {
            claimed_invalidation_count: invalidations.len(),
            ..ProjectionInvalidationApplySummary::default()
        };
        while !invalidations.is_empty() {
            if invalidations[0].projection == "address_names_current" {
                let group = drain_address_names_group(&mut invalidations);
                let group_len = group.len();
                let mut locks = acquire_invalidation_apply_locks(pool, &group).await?;
                let result = apply_address_names_group(pool, &group, &mut loop_heartbeat).await;
                let finish = async {
                    ensure_invalidation_apply_locks_alive(&mut locks).await?;
                    match result {
                        Ok(()) => {
                            for invalidation in &group {
                                complete_invalidation(pool, invalidation).await?;
                            }
                            summary.applied_invalidation_count += group_len;
                        }
                        Err(error) => {
                            for invalidation in &group {
                                fail_invalidation(pool, invalidation, &error).await?;
                            }
                            summary.failed_invalidation_count += group_len;
                        }
                    }
                    Ok::<(), anyhow::Error>(())
                }
                .await;
                let unlock = release_invalidation_apply_locks(&mut locks).await;
                finish?;
                unlock?;
                record_loop_progress(pool, &mut loop_heartbeat).await;
                continue;
            }

            let invalidation = invalidations.remove(0);
            let mut locks =
                acquire_invalidation_apply_locks(pool, std::slice::from_ref(&invalidation)).await?;
            let result = if invalidation.projection == "name_current" {
                apply_name_current_single(pool, &invalidation, &mut loop_heartbeat).await
            } else {
                apply_one(
                    pool,
                    &invalidation,
                    text_hydration_config,
                    &mut loop_heartbeat,
                )
                .await
            };
            let finish = async {
                ensure_invalidation_apply_locks_alive(&mut locks).await?;
                match result {
                    Ok(()) => {
                        complete_invalidation(pool, &invalidation).await?;
                        summary.applied_invalidation_count += 1;
                    }
                    Err(error) => {
                        fail_invalidation(pool, &invalidation, &error).await?;
                        summary.failed_invalidation_count += 1;
                    }
                }
                Ok::<(), anyhow::Error>(())
            }
            .await;
            let unlock = release_invalidation_apply_locks(&mut locks).await;
            finish?;
            unlock?;
            record_loop_progress(pool, &mut loop_heartbeat).await;
        }

        Ok(summary)
    }
    .await;
    stop_claim_heartbeats(heartbeats).await;
    result
}

async fn record_loop_progress(pool: &PgPool, loop_heartbeat: &mut Option<&mut LoopHeartbeat>) {
    if let Some(loop_heartbeat) = loop_heartbeat.as_deref_mut() {
        loop_heartbeat.record_if_due(pool).await;
    }
}

fn drain_address_names_group(
    invalidations: &mut Vec<ClaimedInvalidation>,
) -> Vec<ClaimedInvalidation> {
    let Ok(address) = address_names_invalidation_address(&invalidations[0]) else {
        return invalidations.drain(..1).collect();
    };
    let address = address.to_owned();
    let split_at = invalidations
        .iter()
        .position(|invalidation| {
            invalidation.projection != "address_names_current"
                || address_names_invalidation_address(invalidation)
                    .map(|candidate| candidate != address)
                    .unwrap_or(true)
        })
        .unwrap_or(invalidations.len());

    invalidations.drain(..split_at).collect()
}

async fn apply_address_names_group(
    pool: &PgPool,
    invalidations: &[ClaimedInvalidation],
    loop_heartbeat: &mut Option<&mut LoopHeartbeat>,
) -> Result<()> {
    let Some(first) = invalidations.first() else {
        return Ok(());
    };
    let address = address_names_invalidation_address(first)?;
    if invalidations.iter().any(|invalidation| {
        optional_payload_str(&invalidation.key_payload, "logical_name_id").is_none()
    }) {
        match loop_heartbeat.as_deref_mut() {
            Some(loop_heartbeat) => {
                address_names::rebuild_address_names_current_with_heartbeat(
                    pool,
                    Some(address),
                    loop_heartbeat,
                )
                .await?;
            }
            None => {
                address_names::rebuild_address_names_current(pool, Some(address)).await?;
            }
        }
        return Ok(());
    }

    let logical_name_ids = invalidations
        .iter()
        .map(|invalidation| payload_str(&invalidation.key_payload, "logical_name_id"))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    match loop_heartbeat.as_deref_mut() {
        Some(loop_heartbeat) => {
            address_names::rebuild_address_names_current_logical_names_with_heartbeat(
                pool,
                address,
                &logical_name_ids,
                loop_heartbeat,
            )
            .await?;
        }
        None => {
            address_names::rebuild_address_names_current_logical_names(
                pool,
                address,
                &logical_name_ids,
            )
            .await?;
        }
    }
    Ok(())
}

fn address_names_invalidation_address(invalidation: &ClaimedInvalidation) -> Result<&str> {
    optional_payload_str(&invalidation.key_payload, "address")
        .or_else(|| nonblank_str(&invalidation.projection_key))
        .context("address_names_current invalidation missing address")
}

async fn apply_name_current_single(
    pool: &PgPool,
    invalidation: &ClaimedInvalidation,
    loop_heartbeat: &mut Option<&mut LoopHeartbeat>,
) -> Result<()> {
    let result = match loop_heartbeat.as_deref_mut() {
        Some(loop_heartbeat) => {
            timeout(
                NAME_CURRENT_SINGLE_APPLY_TIMEOUT,
                name_current::rebuild_name_current_with_heartbeat(
                    pool,
                    Some(&invalidation.projection_key),
                    loop_heartbeat,
                ),
            )
            .await
        }
        None => {
            timeout(
                NAME_CURRENT_SINGLE_APPLY_TIMEOUT,
                name_current::rebuild_name_current(pool, Some(&invalidation.projection_key)),
            )
            .await
        }
    };
    result.with_context(|| {
        format!(
            "timed out applying name_current invalidation {}",
            invalidation.projection_key
        )
    })??;
    Ok(())
}

fn sort_claimed_invalidations_for_apply(invalidations: &mut [ClaimedInvalidation]) {
    invalidations.sort_by(|left, right| {
        claimed_invalidation_apply_key(left).cmp(&claimed_invalidation_apply_key(right))
    });
}

fn claimed_invalidation_apply_key(invalidation: &ClaimedInvalidation) -> (u16, u8, &str) {
    let family_rank = match invalidation.projection.as_str() {
        "name_current" => 10,
        "children_current" => 20,
        "permissions_current" => 30,
        "record_inventory_current" => 40,
        "resolver_current" => 50,
        "address_names_current" => 60,
        "primary_names_current" => 70,
        _ => 1000,
    };
    let namespace_rank = if invalidation.projection == "name_current"
        && invalidation.projection_key.starts_with("basenames:")
    {
        0
    } else {
        1
    };

    (
        family_rank,
        namespace_rank,
        invalidation.projection_key.as_str(),
    )
}

fn payload_str<'a>(payload: &'a Value, field: &str) -> Result<&'a str> {
    payload
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("projection invalidation payload missing {field}"))
}

fn optional_payload_str<'a>(payload: &'a Value, field: &str) -> Option<&'a str> {
    payload
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
}

fn nonblank_str(value: &str) -> Option<&str> {
    if value.trim().is_empty() {
        None
    } else {
        Some(value)
    }
}

async fn complete_invalidation(pool: &PgPool, invalidation: &ClaimedInvalidation) -> Result<()> {
    let result = sqlx::query(
        r#"
        DELETE FROM projection_invalidations
        WHERE projection = $1
          AND projection_key = $2
          AND generation = $3
          AND claim_token = $4
        "#,
    )
    .bind(&invalidation.projection)
    .bind(&invalidation.projection_key)
    .bind(invalidation.generation)
    .bind(invalidation.claim_token)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to complete projection invalidation {}:{}",
            invalidation.projection, invalidation.projection_key
        )
    })?;
    if result.rows_affected() == 0 {
        release_superseded_claim(pool, invalidation).await?;
    }

    Ok(())
}

async fn fail_invalidation(
    pool: &PgPool,
    invalidation: &ClaimedInvalidation,
    error: &anyhow::Error,
) -> Result<()> {
    let failure_reason = postgres_text_safe(&format!("{error:#}"));
    let failed_attempt_count = invalidation.attempt_count + 1;
    let should_dead_letter = failed_attempt_count >= MAX_PROJECTION_INVALIDATION_ATTEMPTS;
    let rows_affected = if should_dead_letter {
        dead_letter_invalidation(pool, invalidation, &failure_reason, failed_attempt_count).await?
    } else {
        sqlx::query(
            r#"
        UPDATE projection_invalidations
        SET
            claim_token = NULL,
            claimed_at = NULL,
            attempt_count = $6,
            last_failure_reason = $5,
            last_failure_at = now()
        WHERE projection = $1
          AND projection_key = $2
          AND generation = $3
          AND claim_token = $4
        "#,
        )
        .bind(&invalidation.projection)
        .bind(&invalidation.projection_key)
        .bind(invalidation.generation)
        .bind(invalidation.claim_token)
        .bind(&failure_reason)
        .bind(failed_attempt_count)
        .execute(pool)
        .await
        .with_context(|| {
            format!(
                "failed to record projection invalidation failure {}:{}",
                invalidation.projection, invalidation.projection_key
            )
        })?
        .rows_affected()
    };
    if should_dead_letter && rows_affected > 0 {
        tracing::warn!(
            projection = %invalidation.projection,
            projection_key = %invalidation.projection_key,
            generation = invalidation.generation,
            failed_attempt_count = failed_attempt_count,
            max_attempts = MAX_PROJECTION_INVALIDATION_ATTEMPTS,
            failure_reason = %failure_reason,
            "moved projection invalidation to dead letter"
        );
    }
    if rows_affected == 0 {
        release_superseded_claim(pool, invalidation).await?;
    }

    Ok(())
}

async fn release_superseded_claim(pool: &PgPool, invalidation: &ClaimedInvalidation) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE projection_invalidations
        SET claim_token = NULL,
            claimed_at = NULL
        WHERE projection = $1
          AND projection_key = $2
          AND generation > $3
          AND claim_token = $4
        "#,
    )
    .bind(&invalidation.projection)
    .bind(&invalidation.projection_key)
    .bind(invalidation.generation)
    .bind(invalidation.claim_token)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to release superseded projection invalidation claim {}:{}",
            invalidation.projection, invalidation.projection_key
        )
    })?;

    Ok(())
}

fn postgres_text_safe(text: &str) -> String {
    text.replace('\0', "\\u0000")
}

#[cfg(test)]
#[path = "apply/tests.rs"]
mod tests;
