use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::PgPool;
use tokio::time::timeout;
use uuid::Uuid;

use crate::{
    address_names, children, name_current, permissions, primary_name,
    primary_name::rebuild_heartbeat::LoopHeartbeat, record_inventory, resolver,
};

use super::apply_locks::{
    acquire_invalidation_apply_locks, ensure_invalidation_apply_locks_alive,
    release_invalidation_apply_locks,
};

mod claim;
mod completion;
#[cfg(test)]
use claim::refresh_claimed_invalidation_claim;
use claim::{claim_pending_invalidations, spawn_claim_heartbeats, stop_claim_heartbeats};
use completion::{complete_invalidation, fail_invalidation};

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

async fn apply_one(
    pool: &PgPool,
    invalidation: &ClaimedInvalidation,
    text_hydration_config: Option<&record_inventory::RecordInventoryTextHydrationConfig>,
    loop_heartbeat: &mut Option<&mut LoopHeartbeat>,
) -> Result<()> {
    match invalidation.projection.as_str() {
        "name_current" => match loop_heartbeat.as_deref_mut() {
            Some(loop_heartbeat) => {
                name_current::rebuild_name_current_with_heartbeat(
                    pool,
                    Some(&invalidation.projection_key),
                    loop_heartbeat,
                )
                .await?;
            }
            None => {
                name_current::rebuild_name_current(pool, Some(&invalidation.projection_key))
                    .await?;
            }
        },
        "children_current" => match loop_heartbeat.as_deref_mut() {
            Some(loop_heartbeat) => {
                children::rebuild_children_current_with_heartbeat(
                    pool,
                    Some(&invalidation.projection_key),
                    loop_heartbeat,
                )
                .await?;
            }
            None => {
                children::rebuild_children_current(pool, Some(&invalidation.projection_key))
                    .await?;
            }
        },
        "permissions_current" => match loop_heartbeat.as_deref_mut() {
            Some(loop_heartbeat) => {
                permissions::rebuild_permissions_current_with_heartbeat(
                    pool,
                    Some(&invalidation.projection_key),
                    loop_heartbeat,
                )
                .await?;
            }
            None => {
                permissions::rebuild_permissions_current(pool, Some(&invalidation.projection_key))
                    .await?;
            }
        },
        "record_inventory_current" => {
            match loop_heartbeat.as_deref_mut() {
                Some(loop_heartbeat) => {
                    record_inventory::rebuild_record_inventory_current_with_heartbeat(
                        pool,
                        Some(&invalidation.projection_key),
                        loop_heartbeat,
                    )
                    .await?;
                }
                None => {
                    record_inventory::rebuild_record_inventory_current(
                        pool,
                        Some(&invalidation.projection_key),
                    )
                    .await?;
                }
            }
            if let Some(config) = text_hydration_config {
                let hydration_summary = match loop_heartbeat.as_deref_mut() {
                    Some(loop_heartbeat) => {
                        record_inventory::hydrate_record_inventory_text_values_with_heartbeat(
                            pool,
                            Some(&invalidation.projection_key),
                            config.clone(),
                            loop_heartbeat,
                        )
                        .await?
                    }
                    None => {
                        record_inventory::hydrate_record_inventory_text_values(
                            pool,
                            Some(&invalidation.projection_key),
                            config.clone(),
                        )
                        .await?
                    }
                };
                record_inventory::log_text_hydration_summary(
                    Some(&invalidation.projection_key),
                    &hydration_summary,
                );
            }
        }
        "resolver_current" => {
            let chain_id = payload_str(&invalidation.key_payload, "chain_id")?;
            let resolver_address = payload_str(&invalidation.key_payload, "resolver_address")?;
            match loop_heartbeat.as_deref_mut() {
                Some(loop_heartbeat) => {
                    resolver::rebuild_resolver_current_with_heartbeat(
                        pool,
                        Some(chain_id),
                        Some(resolver_address),
                        loop_heartbeat,
                    )
                    .await?;
                }
                None => {
                    resolver::rebuild_resolver_current(
                        pool,
                        Some(chain_id),
                        Some(resolver_address),
                    )
                    .await?;
                }
            }
        }
        "address_names_current" => {
            if let Some(logical_name_id) =
                optional_payload_str(&invalidation.key_payload, "logical_name_id")
            {
                let address = payload_str(&invalidation.key_payload, "address")?;
                match loop_heartbeat.as_deref_mut() {
                    Some(loop_heartbeat) => {
                        address_names::rebuild_address_names_current_logical_names_with_heartbeat(
                            pool,
                            address,
                            &[logical_name_id.to_owned()],
                            loop_heartbeat,
                        )
                        .await?;
                    }
                    None => {
                        address_names::rebuild_address_names_current_logical_name(
                            pool,
                            address,
                            logical_name_id,
                        )
                        .await?;
                    }
                }
            } else {
                match loop_heartbeat.as_deref_mut() {
                    Some(loop_heartbeat) => {
                        address_names::rebuild_address_names_current_with_heartbeat(
                            pool,
                            Some(&invalidation.projection_key),
                            loop_heartbeat,
                        )
                        .await?;
                    }
                    None => {
                        address_names::rebuild_address_names_current(
                            pool,
                            Some(&invalidation.projection_key),
                        )
                        .await?;
                    }
                }
            }
        }
        "primary_names_current" => {
            let address = payload_str(&invalidation.key_payload, "address")?;
            let namespace = payload_str(&invalidation.key_payload, "namespace")?;
            let coin_type = payload_str(&invalidation.key_payload, "coin_type")?;
            match loop_heartbeat.as_deref_mut() {
                Some(loop_heartbeat) => {
                    primary_name::rebuild_primary_names_current_with_heartbeat(
                        pool,
                        Some(address),
                        Some(namespace),
                        Some(coin_type),
                        loop_heartbeat,
                    )
                    .await?;
                }
                None => {
                    primary_name::rebuild_primary_names_current(
                        pool,
                        Some(address),
                        Some(namespace),
                        Some(coin_type),
                    )
                    .await?;
                }
            }
        }
        projection => bail!("unsupported projection invalidation family {projection}"),
    }

    Ok(())
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

#[cfg(test)]
#[path = "apply/tests.rs"]
mod tests;
