use anyhow::{Context, Result, bail};
use bigname_storage::{ResolverCurrentRow, delete_resolver_current, upsert_resolver_current_rows};
use futures_util::{StreamExt, TryStreamExt, pin_mut, stream};
use serde_json::{Value, json};
use sqlx::PgPool;

use crate::primary_name::rebuild_heartbeat::{
    LoopHeartbeat, record_rebuild_progress, run_rebuild_phase,
};

#[allow(clippy::duplicate_mod)]
#[path = "staged_rebuild.rs"]
mod staged_rebuild;

use staged_rebuild::{
    RESOLVER_CURRENT_COLUMNS, count_rows, publish_stage_table, stage_resolver_current_rows,
};

mod profile;
mod state_helpers;
mod summary_json;
mod target_loading;

use profile::ResolverProfileGate;
use summary_json::{build_resolver_current_row, build_resolver_current_row_with_progress};
use target_loading::{
    ResolverTarget, count_current_binding_candidate_pairs, load_target_resolvers_page,
    normalize_resolver_address,
};

#[cfg(test)]
use bigname_storage::{CanonicalityState, SurfaceBindingKind};
#[cfg(test)]
use sqlx::{Row, types::time::OffsetDateTime};
#[cfg(test)]
use uuid::Uuid;

const EVENT_KIND_PERMISSION_CHANGED: &str = "PermissionChanged";
const EVENT_KIND_ALIAS_CHANGED: &str = "AliasChanged";
const EVENT_KIND_RESOLVER_CHANGED: &str = "ResolverChanged";
#[cfg(test)]
const BASENAMES_NAMESPACE: &str = "basenames";
const SOURCE_FAMILY_ENS_V1_REGISTRY_L1: &str = "ens_v1_registry_l1";
const SOURCE_FAMILY_ENS_V1_RESOLVER_L1: &str = "ens_v1_resolver_l1";
const SOURCE_FAMILY_BASENAMES_BASE_REGISTRY: &str = "basenames_base_registry";
const SOURCE_FAMILY_BASENAMES_BASE_RESOLVER: &str = "basenames_base_resolver";
const ENS_V1_PUBLIC_RESOLVER_COMPATIBLE_PROFILE: &str = "public_resolver_compatible";
const BASENAMES_L2_RESOLVER_COMPATIBLE_PROFILE: &str = "l2_resolver_compatible";
const RESOLVER_CURRENT_DERIVATION_KIND: &str = "resolver_current_rebuild";
const RESOLVER_CURRENT_ENUMERATION_BASIS: &str = "resolver_overview";
#[cfg(not(test))]
const RESOLVER_CURRENT_REBUILD_BATCH_SIZE: usize = 1_000;
#[cfg(test)]
const RESOLVER_CURRENT_REBUILD_BATCH_SIZE: usize = 1;
const RESOLVER_CURRENT_REBUILD_CONCURRENCY: usize = 1;
const RESOLVER_CURRENT_REBUILD_LOG_INTERVAL: usize = 100;
const TARGETED_RESOLVER_BINDING_ENUMERATION_CANDIDATE_LIMIT: i64 = 10_000;
const RESOLVER_BINDING_ENUMERATION_NOT_PROJECTED_REASON: &str =
    "resolver_binding_enumeration_not_projected";
const RESOLVER_PROFILE_STATUS_PENDING: &str = "pending";
const RESOLVER_PROFILE_STATUS_SUPPORTED: &str = "supported";
const RESOLVER_PROFILE_FACT_FAMILY_AUTHORIZATION: &str = "resolver_authorization";
const RESOLVER_PROFILE_FACT_FAMILY_RECORD: &str = "resolver_record";
const RESOLVER_PROFILE_FACT_FAMILY_RECORD_VERSION: &str = "resolver_record_version";
const RESOLVER_FAMILY_PENDING_REASON: &str = "resolver_family_pending";
const CANONICAL_STATE_FILTER: &str = r#"
  IN (
    'canonical'::canonicality_state,
    'safe'::canonicality_state,
    'finalized'::canonicality_state
  )
"#;
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ResolverCurrentRebuildSummary {
    pub requested_resolver_count: usize,
    pub upserted_row_count: usize,
    pub deleted_row_count: u64,
}

pub async fn rebuild_resolver_current(
    pool: &PgPool,
    chain_id: Option<&str>,
    resolver_address: Option<&str>,
) -> Result<ResolverCurrentRebuildSummary> {
    rebuild_resolver_current_inner(pool, chain_id, resolver_address, None).await
}

async fn rebuild_resolver_current_inner(
    pool: &PgPool,
    chain_id: Option<&str>,
    resolver_address: Option<&str>,
    loop_heartbeat: Option<&mut LoopHeartbeat>,
) -> Result<ResolverCurrentRebuildSummary> {
    match (chain_id, resolver_address) {
        (Some(chain_id), Some(resolver_address)) => {
            rebuild_one_resolver(pool, chain_id, resolver_address, loop_heartbeat).await
        }
        (None, None) => {
            let summary = rebuild_all_resolvers(pool, None, loop_heartbeat).await?;
            crate::replay::staging::cleanup_projection_checkpoint(pool, "resolver_current").await?;
            Ok(summary)
        }
        _ => bail!(
            "resolver_current rebuild requires both chain_id and resolver_address when targeting one resolver"
        ),
    }
}

pub(crate) async fn rebuild_resolver_current_for_replay(
    pool: &PgPool,
    normalized_target_block: Option<i64>,
    loop_heartbeat: Option<&mut LoopHeartbeat>,
) -> Result<ResolverCurrentRebuildSummary> {
    rebuild_all_resolvers(pool, normalized_target_block, loop_heartbeat).await
}

async fn rebuild_all_resolvers(
    pool: &PgPool,
    normalized_target_block: Option<i64>,
    mut loop_heartbeat: Option<&mut LoopHeartbeat>,
) -> Result<ResolverCurrentRebuildSummary> {
    let previous_row_count = run_rebuild_phase(
        pool,
        &mut loop_heartbeat,
        "resolver_current.count_existing",
        count_rows(pool, "resolver_current", None),
    )
    .await?;
    tracing::info!(
        projection = "resolver_current",
        rebuild_concurrency = RESOLVER_CURRENT_REBUILD_CONCURRENCY,
        target_page_size = RESOLVER_CURRENT_REBUILD_BATCH_SIZE,
        "resolver_current rebuild staging started"
    );

    let mut checkpoint = crate::replay::staging::ProjectionStagingCheckpoint::load_or_start(
        pool,
        "resolver_current",
        normalized_target_block,
    )
    .await?;
    loop {
        if !checkpoint.staging_complete() {
            loop {
                let input_fence = checkpoint.prepare_next_batch(pool).await?;
                let cursor = resolver_source_cursor(checkpoint.last_source_key())?;
                let page = run_rebuild_phase(
                    pool,
                    &mut loop_heartbeat,
                    "resolver_current.load_targets_page",
                    load_target_resolvers_page(
                        pool,
                        cursor.as_ref(),
                        RESOLVER_CURRENT_REBUILD_BATCH_SIZE,
                    ),
                )
                .await?;
                if page.is_empty() {
                    if checkpoint.mark_staging_complete(pool, input_fence).await? {
                        break;
                    }
                    continue;
                }
                let last = page
                    .last()
                    .expect("resolver_current staging page must not be empty");
                let last_source_key = json!([last.chain_id, last.resolver_address]);
                let profile_gate = run_rebuild_phase(
                    pool,
                    &mut loop_heartbeat,
                    "resolver_current.load_profile_page",
                    ResolverProfileGate::load(pool),
                )
                .await?;
                let rows =
                    build_resolver_page(pool, &profile_gate, &page, &mut loop_heartbeat).await?;
                let mut transaction = pool.begin().await?;
                let staged = stage_resolver_current_rows(
                    &mut transaction,
                    checkpoint.stage_table(0)?,
                    &rows,
                )
                .await?;
                let progress =
                    checkpoint.progress_after_batch(page.len(), last_source_key, staged, 0)?;
                checkpoint
                    .persist_progress(&mut transaction, &progress, &input_fence)
                    .await?;
                transaction.commit().await?;
                checkpoint.accept_progress(progress, input_fence);
                let completed_resolver_count = checkpoint.completed_source_count()?;
                if completed_resolver_count.is_multiple_of(RESOLVER_CURRENT_REBUILD_LOG_INTERVAL) {
                    tracing::info!(
                        projection = "resolver_current",
                        completed_resolver_count,
                        upserted_row_count = checkpoint.staged_row_count()?,
                        "resolver_current rebuild resolvers processed"
                    );
                }
            }
        }
        let requested_resolver_count = checkpoint.completed_source_count()?;
        let upserted_row_count = checkpoint.staged_row_count()?;
        let published = run_rebuild_phase(
            pool,
            &mut loop_heartbeat,
            "resolver_current.publish",
            publish_stage_table(
                pool,
                "resolver_current",
                RESOLVER_CURRENT_COLUMNS,
                None,
                &mut checkpoint,
            ),
        )
        .await?;
        let Some((_deleted_row_count, published_row_count)) = published else {
            continue;
        };
        debug_assert_eq!(published_row_count as usize, upserted_row_count);

        return Ok(ResolverCurrentRebuildSummary {
            requested_resolver_count,
            upserted_row_count,
            deleted_row_count: previous_row_count,
        });
    }
}

async fn build_resolver_page(
    pool: &PgPool,
    profile_gate: &ResolverProfileGate,
    targets: &[ResolverTarget],
    loop_heartbeat: &mut Option<&mut LoopHeartbeat>,
) -> Result<Vec<ResolverCurrentRow>> {
    let rows = stream::iter(targets.iter().cloned())
        .map(|target| {
            let pool = pool.clone();
            let profile_gate = profile_gate.clone();
            async move { build_resolver_current_row(&pool, &profile_gate, &target).await }
        })
        .buffer_unordered(RESOLVER_CURRENT_REBUILD_CONCURRENCY);
    pin_mut!(rows);
    let mut completed = Vec::new();
    while let Some(row) = rows.try_next().await? {
        if let Some(row) = row {
            completed.push(row);
        }
        record_rebuild_progress(pool, loop_heartbeat).await;
    }
    Ok(completed)
}

fn resolver_source_cursor(value: Option<&Value>) -> Result<Option<[String; 2]>> {
    value
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .context("resolver_current staging checkpoint source key must contain two strings")
}

async fn rebuild_one_resolver(
    pool: &PgPool,
    chain_id: &str,
    resolver_address: &str,
    mut loop_heartbeat: Option<&mut LoopHeartbeat>,
) -> Result<ResolverCurrentRebuildSummary> {
    let resolver_address = normalize_resolver_address(resolver_address);
    let target = ResolverTarget {
        chain_id: chain_id.to_owned(),
        resolver_address,
        profile_source_family: None,
        enumerate_bindings: true,
    };
    let target = ResolverTarget {
        enumerate_bindings: should_enumerate_targeted_resolver_bindings(pool, &target).await?,
        ..target
    };
    record_rebuild_progress(pool, &mut loop_heartbeat).await;
    let profile_gate = ResolverProfileGate::load_for_target(pool, &target).await?;
    record_rebuild_progress(pool, &mut loop_heartbeat).await;
    let Some(row) =
        build_resolver_current_row_with_progress(pool, &profile_gate, &target, &mut loop_heartbeat)
            .await?
    else {
        let deleted_row_count =
            delete_resolver_current(pool, &target.chain_id, &target.resolver_address).await?;
        record_rebuild_progress(pool, &mut loop_heartbeat).await;
        return Ok(ResolverCurrentRebuildSummary {
            requested_resolver_count: 1,
            upserted_row_count: 0,
            deleted_row_count,
        });
    };

    let upserted_row_count = upsert_resolver_current_rows(pool, &[row]).await?.len();
    record_rebuild_progress(pool, &mut loop_heartbeat).await;
    Ok(ResolverCurrentRebuildSummary {
        requested_resolver_count: 1,
        upserted_row_count,
        deleted_row_count: 0,
    })
}

async fn should_enumerate_targeted_resolver_bindings(
    pool: &PgPool,
    target: &ResolverTarget,
) -> Result<bool> {
    let candidate_count = count_current_binding_candidate_pairs(
        pool,
        target,
        TARGETED_RESOLVER_BINDING_ENUMERATION_CANDIDATE_LIMIT + 1,
    )
    .await?;
    if candidate_count <= TARGETED_RESOLVER_BINDING_ENUMERATION_CANDIDATE_LIMIT {
        return Ok(true);
    }

    tracing::info!(
        projection = "resolver_current",
        chain_id = %target.chain_id,
        resolver_address = %target.resolver_address,
        candidate_count,
        candidate_limit = TARGETED_RESOLVER_BINDING_ENUMERATION_CANDIDATE_LIMIT,
        "resolver_current targeted binding enumeration skipped because candidate set is too large"
    );
    Ok(false)
}

#[cfg(test)]
mod tests;
