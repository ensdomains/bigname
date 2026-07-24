use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, ensure};
use bigname_manifests::{
    WatchedSourceSelector, WatchedSourceSelectorPlan, WatchedTargetIdentity,
    load_historical_watched_contracts_by_chain, resolve_watched_source_selector,
};
use bigname_storage::{
    BackfillJob, BackfillJobRecord, BackfillLifecycleStatus, fail_backfill_job, load_backfill_job,
};
use sqlx::types::Uuid;

use crate::{
    backfill::{BackfillBlockRange, create_hash_pinned_backfill_job},
    ops_catchup::{
        config::{OpsCatchupConfig, OpsCatchupOutcome},
        planning::CatchupChunk,
    },
    provider::ChainProvider,
    reconciliation::{
        NormalizedEventReplayAdapter,
        sync_manual_full_closure_normalized_events_from_persisted_raw_payloads,
    },
};

use super::{OpsCatchupAdapterPhase, ops_catchup_run_config, run_ops_finalized_catchup_record};

const FINALIZATION_PHASE_KEY_FRAGMENT: &str = ":ens_v2_recovery_phase=finalization";
const SUPERSEDED_FINALIZATION_SCOPE_REASON: &str =
    "ENSv2 finalization source scope superseded by converged authority";
const ENS_V2_FINALIZATION_FULL_CLOSURE_CURSOR_KIND: &str = "ops_ens_v2_finalization_full_closure";
const ENS_V2_FINALIZATION_MAX_RAW_LOGS_PER_PAGE: usize = 100_000;
const ENS_V2_FINALIZATION_ADAPTERS: &[NormalizedEventReplayAdapter] = &[
    NormalizedEventReplayAdapter::EnsV2Registrar,
    NormalizedEventReplayAdapter::EnsV2Resolver,
    NormalizedEventReplayAdapter::EnsV2Permissions,
];

pub(in crate::ops_catchup::runner) async fn precreate_ens_v2_finalization_jobs(
    pool: &sqlx::PgPool,
    chain: &str,
    config: &OpsCatchupConfig,
    chunks: &[CatchupChunk],
) -> Result<()> {
    for chunk in chunks {
        let source_plan = chunk.source_plan(chain)?;
        let run_config = ops_catchup_run_config(
            config,
            chain,
            &source_plan,
            chunk.range,
            OpsCatchupAdapterPhase::EnsV2Finalization,
        )?;
        create_hash_pinned_backfill_job(pool, &source_plan, &run_config).await?;
    }
    Ok(())
}

pub(in crate::ops_catchup::runner) async fn has_pending_ens_v2_finalization_jobs(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
    retention_generation: i64,
) -> Result<bool> {
    let key_suffix = finalization_key_suffix(retention_generation);
    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM backfill_jobs
            WHERE deployment_profile = $1
              AND chain_id = $2
              AND raw_log_retention_generation = $3
              AND status <> 'completed'::backfill_lifecycle_status
              AND right(idempotency_key, char_length($4)) = $4
              AND NOT (
                  status = 'failed'::backfill_lifecycle_status
                  AND failure_reason = $5
              )
        )
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(retention_generation)
    .bind(&key_suffix)
    .bind(SUPERSEDED_FINALIZATION_SCOPE_REASON)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to detect pending ENSv2 finalization jobs for {deployment_profile}/{chain} generation {retention_generation}"
        )
    })
}

#[expect(clippy::too_many_arguments)]
pub(in crate::ops_catchup::runner) async fn resume_pending_ens_v2_finalization_jobs(
    pool: &sqlx::PgPool,
    chain: &str,
    provider: &ChainProvider,
    config: &OpsCatchupConfig,
    retention_generation: i64,
    finalized_head_block_number: i64,
    finalized_head_block_hash: &str,
    outcome: &mut OpsCatchupOutcome,
) -> Result<()> {
    let records =
        load_pending_ens_v2_finalization_jobs(pool, config, chain, retention_generation).await?;
    // These adapters need the complete retained history (permissions in
    // particular carries Named* resource hints forward to later role logs).
    // Run one complete-history pass after the registry proof is current;
    // durable range jobs below remain the restartable fetch/stateless intents.
    sync_manual_full_closure_normalized_events_from_persisted_raw_payloads(
        pool,
        &config.deployment_profile,
        chain,
        ENS_V2_FINALIZATION_FULL_CLOSURE_CURSOR_KIND,
        0,
        finalized_head_block_number,
        ENS_V2_FINALIZATION_ADAPTERS,
        ENS_V2_FINALIZATION_MAX_RAW_LOGS_PER_PAGE,
    )
    .await?;
    for (source_plan, record) in records {
        ensure!(
            record.job.range_end_block_number <= finalized_head_block_number,
            "pending ENSv2 finalization job {} ends at block {}, beyond current finalized head {}",
            record.job.backfill_job_id,
            record.job.range_end_block_number,
            finalized_head_block_number
        );
        let range = BackfillBlockRange::new(
            record.job.range_start_block_number,
            record.job.range_end_block_number,
        )?;
        let run_config = ops_catchup_run_config(
            config,
            chain,
            &source_plan,
            range,
            OpsCatchupAdapterPhase::EnsV2Finalization,
        )?;
        ensure!(
            record.job.idempotency_key
                == format!(
                    "{}:raw_log_retention_generation={retention_generation}",
                    run_config.idempotency_key
                ),
            "pending ENSv2 finalization job {} has an unexpected idempotency key",
            record.job.backfill_job_id
        );
        run_ops_finalized_catchup_record(
            pool,
            chain,
            provider,
            config,
            source_plan,
            run_config,
            record,
            finalized_head_block_number,
            finalized_head_block_hash,
            outcome,
        )
        .await?;
    }
    Ok(())
}

async fn load_pending_ens_v2_finalization_jobs(
    pool: &sqlx::PgPool,
    config: &OpsCatchupConfig,
    chain: &str,
    retention_generation: i64,
) -> Result<Vec<(WatchedSourceSelectorPlan, BackfillJobRecord)>> {
    let key_suffix = finalization_key_suffix(retention_generation);
    let job_ids = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT backfill_job_id
        FROM backfill_jobs
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND raw_log_retention_generation = $3
          AND status <> 'completed'::backfill_lifecycle_status
          AND right(idempotency_key, char_length($4)) = $4
          AND NOT (
              status = 'failed'::backfill_lifecycle_status
              AND failure_reason = $5
          )
        ORDER BY range_start_block_number, range_end_block_number, backfill_job_id
        "#,
    )
    .bind(&config.deployment_profile)
    .bind(chain)
    .bind(retention_generation)
    .bind(&key_suffix)
    .bind(SUPERSEDED_FINALIZATION_SCOPE_REASON)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load pending ENSv2 finalization jobs for {}/{chain} generation {retention_generation}",
            config.deployment_profile
        )
    })?;

    let historical_contracts = load_historical_watched_contracts_by_chain(pool, chain).await?;
    let mut records = BTreeMap::<i64, (WatchedSourceSelectorPlan, BackfillJobRecord)>::new();
    for job_id in job_ids {
        let job = load_backfill_job(pool, job_id)
            .await?
            .with_context(|| format!("pending ENSv2 finalization job {job_id} disappeared"))?;
        if job.status == BackfillLifecycleStatus::Completed {
            continue;
        }
        let Some(source_plan) =
            reconstruct_current_finalization_source_plan(&historical_contracts, &job)?
        else {
            mark_finalization_scope_superseded(pool, &job, None).await?;
            continue;
        };
        let range =
            BackfillBlockRange::new(job.range_start_block_number, job.range_end_block_number)?;
        let run_config = ops_catchup_run_config(
            config,
            chain,
            &source_plan,
            range,
            OpsCatchupAdapterPhase::EnsV2Finalization,
        )?;
        // Create the authority-current replacement before retiring the
        // pre-proof job. A crash between these writes can only leave both
        // durable intents; the next pass repeats this idempotently.
        let current_record =
            create_hash_pinned_backfill_job(pool, &source_plan, &run_config).await?;
        if current_record.job.backfill_job_id != job.backfill_job_id {
            mark_finalization_scope_superseded(
                pool,
                &job,
                Some(current_record.job.backfill_job_id),
            )
            .await?;
        }
        if current_record.job.status != BackfillLifecycleStatus::Completed {
            records.insert(
                current_record.job.backfill_job_id,
                (source_plan, current_record),
            );
        }
    }
    Ok(records.into_values().collect())
}

fn finalization_key_suffix(retention_generation: i64) -> String {
    format!("{FINALIZATION_PHASE_KEY_FRAGMENT}:raw_log_retention_generation={retention_generation}")
}

fn reconstruct_current_finalization_source_plan(
    historical_contracts: &[bigname_manifests::WatchedContract],
    job: &BackfillJob,
) -> Result<Option<WatchedSourceSelectorPlan>> {
    ensure!(
        job.source_identity
            .get("selector_kind")
            .and_then(serde_json::Value::as_str)
            == Some("watched_target_set"),
        "pending ENSv2 finalization job {} is not a watched-target-set job",
        job.backfill_job_id
    );
    let requested = job
        .source_identity
        .get("requested_watched_targets")
        .and_then(serde_json::Value::as_array)
        .with_context(|| {
            format!(
                "pending ENSv2 finalization job {} has no requested target identities",
                job.backfill_job_id
            )
        })?;
    let requested = requested
        .iter()
        .map(|target| {
            let value = target
                .get("contract_instance_id")
                .and_then(serde_json::Value::as_str)
                .with_context(|| {
                    format!(
                        "pending ENSv2 finalization job {} has a malformed requested target",
                        job.backfill_job_id
                    )
                })?;
            Ok(WatchedTargetIdentity {
                contract_instance_id: Uuid::parse_str(value).with_context(|| {
                    format!(
                        "pending ENSv2 finalization job {} has invalid target UUID {value}",
                        job.backfill_job_id
                    )
                })?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    ensure!(
        !requested.is_empty(),
        "pending ENSv2 finalization job {} has an empty requested target set",
        job.backfill_job_id
    );
    let requested_ids = requested
        .iter()
        .map(|target| target.contract_instance_id)
        .collect::<BTreeSet<_>>();
    let current_ids = historical_contracts
        .iter()
        .filter(|contract| requested_ids.contains(&contract.contract_instance_id))
        .filter(|contract| {
            contract
                .active_from_block_number
                .is_none_or(|from| from <= job.range_end_block_number)
                && contract
                    .active_to_block_number
                    .is_none_or(|to| to >= job.range_start_block_number)
        })
        .map(|contract| contract.contract_instance_id)
        .collect::<BTreeSet<_>>();
    if current_ids.is_empty() {
        return Ok(None);
    }
    let current_requested = requested
        .into_iter()
        .filter(|target| current_ids.contains(&target.contract_instance_id))
        .collect::<Vec<_>>();
    let source_plan = resolve_watched_source_selector(
        historical_contracts,
        &job.chain_id,
        WatchedSourceSelector::WatchedTargetSet(current_requested),
        job.range_start_block_number,
        job.range_end_block_number,
    )?;
    Ok(Some(source_plan))
}

async fn mark_finalization_scope_superseded(
    pool: &sqlx::PgPool,
    job: &BackfillJob,
    replacement_backfill_job_id: Option<i64>,
) -> Result<()> {
    fail_backfill_job(
        pool,
        job.backfill_job_id,
        SUPERSEDED_FINALIZATION_SCOPE_REASON,
        serde_json::json!({
            "prior_source_identity_hash": job
                .source_identity
                .get("source_identity_hash")
                .and_then(serde_json::Value::as_str),
            "replacement_backfill_job_id": replacement_backfill_job_id,
        }),
    )
    .await?;
    Ok(())
}
