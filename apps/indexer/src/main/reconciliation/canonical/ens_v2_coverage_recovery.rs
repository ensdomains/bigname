use anyhow::{Context, Result, ensure};
use bigname_adapters::EnsV2NewlyRequiredCoverage;
use bigname_manifests::{
    WatchedSourceSelector, WatchedTargetIdentity, load_historical_watched_contracts_by_chain,
    resolve_watched_source_selector,
};

use crate::{
    backfill::{
        BackfillAdapterSyncMode, BackfillBlockRange, BackfillJobRunConfig,
        DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS, run_resumable_hash_pinned_backfill_job,
    },
    backfill_lease_expires_at, default_backfill_lease_owner, generated_backfill_lease_token,
    provider::ChainProviderOps,
    reconciliation::HeaderAuditMode,
};

const LIVE_COVERAGE_RECOVERY_LEASE_DURATION_SECS: u64 = 300;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EnsV2LiveCoverageRecoveryStatus {
    Recovered,
    AuthorityChanged,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RetentionAuthoritySnapshot {
    retention_generation: i64,
    discovery_admission_epoch: i64,
}

/// Fetch exactly one missing ENSv2 watched tuple through the ordinary
/// hash-pinned raw-only path. The caller retries the unchanged live poll after
/// success, so the retained-history proof is rebuilt from the current
/// authority instead of being carried across an epoch or generation change.
pub(crate) async fn recover_ens_v2_live_coverage_requirement(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    provider: &(impl ChainProviderOps + ?Sized),
    header_audit_mode: HeaderAuditMode,
    requirement: &EnsV2NewlyRequiredCoverage,
) -> Result<EnsV2LiveCoverageRecoveryStatus> {
    let initial_authority = load_retention_authority(pool, &requirement.chain).await?;
    if initial_authority.retention_generation != requirement.retention_generation {
        return Ok(EnsV2LiveCoverageRecoveryStatus::AuthorityChanged);
    }

    let range = BackfillBlockRange::new(
        requirement.required_from_block,
        requirement.required_to_block,
    )?;
    let historical_contracts = load_historical_watched_contracts_by_chain(pool, &requirement.chain)
        .await?
        .into_iter()
        .filter(|contract| {
            contract.source_family == requirement.source_family
                && contract.address.eq_ignore_ascii_case(&requirement.address)
                && contract
                    .active_from_block_number
                    .is_none_or(|from| from <= requirement.required_to_block)
                && contract
                    .active_to_block_number
                    .is_none_or(|to| to >= requirement.required_from_block)
        })
        .collect::<Vec<_>>();
    ensure!(
        !historical_contracts.is_empty(),
        "ENSv2 live coverage recovery cannot resolve watched target {} {} on {} over {}..={}",
        requirement.source_family,
        requirement.address,
        requirement.chain,
        requirement.required_from_block,
        requirement.required_to_block
    );
    let selected_targets = historical_contracts
        .iter()
        .map(|contract| WatchedTargetIdentity {
            contract_instance_id: contract.contract_instance_id,
        })
        .collect::<Vec<_>>();
    let source_plan = resolve_watched_source_selector(
        &historical_contracts,
        &requirement.chain,
        WatchedSourceSelector::WatchedTargetSet(selected_targets),
        range.from_block,
        range.to_block,
    )?;
    ensure!(
        source_plan.selected_targets.iter().all(|target| {
            target.source_family == requirement.source_family
                && target.address.eq_ignore_ascii_case(&requirement.address)
                && target.effective_from_block >= range.from_block
                && target.effective_to_block <= range.to_block
        }),
        "ENSv2 live coverage recovery selected authority outside exact target {} {} over {}..={}",
        requirement.source_family,
        requirement.address,
        range.from_block,
        range.to_block
    );

    // Do not start provider I/O from a plan loaded across an authority change.
    if load_retention_authority(pool, &requirement.chain).await? != initial_authority {
        return Ok(EnsV2LiveCoverageRecoveryStatus::AuthorityChanged);
    }

    let source_identity_hash = source_plan.source_identity_hash();
    let idempotency_key = format!(
        "indexer-live-ens-v2-coverage-recovery:v1:deployment_profile={deployment_profile}:chain={}:generation={}:source_identity_hash={source_identity_hash}:from={}:to={}",
        requirement.chain, requirement.retention_generation, range.from_block, range.to_block,
    );
    run_resumable_hash_pinned_backfill_job(
        pool,
        &source_plan,
        provider,
        BackfillJobRunConfig {
            deployment_profile: deployment_profile.to_owned(),
            idempotency_key,
            scope_idempotency_to_raw_log_retention_generation: true,
            range,
            lease_owner: format!(
                "{}:live-ens-v2-coverage-recovery",
                default_backfill_lease_owner()
            ),
            lease_token: generated_backfill_lease_token()?,
            lease_expires_at: backfill_lease_expires_at(
                LIVE_COVERAGE_RECOVERY_LEASE_DURATION_SECS,
            )?,
            hash_pinned_chunk_blocks: DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS,
            adapter_sync_mode: BackfillAdapterSyncMode::RawOnly,
            header_audit_mode,
        },
    )
    .await
    .with_context(|| {
        format!(
            "failed provider-backed ENSv2 live coverage recovery for {} {} {} over {}..={}",
            requirement.chain,
            requirement.source_family,
            requirement.address,
            requirement.required_from_block,
            requirement.required_to_block
        )
    })?;

    if load_retention_authority(pool, &requirement.chain).await? != initial_authority {
        Ok(EnsV2LiveCoverageRecoveryStatus::AuthorityChanged)
    } else {
        Ok(EnsV2LiveCoverageRecoveryStatus::Recovered)
    }
}

async fn load_retention_authority(
    pool: &sqlx::PgPool,
    chain: &str,
) -> Result<RetentionAuthoritySnapshot> {
    let row = sqlx::query_as::<_, (i64, i64)>(
        r#"
        SELECT revisions.retention_generation, epochs.epoch
        FROM raw_log_staging_input_revisions revisions
        JOIN discovery_admission_epochs epochs
          ON epochs.chain_id = revisions.chain_id
        WHERE revisions.chain_id = $1
        "#,
    )
    .bind(chain)
    .fetch_optional(pool)
    .await
    .with_context(|| format!("failed to load ENSv2 retention authority for {chain}"))?
    .with_context(|| format!("ENSv2 retention authority is absent for {chain}"))?;
    Ok(RetentionAuthoritySnapshot {
        retention_generation: row.0,
        discovery_admission_epoch: row.1,
    })
}
