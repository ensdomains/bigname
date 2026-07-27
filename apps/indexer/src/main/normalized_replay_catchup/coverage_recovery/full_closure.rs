#[path = "full_closure/job.rs"]
mod job;

use std::time::Duration;

use anyhow::{Result, ensure};
use bigname_manifests::{UncoveredWatchedTuple, load_discovery_admission_epoch};
use bigname_storage::{CoverageRecoveryFailureKey, CoverageRecoveryFailureState};
use sqlx::types::time::OffsetDateTime;
use tracing::info;

use self::job::{ViolationRecoveryOutcome, recover_one_violation};
use super::super::{
    CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS, FullClosureCoverageViolations,
    NormalizedReplayHeartbeat,
};
use crate::{
    backfill::{
        CoinbaseSqlBackfillConfig, CoinbaseSqlSourceRegistry, STALE_BACKFILL_CLAIM_MAX_AGE_SECS,
    },
    provider::ChainProviderOps,
    reconciliation::HeaderAuditMode,
};

const ALL_FULL_CLOSURE_JOB_KEY_PREFIX: &str = "indexer-full-closure-coverage-recovery:";

struct ProviderAttemptBudget {
    attempted: usize,
    maximum: usize,
}

impl ProviderAttemptBudget {
    fn new(maximum: usize) -> Self {
        Self {
            attempted: 0,
            maximum,
        }
    }

    fn allows_attempt(&self) -> bool {
        self.attempted < self.maximum
    }

    fn record(&mut self, attempted: bool) {
        self.attempted += usize::from(attempted);
    }
}

pub(crate) async fn sweep_stale_backfill_claims_for_replay(
    pool: &sqlx::PgPool,
    chain: &str,
) -> Result<()> {
    // Generation obsolescence is terminal for automatic recovery. Close it
    // before the ordinary stale-lease sweep so an old active claim cannot be
    // downgraded to a reclaimable stale failure first.
    let obsolete_job_ids = bigname_storage::fail_obsolete_generation_backfill_jobs(
        pool,
        chain,
        ALL_FULL_CLOSURE_JOB_KEY_PREFIX,
    )
    .await?;
    let stale_job_ids = bigname_storage::sweep_stale_backfill_claims(
        pool,
        chain,
        OffsetDateTime::now_utc() - Duration::from_secs(STALE_BACKFILL_CLAIM_MAX_AGE_SECS as u64),
    )
    .await?;
    if !stale_job_ids.is_empty() || !obsolete_job_ids.is_empty() {
        info!(
            service = "indexer",
            command = "run",
            replay_cursor_kind = CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS,
            chain,
            backfill_job_ids = ?stale_job_ids,
            obsolete_generation_job_ids = ?obsolete_job_ids,
            stale_after_secs = STALE_BACKFILL_CLAIM_MAX_AGE_SECS,
            "released stale backfill claims and closed obsolete pending recovery jobs"
        );
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RecoveryAuthority {
    retention_generation: i64,
    discovery_admission_epoch: i64,
}

async fn load_authority(pool: &sqlx::PgPool, chain: &str) -> Result<RecoveryAuthority> {
    let input_version = bigname_storage::load_raw_log_staging_input_version(pool, chain).await?;
    Ok(RecoveryAuthority {
        retention_generation: input_version.retention_generation,
        discovery_admission_epoch: load_discovery_admission_epoch(pool, chain).await?,
    })
}

#[derive(Default)]
pub(super) struct FullClosureRecoveryBatchOutcome {
    attempted_job_ids: Vec<i64>,
    completed_job_ids: Vec<i64>,
    failed_jobs: Vec<(Option<i64>, String)>,
    deferred_job_ids: Vec<i64>,
    terminal_jobs: Vec<(Option<i64>, String)>,
    pending_job_ids: Vec<i64>,
}

impl FullClosureRecoveryBatchOutcome {
    pub(super) fn failure_record_summary(&self) -> String {
        format!(
            "attempted job ids {:?}; completed job ids {:?}; failed {:?}; retry-deferred job ids {:?}; terminal {:?}; prepared but not attempted job ids {:?}",
            self.attempted_job_ids,
            self.completed_job_ids,
            self.failed_jobs,
            self.deferred_job_ids,
            self.terminal_jobs,
            self.pending_job_ids,
        )
    }

    pub(super) fn completed_count(&self) -> usize {
        self.completed_job_ids.len()
    }
}

#[expect(clippy::too_many_arguments)]
pub(super) async fn recover_full_closure_coverage_batch(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    provider: &(impl ChainProviderOps + ?Sized),
    coinbase_sql_recovery: Option<(&CoinbaseSqlSourceRegistry, &CoinbaseSqlBackfillConfig)>,
    hash_pinned_chunk_blocks: i64,
    max_provider_attempts_per_iteration: usize,
    header_audit_mode: HeaderAuditMode,
    requirement: &FullClosureCoverageViolations,
    progress: &mut Option<&mut NormalizedReplayHeartbeat>,
) -> Result<FullClosureRecoveryBatchOutcome> {
    ensure!(
        !requirement.violations.is_empty(),
        "full-closure coverage recovery received an empty violation set"
    );
    ensure!(
        max_provider_attempts_per_iteration > 0,
        "full-closure coverage recovery provider-attempt cap must be positive"
    );
    let initial_authority = load_authority(pool, &requirement.chain).await?;
    ensure!(
        initial_authority.retention_generation == requirement.retention_generation,
        "full-closure coverage recovery authority changed before job creation on {}: expected retention generation {}, observed {}",
        requirement.chain,
        requirement.retention_generation,
        initial_authority.retention_generation
    );

    let mut batch = FullClosureRecoveryBatchOutcome::default();
    let mut provider_attempt_budget =
        ProviderAttemptBudget::new(max_provider_attempts_per_iteration);
    for violation in &requirement.violations {
        let observed_authority = load_authority(pool, &requirement.chain).await?;
        ensure!(
            observed_authority == initial_authority,
            "full-closure retention generation or discovery authority changed after batch outcome {}; replan from current authority",
            batch.failure_record_summary()
        );
        let failure_key = failure_key(deployment_profile, requirement, violation);
        let persisted_failure =
            bigname_storage::load_coverage_recovery_failure(pool, &failure_key).await?;
        if let Some(failure) = &persisted_failure {
            match failure.state {
                CoverageRecoveryFailureState::Terminal => {
                    batch
                        .terminal_jobs
                        .push((failure.last_backfill_job_id, failure.failure_reason.clone()));
                    continue;
                }
                CoverageRecoveryFailureState::RetryBackoff
                    if failure
                        .retry_not_before
                        .is_some_and(|retry_at| retry_at > OffsetDateTime::now_utc()) =>
                {
                    if let Some(job_id) = failure.last_backfill_job_id {
                        batch.deferred_job_ids.push(job_id);
                    }
                    continue;
                }
                CoverageRecoveryFailureState::RetryBackoff => {}
            }
        }

        let expected_epoch =
            bigname_storage::load_coverage_recovery_epoch(pool, &failure_key).await?;
        let allow_provider_attempt = provider_attempt_budget.allows_attempt();
        let mut provider_attempted = false;
        let result = recover_one_violation(
            pool,
            deployment_profile,
            provider,
            coinbase_sql_recovery,
            hash_pinned_chunk_blocks,
            header_audit_mode,
            requirement,
            violation,
            &failure_key,
            expected_epoch,
            persisted_failure.as_ref(),
            allow_provider_attempt,
            &mut provider_attempted,
            progress,
        )
        .await;
        provider_attempt_budget.record(provider_attempted);
        match result {
            Ok(ViolationRecoveryOutcome::Completed { job_id, attempted }) => {
                if attempted {
                    batch.attempted_job_ids.push(job_id);
                }
                batch.completed_job_ids.push(job_id);
            }
            Ok(ViolationRecoveryOutcome::Failed { job_id, error }) => {
                batch.attempted_job_ids.push(job_id);
                batch.failed_jobs.push((Some(job_id), error));
            }
            Ok(ViolationRecoveryOutcome::Deferred { job_id }) => {
                if let Some(job_id) = job_id {
                    batch.deferred_job_ids.push(job_id);
                }
            }
            Ok(ViolationRecoveryOutcome::Terminal {
                job_id,
                cause,
                attempted,
            }) => {
                if attempted && let Some(job_id) = job_id {
                    batch.attempted_job_ids.push(job_id);
                }
                batch.terminal_jobs.push((job_id, cause));
            }
            Ok(ViolationRecoveryOutcome::Pending { job_id }) => {
                batch.pending_job_ids.push(job_id);
            }
            Err(error) => batch.failed_jobs.push((
                None,
                format!(
                    "{} {} over {}..={}: {error:#}",
                    violation.source_family,
                    violation.address,
                    violation.required_from_block,
                    violation.required_to_block
                ),
            )),
        }
    }

    let final_authority = load_authority(pool, &requirement.chain).await?;
    ensure!(
        final_authority == initial_authority,
        "full-closure retention generation or discovery authority changed after batch outcome {}; replan from current authority",
        batch.failure_record_summary()
    );
    Ok(batch)
}

fn failure_key(
    deployment_profile: &str,
    requirement: &FullClosureCoverageViolations,
    violation: &UncoveredWatchedTuple,
) -> CoverageRecoveryFailureKey {
    CoverageRecoveryFailureKey {
        deployment_profile: deployment_profile.to_owned(),
        chain_id: requirement.chain.clone(),
        raw_log_retention_generation: requirement.retention_generation,
        source_family: violation.source_family.clone(),
        emitting_address: violation.address.to_ascii_lowercase(),
        required_from_block: violation.required_from_block,
        required_to_block: violation.required_to_block,
    }
}

#[cfg(test)]
mod tests {
    use super::ProviderAttemptBudget;

    #[test]
    fn provider_attempt_budget_counts_attempts_without_a_recovery_outcome() {
        let non_default_iteration_cap = 7;
        let mut budget = ProviderAttemptBudget::new(non_default_iteration_cap);
        for _ in 0..non_default_iteration_cap {
            assert!(budget.allows_attempt());
            budget.record(true);
        }
        assert!(
            !budget.allows_attempt(),
            "an attempted path must consume its slot even when later persistence returns Err"
        );
    }
}
