use std::future::Future;

use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{
    config::ChainConfig,
    error::{RunnerError, RunnerResult},
    heads::load_marker,
    phase::{BlockRange, PhaseName, RunMode},
    phase_lock::PhaseLock,
    runner_support::Backoff,
};

use super::PhaseRunner;

const DISCOVERY_REPAIR_ITERATION_MARGIN: usize = 8;

async fn retry_discovery_database_read<T, F, Fut>(
    timing: &crate::config::TimingConfig,
    cancellation: &CancellationToken,
    chain_id: &str,
    action: &str,
    mut operation: F,
) -> RunnerResult<Option<T>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    let mut backoff = Backoff::new(timing);
    loop {
        match operation().await {
            Ok(value) => return Ok(Some(value)),
            Err(error) => {
                let error = RunnerError::database_anyhow(
                    format!("failed to {action} for chain {chain_id}"),
                    error,
                );
                if !error.is_retryable() {
                    return Err(error);
                }
                let delay = backoff.next_delay();
                warn!(
                    chain_id,
                    error = %error,
                    retry_delay_ms = delay.as_millis(),
                    "discovery repair database read failed with a retryable error"
                );
                tokio::select! {
                    () = cancellation.cancelled() => return Ok(None),
                    () = tokio::time::sleep(delay) => {}
                }
            }
        }
    }
}

impl PhaseRunner {
    pub(super) async fn discovery_required_ingest_pending(
        &self,
        chain_id: &str,
        cancellation: &CancellationToken,
    ) -> RunnerResult<Option<bool>> {
        retry_discovery_database_read(
            &self.timing,
            cancellation,
            chain_id,
            "classify discovery-owned required Ingest work",
            || bigname_manifests::discovery_required_ingest_pending(self.store.pool(), chain_id),
        )
        .await
    }

    pub(super) async fn run_phase_with_restart(
        &self,
        chain: &ChainConfig,
        phase_name: PhaseName,
        mode: RunMode,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        self.run_phase_with_restart_inner(chain, phase_name, mode, cancellation, None, false)
            .await
    }

    async fn run_automatic_discovery_ingest_redo_with_restart(
        &self,
        chain: &ChainConfig,
        range: BlockRange,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        self.run_phase_with_restart_inner(
            chain,
            PhaseName::Ingest,
            RunMode::Redo(range),
            cancellation,
            None,
            true,
        )
        .await
    }

    pub(super) async fn reject_pending_required_ingest(&self, chain_id: &str) -> RunnerResult<()> {
        if let Some(range) = self
            .store
            .required_redo_range(chain_id, PhaseName::Ingest)
            .await?
        {
            return Err(crate::transitions::required_ingest_redo_error(
                chain_id, range,
            ));
        }
        Ok(())
    }

    pub(super) async fn catch_up_for_required_redo(
        &self,
        chain: &ChainConfig,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        loop {
            let Some(range) = self
                .store
                .required_redo_range(&chain.chain_id, PhaseName::Interpret)
                .await?
            else {
                return Ok(());
            };
            self.catch_up_required_range(chain, range, cancellation.clone())
                .await?;
            if cancellation.is_cancelled() {
                return Ok(());
            }
            if required_range_is_readable(self.store.pool(), &chain.chain_id, range).await? {
                return Ok(());
            }
            tokio::select! {
                () = cancellation.cancelled() => return Ok(()),
                () = tokio::time::sleep(self.timing.live_poll_interval) => {}
            }
        }
    }

    pub(super) async fn catch_up_required_range(
        &self,
        chain: &ChainConfig,
        range: BlockRange,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        while !required_range_is_readable(self.store.pool(), &chain.chain_id, range).await? {
            self.run_phase_with_restart(
                chain,
                PhaseName::Live,
                RunMode::Normal,
                cancellation.clone(),
            )
            .await?;
            if cancellation.is_cancelled() {
                return Ok(());
            }
            if !required_range_is_readable(self.store.pool(), &chain.chain_id, range).await? {
                tokio::select! {
                    () = cancellation.cancelled() => return Ok(()),
                    () = tokio::time::sleep(self.timing.live_poll_interval) => {}
                }
            }
        }
        Ok(())
    }

    pub(super) async fn run_spine_phase(
        &self,
        chain: &ChainConfig,
        phase: PhaseName,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        if cancellation.is_cancelled() {
            return Ok(());
        }
        if let Some(range) = self
            .store
            .required_redo_range(&chain.chain_id, phase)
            .await?
        {
            if phase == PhaseName::Ingest {
                let mut current = range;
                loop {
                    self.catch_up_required_range(chain, current, cancellation.clone())
                        .await?;
                    if cancellation.is_cancelled() {
                        return Ok(());
                    }
                    let Some(updated) = self
                        .store
                        .required_redo_range(&chain.chain_id, PhaseName::Ingest)
                        .await?
                    else {
                        return self
                            .run_phase_with_restart(
                                chain,
                                PhaseName::Ingest,
                                RunMode::Normal,
                                cancellation,
                            )
                            .await;
                    };
                    if updated == current {
                        let Some(discovery_owned) = self
                            .discovery_required_ingest_pending(&chain.chain_id, &cancellation)
                            .await?
                        else {
                            return Ok(());
                        };
                        if !discovery_owned {
                            return Err(crate::transitions::required_ingest_redo_error(
                                &chain.chain_id,
                                current,
                            ));
                        }
                        self.run_automatic_discovery_ingest_redo_with_restart(
                            chain,
                            current,
                            cancellation.clone(),
                        )
                        .await?;
                        break;
                    }
                    current = updated;
                }
            }
            if phase == PhaseName::Interpret {
                self.recover_stopped_live(chain).await?;
                self.run_phase_with_restart(
                    chain,
                    phase,
                    RunMode::Redo(range),
                    cancellation.clone(),
                )
                .await?;
                // Interpret finalization may install upstream discovery repair. Release the
                // Interpret lock and let the outer fixed-point loop drain Ingest before Normal
                // Interpret checks its prerequisite again.
                let Some(discovery_owned) = self
                    .discovery_required_ingest_pending(&chain.chain_id, &cancellation)
                    .await?
                else {
                    return Ok(());
                };
                if discovery_owned {
                    return Ok(());
                }
            } else if phase != PhaseName::Ingest {
                self.run_phase_with_restart(
                    chain,
                    phase,
                    RunMode::Redo(range),
                    cancellation.clone(),
                )
                .await?;
            }
        }
        self.run_phase_with_restart(chain, phase, RunMode::Normal, cancellation)
            .await
    }

    pub(super) async fn repair_discovery_coverage(
        &self,
        chain: &ChainConfig,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        let Some((rule_count, iteration_limit)) = self
            .discovery_repair_iteration_limit(&chain.chain_id, &cancellation)
            .await?
        else {
            return Ok(());
        };
        for _iteration in 1..=iteration_limit {
            if cancellation.is_cancelled() {
                return Ok(());
            }
            self.catch_up_for_required_redo(chain, cancellation.clone())
                .await?;
            if cancellation.is_cancelled() {
                return Ok(());
            }
            let Some(discovery_owned) = self
                .discovery_required_ingest_pending(&chain.chain_id, &cancellation)
                .await?
            else {
                return Ok(());
            };
            if discovery_owned {
                self.run_spine_phase(chain, PhaseName::Ingest, cancellation.clone())
                    .await?;
                if cancellation.is_cancelled() {
                    return Ok(());
                }
                continue;
            }
            self.reject_pending_required_ingest(&chain.chain_id).await?;
            self.run_spine_phase(chain, PhaseName::Interpret, cancellation.clone())
                .await?;
            if cancellation.is_cancelled() {
                return Ok(());
            }
            let Some(discovery_owned) = self
                .discovery_required_ingest_pending(&chain.chain_id, &cancellation)
                .await?
            else {
                return Ok(());
            };
            if discovery_owned {
                self.run_spine_phase(chain, PhaseName::Ingest, cancellation.clone())
                    .await?;
                if cancellation.is_cancelled() {
                    return Ok(());
                }
                continue;
            }
            self.reject_pending_required_ingest(&chain.chain_id).await?;
            if self
                .store
                .required_redo_range(&chain.chain_id, PhaseName::Interpret)
                .await?
                .is_some()
            {
                continue;
            }
            return Ok(());
        }
        Err(Self::discovery_repair_exhausted_error(
            &chain.chain_id,
            rule_count,
            iteration_limit,
        ))
    }

    pub(super) async fn discovery_repair_iteration_limit(
        &self,
        chain_id: &str,
        cancellation: &CancellationToken,
    ) -> RunnerResult<Option<(usize, usize)>> {
        let count: Option<i64> = retry_discovery_database_read(
            &self.timing,
            cancellation,
            chain_id,
            "count active admitted discovery rules",
            || async {
                sqlx::query_scalar(
                    "SELECT count(*)
                     FROM manifest_discovery_rules rule
                     JOIN manifest_versions manifest
                       ON manifest.manifest_id = rule.manifest_id
                     WHERE manifest.chain_id = $1
                       AND manifest.rollout_status = 'active'",
                )
                .bind(chain_id)
                .fetch_one(self.store.pool())
                .await
                .map_err(anyhow::Error::from)
            },
        )
        .await?;
        let Some(count) = count else {
            return Ok(None);
        };
        let rule_count = usize::try_from(count).map_err(|error| {
            RunnerError::data_integrity(format!(
                "active discovery-rule count for chain {chain_id} was invalid: {error}"
            ))
        })?;
        let iteration_limit = rule_count
            .checked_add(DISCOVERY_REPAIR_ITERATION_MARGIN)
            .ok_or_else(|| {
                RunnerError::data_integrity(format!(
                    "active discovery-rule count for chain {chain_id} exceeded the repair bound"
                ))
            })?;
        Ok(Some((rule_count, iteration_limit)))
    }

    pub(super) fn discovery_repair_exhausted_error(
        chain_id: &str,
        rule_count: usize,
        iteration_limit: usize,
    ) -> RunnerError {
        RunnerError::data_integrity(format!(
            "discovery coverage repair for chain {chain_id} did not converge after \
             {iteration_limit} passes; the backstop allows one pass per {rule_count} active \
             admitted discovery rules plus {DISCOVERY_REPAIR_ITERATION_MARGIN} scheduling \
             passes, so repeated work indicates a non-monotonic admission or redo lifecycle; \
             keep serving disabled and inspect discovery_watch_admissions and chain_phase_state \
             before an operator retry"
        ))
    }

    pub(super) async fn recover_stopped_live(&self, chain: &ChainConfig) -> RunnerResult<()> {
        let mut live_lock = PhaseLock::acquire(
            self.database.connect_options(),
            &chain.chain_id,
            PhaseName::Live,
        )
        .await?;
        let result = self
            .store
            .complete_stopped_live(live_lock.connection(), &chain.chain_id)
            .await;
        let release = live_lock.release().await;
        match (result, release) {
            (Ok(()), Ok(())) => Ok(()),
            (Ok(()), Err(error)) | (Err(error), Ok(())) => Err(error),
            (Err(error), Err(release_error)) => {
                Err(error.with_secondary("release stopped live lock before redo", release_error))
            }
        }
    }

    pub(super) async fn recover_stopped_phases(&self, chain: &ChainConfig) -> RunnerResult<()> {
        for phase in [
            PhaseName::Ingest,
            PhaseName::Interpret,
            PhaseName::Project,
            PhaseName::Verify,
        ] {
            let mut phase_lock =
                PhaseLock::acquire(self.database.connect_options(), &chain.chain_id, phase).await?;
            let result =
                resolve_stopped_phase(phase_lock.connection(), &chain.chain_id, phase).await;
            let release = phase_lock.release().await;
            match (result, release) {
                (Ok(()), Ok(())) => {}
                (Ok(()), Err(error)) | (Err(error), Ok(())) => return Err(error),
                (Err(error), Err(release_error)) => {
                    return Err(error.with_secondary(
                        "release stopped finite-phase lock during runner restart",
                        release_error,
                    ));
                }
            }
        }
        self.recover_stopped_live(chain).await
    }
}

async fn resolve_stopped_phase(
    lock_connection: &mut sqlx::PgConnection,
    chain_id: &str,
    phase: PhaseName,
) -> RunnerResult<()> {
    if phase == PhaseName::Ingest {
        sqlx::query(
            "UPDATE chain_phase_state
             SET last_error = $3 || substring(last_error FROM char_length($4) + 1),
                 updated_at = now()
             WHERE chain_id = $1 AND phase_name = $2 AND redo_in_progress
               AND last_error LIKE $5",
        )
        .bind(chain_id)
        .bind(phase.as_str())
        .bind(crate::redo_stamp::REQUIRED_REDO_PREFIX)
        .bind(crate::redo_stamp::REQUIRED_REDO_ACTIVE_PREFIX)
        .bind(format!(
            "{}%",
            crate::redo_stamp::REQUIRED_REDO_ACTIVE_PREFIX
        ))
        .execute(lock_connection)
        .await
        .map_err(|error| {
            RunnerError::lock_connection_lost(format!(
                "advisory-lock connection was lost while settling a stopped required Ingest redo \
                 for chain {chain_id}; stopping so the next runner can recheck durable phase \
                 state: {error}"
            ))
        })?;
        return Ok(());
    }
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = CASE
                 WHEN current_block_number IS NOT NULL
                   AND current_block_number = target_block_number
                   AND current_block_hash IS NOT NULL
                   AND current_block_hash = target_block_hash
                   AND phase_name <> 'verify'
                 THEN 'completed'
                 ELSE 'failed'
             END,
             last_error = CASE
                 WHEN current_block_number IS NOT NULL
                   AND current_block_number = target_block_number
                   AND current_block_hash IS NOT NULL
                   AND current_block_hash = target_block_hash
                   AND phase_name <> 'verify'
                 THEN NULL
                 WHEN phase_name = 'verify'
                   AND current_block_number IS NOT NULL
                   AND current_block_number = target_block_number
                   AND current_block_hash IS NOT NULL
                   AND current_block_hash = target_block_hash
                   AND verification_level IS NOT NULL
                 THEN $3 || 'runner stopped after Verify saved its final checkpoint; \
                     revalidate retained verification before completion'
                 ELSE 'phase stopped before completion; its advisory lock was free at \
                     runner restart'
             END,
             finished_at = now(), updated_at = now()
         WHERE chain_id = $1 AND phase_name = $2
           AND phase_status IN ('running', 'paused')
           AND NOT redo_in_progress",
    )
    .bind(chain_id)
    .bind(phase.as_str())
    .bind(crate::error::COMPLETED_VALIDATION_FAILURE_PREFIX)
    .execute(lock_connection)
    .await
    .map_err(|error| {
        RunnerError::lock_connection_lost(format!(
            "advisory-lock connection was lost while resolving stopped phase {phase} for chain \
             {chain_id}; stopping so the next runner can recheck durable phase state: {error}"
        ))
    })?;
    Ok(())
}

async fn required_range_is_readable(
    pool: &sqlx::PgPool,
    chain_id: &str,
    range: BlockRange,
) -> RunnerResult<bool> {
    let latest: Option<i64> =
        sqlx::query_scalar("SELECT latest_block_number FROM chain_heads WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_optional(pool)
            .await
            .map_err(|error| {
                RunnerError::database(
                    format!(
                        "failed to load canonical head before required redo for chain {chain_id}"
                    ),
                    error,
                )
            })?;
    if latest.is_none_or(|latest| latest < range.to) {
        return Ok(false);
    }
    Ok(load_marker(pool, chain_id, range.to).await?.is_some())
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    use super::*;
    use crate::config::TimingConfig;

    #[tokio::test]
    async fn transient_discovery_repair_database_failure_is_retried() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let operation_attempts = Arc::clone(&attempts);
        let timing = TimingConfig {
            initial_backoff: Duration::from_millis(1),
            maximum_backoff: Duration::from_millis(2),
            live_poll_interval: Duration::from_millis(1),
        };
        let result = retry_discovery_database_read(
            &timing,
            &CancellationToken::new(),
            "fault-injection-chain",
            "classify discovery-owned required Ingest work",
            move || {
                let attempt = operation_attempts.fetch_add(1, Ordering::SeqCst);
                async move {
                    if attempt == 0 {
                        Err(anyhow::Error::new(sqlx::Error::PoolTimedOut)
                            .context("injected repair-path database timeout"))
                    } else {
                        Ok(true)
                    }
                }
            },
        )
        .await
        .expect("the transient failure must recover");

        assert_eq!(result, Some(true));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }
}
