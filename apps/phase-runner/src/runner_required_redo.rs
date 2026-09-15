use std::future::Future;

use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{
    config::ChainConfig,
    error::{RunnerError, RunnerResult},
    heads::load_marker,
    phase::{BlockRange, PhaseName, RunMode},
    runner_support::Backoff,
};

use super::{PhaseRunner, chain::bounded_recovery};

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
        // The read itself is raced, not only the wait between attempts.
        let Some(outcome) = crate::shutdown::until_cancelled(cancellation, async {
            Ok::<_, RunnerError>(operation().await)
        })
        .await?
        else {
            return Ok(None);
        };
        match outcome {
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
        self.run_phase_with_restart_inner(chain, phase_name, mode, cancellation, false)
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
            true,
        )
        .await
    }

    /// A required-redo read on the stop-observable path: `None` once a stop has
    /// won, so the read cannot hold the stop while the database stalls.
    pub(super) async fn required_redo_range_unless_stopped(
        &self,
        chain_id: &str,
        phase: PhaseName,
        cancellation: &CancellationToken,
    ) -> RunnerResult<Option<Option<BlockRange>>> {
        crate::shutdown::until_cancelled(
            cancellation,
            self.store.required_redo_range(chain_id, phase),
        )
        .await
    }

    /// `reject_pending_required_ingest` on the stop-observable path; `false`
    /// means a stop won before the barrier could be read.
    pub(super) async fn require_no_pending_ingest_unless_stopped(
        &self,
        chain_id: &str,
        cancellation: &CancellationToken,
    ) -> RunnerResult<bool> {
        Ok(crate::shutdown::until_cancelled(
            cancellation,
            self.reject_pending_required_ingest(chain_id),
        )
        .await?
        .is_some())
    }

    /// `None` means a stop won before the read; callers treat it as nothing left
    /// to wait for, since the loops it guards only ever return clean on a stop.
    async fn required_range_readable_unless_stopped(
        &self,
        chain_id: &str,
        range: BlockRange,
        cancellation: &CancellationToken,
    ) -> RunnerResult<Option<bool>> {
        crate::shutdown::until_cancelled(
            cancellation,
            required_range_is_readable(self.store.pool(), chain_id, range),
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
            let Some(Some(range)) = self
                .required_redo_range_unless_stopped(
                    &chain.chain_id,
                    PhaseName::Interpret,
                    &cancellation,
                )
                .await?
            else {
                return Ok(());
            };
            self.catch_up_required_range(chain, range, cancellation.clone())
                .await?;
            if self
                .required_range_readable_unless_stopped(&chain.chain_id, range, &cancellation)
                .await?
                .unwrap_or(true)
            {
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
        while !self
            .required_range_readable_unless_stopped(&chain.chain_id, range, &cancellation)
            .await?
            .unwrap_or(true)
        {
            self.run_phase_with_restart(
                chain,
                PhaseName::Live,
                RunMode::Normal,
                cancellation.clone(),
            )
            .await?;
            if !self
                .required_range_readable_unless_stopped(&chain.chain_id, range, &cancellation)
                .await?
                .unwrap_or(true)
            {
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
        let Some(required) = self
            .required_redo_range_unless_stopped(&chain.chain_id, phase, &cancellation)
            .await?
        else {
            return Ok(());
        };
        if let Some(range) = required {
            if phase == PhaseName::Ingest {
                let mut current = range;
                loop {
                    self.catch_up_required_range(chain, current, cancellation.clone())
                        .await?;
                    let Some(updated) = self
                        .required_redo_range_unless_stopped(
                            &chain.chain_id,
                            PhaseName::Ingest,
                            &cancellation,
                        )
                        .await?
                        .flatten()
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
                // Settling a stopped Live is required cleanup before the redo, so
                // a pending stop bounds it instead of skipping it.
                bounded_recovery(
                    &self.stop_clock,
                    "start-up stopped Live recovery",
                    &chain.chain_id,
                    &cancellation,
                    self.recover_stopped_live(chain),
                )
                .await?;
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
            if !self
                .require_no_pending_ingest_unless_stopped(&chain.chain_id, &cancellation)
                .await?
            {
                return Ok(());
            }
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
            if !self
                .require_no_pending_ingest_unless_stopped(&chain.chain_id, &cancellation)
                .await?
            {
                return Ok(());
            }
            match self
                .required_redo_range_unless_stopped(
                    &chain.chain_id,
                    PhaseName::Interpret,
                    &cancellation,
                )
                .await?
            {
                None => return Ok(()),
                Some(Some(_)) => continue,
                Some(None) => {}
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
