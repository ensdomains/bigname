use std::{future::Future, sync::Arc};

use anyhow::{Context, Result};
use sqlx::PgPool;
use tokio::{
    sync::{Mutex, OwnedMutexGuard},
    time::{Duration, Instant},
};
use tracing::warn;

const MAX_PROGRESS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone, Default)]
pub(crate) struct RequiredSubtaskActivity {
    phase_exclusion: Arc<Mutex<()>>,
}

pub(crate) struct RequiredSubtaskActivityGuard {
    _phase_exclusion: OwnedMutexGuard<()>,
}

impl RequiredSubtaskActivity {
    pub(crate) async fn begin(&self) -> RequiredSubtaskActivityGuard {
        let phase_exclusion = Arc::clone(&self.phase_exclusion).lock_owned().await;
        RequiredSubtaskActivityGuard {
            _phase_exclusion: phase_exclusion,
        }
    }

    pub(crate) async fn exclude_required_subtask(&self) -> OwnedMutexGuard<()> {
        Arc::clone(&self.phase_exclusion).lock_owned().await
    }
}

pub(crate) struct LoopHeartbeat {
    instance_id: String,
    interval: Duration,
    last_recorded_at: Option<Instant>,
    #[cfg(test)]
    progress_record_count: usize,
}

impl LoopHeartbeat {
    pub(crate) fn new(instance_id: String, interval: Duration) -> Self {
        Self {
            instance_id,
            interval: interval.min(MAX_PROGRESS_HEARTBEAT_INTERVAL),
            last_recorded_at: None,
            #[cfg(test)]
            progress_record_count: 0,
        }
    }

    pub(crate) async fn record_if_due(&mut self, pool: &PgPool) {
        if !self.is_due() {
            return;
        }

        let result = bigname_storage::record_service_loop_heartbeat(
            pool,
            bigname_storage::WORKER_SERVICE_NAME,
            &self.instance_id,
            &[],
        )
        .await;
        match result {
            Ok(()) => {
                self.last_recorded_at = Some(Instant::now());
                #[cfg(test)]
                {
                    self.progress_record_count += 1;
                }
            }
            Err(error) => warn!(
                service = "worker",
                heartbeat_instance_id = %self.instance_id,
                error = %format!("{error:#}"),
                "failed to record worker loop heartbeat; continuing so the missed beat degrades liveness without restarting the worker"
            ),
        }
    }

    pub(crate) async fn run_phase<T, Fut>(
        &mut self,
        pool: &PgPool,
        phase: &'static str,
        future: Fut,
    ) -> Result<T>
    where
        Fut: Future<Output = Result<T>>,
    {
        self.begin_phase(pool, phase).await?;
        let result = future.await;
        self.finish_phase(pool, phase).await;
        result
    }

    async fn run_phases<T, Fut>(
        &mut self,
        pool: &PgPool,
        phases: &[&'static str],
        future: Fut,
    ) -> Result<T>
    where
        Fut: Future<Output = Result<T>>,
    {
        let mut started_phases = Vec::with_capacity(phases.len());
        for &phase in phases {
            if let Err(error) = self.begin_phase(pool, phase).await {
                for &started_phase in started_phases.iter().rev() {
                    self.finish_phase(pool, started_phase).await;
                }
                return Err(error);
            }
            started_phases.push(phase);
        }

        let result = future.await;
        for &phase in started_phases.iter().rev() {
            self.finish_phase(pool, phase).await;
        }
        result
    }

    async fn begin_phase(&mut self, pool: &PgPool, phase: &'static str) -> Result<()> {
        bigname_storage::begin_service_loop_phase(
            pool,
            bigname_storage::WORKER_SERVICE_NAME,
            &self.instance_id,
            phase,
        )
        .await
        .with_context(|| format!("failed to establish worker loop phase {phase}"))?;
        self.last_recorded_at = Some(Instant::now());
        Ok(())
    }

    async fn finish_phase(&mut self, pool: &PgPool, phase: &'static str) {
        match bigname_storage::finish_service_loop_phase(
            pool,
            bigname_storage::WORKER_SERVICE_NAME,
            &self.instance_id,
            phase,
        )
        .await
        {
            Ok(()) => self.last_recorded_at = Some(Instant::now()),
            Err(error) => warn!(
                service = "worker",
                heartbeat_instance_id = %self.instance_id,
                phase,
                error = %format!("{error:#}"),
                "failed to finish worker loop phase heartbeat; continuing with degraded liveness evidence"
            ),
        }
    }

    fn is_due(&self) -> bool {
        self.last_recorded_at
            .map(|recorded_at| recorded_at.elapsed() >= self.interval)
            .unwrap_or(true)
    }

    #[cfg(test)]
    pub(crate) const fn progress_record_count(&self) -> usize {
        self.progress_record_count
    }
}

pub(crate) async fn record_rebuild_progress(
    pool: &PgPool,
    loop_heartbeat: &mut Option<&mut LoopHeartbeat>,
) {
    if let Some(loop_heartbeat) = loop_heartbeat.as_deref_mut() {
        loop_heartbeat.record_if_due(pool).await;
    }
}

pub(crate) async fn run_rebuild_phase<T, Fut>(
    pool: &PgPool,
    loop_heartbeat: &mut Option<&mut LoopHeartbeat>,
    phase: &'static str,
    future: Fut,
) -> Result<T>
where
    Fut: Future<Output = Result<T>>,
{
    if let Some(loop_heartbeat) = loop_heartbeat.as_deref_mut() {
        loop_heartbeat.run_phase(pool, phase, future).await
    } else {
        future.await
    }
}

pub(crate) async fn run_rebuild_phases<T, Fut>(
    pool: &PgPool,
    loop_heartbeat: &mut Option<&mut LoopHeartbeat>,
    phases: &[&'static str],
    future: Fut,
) -> Result<T>
where
    Fut: Future<Output = Result<T>>,
{
    if let Some(loop_heartbeat) = loop_heartbeat.as_deref_mut() {
        loop_heartbeat.run_phases(pool, phases, future).await
    } else {
        future.await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use super::*;

    #[test]
    fn heartbeat_is_due_initially_but_not_again_inside_the_poll_interval() {
        let mut heartbeat = LoopHeartbeat::new("worker-test".to_owned(), Duration::from_secs(5));
        assert!(heartbeat.is_due());

        heartbeat.last_recorded_at = Some(Instant::now());
        assert!(!heartbeat.is_due());
    }

    #[test]
    fn progress_heartbeat_throttle_never_inherits_a_stale_poll_interval() {
        let heartbeat = LoopHeartbeat::new("long-poll-test".to_owned(), Duration::from_secs(60));

        assert_eq!(heartbeat.interval, Duration::from_secs(5));
    }

    #[tokio::test]
    async fn heartbeat_write_failure_is_warn_and_continue() {
        let pool = PgPool::connect_lazy_with(bigname_storage::stamp_projection_replay_version(
            "postgres://bigname:bigname@127.0.0.1:5432/bigname"
                .parse()
                .expect("static test database URL must parse"),
        ));
        pool.close().await;
        let mut heartbeat =
            LoopHeartbeat::new("worker-closed-pool".to_owned(), Duration::from_secs(5));

        heartbeat.record_if_due(&pool).await;

        assert!(
            heartbeat.is_due(),
            "a failed beat must remain due so the next progress boundary retries"
        );
    }

    #[tokio::test]
    async fn rebuild_phase_does_not_start_without_a_durable_phase_marker() {
        let pool = PgPool::connect_lazy_with(bigname_storage::stamp_projection_replay_version(
            "postgres://bigname:bigname@127.0.0.1:5432/bigname"
                .parse()
                .expect("static test database URL must parse"),
        ));
        pool.close().await;
        let mut heartbeat =
            LoopHeartbeat::new("worker-closed-pool".to_owned(), Duration::from_secs(5));
        let work_started = Arc::new(AtomicBool::new(false));
        let observed_work_started = Arc::clone(&work_started);

        let result = heartbeat
            .run_phase(&pool, "test_monolithic_phase", async move {
                observed_work_started.store(true, Ordering::SeqCst);
                Ok(())
            })
            .await;

        assert!(
            result.is_err(),
            "phase registration failure must abort the attempt"
        );
        assert!(
            !work_started.load(Ordering::SeqCst),
            "monolithic work must not start without its phase evidence"
        );
    }
}
