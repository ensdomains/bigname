use anyhow::Result;
use sqlx::PgPool;
use tokio::time::{Duration, Instant};

pub(crate) struct LoopHeartbeat {
    instance_id: String,
    interval: Duration,
    last_recorded_at: Option<Instant>,
}

impl LoopHeartbeat {
    pub(crate) fn new(instance_id: String, interval: Duration) -> Self {
        Self {
            instance_id,
            interval,
            last_recorded_at: None,
        }
    }

    pub(crate) async fn record_if_due(&mut self, pool: &PgPool) -> Result<()> {
        if !self.is_due() {
            return Ok(());
        }

        bigname_storage::record_service_loop_heartbeat(
            pool,
            bigname_storage::WORKER_SERVICE_NAME,
            &self.instance_id,
            &[],
        )
        .await?;
        self.last_recorded_at = Some(Instant::now());
        Ok(())
    }

    fn is_due(&self) -> bool {
        self.last_recorded_at
            .map(|recorded_at| recorded_at.elapsed() >= self.interval)
            .unwrap_or(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_is_due_initially_but_not_again_inside_the_poll_interval() {
        let mut heartbeat = LoopHeartbeat::new("worker-test".to_owned(), Duration::from_secs(5));
        assert!(heartbeat.is_due());

        heartbeat.last_recorded_at = Some(Instant::now());
        assert!(!heartbeat.is_due());
    }
}
