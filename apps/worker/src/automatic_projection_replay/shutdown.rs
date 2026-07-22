use std::future::Future;

use anyhow::{Context, Result};
use sqlx::PgPool;
use tracing::info;

pub(super) async fn run_until_shutdown<Work, Shutdown>(
    pool: &PgPool,
    heartbeat_instance_id: &str,
    work: Work,
    shutdown: Shutdown,
) -> Result<()>
where
    Work: Future<Output = Result<()>>,
    Shutdown: Future<Output = Result<()>>,
{
    let (result, shutdown_received) = tokio::select! {
        result = work => (result, false),
        signal = shutdown => (signal, true),
    };

    let deregistration = bigname_storage::deregister_service_loop(
        pool,
        bigname_storage::WORKER_SERVICE_NAME,
        heartbeat_instance_id,
    )
    .await;
    if let Err(deregistration_error) = deregistration {
        return match result {
            Ok(()) => Err(deregistration_error),
            Err(work_error) => Err(work_error.context(format!(
                "failed to deregister worker service loop after the parent loop stopped: {deregistration_error:#}"
            ))),
        };
    }
    if shutdown_received && result.is_ok() {
        info!(service = "worker", "shutdown signal received");
    }
    result
}

#[cfg(unix)]
pub(super) async fn shutdown_signal() -> Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("failed to listen for termination signal")?;
    tokio::select! {
        signal = tokio::signal::ctrl_c() => {
            signal.context("failed to listen for interrupt signal")
        }
        signal = terminate.recv() => {
            signal.context("termination signal listener closed")
        }
    }
}

#[cfg(not(unix))]
pub(super) async fn shutdown_signal() -> Result<()> {
    tokio::signal::ctrl_c()
        .await
        .context("failed to listen for shutdown signal")
}
