use sqlx::PgPool;
use tokio::time::{Duration, sleep};
use tracing::{info, warn};

pub(super) fn spawn(
    pool: PgPool,
    poll_interval_secs: u64,
    retention_checkpoints: i64,
    batch_size: i64,
) {
    tokio::spawn(async move {
        run(pool, poll_interval_secs, retention_checkpoints, batch_size).await;
    });
}

async fn run(pool: PgPool, poll_interval_secs: u64, retention_checkpoints: i64, batch_size: i64) {
    let poll_interval = Duration::from_secs(poll_interval_secs.max(1));
    info!(
        service = "worker",
        retention_checkpoints, batch_size, "route-local primary-name execution pruning started"
    );

    loop {
        match bigname_storage::prune_route_local_primary_name_execution(
            &pool,
            retention_checkpoints,
            batch_size,
        )
        .await
        {
            Ok(summary) if summary.deleted_outcome_count > 0 => {
                info!(
                    service = "worker",
                    head_block_number = summary.head_block_number,
                    cutoff_block_number = summary.cutoff_block_number,
                    deleted_outcome_count = summary.deleted_outcome_count,
                    deleted_trace_count = summary.deleted_trace_count,
                    "pruned stale route-local primary-name execution artifacts"
                );
            }
            Ok(_) => {}
            Err(error) => {
                warn!(
                    service = "worker",
                    error = %format!("{error:#}"),
                    "route-local primary-name execution pruning failed"
                );
            }
        }
        sleep(poll_interval).await;
    }
}
