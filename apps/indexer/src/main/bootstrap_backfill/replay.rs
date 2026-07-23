use anyhow::Result;

use crate::{
    reconciliation::{
        RawFactNormalizedEventReplayOutcome, RawFactNormalizedEventReplayRequest,
        replay_raw_fact_normalized_events, replay_raw_fact_normalized_events_with_progress,
    },
    run::startup_heartbeat::{StartupAdapterHeartbeat, StartupHeartbeat},
};

pub(crate) async fn replay_completed_bootstrap_raw_range(
    pool: &sqlx::PgPool,
    request: RawFactNormalizedEventReplayRequest,
    heartbeat: Option<(&mut StartupHeartbeat, &[String])>,
) -> Result<RawFactNormalizedEventReplayOutcome> {
    match heartbeat {
        Some((heartbeat, chain_ids)) => {
            let mut progress = StartupAdapterHeartbeat::new(heartbeat, chain_ids);
            replay_raw_fact_normalized_events_with_progress(pool, request, &mut progress).await
        }
        None => replay_raw_fact_normalized_events(pool, request).await,
    }
}
