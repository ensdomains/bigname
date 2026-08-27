use std::net::SocketAddr;

use clap::Args;

#[derive(Clone, Debug, Args)]
pub(super) struct HeartbeatArgs {
    #[arg(
        long,
        env = "BIGNAME_PHASE_RUNNER_HEARTBEAT_STALE_AFTER_SECS",
        default_value_t = 900
    )]
    pub(super) heartbeat_stale_after_secs: i64,
}

#[derive(Clone, Debug, Args)]
pub(super) struct MonitoringArgs {
    #[arg(
        long,
        env = "BIGNAME_PHASE_RUNNER_METRICS_BIND_ADDR",
        default_value = "127.0.0.1:9465"
    )]
    pub(super) metrics_bind_addr: SocketAddr,

    #[command(flatten)]
    pub(super) heartbeat: HeartbeatArgs,
}
