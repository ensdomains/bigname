use bigname_execution::ChainRpcUrls;
use sqlx::PgPool;

use crate::status_freshness::{StatusFreshness, StatusFreshnessConfig};

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) pool: PgPool,
    pub(crate) chain_rpc_urls: ChainRpcUrls,
    pub(crate) heartbeat_max_age_secs: i64,
    pub(crate) status_freshness: StatusFreshness,
}

impl AppState {
    pub(crate) fn new(pool: PgPool, chain_rpc_urls: ChainRpcUrls) -> Self {
        Self {
            pool,
            chain_rpc_urls,
            heartbeat_max_age_secs: 20,
            status_freshness: StatusFreshness::new(StatusFreshnessConfig::default()),
        }
    }

    pub(crate) fn with_heartbeat_max_age_secs(mut self, heartbeat_max_age_secs: i64) -> Self {
        self.heartbeat_max_age_secs = heartbeat_max_age_secs;
        self
    }

    pub(crate) fn with_status_freshness_config(
        mut self,
        status_freshness_config: StatusFreshnessConfig,
    ) -> Self {
        self.status_freshness = StatusFreshness::new(status_freshness_config);
        self
    }
}
