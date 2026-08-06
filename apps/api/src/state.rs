use std::{str::FromStr, time::Duration};

use anyhow::{Context, Result};
use sqlx::{PgPool, postgres::PgConnectOptions};

use crate::v2::support::status_freshness::{StatusFreshness, StatusFreshnessConfig};

pub(crate) const DEFAULT_PHASE_HEARTBEAT_MAX_AGE_SECS: i64 = 60;

pub(crate) async fn is_absent_phase_schema(pool: &PgPool, error: &anyhow::Error) -> bool {
    let relation_is_undefined = error.chain().any(|source| {
        source
            .downcast_ref::<sqlx::Error>()
            .and_then(sqlx::Error::as_database_error)
            .and_then(|database_error| database_error.code())
            .is_some_and(|code| matches!(code.as_ref(), "42P01" | "3F000"))
    });
    if !relation_is_undefined {
        return false;
    }

    sqlx::query_scalar::<_, bool>(
        "SELECT NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'bigname_phase')",
    )
    .fetch_one(pool)
    .await
    .unwrap_or(false)
}

pub(crate) async fn connect_lookup_pool(
    config: &bigname_storage::DatabaseConfig,
    application_name: &str,
    statement_timeout: Duration,
) -> Result<PgPool> {
    let database_url = config
        .database_url
        .clone()
        .or_else(|| std::env::var("DATABASE_URL").ok())
        .unwrap_or_else(|| bigname_storage::default_database_url().to_owned());
    let options = bigname_storage::stamp_projection_replay_version(
        PgConnectOptions::from_str(&database_url)
            .context("failed to parse schema-v2 lookup database URL")?
            .application_name(application_name)
            .options([
                ("search_path", "bigname_phase".to_owned()),
                (
                    "statement_timeout",
                    format!("{}ms", statement_timeout.as_millis()),
                ),
            ]),
    );
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(config.max_connections)
        .connect_with(options)
        .await
        .context("failed to connect schema-v2 lookup PostgreSQL pool")
}

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) pool: PgPool,
    pub(crate) lookup_pool: PgPool,
    pub(crate) lookup_chain_rpc_urls: bigname_lookup::ChainRpcUrls,
    pub(crate) heartbeat_max_age_secs: i64,
    pub(crate) phase_heartbeat_max_age_secs: i64,
    pub(crate) indexer_chain_heartbeat_max_age_secs: i64,
    pub(crate) worker_rebuild_phase_max_age_secs: i64,
    pub(crate) status_freshness: StatusFreshness,
}

impl AppState {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn new(pool: PgPool, chain_rpc_urls: bigname_lookup::ChainRpcUrls) -> Self {
        Self::new_with_rpc_urls(pool.clone(), pool, chain_rpc_urls)
    }

    pub(crate) fn new_with_rpc_urls(
        pool: PgPool,
        lookup_pool: PgPool,
        lookup_chain_rpc_urls: bigname_lookup::ChainRpcUrls,
    ) -> Self {
        Self {
            pool,
            lookup_pool,
            lookup_chain_rpc_urls,
            heartbeat_max_age_secs: 20,
            phase_heartbeat_max_age_secs: DEFAULT_PHASE_HEARTBEAT_MAX_AGE_SECS,
            indexer_chain_heartbeat_max_age_secs:
                bigname_storage::DEFAULT_INDEXER_CHAIN_HEARTBEAT_MAX_AGE_SECS,
            worker_rebuild_phase_max_age_secs:
                bigname_storage::DEFAULT_WORKER_REBUILD_PHASE_MAX_AGE_SECS,
            status_freshness: StatusFreshness::new(StatusFreshnessConfig::default()),
        }
    }

    pub(crate) fn with_worker_rebuild_phase_max_age_secs(
        mut self,
        worker_rebuild_phase_max_age_secs: i64,
    ) -> Self {
        self.worker_rebuild_phase_max_age_secs = worker_rebuild_phase_max_age_secs;
        self
    }

    pub(crate) fn with_heartbeat_max_age_secs(mut self, heartbeat_max_age_secs: i64) -> Self {
        self.heartbeat_max_age_secs = heartbeat_max_age_secs;
        self
    }

    pub(crate) fn with_phase_heartbeat_max_age_secs(
        mut self,
        phase_heartbeat_max_age_secs: i64,
    ) -> Self {
        self.phase_heartbeat_max_age_secs = phase_heartbeat_max_age_secs;
        self
    }

    pub(crate) fn with_indexer_chain_heartbeat_max_age_secs(
        mut self,
        indexer_chain_heartbeat_max_age_secs: i64,
    ) -> Self {
        self.indexer_chain_heartbeat_max_age_secs = indexer_chain_heartbeat_max_age_secs;
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
