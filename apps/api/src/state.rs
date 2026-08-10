use std::sync::Arc;

use sqlx::PgPool;

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

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) pool: PgPool,
    pub(crate) lookup_chain_rpc_urls: bigname_lookup::ChainRpcUrls,
    pub(crate) phase_heartbeat_max_age_secs: i64,
    pub(crate) status_freshness: StatusFreshness,
    public_namespaces_override: Option<Arc<[String]>>,
}

impl AppState {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn new(pool: PgPool, chain_rpc_urls: bigname_lookup::ChainRpcUrls) -> Self {
        Self::new_with_rpc_urls(pool, chain_rpc_urls)
    }

    pub(crate) fn new_with_rpc_urls(
        pool: PgPool,
        lookup_chain_rpc_urls: bigname_lookup::ChainRpcUrls,
    ) -> Self {
        Self {
            pool,
            lookup_chain_rpc_urls,
            phase_heartbeat_max_age_secs: DEFAULT_PHASE_HEARTBEAT_MAX_AGE_SECS,
            status_freshness: StatusFreshness::new(StatusFreshnessConfig::default()),
            public_namespaces_override: None,
        }
    }

    pub(crate) fn public_namespaces_override(&self) -> Option<Arc<[String]>> {
        self.public_namespaces_override.clone()
    }

    #[cfg(test)]
    pub(crate) fn with_public_namespaces_for_test(
        mut self,
        namespaces: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let mut namespaces = namespaces.into_iter().map(Into::into).collect::<Vec<_>>();
        namespaces.sort();
        namespaces.dedup();
        self.public_namespaces_override = Some(Arc::from(namespaces));
        self
    }

    pub(crate) fn with_phase_heartbeat_max_age_secs(
        mut self,
        phase_heartbeat_max_age_secs: i64,
    ) -> Self {
        self.phase_heartbeat_max_age_secs = phase_heartbeat_max_age_secs;
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

pub(crate) fn is_recognized_public_namespace(namespace: &str) -> bool {
    matches!(namespace, "ens" | "basenames")
}
