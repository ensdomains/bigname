use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::{Result, ensure};
use bigname_execution::ChainRpcUrls;
use sqlx::types::time::OffsetDateTime;
use tokio::{sync::RwLock, task::JoinSet, time::MissedTickBehavior};
use tracing::warn;

pub(crate) const DEFAULT_PROVIDER_TIMEOUT_MS: u64 = 750;
pub(crate) const DEFAULT_PROVIDER_REFRESH_SECS: u64 = 5;
pub(crate) const DEFAULT_PROVIDER_CACHE_TTL_SECS: u64 = 30;
pub(crate) const DEFAULT_MAX_BLOCK_LAG: i64 = 5;
pub(crate) const DEFAULT_MAX_LAG_SECS: i64 = 60;

#[derive(Clone, Debug)]
pub(crate) struct StatusFreshnessConfig {
    provider_timeout: Duration,
    provider_refresh: Duration,
    provider_cache_ttl: Duration,
    max_block_lag: i64,
    max_lag_seconds: i64,
}

impl StatusFreshnessConfig {
    pub(crate) fn new(
        provider_timeout_ms: u64,
        provider_refresh_secs: u64,
        provider_cache_ttl_secs: u64,
        max_block_lag: i64,
        max_lag_seconds: i64,
    ) -> Result<Self> {
        ensure!(
            provider_timeout_ms > 0,
            "status provider timeout must be positive"
        );
        ensure!(
            provider_refresh_secs > 0,
            "status provider refresh must be positive"
        );
        ensure!(
            provider_cache_ttl_secs > 0,
            "status provider cache TTL must be positive"
        );
        ensure!(
            max_block_lag >= 0,
            "status maximum block lag must not be negative"
        );
        ensure!(
            max_lag_seconds >= 0,
            "status maximum seconds lag must not be negative"
        );

        Ok(Self {
            provider_timeout: Duration::from_millis(provider_timeout_ms),
            provider_refresh: Duration::from_secs(provider_refresh_secs),
            provider_cache_ttl: Duration::from_secs(provider_cache_ttl_secs),
            max_block_lag,
            max_lag_seconds,
        })
    }
}

impl Default for StatusFreshnessConfig {
    fn default() -> Self {
        Self::new(
            DEFAULT_PROVIDER_TIMEOUT_MS,
            DEFAULT_PROVIDER_REFRESH_SECS,
            DEFAULT_PROVIDER_CACHE_TTL_SECS,
            DEFAULT_MAX_BLOCK_LAG,
            DEFAULT_MAX_LAG_SECS,
        )
        .expect("default status freshness configuration must be valid")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NetworkHeadStatus {
    Fresh,
    Stale,
    Unavailable,
    Pending,
    Unconfigured,
}

impl NetworkHeadStatus {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::Stale => "stale",
            Self::Unavailable => "unavailable",
            Self::Pending => "pending",
            Self::Unconfigured => "unconfigured",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NetworkHeadComparison {
    pub(crate) status: NetworkHeadStatus,
    pub(crate) block: Option<i64>,
    pub(crate) observed_at: Option<OffsetDateTime>,
    pub(crate) age_seconds: Option<i64>,
    pub(crate) ingestion_lag_blocks: Option<i64>,
    pub(crate) ingestion_lag_seconds: Option<i64>,
    pub(crate) data_is_stale: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StatusReadiness {
    Ready,
    Degraded,
    Stale,
}

pub(crate) fn status_readiness(
    canonical_block: Option<i64>,
    latest_projected_block: Option<i64>,
    projection_lag_blocks: Option<i64>,
    network_head: &NetworkHeadComparison,
) -> StatusReadiness {
    if canonical_block.is_none() || latest_projected_block.is_none() {
        return StatusReadiness::Degraded;
    }
    if projection_lag_blocks.is_some_and(|lag| lag > 0) || network_head.data_is_stale {
        return StatusReadiness::Stale;
    }
    if network_head.status != NetworkHeadStatus::Fresh {
        return StatusReadiness::Degraded;
    }
    StatusReadiness::Ready
}

#[derive(Clone, Debug)]
struct SuccessfulNetworkHead {
    block: i64,
    observed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Default)]
struct CachedNetworkHead {
    attempted: bool,
    latest_attempt_failed: bool,
    successful: Option<SuccessfulNetworkHead>,
}

#[derive(Clone, Debug)]
pub(crate) struct StatusFreshness {
    config: StatusFreshnessConfig,
    cache: Arc<RwLock<BTreeMap<String, CachedNetworkHead>>>,
}

impl StatusFreshness {
    pub(crate) fn new(config: StatusFreshnessConfig) -> Self {
        Self {
            config,
            cache: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub(crate) fn spawn_refresh(&self, chain_rpc_urls: ChainRpcUrls) {
        let freshness = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(freshness.config.provider_refresh);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                freshness.refresh_once(&chain_rpc_urls).await;
            }
        });
    }

    async fn refresh_once(&self, chain_rpc_urls: &ChainRpcUrls) {
        let mut probes = JoinSet::new();
        for (chain_id, endpoint) in chain_rpc_urls.iter() {
            let chain_id = chain_id.to_owned();
            let endpoint = endpoint.to_owned();
            let timeout = self.config.provider_timeout;
            probes.spawn(async move {
                let result = tokio::time::timeout(
                    timeout,
                    bigname_execution::fetch_network_head_block_number(&endpoint),
                )
                .await
                .map_err(|_| ())
                .and_then(|result| result.map_err(|_| ()));
                (chain_id, result)
            });
        }

        while let Some(probe) = probes.join_next().await {
            let Ok((chain_id, result)) = probe else {
                warn!(
                    service = "api",
                    probe = "network_head",
                    "network-head refresh task failed"
                );
                continue;
            };
            let mut cache = self.cache.write().await;
            let entry = cache.entry(chain_id.clone()).or_default();
            entry.attempted = true;
            match result {
                Ok(block) => {
                    entry.latest_attempt_failed = false;
                    entry.successful = Some(SuccessfulNetworkHead {
                        block,
                        observed_at: OffsetDateTime::now_utc(),
                    });
                }
                Err(()) => {
                    entry.latest_attempt_failed = true;
                    warn!(
                        service = "api",
                        chain_id,
                        probe = "network_head",
                        "network-head refresh failed or timed out"
                    );
                }
            }
        }
    }

    pub(crate) async fn compare(
        &self,
        chain_rpc_urls: &ChainRpcUrls,
        chain_id: &str,
        canonical_block: Option<i64>,
        canonical_timestamp: Option<OffsetDateTime>,
    ) -> NetworkHeadComparison {
        if chain_rpc_urls.url_for(chain_id).is_none() {
            return empty_comparison(NetworkHeadStatus::Unconfigured);
        }

        let cached = self.cache.read().await.get(chain_id).cloned();
        let Some(cached) = cached else {
            return empty_comparison(NetworkHeadStatus::Pending);
        };
        let Some(successful) = cached.successful else {
            return empty_comparison(if cached.attempted {
                NetworkHeadStatus::Unavailable
            } else {
                NetworkHeadStatus::Pending
            });
        };

        let mut comparison =
            self.compare_successful(successful, canonical_block, canonical_timestamp);
        if cached.latest_attempt_failed {
            comparison.status = NetworkHeadStatus::Unavailable;
            comparison.data_is_stale = false;
        }
        comparison
    }

    fn compare_successful(
        &self,
        successful: SuccessfulNetworkHead,
        canonical_block: Option<i64>,
        canonical_timestamp: Option<OffsetDateTime>,
    ) -> NetworkHeadComparison {
        let age_seconds = (OffsetDateTime::now_utc() - successful.observed_at)
            .whole_seconds()
            .max(0);
        let status = if age_seconds
            > i64::try_from(self.config.provider_cache_ttl.as_secs()).unwrap_or(i64::MAX)
        {
            NetworkHeadStatus::Stale
        } else {
            NetworkHeadStatus::Fresh
        };
        let ingestion_lag_blocks =
            canonical_block.map(|canonical| successful.block.saturating_sub(canonical).max(0));
        let ingestion_lag_seconds =
            canonical_block
                .zip(canonical_timestamp)
                .map(|(canonical, canonical_timestamp)| {
                    if successful.block <= canonical {
                        0
                    } else {
                        (successful.observed_at - canonical_timestamp)
                            .whole_seconds()
                            .max(0)
                    }
                });
        let data_is_stale = status == NetworkHeadStatus::Fresh
            && (ingestion_lag_blocks.is_some_and(|lag| lag > self.config.max_block_lag)
                || ingestion_lag_seconds.is_some_and(|lag| lag > self.config.max_lag_seconds));

        NetworkHeadComparison {
            status,
            block: Some(successful.block),
            observed_at: Some(successful.observed_at),
            age_seconds: Some(age_seconds),
            ingestion_lag_blocks,
            ingestion_lag_seconds,
            data_is_stale,
        }
    }

    #[cfg(test)]
    pub(crate) async fn seed_success(
        &self,
        chain_id: &str,
        block: i64,
        observed_at: OffsetDateTime,
    ) {
        self.cache.write().await.insert(
            chain_id.to_owned(),
            CachedNetworkHead {
                attempted: true,
                latest_attempt_failed: false,
                successful: Some(SuccessfulNetworkHead { block, observed_at }),
            },
        );
    }

    #[cfg(test)]
    pub(crate) async fn seed_unavailable(&self, chain_id: &str) {
        self.cache.write().await.insert(
            chain_id.to_owned(),
            CachedNetworkHead {
                attempted: true,
                latest_attempt_failed: true,
                successful: None,
            },
        );
    }
}

fn empty_comparison(status: NetworkHeadStatus) -> NetworkHeadComparison {
    NetworkHeadComparison {
        status,
        block: None,
        observed_at: None,
        age_seconds: None,
        ingestion_lag_blocks: None,
        ingestion_lag_seconds: None,
        data_is_stale: false,
    }
}

#[cfg(test)]
#[path = "status_freshness/tests.rs"]
mod tests;
