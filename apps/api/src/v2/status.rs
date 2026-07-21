use std::collections::BTreeMap;

use axum::{Json, extract::State};
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::AppState;

use super::{
    Envelope, Meta, NoQueryParams, OpsStatus, V2Error, V2Result, format_timestamp, slug_to_numeric,
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct StatusData {
    pub(crate) status: OpsStatus,
    pub(crate) pending_invalidation_count: i64,
    pub(crate) pending_invalidation_count_capped: bool,
    pub(crate) dead_letter_count: i64,
    pub(crate) chains: BTreeMap<String, ChainStatus>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ChainStatus {
    pub(crate) latest_block: Option<i64>,
    pub(crate) indexed_block: Option<i64>,
    pub(crate) safe_block: Option<i64>,
    pub(crate) finalized_block: Option<i64>,
    pub(crate) lag_blocks: Option<i64>,
    pub(crate) lag_seconds: Option<i64>,
    pub(crate) network_block: Option<i64>,
    pub(crate) network_head_observed_at: Option<String>,
    pub(crate) network_head_age_seconds: Option<i64>,
    pub(crate) network_head_status: String,
    pub(crate) ingestion_lag_blocks: Option<i64>,
    pub(crate) ingestion_lag_seconds: Option<i64>,
    pub(crate) status: OpsStatus,
}

pub(crate) async fn get_status(
    _no_query: NoQueryParams,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<StatusData>>> {
    let read = bigname_storage::load_indexing_status(&state.pool)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                error = ?load_error,
                "failed to load v2 indexing status"
            );
            V2Error::internal_error("failed to load indexing status")
        })?;

    Ok(Json(Envelope {
        data: build_status_data(&read, &state).await?,
        page: None,
        meta: Meta::default(),
    }))
}

async fn build_status_data(
    read: &bigname_storage::IndexingStatusRead,
    state: &AppState,
) -> V2Result<StatusData> {
    let mut chains = BTreeMap::new();
    for row in &read.chains {
        let chain_key = status_chain_key(&row.chain_id)?;
        let network_head = state
            .status_freshness
            .compare(
                &state.chain_rpc_urls,
                &row.chain_id,
                row.canonical_block,
                row.canonical_timestamp,
            )
            .await;
        chains.insert(chain_key, build_chain_status(row, network_head));
    }

    Ok(StatusData {
        status: root_status(chains.values(), read.has_unscoped_pending_invalidations),
        pending_invalidation_count: read.pending_invalidation_count,
        pending_invalidation_count_capped: read.pending_invalidation_count_capped,
        dead_letter_count: read.dead_letter_count,
        chains,
    })
}

fn status_chain_key(storage_chain_id: &str) -> V2Result<String> {
    let numeric = slug_to_numeric(storage_chain_id).ok_or_else(|| {
        V2Error::internal_error(format!(
            "indexing status row uses unmapped chain_id {storage_chain_id}"
        ))
    })?;

    Ok(numeric.to_string())
}

fn build_chain_status(
    row: &bigname_storage::IndexingStatusChainRow,
    network_head: crate::status_freshness::NetworkHeadComparison,
) -> ChainStatus {
    let lag_blocks = row
        .canonical_block
        .zip(row.latest_projected_block)
        .map(|(canonical, projected)| canonical.saturating_sub(projected));
    let lag_seconds = row
        .canonical_timestamp
        .zip(row.latest_projected_timestamp)
        .map(|(canonical, projected)| (canonical - projected).whole_seconds().max(0));
    let status = chain_status(
        row.canonical_block,
        row.latest_projected_block,
        lag_blocks,
        &network_head,
    );

    ChainStatus {
        latest_block: row.canonical_block,
        indexed_block: row.latest_projected_block,
        safe_block: row.safe_block,
        finalized_block: row.finalized_block,
        lag_blocks,
        lag_seconds,
        network_block: network_head.block,
        network_head_observed_at: network_head.observed_at.map(format_timestamp),
        network_head_age_seconds: network_head.age_seconds,
        network_head_status: network_head.status.as_str().to_owned(),
        ingestion_lag_blocks: network_head.ingestion_lag_blocks,
        ingestion_lag_seconds: network_head.ingestion_lag_seconds,
        status,
    }
}

fn chain_status(
    latest_block: Option<i64>,
    indexed_block: Option<i64>,
    lag_blocks: Option<i64>,
    network_head: &crate::status_freshness::NetworkHeadComparison,
) -> OpsStatus {
    match crate::status_freshness::status_readiness(
        latest_block,
        indexed_block,
        lag_blocks,
        network_head,
    ) {
        crate::status_freshness::StatusReadiness::Ready => OpsStatus::Ready,
        crate::status_freshness::StatusReadiness::Degraded => OpsStatus::Degraded,
        crate::status_freshness::StatusReadiness::Stale => OpsStatus::Stale,
    }
}

fn root_status<'a>(
    chains: impl Iterator<Item = &'a ChainStatus>,
    has_unscoped_pending_invalidations: bool,
) -> OpsStatus {
    let mut saw_degraded = has_unscoped_pending_invalidations;
    let mut saw_chain = false;

    for chain in chains {
        saw_chain = true;
        match chain.status {
            OpsStatus::Stale => return OpsStatus::Stale,
            OpsStatus::Degraded => saw_degraded = true,
            OpsStatus::Ready => {}
        }
    }

    if saw_degraded || !saw_chain {
        OpsStatus::Degraded
    } else {
        OpsStatus::Ready
    }
}

#[cfg(test)]
mod tests {
    use axum::{
        extract::FromRequestParts,
        http::{Request, StatusCode},
        response::IntoResponse,
    };
    use sqlx::{PgPool, types::time::OffsetDateTime};

    use super::*;

    #[tokio::test]
    async fn status_data_maps_chains_by_numeric_chain_id_and_derives_chain_statuses() {
        let read = bigname_storage::IndexingStatusRead {
            chains: vec![
                row(
                    bigname_storage::ETHEREUM_MAINNET_CHAIN_ID,
                    Some(100),
                    Some(100),
                    Some(90),
                    Some(80),
                ),
                row(
                    bigname_storage::BASE_MAINNET_CHAIN_ID,
                    None,
                    Some(50),
                    Some(40),
                    Some(30),
                ),
                row("base-sepolia", Some(100), Some(95), Some(90), Some(80)),
            ],
            has_unscoped_pending_invalidations: false,
            pending_invalidation_count: 7,
            pending_invalidation_count_capped: false,
            dead_letter_count: 2,
        };
        let chain_rpc_urls = bigname_execution::ChainRpcUrls::from_entries(&[
            "ethereum-mainnet=http://rpc.test".to_owned(),
            "base-mainnet=http://rpc.test".to_owned(),
            "base-sepolia=http://rpc.test".to_owned(),
        ])
        .expect("test RPC map must be valid");
        let state = AppState::new(
            PgPool::connect_lazy("postgres://bigname:bigname@127.0.0.1:5432/bigname")
                .expect("status builder test does not use the database"),
            chain_rpc_urls,
        );
        let observed_at = OffsetDateTime::now_utc();
        for (chain_id, block) in [
            (bigname_storage::ETHEREUM_MAINNET_CHAIN_ID, 100),
            (bigname_storage::BASE_MAINNET_CHAIN_ID, 50),
            ("base-sepolia", 100),
        ] {
            state
                .status_freshness
                .seed_success(chain_id, block, observed_at)
                .await;
        }

        let data = build_status_data(&read, &state)
            .await
            .expect("known storage chain slugs must map");

        assert_eq!(data.status, OpsStatus::Stale);
        assert_eq!(data.pending_invalidation_count, 7);
        assert!(!data.pending_invalidation_count_capped);
        assert_eq!(data.dead_letter_count, 2);
        assert_eq!(
            data.chains.keys().collect::<Vec<_>>(),
            vec!["1", "8453", "84532"]
        );
        assert_eq!(data.chains["1"].status, OpsStatus::Ready);
        assert_eq!(data.chains["1"].latest_block, Some(100));
        assert_eq!(data.chains["1"].indexed_block, Some(100));
        assert_eq!(data.chains["1"].lag_blocks, Some(0));
        assert_eq!(data.chains["1"].lag_seconds, Some(0));
        assert_eq!(data.chains["1"].network_block, Some(100));
        assert_eq!(data.chains["1"].network_head_status, "fresh");
        assert_eq!(data.chains["1"].ingestion_lag_blocks, Some(0));
        assert_eq!(data.chains["1"].ingestion_lag_seconds, Some(0));
        assert_eq!(data.chains["8453"].status, OpsStatus::Degraded);
        assert_eq!(data.chains["8453"].lag_blocks, None);
        assert_eq!(data.chains["84532"].status, OpsStatus::Stale);
        assert_eq!(data.chains["84532"].lag_blocks, Some(5));
        assert_eq!(data.chains["84532"].lag_seconds, Some(50));
    }

    #[tokio::test]
    async fn status_data_rejects_unmapped_storage_chain_slugs() {
        let read = bigname_storage::IndexingStatusRead {
            chains: vec![row(
                "future-mainnet",
                Some(100),
                Some(100),
                Some(90),
                Some(80),
            )],
            has_unscoped_pending_invalidations: false,
            pending_invalidation_count: 0,
            pending_invalidation_count_capped: false,
            dead_letter_count: 0,
        };
        let state = AppState::new(
            PgPool::connect_lazy("postgres://bigname:bigname@127.0.0.1:5432/bigname")
                .expect("status builder test does not use the database"),
            bigname_execution::ChainRpcUrls::default(),
        );

        let error = build_status_data(&read, &state)
            .await
            .expect_err("unknown storage slug must fail loudly");

        assert_eq!(error.envelope().error.code, "internal_error");
    }

    #[test]
    fn root_status_aggregates_stale_degraded_invalidations_and_empty_reads() {
        let ready = ChainStatus {
            latest_block: Some(100),
            indexed_block: Some(100),
            safe_block: Some(90),
            finalized_block: Some(80),
            lag_blocks: Some(0),
            lag_seconds: Some(0),
            network_block: Some(100),
            network_head_observed_at: None,
            network_head_age_seconds: Some(0),
            network_head_status: "fresh".to_owned(),
            ingestion_lag_blocks: Some(0),
            ingestion_lag_seconds: Some(0),
            status: OpsStatus::Ready,
        };
        let degraded = ChainStatus {
            latest_block: None,
            indexed_block: Some(100),
            safe_block: Some(90),
            finalized_block: Some(80),
            lag_blocks: None,
            lag_seconds: None,
            network_block: None,
            network_head_observed_at: None,
            network_head_age_seconds: None,
            network_head_status: "unavailable".to_owned(),
            ingestion_lag_blocks: None,
            ingestion_lag_seconds: None,
            status: OpsStatus::Degraded,
        };
        let stale = ChainStatus {
            latest_block: Some(100),
            indexed_block: Some(99),
            safe_block: Some(90),
            finalized_block: Some(80),
            lag_blocks: Some(1),
            lag_seconds: Some(10),
            network_block: Some(100),
            network_head_observed_at: None,
            network_head_age_seconds: Some(0),
            network_head_status: "fresh".to_owned(),
            ingestion_lag_blocks: Some(0),
            ingestion_lag_seconds: Some(0),
            status: OpsStatus::Stale,
        };

        assert_eq!(root_status([&ready].into_iter(), false), OpsStatus::Ready);
        assert_eq!(
            root_status([&ready, &degraded].into_iter(), false),
            OpsStatus::Degraded
        );
        assert_eq!(
            root_status([&ready, &stale, &degraded].into_iter(), false),
            OpsStatus::Stale
        );
        assert_eq!(root_status([&ready].into_iter(), true), OpsStatus::Degraded);
        assert_eq!(root_status([].into_iter(), false), OpsStatus::Degraded);
    }

    #[tokio::test]
    async fn no_query_params_rejects_status_controls_with_bad_request() {
        let request = Request::builder()
            .uri("/v2/status?finality=safe")
            .body(())
            .expect("request must build");
        let (mut parts, ()) = request.into_parts();

        let error = NoQueryParams::from_request_parts(&mut parts, &())
            .await
            .expect_err("status route must reject query params");
        let envelope = error.envelope();
        let response = error.into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(envelope.error.code, "invalid_input");
    }

    fn row(
        chain_id: &str,
        canonical_block: Option<i64>,
        latest_projected_block: Option<i64>,
        safe_block: Option<i64>,
        finalized_block: Option<i64>,
    ) -> bigname_storage::IndexingStatusChainRow {
        bigname_storage::IndexingStatusChainRow {
            chain_id: chain_id.to_owned(),
            canonical_block,
            safe_block,
            finalized_block,
            canonical_timestamp: canonical_block.map(timestamp_for_block),
            latest_projected_block,
            latest_projected_timestamp: latest_projected_block.map(timestamp_for_block),
        }
    }

    fn timestamp_for_block(block: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(block * 10).expect("test timestamp must be valid")
    }
}
