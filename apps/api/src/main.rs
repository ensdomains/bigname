#![recursion_limit = "256"]

use anyhow::{Context, Result, ensure};
use axum::{Router, routing::get};
use clap::Parser;
use sqlx::PgPool;
use tower_http::cors::CorsLayer;
use tracing::{info, warn};

#[cfg(test)]
use crate::errors::ErrorResponse;
#[cfg(test)]
use axum::http::StatusCode;
#[cfg(test)]
use std::collections::BTreeMap;

mod bounds;
mod cli;
mod errors;
mod graphql;
mod health;
mod metrics;
#[path = "support/service.rs"]
mod service;
mod state;
mod v2;

use crate::{
    bounds::ApiBoundsConfig,
    cli::*,
    errors::ApiError,
    health::{HEALTH_DATABASE_CHECK_TIMEOUT, HealthDatabasePool, health},
    service::{init_tracing, shutdown_signal},
    state::AppState,
};

pub(crate) const PUBLIC_NAMESPACES: &[&str] = &["ens", "basenames"];
pub(crate) const SOFTWARE_VERSION: &str = env!("CARGO_PKG_VERSION");
pub(crate) const BUILD_SHA: &str = match option_env!("BIGNAME_BUILD_SHA") {
    Some(build_sha) => build_sha,
    None => "unknown",
};
#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Serve(args) => {
            init_tracing("bigname-api");
            serve(*args).await
        }
    }
}

async fn serve(args: ServeArgs) -> Result<()> {
    args.bounds.validate()?;
    let chain_rpc_urls = args.effective_lookup_chain_rpc_urls()?;
    let pool = bigname_storage::connect_phase_with_application_name_and_statement_timeout(
        &args.database,
        "bigname-api",
        args.bounds.db_statement_timeout(),
    )
    .await?;
    let health_pool = bigname_storage::connect_phase_reserved_readiness_pool(
        &args.database,
        "bigname-api-health",
        HEALTH_DATABASE_CHECK_TIMEOUT,
    )
    .await?;
    let expected_status_chain_ids = load_expected_status_chain_ids_at_startup(&pool).await?;
    let missing_status_rpc_chains = v2::support::status_freshness::missing_status_rpc_chains(
        &expected_status_chain_ids,
        &chain_rpc_urls,
    );
    if !missing_status_rpc_chains.is_empty() {
        warn!(
            service = "api",
            configuration = "BIGNAME_API_CHAIN_RPC_URLS",
            missing_chain_ids = ?missing_status_rpc_chains,
            expected_chain_ids = ?expected_status_chain_ids,
            "status network-head RPC configuration is incomplete; indexing status remains fail-closed for the named chains"
        );
    }
    ensure!(
        args.phase_heartbeat_max_age_secs > 0,
        "BIGNAME_API_PHASE_HEARTBEAT_MAX_AGE_SECS must be greater than zero"
    );
    let status_freshness_config = v2::support::status_freshness::StatusFreshnessConfig::new(
        args.status_provider_timeout_ms,
        args.status_provider_refresh_secs,
        args.status_provider_cache_ttl_secs,
        args.status_max_block_lag,
        args.status_max_lag_secs,
    )?;
    let state = AppState::new_with_rpc_urls(pool, chain_rpc_urls)
        .with_phase_heartbeat_max_age_secs(args.phase_heartbeat_max_age_secs)
        .with_status_freshness_config(status_freshness_config);
    state
        .status_freshness
        .spawn_refresh(state.lookup_chain_rpc_urls.clone());
    let router = app_router_with_bounds(state, health_pool, &args.bounds);
    let listener = tokio::net::TcpListener::bind(args.bind_addr)
        .await
        .context("failed to bind the API listener")?;
    let metrics_server = metrics::bind(args.metrics_bind_addr).await?;

    info!(
        service = "api",
        bind_addr = %args.bind_addr,
        metrics_bind_addr = %args.metrics_bind_addr,
        version = SOFTWARE_VERSION,
        build_sha = BUILD_SHA,
        interpreter_content_hash = bigname_content_hash::INTERPRETER_CONTENT_HASH,
        request_timeout_ms = args.bounds.request_timeout_ms,
        db_statement_timeout_ms = args.bounds.db_statement_timeout_ms,
        health_database_check_timeout_ms = HEALTH_DATABASE_CHECK_TIMEOUT.as_millis(),
        health_database_reserved_connections = 1,
        max_in_flight = args.bounds.max_in_flight,
        health_max_in_flight = args.bounds.health_max_in_flight,
        verified_execution_max_in_flight = args.bounds.verified_execution_max_in_flight,
        rpc_connect_timeout_ms = args.rpc_connect_timeout_ms,
        rpc_timeout_ms = args.rpc_timeout_ms,
        phase_heartbeat_max_age_secs = args.phase_heartbeat_max_age_secs,
        verified_rate_limit_per_second = args.bounds.verified_rate_limit_per_second,
        verified_rate_limit_burst = args.bounds.verified_rate_limit_burst,
        verified_rate_limit_max_clients = args.bounds.verified_rate_limit_max_clients,
        trust_x_forwarded_for = args.bounds.trust_x_forwarded_for,
        "API booted"
    );

    let _metrics_task = tokio::spawn(async move {
        if let Err(error) = metrics_server.serve().await {
            tracing::error!(
                service = "api",
                error = %format!("{error:#}"),
                "metrics listener exited"
            );
        }
    });
    axum::serve(
        listener,
        router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal("api"))
    .await
    .context("API server exited unexpectedly")
}

async fn load_expected_status_chain_ids_at_startup(pool: &PgPool) -> Result<Vec<String>> {
    match bigname_storage::load_phase_expected_status_chain_ids(pool).await {
        Ok(chain_ids) => Ok(chain_ids),
        Err(error) => {
            if state::is_absent_phase_schema(pool, &error).await {
                warn!(
                    service = "api",
                    error = ?error,
                    "phase schema is not available at API startup; status starts without expected chains"
                );
                Ok(Vec::new())
            } else {
                Err(error)
            }
        }
    }
}

#[cfg(test)]
pub(crate) fn app_router(state: AppState) -> Router {
    let health_pool = state.pool.clone();
    app_router_with_bounds(state, health_pool, &ApiBoundsConfig::default())
}

fn app_router_with_bounds(
    state: AppState,
    health_pool: PgPool,
    bounds: &ApiBoundsConfig,
) -> Router {
    let bounded_router = v2::router()
        .with_state(state.clone())
        .merge(graphql::graphql_routes(state.clone()))
        .route_layer(CorsLayer::permissive());
    let health_router = Router::new()
        .route("/healthz", get(health))
        .route_layer(CorsLayer::permissive())
        .layer(axum::Extension(HealthDatabasePool(health_pool)))
        .with_state(state);
    // The API is read-only public data served cross-origin to browser clients (the ENS Manager
    // dev build, deployed on a different origin). Permissive CORS — wildcard origin, no
    // credentials — lets the browser read responses and answers the GraphQL POST preflight.
    // This is not access control: the endpoint is unauthenticated and reachable regardless;
    // CORS only governs whether browser JS on another origin may read the response.
    // Request bounds wrap CORS so even preflight responses pass through the family-wide backstop;
    // bound errors add the same wildcard origin header directly. Health uses reserved admission
    // outside the global ceiling and retains the request-timeout backstop.
    bounds::apply_request_bounds(bounded_router, health_router, bounds)
        .layer(axum::middleware::from_fn(metrics::track_http_request))
}

#[cfg(test)]
mod tests;
