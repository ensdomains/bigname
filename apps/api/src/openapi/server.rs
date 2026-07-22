use anyhow::{Context, Result, ensure};
use axum::{Json, response::Html, routing::get};
use serde_json::{Map as JsonMap, json};
use sqlx::types::JsonValue;
use tower_http::cors::CorsLayer;
use tracing::{info, warn};

use crate::{
    API_ROUTE_DEFINITIONS, ApiBoundsConfig, AppState, BUILD_SHA, Router, SOFTWARE_VERSION, ServeArgs,
    HEALTH_DATABASE_CHECK_TIMEOUT, HealthDatabasePool, shutdown_signal,
    status_freshness::{StatusFreshnessConfig, missing_status_rpc_chains},
    warm_compact_records_route_sql_path,
};

use super::schemas::openapi_components;

const OPENAPI_DOCS_HTML: &str = include_str!("docs.html");

pub(crate) async fn serve(args: ServeArgs) -> Result<()> {
    args.bounds.validate()?;
    let chain_rpc_urls = args.effective_chain_rpc_urls()?;
    let pool = bigname_storage::connect_with_application_name_and_statement_timeout(
        &args.database,
        "bigname-api",
        args.bounds.db_statement_timeout(),
    )
    .await?;
    let health_pool = bigname_storage::connect_reserved_readiness_pool(
        &args.database,
        "bigname-api-health",
        HEALTH_DATABASE_CHECK_TIMEOUT,
    )
    .await?;
    let expected_status_chain_ids =
        bigname_storage::load_expected_status_chain_ids(&pool).await?;
    let missing_status_rpc_chains =
        missing_status_rpc_chains(&expected_status_chain_ids, &chain_rpc_urls);
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
        args.heartbeat_max_age_secs > 0,
        "BIGNAME_API_HEARTBEAT_MAX_AGE_SECS must be greater than zero"
    );
    ensure!(
        args.worker_rebuild_phase_max_age_secs > 0,
        "BIGNAME_API_WORKER_REBUILD_PHASE_MAX_AGE_SECS must be greater than zero"
    );
    let status_freshness_config = StatusFreshnessConfig::new(
        args.status_provider_timeout_ms,
        args.status_provider_refresh_secs,
        args.status_provider_cache_ttl_secs,
        args.status_max_block_lag,
        args.status_max_lag_secs,
    )?;
    let state = AppState::new(pool, chain_rpc_urls)
        .with_heartbeat_max_age_secs(args.heartbeat_max_age_secs)
        .with_worker_rebuild_phase_max_age_secs(args.worker_rebuild_phase_max_age_secs)
        .with_status_freshness_config(status_freshness_config);
    state
        .status_freshness
        .spawn_refresh(state.chain_rpc_urls.clone());
    warm_compact_records_route_sql_path(&state, args.database.max_connections)
        .await
        .context("failed to warm compact records route SQL path")?;
    let router = app_router_with_bounds(state, health_pool, &args.bounds);
    let listener = tokio::net::TcpListener::bind(args.bind_addr)
        .await
        .context("failed to bind the API listener")?;

    info!(
        service = "api",
        bind_addr = %args.bind_addr,
        version = SOFTWARE_VERSION,
        build_sha = BUILD_SHA,
        schema_migration_version = bigname_storage::latest_migration_version(),
        projection_replay_version = bigname_storage::CURRENT_PROJECTION_REPLAY_VERSION,
        permissions_current_publication_version = bigname_storage::PERMISSIONS_CURRENT_PUBLICATION_VERSION,
        request_timeout_ms = args.bounds.request_timeout_ms,
        db_statement_timeout_ms = args.bounds.db_statement_timeout_ms,
        health_database_check_timeout_ms = HEALTH_DATABASE_CHECK_TIMEOUT.as_millis(),
        health_database_reserved_connections = 1,
        max_in_flight = args.bounds.max_in_flight,
        health_max_in_flight = args.bounds.health_max_in_flight,
        verified_execution_max_in_flight = args.bounds.verified_execution_max_in_flight,
        rpc_connect_timeout_ms = args.rpc_connect_timeout_ms,
        rpc_timeout_ms = args.rpc_timeout_ms,
        verified_rate_limit_per_second = args.bounds.verified_rate_limit_per_second,
        verified_rate_limit_burst = args.bounds.verified_rate_limit_burst,
        verified_rate_limit_max_clients = args.bounds.verified_rate_limit_max_clients,
        trust_x_forwarded_for = args.bounds.trust_x_forwarded_for,
        "API booted"
    );

    axum::serve(
        listener,
        router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
        .with_graceful_shutdown(shutdown_signal("api"))
        .await
        .context("API server exited unexpectedly")
}

#[cfg(test)]
pub(crate) fn app_router(state: AppState) -> Router {
    let health_pool = state.pool.clone();
    app_router_with_bounds(state, health_pool, &ApiBoundsConfig::default())
}

#[cfg(test)]
pub(crate) fn app_router_with_health_pool(state: AppState, health_pool: sqlx::PgPool) -> Router {
    app_router_with_bounds(state, health_pool, &ApiBoundsConfig::default())
}

fn app_router_with_bounds(
    state: AppState,
    health_pool: sqlx::PgPool,
    bounds: &ApiBoundsConfig,
) -> Router {
    let bounded_router = API_ROUTE_DEFINITIONS
        .iter()
        .copied()
        .filter(|route| !route.bypasses_global_load_shed())
        .fold(Router::new(), |router, route| route.register(router))
        .route("/", get(openapi_docs))
        .route("/openapi.json", get(openapi_json))
        .route("/docs", get(openapi_docs))
        .route("/docs/", get(openapi_docs))
        .merge(crate::v2::router())
        .with_state(state.clone())
        .merge(crate::graphql::graphql_routes(state.clone()));
    let health_router = API_ROUTE_DEFINITIONS
        .iter()
        .copied()
        .filter(|route| route.bypasses_global_load_shed())
        .fold(Router::new(), |router, route| route.register(router))
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
    let cors = CorsLayer::permissive();
    crate::bounds::apply_request_bounds(
        bounded_router.layer(cors.clone()),
        health_router.layer(cors),
        bounds,
    )
}

async fn openapi_json() -> Json<JsonValue> {
    Json(openapi_document())
}

async fn openapi_docs() -> Html<&'static str> {
    Html(OPENAPI_DOCS_HTML)
}

pub(crate) fn render_openapi_document() -> String {
    let mut rendered =
        serde_json::to_string_pretty(&openapi_document()).expect("OpenAPI document must render");
    rendered.push('\n');
    rendered
}

pub(crate) fn openapi_document() -> JsonValue {
    let mut paths = JsonMap::new();
    for route in API_ROUTE_DEFINITIONS {
        if let Some(path_item) = route.openapi_path_item() {
            paths.insert(route.path.to_owned(), path_item);
        }
    }

    json!({
        "openapi": "3.1.0",
        "info": {
            "title": "bigname API v1",
            "version": SOFTWARE_VERSION,
            "description": "Machine-readable publication of the currently shipped public API surface derived from apps/api/src/main.rs.",
        },
        "paths": JsonValue::Object(paths),
        "components": openapi_components(),
    })
}
