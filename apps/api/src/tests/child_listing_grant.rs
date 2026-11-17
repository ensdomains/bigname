//! An API role without `SELECT` on the stored child listing (docs/deployment.md, "The child
//! listing family and its counts").
//!
//! | Row | Input | Expected | Test |
//! | --- | --- | --- | --- |
//! | G1 | The documented grants without the two listing tables | The API starts with one WARN naming both tables and the routes. The subnames page, the labels page and `include=counts` on the registry, name and address-names routes answer `internal_error`, each with one ERROR line. The registry overview and the name record without counts answer 200 with the granted role's data. After the grant, every route answers as the granted role did, without a restart | `a_role_without_the_listing_grant_starts_and_fails_only_the_child_routes` |
use std::sync::atomic::Ordering;

use super::walk_index_set_check::{GAUGE, STARTED, line_with, logged, serve_args};
use super::*;
use crate::cli::WalkIndexSetCheck;

const PASSWORD: &str = "bigname-child-listing-grant-test";
const UNREADABLE: &str = "cannot read the stored child listing";

/// The documented API role's grants (docs/deployment.md, "Surviving services").
fn documented_grants() -> Result<(&'static str, &'static str)> {
    let deployment = include_str!("../../../../docs/deployment.md")
        .split_once("## Surviving services")
        .context("deployment docs must contain the API service grants")?
        .1;
    let grants = deployment
        .split_once("GRANT SELECT ON TABLE\n")
        .context("deployment docs must contain the API SELECT grant")?
        .1
        .split_once("TO bigname_api;")
        .context("deployment docs must terminate the API SELECT grant")?
        .0;
    let guard = deployment
        .split_once("GRANT EXECUTE ON FUNCTION ")
        .context("deployment docs must contain the API guard grant")?
        .1
        .split_once("TO bigname_api;")
        .context("deployment docs must terminate the API guard grant")?
        .0;
    Ok((grants, guard))
}

async fn get(state: &AppState, uri: &str) -> Result<(StatusCode, Value)> {
    let response = app_router(state.clone())
        .oneshot(Request::builder().uri(uri).body(Body::empty())?)
        .await?;
    Ok((response.status(), read_json(response).await?))
}

async fn read_all(state: &AppState, uris: &[String]) -> Result<Vec<(StatusCode, Value)>> {
    let mut responses = Vec::with_capacity(uris.len());
    for uri in uris {
        responses.push(get(state, uri).await?);
    }
    Ok(responses)
}

#[tokio::test]
async fn a_role_without_the_listing_grant_starts_and_fails_only_the_child_routes() -> Result<()> {
    let _gauge = GAUGE.lock().await;
    let database = TestDatabase::new_migrated().await?;
    seed_family_children_fixture(&database).await?;
    let role = format!(
        "bigname_listing_grant_{}_{}",
        std::process::id(),
        NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed)
    );
    let (grants, guard) = documented_grants()?;
    for statement in [
        format!("CREATE ROLE {role} LOGIN PASSWORD '{PASSWORD}'"),
        format!("GRANT USAGE ON SCHEMA bigname_phase TO {role}"),
        format!("GRANT SELECT ON TABLE {grants} TO {role}"),
        format!("GRANT EXECUTE ON FUNCTION {guard} TO {role}"),
    ] {
        sqlx::query(&statement)
            .execute(&database.lookup_pool)
            .await?;
    }
    let config = database.database_config(6)?;
    let options = PgConnectOptions::from_str(
        config
            .database_url
            .as_deref()
            .context("test database URL is missing")?,
    )?
    .username(&role)
    .password(PASSWORD);
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(
            options
                .clone()
                .options([("search_path", "bigname_phase".to_owned())]),
        )
        .await?;
    let state = AppState::new_with_rpc_urls(pool.clone(), bigname_lookup::ChainRpcUrls::default())
        .with_public_namespaces_for_test(["ens", "basenames"]);

    let child = [
        "/v1/names/alpha.eth/subnames".to_owned(),
        format!("/v1/registries/1/{CHILD_ALPHA_REGISTRY}/labels"),
        format!("/v1/registries/1/{CHILD_ALPHA_REGISTRY}?include=counts"),
        "/v1/names/alpha.eth?include=counts".to_owned(),
        format!("/v1/addresses/{CHILD_OWNER}/names?namespace=ens&include=counts"),
    ];
    let other = [
        format!("/v1/registries/1/{CHILD_ALPHA_REGISTRY}"),
        "/v1/names/alpha.eth".to_owned(),
    ];
    let granted_child = read_all(&state, &child).await?;
    let granted_other = read_all(&state, &other).await?;

    sqlx::query(&format!(
        "REVOKE SELECT ON bigname_phase.project_child_listing,
             bigname_phase.project_child_listing_count FROM {role}"
    ))
    .execute(&database.lookup_pool)
    .await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let args = ServeArgs {
        database: bigname_storage::DatabaseConfig {
            database_url: Some(options.to_url_lossy().to_string()),
            max_connections: 6,
        },
        ..serve_args(&database, &listener, WalkIndexSetCheck::Refuse)?
    };
    let (started, startup_logs) = logged(serve(args)).await;
    let (revoked, request_logs) = logged(async {
        anyhow::Ok((
            read_all(&state, &child).await?,
            read_all(&state, &other).await?,
        ))
    })
    .await;
    let (revoked_child, revoked_other) = revoked?;

    sqlx::query(&format!(
        "GRANT SELECT ON bigname_phase.project_child_listing,
             bigname_phase.project_child_listing_count TO {role}"
    ))
    .execute(&database.lookup_pool)
    .await?;
    let regranted_child = read_all(&state, &child).await?;
    pool.close().await;
    sqlx::query(&format!("DROP OWNED BY {role}"))
        .execute(&database.lookup_pool)
        .await?;
    sqlx::query(&format!("DROP ROLE {role}"))
        .execute(&database.lookup_pool)
        .await?;

    for ((status, body), uri) in granted_child
        .iter()
        .chain(&granted_other)
        .zip(child.iter().chain(&other))
    {
        assert_eq!(*status, StatusCode::OK, "{uri} with the grant: {body:#}");
    }
    let started = format!(
        "{:#}",
        started.expect_err("an occupied listener stops serve")
    );
    assert!(
        started.contains(STARTED),
        "the API must start without the grant: {started}"
    );
    let warning = line_with(&startup_logs, "WARN", UNREADABLE);
    for named in [
        "bigname_phase.project_child_listing,bigname_phase.project_child_listing_count",
        "/v1/names/{name}/subnames",
        "/v1/registries/{chain_id}/{address}/labels",
    ] {
        assert!(warning.contains(named), "{named:?} in {warning}");
    }
    assert_eq!(
        startup_logs
            .lines()
            .filter(|line| line.contains(UNREADABLE))
            .count(),
        1,
        "{startup_logs}"
    );
    for ((status, body), uri) in revoked_child.iter().zip(&child) {
        assert_eq!(
            *status,
            StatusCode::INTERNAL_SERVER_ERROR,
            "{uri}: {body:#}"
        );
        assert_eq!(
            body["error"]["code"],
            json!("internal_error"),
            "{uri}: {body:#}"
        );
    }
    let errors: Vec<&str> = request_logs
        .lines()
        .filter(|line| line.contains("ERROR") && line.contains(UNREADABLE))
        .collect();
    assert_eq!(
        errors.len(),
        child.len(),
        "one ERROR line per request: {request_logs}"
    );
    for ((revoked, granted), uri) in revoked_other.iter().zip(&granted_other).zip(&other) {
        assert_eq!(revoked.0, StatusCode::OK, "{uri}: {:#}", revoked.1);
        assert_eq!(revoked.1["data"], granted.1["data"], "{uri}");
    }
    for ((regranted, granted), uri) in regranted_child.iter().zip(&granted_child).zip(&child) {
        assert_eq!(
            regranted.0,
            StatusCode::OK,
            "{uri} after the grant: {:#}",
            regranted.1
        );
        assert_eq!(regranted.1["data"], granted.1["data"], "{uri}");
    }
    database.cleanup().await
}
