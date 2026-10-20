//! A request with no `namespace` covers the namespaces whose chains this deployment follows: the
//! chains with phase state or a stored head, the set `/v1/status` reports. The checked-in mainnet
//! deployment profile (docs/glossary.md#deployment-profile) holds Base manifests for Basenames,
//! and a mainnet deployment follows Ethereum only.
use super::v2_mainnet_admission::{NAME, OWNER, PUBLIC_RESOLVER, REGISTERED, lease, publish, text};
use super::v2_sepolia_redeploy::{get, lookup};
use super::*;
use axum::http::StatusCode;

const BASE: &str = "base-mainnet";
const BASE_HASH: &str = "0xfollowed-base";
const NOT_AVAILABLE: &str =
    "collection publication is not available; retry after indexing is ready";

/// The checked-in mainnet deployment profile synced whole, Ethereum followed and published.
/// `NAME` is `OWNER`'s. Nothing is written for Base.
async fn mainnet() -> Result<TestDatabase> {
    let database = TestDatabase::new_migrated().await?;
    publish(
        &database,
        vec![(REGISTERED, lease()?), (REGISTERED + 60, text()?)],
    )
    .await?;
    Ok(database)
}

/// Base followed the way the runner starts a configured chain: phase rows, no head, no marker.
async fn follow_base(database: &TestDatabase) -> Result<()> {
    phase_runner::state::PhaseStore::new(database.pool.clone())
        .initialize_chain(BASE)
        .await?;
    Ok(())
}

/// Base followed and published: a stored head, completed phases and a live family marker.
async fn publish_base(database: &TestDatabase) -> Result<()> {
    database
        .seed_snapshot_selector_chain_positions(&json!({"base": {
            "chain_id": BASE, "block_number": 1, "block_hash": BASE_HASH,
            "timestamp": "2024-01-01T00:00:00Z"
        }}))
        .await?;
    rebuild_fixture_families(&database.pool, BASE, 1, BASE_HASH).await
}

fn keys(body: &Value, field: &str) -> Vec<String> {
    body["meta"][field]
        .as_object()
        .map(|map| map.keys().cloned().collect())
        .unwrap_or_default()
}

fn row_names(body: &Value) -> Vec<String> {
    body["data"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|row| row["name"].as_str().map(str::to_owned))
        .collect()
}

/// The routes whose request with no `namespace` is a collection read over the public
/// namespace set, each with the names it serves for `OWNER` on the fixture.
fn bare_collections() -> Vec<(String, Vec<&'static str>)> {
    vec![
        (format!("/v1/addresses/{OWNER}/names"), vec![NAME]),
        (
            format!("/v1/addresses/{OWNER}/names?relation=former_owner"),
            vec![],
        ),
        (
            format!("/v1/addresses/{OWNER}/names?relation=resolves_to"),
            vec![],
        ),
        (format!("/v1/permissions?address={OWNER}"), vec![NAME, NAME]),
        (
            format!("/v1/events?resolver=1:{PUBLIC_RESOLVER}"),
            vec![NAME; 3],
        ),
    ]
}

async fn assert_served(database: &TestDatabase, uri: &str, as_of: &[&str]) -> Result<Value> {
    let (status, body) = get(database, uri).await?;
    assert_eq!(status, StatusCode::OK, "{uri}: {body:#}");
    assert_eq!(keys(&body, "as_of"), as_of, "{uri}: {body:#}");
    assert!(
        body["meta"].get("as_of_completeness").is_none(),
        "{uri}: {body:#}"
    );
    Ok(body)
}

async fn assert_refused(
    database: &TestDatabase,
    uri: &str,
    code: &str,
    message: &str,
) -> Result<()> {
    let (status, body) = get(database, uri).await?;
    assert_eq!(status, StatusCode::CONFLICT, "{uri}: {body:#}");
    assert_eq!(body["error"]["code"], json!(code), "{uri}: {body:#}");
    assert_eq!(body["error"]["message"], json!(message), "{uri}: {body:#}");
    assert!(body.get("data").is_none(), "{uri}: {body:#}");
    Ok(())
}

async fn lookup_batch(database: &TestDatabase) -> Result<Value> {
    let (status, body) = lookup(
        database,
        json!({"profile": "feed", "inputs": [
            {"name": NAME},
            {"address": OWNER, "relation": "any"},
        ]}),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    Ok(body)
}

/// Row 1. Every manifest chain is followed and published: both namespaces stay in scope.
#[tokio::test]
async fn every_followed_namespace_stays_in_the_bare_scope() -> Result<()> {
    let database = mainnet().await?;
    publish_base(&database).await?;

    for (uri, names) in bare_collections() {
        let body = assert_served(&database, &uri, &["1", "8453"]).await?;
        assert_eq!(row_names(&body), names, "{uri}: {body:#}");
    }
    let search = assert_served(&database, "/v1/search?q=premigrated", &["1", "8453"]).await?;
    assert_eq!(row_names(&search), [NAME], "{search:#}");
    let batch = lookup_batch(&database).await?;
    assert_eq!(keys(&batch, "as_of"), ["1", "8453"], "{batch:#}");
    assert!(
        batch["meta"].get("as_of_completeness").is_none(),
        "{batch:#}"
    );
    database.cleanup().await
}

/// Row 2. Base has manifests but no phase rows or head. The request with no `namespace` on route
/// `index` of [`bare_collections`] is served from Ethereum alone and reports Ethereum's scope.
async fn assert_bare_collection_without_base(index: usize) -> Result<()> {
    let database = mainnet().await?;
    let (uri, names) = bare_collections().swap_remove(index);
    let body = assert_served(&database, &uri, &["1"]).await?;
    assert_eq!(row_names(&body), names, "{uri}: {body:#}");
    database.cleanup().await
}

#[tokio::test]
async fn an_unfollowed_namespace_leaves_the_bare_address_names_scope() -> Result<()> {
    assert_bare_collection_without_base(0).await
}

#[tokio::test]
async fn an_unfollowed_namespace_leaves_the_bare_former_owner_scope() -> Result<()> {
    assert_bare_collection_without_base(1).await
}

#[tokio::test]
async fn an_unfollowed_namespace_leaves_the_bare_resolves_to_scope() -> Result<()> {
    assert_bare_collection_without_base(2).await
}

#[tokio::test]
async fn an_unfollowed_namespace_leaves_the_bare_permissions_scope() -> Result<()> {
    assert_bare_collection_without_base(3).await
}

#[tokio::test]
async fn an_unfollowed_namespace_leaves_the_resolver_events_scope() -> Result<()> {
    assert_bare_collection_without_base(4).await
}

/// Row 2, search. It served before, but disclosed Base as temporarily unavailable, a chain
/// nothing is indexing. The scope is now Ethereum's alone.
#[tokio::test]
async fn an_unfollowed_namespace_leaves_the_bare_search_scope() -> Result<()> {
    let database = mainnet().await?;
    let search = assert_served(&database, "/v1/search?q=premigrated", &["1"]).await?;
    assert_eq!(row_names(&search), [NAME], "{search:#}");
    database.cleanup().await
}

/// Row 2, lookup. A batch with a name input and an address input, as search.
#[tokio::test]
async fn an_unfollowed_namespace_leaves_the_public_reverse_lookup_scope() -> Result<()> {
    let database = mainnet().await?;
    let batch = lookup_batch(&database).await?;
    assert_eq!(keys(&batch, "as_of"), ["1"], "{batch:#}");
    assert!(
        batch["meta"].get("as_of_completeness").is_none(),
        "{batch:#}"
    );
    database.cleanup().await
}

/// Row 2, address history. With no `namespace` it reads `ens`, never the public namespace set,
/// so an unfollowed Base never reached it.
#[tokio::test]
async fn address_history_reads_ens_whatever_base_is() -> Result<()> {
    let database = mainnet().await?;
    let body = assert_served(&database, &format!("/v1/addresses/{OWNER}/history"), &["1"]).await?;
    assert!(
        body["data"].as_array().is_some_and(|rows| !rows.is_empty()),
        "{body:#}"
    );
    database.cleanup().await
}

/// Row 3. A followed chain without a servable publication still refuses every collection, and
/// search and lookup still disclose it as temporarily unavailable.
#[tokio::test]
async fn a_followed_unpublished_namespace_still_refuses() -> Result<()> {
    let database = mainnet().await?;
    follow_base(&database).await?;
    assert_followed_base_withheld(&database).await?;

    // The base case above is the production `initialize_chain` path. The variants start from
    // the published base and change only the marker.
    for unservable in [
        "UPDATE bigname_phase.project_family_marker SET state = 'bootstrap_pending'
         WHERE chain_id = 'base-mainnet'",
        "UPDATE bigname_phase.project_family_marker SET input_content_hash = 'foreign'
         WHERE chain_id = 'base-mainnet'",
    ] {
        let database = mainnet().await?;
        publish_base(&database).await?;
        sqlx::query(unservable).execute(&database.pool).await?;
        assert_followed_base_withheld(&database).await?;
        database.cleanup().await?;
    }
    database.cleanup().await
}

async fn assert_followed_base_withheld(database: &TestDatabase) -> Result<()> {
    for (uri, _) in bare_collections() {
        assert_refused(database, &uri, "stale", NOT_AVAILABLE).await?;
    }
    let (status, search) = get(database, "/v1/search?q=premigrated").await?;
    assert_eq!(status, StatusCode::OK, "{search:#}");
    assert_eq!(keys(&search, "as_of"), ["1"], "{search:#}");
    assert_eq!(
        search["meta"]["as_of_completeness"],
        json!({"8453": {"completeness": "unsupported", "unsupported_reason": "temporarily_unavailable"}}),
        "{search:#}"
    );
    let batch = lookup_batch(database).await?;
    assert_eq!(keys(&batch, "as_of_completeness"), ["8453"], "{batch:#}");
    Ok(())
}

/// Row 4. Naming an unfollowed namespace keeps the existing answer: collections refuse, and
/// search serves no rows with the chain disclosed as temporarily unavailable.
#[tokio::test]
async fn an_explicit_unfollowed_namespace_still_refuses() -> Result<()> {
    let database = mainnet().await?;
    assert_refused(
        &database,
        &format!("/v1/addresses/{OWNER}/names?namespace=basenames"),
        "stale",
        NOT_AVAILABLE,
    )
    .await?;
    assert_refused(
        &database,
        &format!("/v1/permissions?address={OWNER}&namespace=basenames"),
        "stale",
        NOT_AVAILABLE,
    )
    .await?;
    let (status, search) = get(&database, "/v1/search?q=premigrated&namespace=basenames").await?;
    assert_eq!(status, StatusCode::OK, "{search:#}");
    assert_eq!(search["data"], json!([]), "{search:#}");
    assert_eq!(keys(&search, "as_of_completeness"), ["8453"], "{search:#}");
    database.cleanup().await
}

/// Row 5. A deployment that follows no chain has nothing to serve.
#[tokio::test]
async fn a_deployment_following_no_chain_refuses() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    bigname_manifests::sync_schema_v2_repository(
        &database.pool,
        &bigname_manifests::load_repository(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/mainnet"),
        )?,
    )
    .await?;
    for (uri, _) in bare_collections() {
        assert_refused(&database, &uri, "stale", NOT_AVAILABLE).await?;
    }
    assert_refused(
        &database,
        "/v1/search?q=premigrated",
        "conflict",
        "the deployment does not serve a public namespace",
    )
    .await?;
    database.cleanup().await
}
