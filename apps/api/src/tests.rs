use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use axum::{
    body::{Body, to_bytes},
    http::Request,
};
use bigname_storage::default_database_url;
use serde::de::DeserializeOwned;
use sqlx::{
    PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tower::ServiceExt;

use super::*;

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

struct TestDatabase {
    admin_pool: PgPool,
    pool: PgPool,
    database_name: String,
}

impl TestDatabase {
    async fn new(initialize_manifest_schema: bool) -> Result<Self> {
        let database_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| default_database_url().to_owned());
        let base_options = PgConnectOptions::from_str(&database_url)
            .context("failed to parse database URL for API tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bigname_api_test_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone())
            .await
            .context("failed to connect admin pool for API tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect API test pool")?;

        if initialize_manifest_schema {
            sqlx::query(
                r#"
                    CREATE TYPE manifest_rollout_status AS ENUM (
                        'draft',
                        'shadow',
                        'active',
                        'deprecated'
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create manifest_rollout_status for API tests")?;
            sqlx::query(
                r#"
                    CREATE TYPE capability_support_status AS ENUM (
                        'unsupported',
                        'shadow',
                        'supported'
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create capability_support_status for API tests")?;
            sqlx::query(
                r#"
                    CREATE TABLE manifest_versions (
                        manifest_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        manifest_version BIGINT NOT NULL CHECK (manifest_version > 0),
                        namespace TEXT NOT NULL,
                        source_family TEXT NOT NULL,
                        chain TEXT NOT NULL,
                        deployment_epoch TEXT NOT NULL,
                        rollout_status manifest_rollout_status NOT NULL,
                        normalizer_version TEXT NOT NULL,
                        file_path TEXT NOT NULL,
                        manifest_payload JSONB NOT NULL,
                        loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create manifest_versions for API tests")?;
            sqlx::query(
                    r#"
                    CREATE TABLE manifest_capability_flags (
                        manifest_id BIGINT NOT NULL REFERENCES manifest_versions (manifest_id) ON DELETE CASCADE,
                        capability_name TEXT NOT NULL,
                        status capability_support_status NOT NULL,
                        notes TEXT,
                        PRIMARY KEY (manifest_id, capability_name)
                    )
                    "#,
                )
                .execute(&pool)
                .await
                .context("failed to create manifest_capability_flags for API tests")?;
        }

        Ok(Self {
            admin_pool,
            pool,
            database_name,
        })
    }

    fn app_state(&self) -> AppState {
        AppState {
            phase: "test",
            pool: self.pool.clone(),
        }
    }

    async fn insert_manifest(
        &self,
        namespace: &str,
        source_family: &str,
        chain: &str,
        deployment_epoch: &str,
        manifest_version: u64,
        rollout_status: &str,
        normalizer_version: &str,
    ) -> Result<i64> {
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let file_path =
            format!("tests/{namespace}/{source_family}/{manifest_version}-{sequence}.toml");

        sqlx::query(
            r#"
                INSERT INTO manifest_versions (
                    manifest_version,
                    namespace,
                    source_family,
                    chain,
                    deployment_epoch,
                    rollout_status,
                    normalizer_version,
                    file_path,
                    manifest_payload
                )
                VALUES ($1, $2, $3, $4, $5, $6::manifest_rollout_status, $7, $8, $9::jsonb)
                RETURNING manifest_id
                "#,
        )
        .bind(i64::try_from(manifest_version).context("manifest_version exceeds BIGINT")?)
        .bind(namespace)
        .bind(source_family)
        .bind(chain)
        .bind(deployment_epoch)
        .bind(rollout_status)
        .bind(normalizer_version)
        .bind(file_path)
        .bind("{}")
        .fetch_one(&self.pool)
        .await
        .context("failed to insert manifest_version for API test")?
        .try_get("manifest_id")
        .context("failed to read manifest_id for API test")
    }

    async fn insert_capability_flag(
        &self,
        manifest_id: i64,
        capability_name: &str,
        status: &str,
        notes: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            r#"
                INSERT INTO manifest_capability_flags (
                    manifest_id,
                    capability_name,
                    status,
                    notes
                )
                VALUES ($1, $2, $3::capability_support_status, $4)
                "#,
        )
        .bind(manifest_id)
        .bind(capability_name)
        .bind(status)
        .bind(notes)
        .execute(&self.pool)
        .await
        .context("failed to insert manifest capability flag for API test")?;

        Ok(())
    }

    async fn cleanup(self) -> Result<()> {
        self.pool.close().await;
        sqlx::query(&format!(
            r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
            self.database_name
        ))
        .execute(&self.admin_pool)
        .await
        .with_context(|| format!("failed to drop test database {}", self.database_name))?;
        self.admin_pool.close().await;
        Ok(())
    }
}

async fn read_json<T: DeserializeOwned>(response: Response) -> Result<T> {
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .context("failed to read API response body")?;
    serde_json::from_slice(&bytes).context("failed to decode API response JSON")
}

#[tokio::test]
async fn get_namespace_manifests_returns_active_entries() -> Result<()> {
    let database = TestDatabase::new(true).await?;

    let ens_l1 = database
        .insert_manifest(
            "ens",
            "ens_v2_registry_l1",
            "ethereum-mainnet",
            "ens_v2",
            1,
            "active",
            "uts46-v1",
        )
        .await?;
    database
        .insert_capability_flag(ens_l1, "declared_children", "supported", None)
        .await?;
    database
        .insert_capability_flag(
            ens_l1,
            "verified_resolution",
            "shadow",
            Some("tracked but not yet served"),
        )
        .await?;

    let ens_l2 = database
        .insert_manifest(
            "ens",
            "ens_v2_registry_l2",
            "base-mainnet",
            "ens_v2_base",
            2,
            "active",
            "uts46-v2",
        )
        .await?;
    database
        .insert_capability_flag(ens_l2, "declared_children", "unsupported", Some("pending"))
        .await?;

    let ens_shadow = database
        .insert_manifest(
            "ens",
            "ens_shadow_registry",
            "ethereum-mainnet",
            "ens_shadow",
            3,
            "shadow",
            "uts46-v1",
        )
        .await?;
    database
        .insert_capability_flag(ens_shadow, "declared_children", "supported", None)
        .await?;

    let basenames = database
        .insert_manifest(
            "basenames",
            "base_registry",
            "base-mainnet",
            "basenames_v1",
            1,
            "active",
            "uts46-v1",
        )
        .await?;
    database
        .insert_capability_flag(basenames, "declared_children", "supported", None)
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/manifests/ens")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("manifest request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: NamespaceManifestsResponse = read_json(response).await?;
    assert_eq!(payload.data.namespace, "ens");
    assert_eq!(payload.consistency, "head");
    assert!(payload.last_updated.ends_with('Z'));
    assert!(payload.verified_state.is_none());
    assert!(payload.chain_positions.is_empty());
    assert_eq!(payload.coverage.status, "full");
    assert_eq!(payload.coverage.exhaustiveness, "authoritative");
    assert_eq!(
        payload.coverage.source_classes_considered,
        vec!["source_manifests".to_owned()]
    );
    assert_eq!(
        payload.coverage.enumeration_basis,
        "active manifests for the requested namespace"
    );
    assert_eq!(payload.coverage.unsupported_reason, None);
    assert!(payload.provenance.normalized_event_ids.is_empty());
    assert!(payload.provenance.raw_fact_refs.is_empty());
    assert_eq!(payload.provenance.derivation_kind, "declared");
    assert_eq!(payload.provenance.execution_trace_id, None);
    assert_eq!(payload.provenance.manifest_versions.len(), 2);
    assert_eq!(payload.declared_state.manifests.len(), 2);

    assert_eq!(payload.declared_state.manifests[0].manifest_version, 1);
    assert_eq!(
        payload.declared_state.manifests[0].source_family,
        "ens_v2_registry_l1"
    );
    assert_eq!(
        payload.declared_state.manifests[0].chain,
        "ethereum-mainnet"
    );
    assert_eq!(
        payload.declared_state.manifests[0].deployment_epoch,
        "ens_v2"
    );
    assert_eq!(
        payload.declared_state.manifests[0].normalizer_version,
        "uts46-v1"
    );
    assert_eq!(
        payload.declared_state.manifests[0]
            .capability_flags
            .get("declared_children")
            .expect("declared_children capability")
            .status,
        bigname_manifests::CapabilitySupportStatus::Supported
    );
    assert_eq!(
        payload.declared_state.manifests[0]
            .capability_flags
            .get("verified_resolution")
            .expect("verified_resolution capability")
            .notes
            .as_deref(),
        Some("tracked but not yet served")
    );
    assert_eq!(
        payload.provenance.manifest_versions[0],
        ManifestVersionRef {
            manifest_version: 1,
            source_family: "ens_v2_registry_l1".to_owned(),
            chain: "ethereum-mainnet".to_owned(),
            deployment_epoch: "ens_v2".to_owned(),
        }
    );

    assert_eq!(payload.declared_state.manifests[1].manifest_version, 2);
    assert_eq!(
        payload.declared_state.manifests[1].source_family,
        "ens_v2_registry_l2"
    );
    assert_eq!(payload.declared_state.manifests[1].chain, "base-mainnet");
    assert_eq!(
        payload.declared_state.manifests[1].deployment_epoch,
        "ens_v2_base"
    );
    assert_eq!(
        payload.declared_state.manifests[1].normalizer_version,
        "uts46-v2"
    );
    assert_eq!(
        payload.declared_state.manifests[1]
            .capability_flags
            .get("declared_children")
            .expect("declared_children capability")
            .status,
        bigname_manifests::CapabilitySupportStatus::Unsupported
    );
    assert_eq!(
        payload.provenance.manifest_versions[1],
        ManifestVersionRef {
            manifest_version: 2,
            source_family: "ens_v2_registry_l2".to_owned(),
            chain: "base-mainnet".to_owned(),
            deployment_epoch: "ens_v2_base".to_owned(),
        }
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_namespace_metadata_returns_active_summary() -> Result<()> {
    let database = TestDatabase::new(true).await?;

    let ens_l1 = database
        .insert_manifest(
            "ens",
            "ens_v2_registry_l1",
            "ethereum-mainnet",
            "ens_v2",
            1,
            "active",
            "uts46-v1",
        )
        .await?;
    database
        .insert_capability_flag(ens_l1, "declared_children", "supported", None)
        .await?;

    let ens_l2 = database
        .insert_manifest(
            "ens",
            "ens_v2_registry_l2",
            "base-mainnet",
            "ens_v2_base",
            2,
            "active",
            "uts46-v2",
        )
        .await?;
    database
        .insert_capability_flag(ens_l2, "declared_children", "shadow", Some("shadowed"))
        .await?;

    let ens_shadow = database
        .insert_manifest(
            "ens",
            "ens_shadow_registry",
            "ethereum-mainnet",
            "ens_shadow",
            3,
            "shadow",
            "uts46-v1",
        )
        .await?;
    database
        .insert_capability_flag(ens_shadow, "verified_resolution", "shadow", None)
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/namespaces/ens")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("namespace metadata request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: NamespaceMetadataResponse = read_json(response).await?;
    assert_eq!(payload.data.namespace, "ens");
    assert_eq!(payload.declared_state.active_manifest_count, 2);
    assert_eq!(
        payload.declared_state.active_source_families,
        vec![
            "ens_v2_registry_l1".to_owned(),
            "ens_v2_registry_l2".to_owned()
        ]
    );
    assert_eq!(
        payload.declared_state.chains,
        vec!["base-mainnet".to_owned(), "ethereum-mainnet".to_owned()]
    );
    assert_eq!(
        payload.declared_state.normalizer_versions,
        vec!["uts46-v1".to_owned(), "uts46-v2".to_owned()]
    );
    assert_eq!(payload.coverage.status, "full");
    assert_eq!(payload.coverage.exhaustiveness, "authoritative");
    assert_eq!(
        payload.coverage.source_classes_considered,
        vec!["source_manifests".to_owned()]
    );
    assert_eq!(
        payload.coverage.enumeration_basis,
        "active manifests for the requested namespace"
    );
    assert_eq!(payload.coverage.unsupported_reason, None);
    assert_eq!(payload.provenance.derivation_kind, "declared");
    assert_eq!(payload.provenance.execution_trace_id, None);
    assert!(payload.provenance.normalized_event_ids.is_empty());
    assert!(payload.provenance.raw_fact_refs.is_empty());
    assert_eq!(payload.provenance.manifest_versions.len(), 2);
    assert_eq!(
        payload.provenance.manifest_versions[0],
        ManifestVersionRef {
            manifest_version: 1,
            source_family: "ens_v2_registry_l1".to_owned(),
            chain: "ethereum-mainnet".to_owned(),
            deployment_epoch: "ens_v2".to_owned(),
        }
    );
    assert_eq!(
        payload.provenance.manifest_versions[1],
        ManifestVersionRef {
            manifest_version: 2,
            source_family: "ens_v2_registry_l2".to_owned(),
            chain: "base-mainnet".to_owned(),
            deployment_epoch: "ens_v2_base".to_owned(),
        }
    );
    assert_eq!(payload.consistency, "head");
    assert!(payload.last_updated.ends_with('Z'));
    assert!(payload.verified_state.is_none());
    assert!(payload.chain_positions.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_namespace_metadata_returns_empty_summary_when_namespace_has_no_active_manifests()
-> Result<()> {
    let database = TestDatabase::new(true).await?;

    let ens_shadow = database
        .insert_manifest(
            "ens",
            "ens_shadow_registry",
            "ethereum-mainnet",
            "ens_shadow",
            1,
            "shadow",
            "uts46-v1",
        )
        .await?;
    database
        .insert_capability_flag(ens_shadow, "declared_children", "supported", None)
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/namespaces/ens")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("namespace metadata request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: NamespaceMetadataResponse = read_json(response).await?;
    assert_eq!(payload.data.namespace, "ens");
    assert_eq!(payload.declared_state.active_manifest_count, 0);
    assert!(payload.declared_state.active_source_families.is_empty());
    assert!(payload.declared_state.chains.is_empty());
    assert!(payload.declared_state.normalizer_versions.is_empty());
    assert!(payload.provenance.manifest_versions.is_empty());
    assert_eq!(payload.coverage.status, "full");
    assert_eq!(payload.coverage.exhaustiveness, "authoritative");
    assert_eq!(
        payload.coverage.source_classes_considered,
        vec!["source_manifests".to_owned()]
    );
    assert_eq!(
        payload.coverage.enumeration_basis,
        "active manifests for the requested namespace"
    );
    assert_eq!(payload.coverage.unsupported_reason, None);
    assert_eq!(payload.provenance.derivation_kind, "declared");
    assert_eq!(payload.consistency, "head");
    assert!(payload.last_updated.ends_with('Z'));
    assert!(payload.verified_state.is_none());
    assert!(payload.chain_positions.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_namespace_metadata_returns_internal_error_envelope_on_load_failure() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/namespaces/ens")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("namespace metadata request failed")?;

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "internal_error");
    assert_eq!(
        payload.error.message,
        "failed to load namespace metadata for namespace ens"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_namespace_metadata_returns_not_found_for_unknown_namespace() -> Result<()> {
    let database = TestDatabase::new(true).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/namespaces/unknown")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("namespace metadata request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(payload.error.message, "namespace unknown is not supported");
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_namespace_manifests_returns_empty_list_when_namespace_has_no_active_entries()
-> Result<()> {
    let database = TestDatabase::new(true).await?;

    let ens_shadow = database
        .insert_manifest(
            "ens",
            "ens_shadow_registry",
            "ethereum-mainnet",
            "ens_shadow",
            1,
            "shadow",
            "uts46-v1",
        )
        .await?;
    database
        .insert_capability_flag(ens_shadow, "declared_children", "supported", None)
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/manifests/ens")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("manifest request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: NamespaceManifestsResponse = read_json(response).await?;
    assert_eq!(payload.data.namespace, "ens");
    assert!(payload.declared_state.manifests.is_empty());
    assert!(payload.provenance.manifest_versions.is_empty());
    assert_eq!(payload.coverage.status, "full");
    assert_eq!(payload.coverage.exhaustiveness, "authoritative");
    assert_eq!(
        payload.coverage.source_classes_considered,
        vec!["source_manifests".to_owned()]
    );
    assert_eq!(
        payload.coverage.enumeration_basis,
        "active manifests for the requested namespace"
    );
    assert_eq!(payload.provenance.derivation_kind, "declared");
    assert_eq!(payload.consistency, "head");
    assert!(payload.last_updated.ends_with('Z'));
    assert!(payload.verified_state.is_none());
    assert!(payload.chain_positions.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_namespace_manifests_returns_internal_error_envelope_on_load_failure() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/manifests/ens")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("manifest request failed")?;

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "internal_error");
    assert_eq!(
        payload.error.message,
        "failed to load manifest snapshot for namespace ens"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_namespace_manifests_returns_not_found_for_unknown_namespace() -> Result<()> {
    let database = TestDatabase::new(true).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/manifests/unknown")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("manifest request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(payload.error.message, "namespace unknown is not supported");
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}
