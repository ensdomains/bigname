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
use serde_json::{Value, json};
use sqlx::{
    PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
    types::{Uuid, time::OffsetDateTime},
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
        Self::new_with_schemas(initialize_manifest_schema, false).await
    }

    async fn new_with_schemas(
        initialize_manifest_schema: bool,
        initialize_name_current_schema: bool,
    ) -> Result<Self> {
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

        if initialize_name_current_schema {
            sqlx::query(
                r#"
                    CREATE TABLE name_surfaces (
                        logical_name_id TEXT PRIMARY KEY,
                        namespace TEXT NOT NULL,
                        canonical_display_name TEXT NOT NULL,
                        normalized_name TEXT NOT NULL,
                        namehash TEXT NOT NULL,
                        CHECK (logical_name_id = namespace || ':' || normalized_name)
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create name_surfaces for API tests")?;
            sqlx::query(
                r#"
                    CREATE TABLE resources (
                        resource_id UUID PRIMARY KEY
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create resources for API tests")?;
            sqlx::query(
                r#"
                    CREATE TABLE token_lineages (
                        token_lineage_id UUID PRIMARY KEY
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create token_lineages for API tests")?;
            sqlx::query(
                r#"
                    CREATE TABLE surface_bindings (
                        surface_binding_id UUID PRIMARY KEY,
                        logical_name_id TEXT NOT NULL REFERENCES name_surfaces (logical_name_id),
                        resource_id UUID NOT NULL REFERENCES resources (resource_id),
                        binding_kind TEXT NOT NULL,
                        CHECK (
                            binding_kind IN (
                                'declared_registry_path',
                                'linked_subregistry_path',
                                'resolver_alias_path',
                                'observed_wildcard_path',
                                'migration_rebind',
                                'observed_only'
                            )
                        )
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create surface_bindings for API tests")?;
            sqlx::query(
                r#"
                    CREATE TABLE name_current (
                        logical_name_id TEXT PRIMARY KEY REFERENCES name_surfaces (logical_name_id),
                        namespace TEXT NOT NULL,
                        canonical_display_name TEXT NOT NULL,
                        normalized_name TEXT NOT NULL,
                        namehash TEXT NOT NULL,
                        surface_binding_id UUID REFERENCES surface_bindings (surface_binding_id),
                        resource_id UUID REFERENCES resources (resource_id),
                        token_lineage_id UUID REFERENCES token_lineages (token_lineage_id),
                        binding_kind TEXT,
                        declared_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
                        provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
                        coverage JSONB NOT NULL DEFAULT '{}'::jsonb,
                        chain_positions JSONB NOT NULL DEFAULT '{}'::jsonb,
                        canonicality_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
                        manifest_version BIGINT NOT NULL CHECK (manifest_version > 0),
                        last_recomputed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        CHECK (logical_name_id = namespace || ':' || normalized_name),
                        CHECK (
                            (surface_binding_id IS NULL AND resource_id IS NULL AND binding_kind IS NULL)
                            OR
                            (surface_binding_id IS NOT NULL AND resource_id IS NOT NULL AND binding_kind IS NOT NULL)
                        ),
                        CHECK (
                            token_lineage_id IS NULL
                            OR resource_id IS NOT NULL
                        ),
                        CHECK (
                            binding_kind IS NULL
                            OR binding_kind IN (
                                'declared_registry_path',
                                'linked_subregistry_path',
                                'resolver_alias_path',
                                'observed_wildcard_path',
                                'migration_rebind',
                                'observed_only'
                            )
                        )
                    )
                    "#,
            )
            .execute(&pool)
            .await
            .context("failed to create name_current for API tests")?;
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

    async fn seed_name_current_binding(
        &self,
        logical_name_id: &str,
        namespace: &str,
        normalized_name: &str,
        canonical_display_name: &str,
        namehash: &str,
        resource_id: Uuid,
        token_lineage_id: Uuid,
        surface_binding_id: Uuid,
    ) -> Result<()> {
        sqlx::query(
            r#"
                INSERT INTO name_surfaces (
                    logical_name_id,
                    namespace,
                    canonical_display_name,
                    normalized_name,
                    namehash
                )
                VALUES ($1, $2, $3, $4, $5)
                "#,
        )
        .bind(logical_name_id)
        .bind(namespace)
        .bind(canonical_display_name)
        .bind(normalized_name)
        .bind(namehash)
        .execute(&self.pool)
        .await
        .context("failed to insert name_surface for API test")?;

        sqlx::query("INSERT INTO resources (resource_id) VALUES ($1)")
            .bind(resource_id)
            .execute(&self.pool)
            .await
            .context("failed to insert resource for API test")?;

        sqlx::query("INSERT INTO token_lineages (token_lineage_id) VALUES ($1)")
            .bind(token_lineage_id)
            .execute(&self.pool)
            .await
            .context("failed to insert token_lineage for API test")?;

        sqlx::query(
            r#"
                INSERT INTO surface_bindings (
                    surface_binding_id,
                    logical_name_id,
                    resource_id,
                    binding_kind
                )
                VALUES ($1, $2, $3, $4)
                "#,
        )
        .bind(surface_binding_id)
        .bind(logical_name_id)
        .bind(resource_id)
        .bind("declared_registry_path")
        .execute(&self.pool)
        .await
        .context("failed to insert surface_binding for API test")?;

        Ok(())
    }

    async fn insert_name_current_row(&self, row: bigname_storage::NameCurrentRow) -> Result<()> {
        bigname_storage::upsert_name_current_rows(&self.pool, &[row])
            .await
            .context("failed to upsert name_current row for API test")?;
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

fn timestamp(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(seconds).expect("test timestamp must be valid")
}

fn exact_name_row(
    logical_name_id: &str,
    surface_binding_id: Uuid,
    resource_id: Uuid,
    token_lineage_id: Uuid,
) -> bigname_storage::NameCurrentRow {
    bigname_storage::NameCurrentRow {
        logical_name_id: logical_name_id.to_owned(),
        namespace: "ens".to_owned(),
        canonical_display_name: "Alice.eth".to_owned(),
        normalized_name: "alice.eth".to_owned(),
        namehash: "namehash:alice.eth".to_owned(),
        surface_binding_id: Some(surface_binding_id),
        resource_id: Some(resource_id),
        token_lineage_id: Some(token_lineage_id),
        binding_kind: Some(bigname_storage::SurfaceBindingKind::DeclaredRegistryPath),
        declared_summary: json!({
            "registration": {
                "status": "active",
                "authority_kind": "registrar"
            },
            "resolver": {
                "address": "0x0000000000000000000000000000000000000abc"
            }
        }),
        provenance: json!({
            "normalized_event_ids": [101, 102],
            "raw_fact_refs": [
                {
                    "kind": "log",
                    "chain_id": "ethereum-mainnet",
                    "block_hash": "0xabc"
                }
            ],
            "manifest_versions": [
                {
                    "manifest_version": 3,
                    "source_family": "ens_v1_registry",
                    "chain": "ethereum-mainnet",
                    "deployment_epoch": "ens_v1"
                }
            ],
            "execution_trace_id": null,
            "derivation_kind": "projection_apply"
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["ensv1_registry_path"],
            "unsupported_reason": null,
            "enumeration_basis": "exact_name"
        }),
        chain_positions: json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbinding",
                "timestamp": "2026-04-17T00:00:03Z"
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                "ethereum-mainnet": "finalized"
            }
        }),
        manifest_version: 3,
        last_recomputed_at: timestamp(1_717_171_717),
    }
}

#[tokio::test]
async fn get_name_returns_current_projection_envelope() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "alice.eth",
            "Alice.eth",
            "namehash:alice.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row(exact_name_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        ))
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: NameResponse = read_json(response).await?;
    assert_eq!(payload.verified_state, None);
    assert_eq!(payload.consistency, "finalized");
    assert_eq!(payload.last_updated, "2024-05-31T16:08:37Z");

    let data = payload.data.as_object().expect("data must be an object");
    assert_eq!(
        data.get("logical_name_id"),
        Some(&Value::String("ens:alice.eth".to_owned()))
    );
    assert_eq!(
        data.get("namespace"),
        Some(&Value::String("ens".to_owned()))
    );
    assert_eq!(
        data.get("normalized_name"),
        Some(&Value::String("alice.eth".to_owned()))
    );
    assert_eq!(
        data.get("canonical_display_name"),
        Some(&Value::String("Alice.eth".to_owned()))
    );
    assert_eq!(
        data.get("namehash"),
        Some(&Value::String("namehash:alice.eth".to_owned()))
    );
    assert_eq!(
        data.get("resource_id"),
        Some(&Value::String(resource_id.to_string()))
    );
    assert_eq!(
        data.get("token_lineage_id"),
        Some(&Value::String(token_lineage_id.to_string()))
    );
    assert_eq!(
        data.get("binding_kind"),
        Some(&Value::String("declared_registry_path".to_owned()))
    );

    let declared_state = payload
        .declared_state
        .as_object()
        .expect("declared_state must be an object");
    assert_eq!(
        declared_state
            .get("registration")
            .and_then(Value::as_object)
            .and_then(|value| value.get("status"))
            .and_then(Value::as_str),
        Some("active")
    );
    assert_eq!(
        declared_state
            .get("resolver")
            .and_then(Value::as_object)
            .and_then(|value| value.get("address"))
            .and_then(Value::as_str),
        Some("0x0000000000000000000000000000000000000abc")
    );
    assert_eq!(
        declared_state
            .get("authority")
            .and_then(Value::as_object)
            .and_then(|value| value.get("resource_id")),
        Some(&Value::String(resource_id.to_string()))
    );
    assert_eq!(
        declared_state
            .get("control")
            .and_then(Value::as_object)
            .and_then(|value| value.get("status"))
            .and_then(Value::as_str),
        Some("unsupported")
    );
    assert_eq!(
        declared_state
            .get("record_inventory")
            .and_then(Value::as_object)
            .and_then(|value| value.get("status"))
            .and_then(Value::as_str),
        Some("unsupported")
    );
    assert_eq!(
        declared_state
            .get("history")
            .and_then(Value::as_object)
            .and_then(|value| value.get("status"))
            .and_then(Value::as_str),
        Some("unsupported")
    );

    let provenance = payload
        .provenance
        .as_object()
        .expect("provenance must be an object");
    assert_eq!(
        provenance.get("normalized_event_ids"),
        Some(&json!(["101", "102"]))
    );
    assert_eq!(
        provenance.get("derivation_kind").and_then(Value::as_str),
        Some("projection_apply")
    );
    assert_eq!(provenance.get("execution_trace_id"), Some(&Value::Null));
    assert_eq!(
        provenance.get("manifest_versions"),
        Some(&json!([
            {
                "manifest_version": 3,
                "source_family": "ens_v1_registry",
                "chain": "ethereum-mainnet",
                "deployment_epoch": "ens_v1"
            }
        ]))
    );

    let coverage = payload
        .coverage
        .as_object()
        .expect("coverage must be an object");
    assert_eq!(coverage.get("status").and_then(Value::as_str), Some("full"));
    assert_eq!(
        coverage.get("exhaustiveness").and_then(Value::as_str),
        Some("authoritative")
    );
    assert_eq!(
        coverage.get("source_classes_considered"),
        Some(&json!(["ensv1_registry_path"]))
    );
    assert_eq!(
        coverage.get("enumeration_basis").and_then(Value::as_str),
        Some("exact_name")
    );
    assert_eq!(coverage.get("unsupported_reason"), Some(&Value::Null));

    assert_eq!(
        payload.chain_positions,
        json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbinding",
                "timestamp": "2026-04-17T00:00:03Z"
            }
        })
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_returns_not_found_when_projection_row_is_missing() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/missing.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(
        payload.error.message,
        "name missing.eth was not found in namespace ens"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_returns_not_found_for_unsupported_namespace_without_storage_read() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/unknown/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(payload.error.message, "namespace unknown is not supported");
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_returns_internal_error_envelope_on_storage_failure() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name request failed")?;

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "internal_error");
    assert_eq!(
        payload.error.message,
        "failed to load current projection for name ens/alice.eth"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
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
