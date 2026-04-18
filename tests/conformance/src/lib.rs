mod shipped_api {
    #![allow(dead_code)]

    include!(concat!(env!("OUT_DIR"), "/api_main.rs"));

    #[cfg(test)]
    mod conformance {
        use std::{
            str::FromStr,
            sync::atomic::{AtomicU64, Ordering},
            time::{SystemTime, UNIX_EPOCH},
        };

        use anyhow::{Context, Result};
        use axum::{
            body::{Body, to_bytes},
            http::{Request, StatusCode},
            response::Response,
        };
        use bigname_storage::{
            CanonicalityState, NameSurface, NormalizedEvent, RawBlock, Resource, SurfaceBinding,
            SurfaceBindingKind, default_database_url,
        };
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

        struct HarnessDatabase {
            admin_pool: PgPool,
            pool: PgPool,
            database_name: String,
        }

        impl HarnessDatabase {
            async fn new() -> Result<Self> {
                let database_url = std::env::var("BIGNAME_DATABASE_URL")
                    .or_else(|_| std::env::var("DATABASE_URL"))
                    .unwrap_or_else(|_| default_database_url().to_owned());
                let base_options = PgConnectOptions::from_str(&database_url)
                    .context("failed to parse database URL for conformance harness")?;
                let unique = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .context("system clock is before unix epoch")?
                    .as_nanos();
                let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
                let database_name = format!(
                    "bigname_conformance_{}_{}_{}",
                    std::process::id(),
                    unique,
                    sequence
                );

                let admin_pool = PgPoolOptions::new()
                    .max_connections(1)
                    .connect_with(base_options.clone())
                    .await
                    .context("failed to connect admin pool for conformance harness")?;

                sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                    .execute(&admin_pool)
                    .await
                    .with_context(|| {
                        format!("failed to create conformance database {database_name}")
                    })?;

                let pool = PgPoolOptions::new()
                    .max_connections(1)
                    .connect_with(base_options.database(&database_name))
                    .await
                    .context("failed to connect conformance pool")?;

                bigname_storage::MIGRATOR
                    .run(&pool)
                    .await
                    .context("failed to apply checked-in migrations for conformance harness")?;

                Ok(Self {
                    admin_pool,
                    pool,
                    database_name,
                })
            }

            fn app_state(&self) -> AppState {
                AppState {
                    phase: "conformance",
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
                .context("failed to insert manifest_version for conformance harness")?
                .try_get("manifest_id")
                .context("failed to read manifest_id for conformance harness")
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
                .context("failed to insert manifest capability flag for conformance harness")?;

                Ok(())
            }

            async fn seed_name_current_binding(
                &self,
                logical_name_id: &str,
                resource_id: Uuid,
                token_lineage_id: Uuid,
                surface_binding_id: Uuid,
            ) -> Result<()> {
                bigname_storage::upsert_name_surfaces(&self.pool, &[name_surface(logical_name_id)])
                    .await
                    .context("failed to upsert name surface for exact-name conformance")?;
                bigname_storage::upsert_resources(&self.pool, &[resource(resource_id)])
                    .await
                    .context("failed to upsert resource for exact-name conformance")?;
                bigname_storage::upsert_token_lineages(
                    &self.pool,
                    &[bigname_storage::TokenLineage {
                        token_lineage_id,
                        chain_id: "ethereum-mainnet".to_owned(),
                        block_hash: "0xtoken-lineage".to_owned(),
                        block_number: 101,
                        provenance: json!({"seed": "token_lineage"}),
                        canonicality_state: CanonicalityState::Finalized,
                    }],
                )
                .await
                .context("failed to upsert token lineage for exact-name conformance")?;
                bigname_storage::upsert_surface_bindings(
                    &self.pool,
                    &[surface_binding(
                        surface_binding_id,
                        logical_name_id,
                        resource_id,
                        timestamp(1_700_000_001),
                    )],
                )
                .await
                .context("failed to upsert surface binding for exact-name conformance")?;

                Ok(())
            }

            async fn insert_name_current_row(
                &self,
                row: bigname_storage::NameCurrentRow,
            ) -> Result<()> {
                bigname_storage::upsert_name_current_rows(&self.pool, &[row])
                    .await
                    .context("failed to upsert name_current row for conformance harness")?;
                Ok(())
            }

            async fn seed_history_binding(
                &self,
                logical_name_id: &str,
                resource_id: Uuid,
                surface_binding_id: Uuid,
            ) -> Result<()> {
                bigname_storage::upsert_name_surfaces(&self.pool, &[name_surface(logical_name_id)])
                    .await
                    .context("failed to upsert name surface for history conformance")?;
                bigname_storage::upsert_resources(&self.pool, &[resource(resource_id)])
                    .await
                    .context("failed to upsert resource for history conformance")?;
                bigname_storage::upsert_surface_bindings(
                    &self.pool,
                    &[surface_binding(
                        surface_binding_id,
                        logical_name_id,
                        resource_id,
                        timestamp(1_700_000_010),
                    )],
                )
                .await
                .context("failed to upsert surface binding for history conformance")?;

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
                .with_context(|| {
                    format!("failed to drop conformance database {}", self.database_name)
                })?;
                self.admin_pool.close().await;
                Ok(())
            }
        }

        #[tokio::test]
        async fn smoke_supported_reads_contract_bootstrap() -> Result<()> {
            let database = HarnessDatabase::new().await?;

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
                .insert_manifest(
                    "ens",
                    "ens_shadow_registry",
                    "ethereum-mainnet",
                    "ens_shadow",
                    2,
                    "shadow",
                    "uts46-v1",
                )
                .await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/namespaces/ens")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("namespace metadata smoke request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: NamespaceMetadataResponse = read_json(response).await?;
            assert_eq!(payload.data.namespace, "ens");
            assert_eq!(payload.declared_state.active_manifest_count, 1);
            assert_eq!(
                payload.declared_state.active_source_families,
                vec!["ens_v2_registry_l1".to_owned()]
            );
            assert_eq!(payload.coverage.status, "full");
            assert_eq!(payload.consistency, "head");

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn namespace_manifests_contract_lists_active_manifests() -> Result<()> {
            let database = HarnessDatabase::new().await?;

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

            database
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

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/manifests/ens")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("namespace manifests request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: NamespaceManifestsResponse = read_json(response).await?;
            assert_eq!(payload.data.namespace, "ens");
            assert_eq!(payload.declared_state.manifests.len(), 2);
            assert_eq!(payload.declared_state.manifests[0].manifest_version, 1);
            assert_eq!(
                payload.declared_state.manifests[0].source_family,
                "ens_v2_registry_l1"
            );
            assert_eq!(
                payload.declared_state.manifests[0]
                    .capability_flags
                    .get("verified_resolution")
                    .and_then(|flag| flag.notes.as_deref()),
                Some("tracked but not yet served")
            );
            assert_eq!(payload.declared_state.manifests[1].manifest_version, 2);
            assert_eq!(
                payload.coverage.enumeration_basis,
                "active manifests for the requested namespace"
            );
            assert_eq!(payload.provenance.manifest_versions.len(), 2);
            assert!(payload.verified_state.is_none());

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn exact_name_contract_includes_declared_sections_and_authority_fallback()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0x2200);
            let token_lineage_id = Uuid::from_u128(0x1100);
            let surface_binding_id = Uuid::from_u128(0x3300);

            database
                .seed_name_current_binding(
                    logical_name_id,
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
                .context("exact name request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: NameResponse = read_json(response).await?;
            let declared_state = payload
                .declared_state
                .as_object()
                .expect("declared_state must be an object");
            let expected_resource_id = resource_id.to_string();

            assert_eq!(payload.verified_state, None);
            assert_eq!(payload.consistency, "finalized");
            assert_eq!(
                payload.data.get("resource_id").and_then(Value::as_str),
                Some(expected_resource_id.as_str())
            );
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
                    .get("authority")
                    .and_then(Value::as_object)
                    .and_then(|value| value.get("resource_id"))
                    .and_then(Value::as_str),
                Some(expected_resource_id.as_str())
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
                    .get("history")
                    .and_then(Value::as_object)
                    .and_then(|value| value.get("unsupported_reason"))
                    .and_then(Value::as_str),
                Some("declared history pointers are not yet projected")
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn name_history_contract_returns_declared_rows_with_empty_declared_state()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0xa001);
            let surface_binding_id = Uuid::from_u128(0xb001);
            let manifest_id_v7 = database
                .insert_manifest(
                    "ens",
                    "ens_v1_registry_l1",
                    "ethereum-mainnet",
                    "bootstrap",
                    7,
                    "active",
                    "history-test-v1",
                )
                .await?;
            let manifest_id_v8 = database
                .insert_manifest(
                    "ens",
                    "ens_v1_registry_l1",
                    "ethereum-mainnet",
                    "bootstrap-next",
                    8,
                    "active",
                    "history-test-v2",
                )
                .await?;

            database
                .seed_history_binding(logical_name_id, resource_id, surface_binding_id)
                .await?;

            bigname_storage::upsert_raw_blocks(
                &database.pool,
                &[
                    raw_block("ethereum-mainnet", "0x100", None, 100, 1_700_000_100),
                    raw_block(
                        "ethereum-mainnet",
                        "0x101",
                        Some("0x100"),
                        101,
                        1_700_000_101,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x102",
                        Some("0x101"),
                        102,
                        1_700_000_102,
                    ),
                    raw_block(
                        "ethereum-mainnet",
                        "0x103",
                        Some("0x102"),
                        103,
                        1_700_000_103,
                    ),
                ],
            )
            .await?;

            bigname_storage::upsert_normalized_events(
                &database.pool,
                &[
                    NormalizedEvent {
                        manifest_version: 7,
                        source_manifest_id: Some(manifest_id_v7),
                        ..history_event(
                            "history:canonical",
                            Some(logical_name_id),
                            Some(resource_id),
                            Some("ethereum-mainnet"),
                            Some(100),
                            Some("0x100"),
                            Some("0xtx100"),
                            Some(0),
                            CanonicalityState::Canonical,
                        )
                    },
                    NormalizedEvent {
                        manifest_version: 8,
                        source_manifest_id: Some(manifest_id_v8),
                        ..history_event(
                            "history:safe",
                            Some(logical_name_id),
                            Some(resource_id),
                            Some("ethereum-mainnet"),
                            Some(101),
                            Some("0x101"),
                            Some("0xtx101"),
                            Some(0),
                            CanonicalityState::Safe,
                        )
                    },
                    NormalizedEvent {
                        manifest_version: 7,
                        source_manifest_id: Some(manifest_id_v7),
                        ..history_event(
                            "history:finalized",
                            Some(logical_name_id),
                            Some(resource_id),
                            Some("ethereum-mainnet"),
                            Some(102),
                            Some("0x102"),
                            Some("0xtx102"),
                            Some(0),
                            CanonicalityState::Finalized,
                        )
                    },
                    history_event(
                        "history:observed",
                        Some(logical_name_id),
                        Some(resource_id),
                        Some("ethereum-mainnet"),
                        Some(103),
                        Some("0x103"),
                        Some("0xtx103"),
                        Some(0),
                        CanonicalityState::Observed,
                    ),
                ],
            )
            .await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/history/names/ens/alice.eth")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("name history request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: HistoryResponse = read_json(response).await?;
            assert_eq!(
                history_event_identities(&payload),
                vec!["history:finalized", "history:safe", "history:canonical"]
            );
            assert_eq!(payload.declared_state, json!({}));
            assert_eq!(payload.page.sort, "chain_position_desc");
            assert_eq!(
                payload.coverage.enumeration_basis,
                "canonical normalized-event history for the requested both scope"
            );
            assert_eq!(
                payload
                    .provenance
                    .get("manifest_versions")
                    .and_then(Value::as_array)
                    .map(Vec::len),
                Some(2)
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn resource_history_contract_returns_declared_rows_with_empty_declared_state()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0xa300);
            let surface_binding_id = Uuid::from_u128(0xb300);

            database
                .seed_history_binding(logical_name_id, resource_id, surface_binding_id)
                .await?;

            bigname_storage::upsert_raw_blocks(
                &database.pool,
                &[
                    raw_block("base-mainnet", "0xb101", None, 101, 1_700_000_401),
                    raw_block("ethereum-mainnet", "0xe100", None, 100, 1_700_000_400),
                    raw_block("base-mainnet", "0xb100", Some("0xb101"), 100, 1_700_000_399),
                ],
            )
            .await?;

            bigname_storage::upsert_normalized_events(
                &database.pool,
                &[
                    history_event(
                        "no-chain-position",
                        Some(logical_name_id),
                        Some(resource_id),
                        None,
                        None,
                        None,
                        None,
                        None,
                        CanonicalityState::Canonical,
                    ),
                    history_event(
                        "ethereum-higher-log",
                        Some(logical_name_id),
                        Some(resource_id),
                        Some("ethereum-mainnet"),
                        Some(100),
                        Some("0xe100"),
                        Some("0xtx100"),
                        Some(7),
                        CanonicalityState::Canonical,
                    ),
                    history_event(
                        "base-same-height",
                        Some(logical_name_id),
                        Some(resource_id),
                        Some("base-mainnet"),
                        Some(100),
                        Some("0xb100"),
                        Some("0xtx090"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                    history_event(
                        "base-higher-height",
                        Some(logical_name_id),
                        Some(resource_id),
                        Some("base-mainnet"),
                        Some(101),
                        Some("0xb101"),
                        Some("0xtx101"),
                        Some(0),
                        CanonicalityState::Canonical,
                    ),
                ],
            )
            .await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(&format!("/v1/history/resources/{resource_id}"))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resource history request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: HistoryResponse = read_json(response).await?;
            assert_eq!(
                history_event_identities(&payload),
                vec![
                    "base-higher-height",
                    "base-same-height",
                    "ethereum-higher-log",
                    "no-chain-position",
                ]
            );
            assert_eq!(payload.declared_state, json!({}));
            assert_eq!(payload.page.sort, "chain_position_desc");
            assert_eq!(
                payload.chain_positions,
                json!({
                    "base": {
                        "chain_id": "base-mainnet",
                        "block_number": 101,
                        "block_hash": "0xb101",
                        "timestamp": "2023-11-14T22:20:01Z"
                    },
                    "ethereum": {
                        "chain_id": "ethereum-mainnet",
                        "block_number": 100,
                        "block_hash": "0xe100",
                        "timestamp": "2023-11-14T22:20:00Z"
                    }
                })
            );

            database.cleanup().await?;
            Ok(())
        }

        async fn read_json<T: DeserializeOwned>(response: Response) -> Result<T> {
            let bytes = to_bytes(response.into_body(), usize::MAX)
                .await
                .context("failed to read conformance response body")?;
            serde_json::from_slice(&bytes).context("failed to decode conformance response JSON")
        }

        fn timestamp(seconds: i64) -> OffsetDateTime {
            OffsetDateTime::from_unix_timestamp(seconds)
                .expect("conformance timestamp must be valid")
        }

        fn raw_block(
            chain_id: &str,
            block_hash: &str,
            parent_hash: Option<&str>,
            block_number: i64,
            block_timestamp: i64,
        ) -> RawBlock {
            RawBlock {
                chain_id: chain_id.to_owned(),
                block_hash: block_hash.to_owned(),
                parent_hash: parent_hash.map(str::to_owned),
                block_number,
                block_timestamp: timestamp(block_timestamp),
                logs_bloom: None,
                transactions_root: None,
                receipts_root: None,
                state_root: None,
                canonicality_state: CanonicalityState::Canonical,
            }
        }

        fn resource(resource_id: Uuid) -> Resource {
            Resource {
                resource_id,
                token_lineage_id: None,
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0xresource".to_owned(),
                block_number: 99,
                provenance: json!({"seed": "resource"}),
                canonicality_state: CanonicalityState::Canonical,
            }
        }

        fn name_surface(logical_name_id: &str) -> NameSurface {
            let (namespace, normalized_name) = logical_name_id
                .split_once(':')
                .expect("logical_name_id must include namespace");

            NameSurface {
                logical_name_id: logical_name_id.to_owned(),
                namespace: namespace.to_owned(),
                input_name: normalized_name.to_owned(),
                canonical_display_name: "Alice.eth".to_owned(),
                normalized_name: normalized_name.to_owned(),
                dns_encoded_name: vec![5, b'a', b'l', b'i', b'c', b'e'],
                namehash: format!("namehash:{normalized_name}"),
                labelhashes: vec!["labelhash:alice".to_owned()],
                normalizer_version: "uts46-v1".to_owned(),
                normalization_warnings: json!([]),
                normalization_errors: json!([]),
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0xsurface".to_owned(),
                block_number: 98,
                provenance: json!({"seed": "surface"}),
                canonicality_state: CanonicalityState::Canonical,
            }
        }

        fn surface_binding(
            surface_binding_id: Uuid,
            logical_name_id: &str,
            resource_id: Uuid,
            active_from: OffsetDateTime,
        ) -> SurfaceBinding {
            SurfaceBinding {
                surface_binding_id,
                logical_name_id: logical_name_id.to_owned(),
                resource_id,
                binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
                active_from,
                active_to: None,
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0xbinding".to_owned(),
                block_number: 100,
                provenance: json!({"seed": "binding"}),
                canonicality_state: CanonicalityState::Canonical,
            }
        }

        #[allow(clippy::too_many_arguments)]
        fn history_event(
            event_identity: &str,
            logical_name_id: Option<&str>,
            resource_id: Option<Uuid>,
            chain_id: Option<&str>,
            block_number: Option<i64>,
            block_hash: Option<&str>,
            transaction_hash: Option<&str>,
            log_index: Option<i64>,
            canonicality_state: CanonicalityState,
        ) -> NormalizedEvent {
            NormalizedEvent {
                event_identity: event_identity.to_owned(),
                namespace: "ens".to_owned(),
                logical_name_id: logical_name_id.map(str::to_owned),
                resource_id,
                event_kind: "HistoryEvent".to_owned(),
                source_family: "ens_v1_registry_l1".to_owned(),
                manifest_version: 7,
                source_manifest_id: None,
                chain_id: chain_id.map(str::to_owned),
                block_number,
                block_hash: block_hash.map(str::to_owned),
                transaction_hash: transaction_hash.map(str::to_owned),
                log_index,
                raw_fact_ref: json!({
                    "kind": "raw_log",
                    "event_identity": event_identity,
                }),
                derivation_kind: "history_test".to_owned(),
                canonicality_state,
                before_state: json!({
                    "provenance": {
                        "before": event_identity,
                    }
                }),
                after_state: json!({
                    "provenance": {
                        "after": event_identity,
                    },
                    "coverage": {
                        "status": "full",
                        "exhaustiveness": "authoritative",
                        "source_classes_considered": ["normalized_events"],
                        "enumeration_basis": event_identity,
                        "unsupported_reason": null,
                    }
                }),
            }
        }

        fn history_event_identities(payload: &HistoryResponse) -> Vec<&str> {
            payload
                .data
                .iter()
                .map(|row| {
                    row.get("event_identity")
                        .and_then(Value::as_str)
                        .expect("history row must include event_identity")
                })
                .collect()
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
    }
}
