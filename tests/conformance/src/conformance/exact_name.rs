        #[tokio::test]
        async fn exact_name_contract_returns_frozen_control_resolver_and_history_summaries()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0x2200);
            let token_lineage_id = Uuid::from_u128(0x1100);
            let surface_binding_id = Uuid::from_u128(0x3300);

            database
                .seed_exact_name_rebuild_inputs(
                    logical_name_id,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            database.rebuild_name_current(logical_name_id).await?;

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

            assert_eq!(
                declared_state.get("control"),
                Some(&exact_name_control_summary())
            );
            assert_eq!(
                declared_state.get("resolver"),
                Some(&exact_name_resolver_summary())
            );
            assert_exact_name_history_summary_matches_history_route(
                &database,
                "ens",
                "alice.eth",
                declared_state
                    .get("history")
                    .expect("history summary must be present"),
            )
            .await?;

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn coverage_contract_returns_declared_state_explain_with_shared_top_level_coverage()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0x2200);
            let token_lineage_id = Uuid::from_u128(0x1100);
            let surface_binding_id = Uuid::from_u128(0x3300);

            database
                .seed_exact_name_rebuild_inputs(
                    logical_name_id,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            database.rebuild_name_current(logical_name_id).await?;

            let coverage_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/coverage/ens/alice.eth")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("coverage request failed")?;
            let name_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/names/ens/alice.eth")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("name request failed")?;

            assert_eq!(coverage_response.status(), StatusCode::OK);
            assert_eq!(name_response.status(), StatusCode::OK);

            let coverage_payload: NameResponse = read_json(coverage_response).await?;
            let name_payload: NameResponse = read_json(name_response).await?;
            let name_declared_state = name_payload
                .declared_state
                .as_object()
                .expect("declared_state must be an object");

            assert_eq!(coverage_payload.data, name_payload.data);
            assert_eq!(coverage_payload.coverage, name_payload.coverage);
            assert_eq!(coverage_payload.verified_state, None);
            assert_eq!(
                name_declared_state.get("control"),
                Some(&exact_name_control_summary())
            );
            assert_eq!(
                name_declared_state.get("resolver"),
                Some(&exact_name_resolver_summary())
            );
            assert_exact_name_history_summary_matches_history_route(
                &database,
                "ens",
                "alice.eth",
                name_declared_state
                    .get("history")
                    .expect("history summary must be present"),
            )
            .await?;
            assert_eq!(coverage_payload.declared_state, coverage_payload.coverage);
            assert_eq!(
                coverage_payload.declared_state,
                json!({
                    "status": "full",
                    "exhaustiveness": "authoritative",
                    "source_classes_considered": ["ensv1_registry_path"],
                    "enumeration_basis": "exact_name",
                    "unsupported_reason": null
                })
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn surface_binding_explain_contract_is_declared_only_with_exact_name_coverage_and_frozen_summary()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0x2200);
            let token_lineage_id = Uuid::from_u128(0x1100);
            let surface_binding_id = Uuid::from_u128(0x3300);

            database
                .seed_exact_name_rebuild_inputs(
                    logical_name_id,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            database.rebuild_name_current(logical_name_id).await?;

            let explain_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/explain/names/ens/alice.eth/surface-binding")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("surface-binding explain request failed")?;
            let name_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/names/ens/alice.eth")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("exact name request failed")?;

            assert_eq!(explain_response.status(), StatusCode::OK);
            assert_eq!(name_response.status(), StatusCode::OK);

            let explain_payload: NameResponse = read_json(explain_response).await?;
            let name_payload: NameResponse = read_json(name_response).await?;
            let name_declared_state = name_payload
                .declared_state
                .as_object()
                .expect("declared_state must be an object");
            let history = name_declared_state
                .get("history")
                .cloned()
                .expect("history summary must be present");

            assert_eq!(explain_payload.data, name_payload.data);
            assert_eq!(explain_payload.provenance, name_payload.provenance);
            assert_eq!(explain_payload.coverage, name_payload.coverage);
            assert_eq!(
                explain_payload.chain_positions,
                name_payload.chain_positions
            );
            assert_eq!(explain_payload.consistency, name_payload.consistency);
            assert_eq!(explain_payload.last_updated, name_payload.last_updated);
            assert_eq!(explain_payload.verified_state, None);
            assert_eq!(
                explain_payload.declared_state.get("history"),
                Some(&history)
            );
            assert_eq!(
                explain_payload.declared_state,
                json!({
                    "surface_binding": exact_name_surface_binding_summary(surface_binding_id),
                    "history": history,
                })
            );
            assert_exact_name_history_summary_matches_history_route(
                &database,
                "ens",
                "alice.eth",
                explain_payload
                    .declared_state
                    .get("history")
                    .expect("history summary must be present"),
            )
            .await?;

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn authority_control_explain_contract_is_declared_only_with_exact_name_coverage_and_frozen_summaries()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0x2200);
            let token_lineage_id = Uuid::from_u128(0x1100);
            let surface_binding_id = Uuid::from_u128(0x3300);

            database
                .seed_exact_name_rebuild_inputs(
                    logical_name_id,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            database.rebuild_name_current(logical_name_id).await?;

            let explain_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/explain/names/ens/alice.eth/authority-control")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("authority-control explain request failed")?;
            let name_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/names/ens/alice.eth")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("exact name request failed")?;

            assert_eq!(explain_response.status(), StatusCode::OK);
            assert_eq!(name_response.status(), StatusCode::OK);

            let explain_payload: NameResponse = read_json(explain_response).await?;
            let name_payload: NameResponse = read_json(name_response).await?;
            let name_declared_state = name_payload
                .declared_state
                .as_object()
                .expect("declared_state must be an object");
            let authority = exact_name_authority_summary(resource_id, token_lineage_id);
            let control = exact_name_control_summary();

            assert_eq!(explain_payload.data, name_payload.data);
            assert_eq!(explain_payload.provenance, name_payload.provenance);
            assert_eq!(explain_payload.coverage, name_payload.coverage);
            assert_eq!(
                explain_payload.chain_positions,
                name_payload.chain_positions
            );
            assert_eq!(explain_payload.consistency, name_payload.consistency);
            assert_eq!(explain_payload.last_updated, name_payload.last_updated);
            assert_eq!(explain_payload.verified_state, None);
            assert_eq!(name_declared_state.get("authority"), Some(&authority));
            assert_eq!(name_declared_state.get("control"), Some(&control));
            assert_eq!(
                explain_payload.declared_state.get("authority"),
                Some(&authority)
            );
            assert_eq!(
                explain_payload.declared_state.get("control"),
                Some(&control)
            );
            assert_eq!(
                explain_payload.declared_state,
                json!({
                    "authority": authority,
                    "control": control,
                })
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn basenames_exact_name_contract_reads_rebuilt_base_authority_projection()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "basenames:alice.base.eth";
            let resource_id = Uuid::from_u128(0x9230);
            let token_lineage_id = Uuid::from_u128(0x9231);
            let surface_binding_id = Uuid::from_u128(0x9232);

            database
                .seed_basenames_exact_name_rebuild_inputs(
                    logical_name_id,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            database.rebuild_name_current(logical_name_id).await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/names/basenames/alice.base.eth")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("basenames exact-name request failed")?;

            assert_eq!(response.status(), StatusCode::OK);

            let payload: NameResponse = read_json(response).await?;
            assert_eq!(payload.data["logical_name_id"], json!(logical_name_id));
            assert_eq!(payload.data["namespace"], json!("basenames"));
            assert_eq!(
                payload.declared_state.get("control"),
                Some(&basenames_exact_name_control_summary())
            );
            assert_eq!(
                payload.declared_state.get("resolver"),
                Some(&basenames_exact_name_resolver_summary())
            );
            assert_eq!(
                payload
                    .declared_state
                    .get("history")
                    .and_then(|value| value.get("surface_head"))
                    .and_then(|value| value.get("event_kind")),
                Some(&json!("ResolverChanged"))
            );
            assert_eq!(
                payload
                    .declared_state
                    .get("history")
                    .and_then(|value| value.get("resource_head"))
                    .and_then(|value| value.get("event_kind")),
                Some(&json!("ResolverChanged"))
            );
            assert_eq!(payload.coverage["status"], json!("full"));
            assert_eq!(
                payload.coverage["source_classes_considered"],
                json!(["ensv1_registry_path"])
            );
            assert_eq!(payload.verified_state, None);

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn basenames_coverage_contract_returns_shared_exact_name_coverage() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "basenames:alice.base.eth";
            let resource_id = Uuid::from_u128(0x9240);
            let token_lineage_id = Uuid::from_u128(0x9241);
            let surface_binding_id = Uuid::from_u128(0x9242);

            database
                .seed_basenames_exact_name_rebuild_inputs(
                    logical_name_id,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            database.rebuild_name_current(logical_name_id).await?;

            let coverage_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/coverage/basenames/alice.base.eth")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("basenames coverage request failed")?;
            let name_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/names/basenames/alice.base.eth")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("basenames name request failed")?;

            assert_eq!(coverage_response.status(), StatusCode::OK);
            assert_eq!(name_response.status(), StatusCode::OK);

            let coverage_payload: NameResponse = read_json(coverage_response).await?;
            let name_payload: NameResponse = read_json(name_response).await?;

            assert_eq!(coverage_payload.data, name_payload.data);
            assert_eq!(coverage_payload.coverage, name_payload.coverage);
            assert_eq!(coverage_payload.provenance, name_payload.provenance);
            assert_eq!(coverage_payload.chain_positions, name_payload.chain_positions);
            assert_eq!(coverage_payload.consistency, name_payload.consistency);
            assert_eq!(coverage_payload.last_updated, name_payload.last_updated);
            assert_eq!(coverage_payload.verified_state, None);
            assert_eq!(
                coverage_payload.declared_state,
                json!({
                    "status": "full",
                    "exhaustiveness": "authoritative",
                    "source_classes_considered": ["ensv1_registry_path"],
                    "enumeration_basis": "exact_name",
                    "unsupported_reason": null
                })
            );

            database.cleanup().await?;
            Ok(())
        }

        #[tokio::test]
        async fn basenames_exact_name_explain_contract_reuses_rebuilt_projection_envelope()
        -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let logical_name_id = "basenames:alice.base.eth";
            let resource_id = Uuid::from_u128(0x9250);
            let token_lineage_id = Uuid::from_u128(0x9251);
            let surface_binding_id = Uuid::from_u128(0x9252);

            database
                .seed_basenames_exact_name_rebuild_inputs(
                    logical_name_id,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            database.rebuild_name_current(logical_name_id).await?;

            let surface_explain_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/explain/names/basenames/alice.base.eth/surface-binding")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("basenames surface-binding explain request failed")?;
            let authority_explain_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/explain/names/basenames/alice.base.eth/authority-control")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("basenames authority-control explain request failed")?;
            let name_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/names/basenames/alice.base.eth")
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("basenames exact-name request failed")?;

            assert_eq!(surface_explain_response.status(), StatusCode::OK);
            assert_eq!(authority_explain_response.status(), StatusCode::OK);
            assert_eq!(name_response.status(), StatusCode::OK);

            let surface_payload: NameResponse = read_json(surface_explain_response).await?;
            let authority_payload: NameResponse = read_json(authority_explain_response).await?;
            let name_payload: NameResponse = read_json(name_response).await?;
            let history = name_payload
                .declared_state
                .get("history")
                .cloned()
                .expect("history summary must be present");

            assert_eq!(surface_payload.data, name_payload.data);
            assert_eq!(surface_payload.coverage, name_payload.coverage);
            assert_eq!(surface_payload.provenance, name_payload.provenance);
            assert_eq!(surface_payload.chain_positions, name_payload.chain_positions);
            assert_eq!(surface_payload.consistency, name_payload.consistency);
            assert_eq!(surface_payload.last_updated, name_payload.last_updated);
            assert_eq!(surface_payload.verified_state, None);
            assert_eq!(
                surface_payload.declared_state,
                json!({
                    "surface_binding": exact_name_surface_binding_summary(surface_binding_id),
                    "history": history.clone(),
                })
            );

            assert_eq!(authority_payload.data, name_payload.data);
            assert_eq!(authority_payload.coverage, name_payload.coverage);
            assert_eq!(authority_payload.provenance, name_payload.provenance);
            assert_eq!(authority_payload.chain_positions, name_payload.chain_positions);
            assert_eq!(authority_payload.consistency, name_payload.consistency);
            assert_eq!(authority_payload.last_updated, name_payload.last_updated);
            assert_eq!(authority_payload.verified_state, None);
            assert_eq!(
                authority_payload.declared_state,
                json!({
                    "authority": exact_name_authority_summary(resource_id, token_lineage_id),
                    "control": basenames_exact_name_control_summary(),
                })
            );

            database.cleanup().await?;
            Ok(())
        }

