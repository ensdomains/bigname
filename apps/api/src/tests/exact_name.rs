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
    database
        .insert_record_inventory_current_row(record_inventory_current_row(
            logical_name_id,
            resource_id,
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
        declared_state.get("record_inventory").cloned(),
        Some(json!({
            "record_version_boundary": record_inventory_boundary(logical_name_id, resource_id),
            "enumeration_basis": {
                "observed_selectors": true,
                "capability_declared_families": true,
                "globally_enumerable": false
            },
            "selectors": [
                {
                    "record_key": "addr:60",
                    "record_family": "addr",
                    "selector_key": "60",
                    "cacheable": true
                },
                {
                    "record_key": "avatar",
                    "record_family": "avatar",
                    "selector_key": null,
                    "cacheable": true
                },
                {
                    "record_key": "text:com.twitter",
                    "record_family": "text",
                    "selector_key": "com.twitter",
                    "cacheable": false
                }
            ],
            "explicit_gaps": [
                {
                    "record_key": "contenthash",
                    "record_family": "contenthash",
                    "selector_key": null,
                    "gap_reason": "not_observed_on_current_resolver"
                }
            ],
            "unsupported_families": [
                {
                    "record_family": "abi",
                    "unsupported_reason": "resolver_family_pending"
                },
                {
                    "record_family": "pubkey",
                    "unsupported_reason": "resolver_family_pending"
                }
            ],
            "last_change": {
                "normalized_event_id": 1200,
                "event_kind": "RecordsChanged",
                "chain_position": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_000_003,
                    "block_hash": "0xlastchange",
                    "timestamp": "2026-04-17T00:00:04Z"
                }
            }
        }))
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
async fn get_name_preserves_worker_record_inventory_boundary_pointer() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let worker_boundary = record_inventory_boundary_with_pointer(
        logical_name_id,
        resource_id,
        Some(1201),
        Some("RecordVersionChanged"),
    );

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
    database
        .insert_record_inventory_current_row(worker_record_inventory_current_row(
            logical_name_id,
            resource_id,
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
        .context("name request with worker-shaped record inventory projection failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: NameResponse = read_json(response).await?;
    assert_eq!(
        payload
            .declared_state
            .get("record_inventory")
            .and_then(|value| value.get("record_version_boundary")),
        Some(&worker_boundary)
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_returns_unsupported_record_inventory_when_projection_row_is_missing() -> Result<()>
{
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
        .context("name request without record inventory projection failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: NameResponse = read_json(response).await?;
    assert_eq!(
        payload
            .declared_state
            .get("record_inventory")
            .and_then(Value::as_object)
            .and_then(|value| value.get("status"))
            .and_then(Value::as_str),
        Some("unsupported")
    );
    assert_eq!(
        payload
            .declared_state
            .get("record_inventory")
            .and_then(Value::as_object)
            .and_then(|value| value.get("unsupported_reason"))
            .and_then(Value::as_str),
        Some("declared record inventory summary is not yet projected")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_coverage_returns_declared_state_explain_with_shared_top_level_coverage() -> Result<()>
{
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

    assert_eq!(coverage_payload.data, name_payload.data);
    assert_eq!(coverage_payload.coverage, name_payload.coverage);
    assert_eq!(coverage_payload.provenance, name_payload.provenance);
    assert_eq!(
        coverage_payload.chain_positions,
        name_payload.chain_positions
    );
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
async fn get_surface_binding_explain_reuses_exact_name_envelope_fields() -> Result<()> {
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

    let mut row = exact_name_row(
        logical_name_id,
        surface_binding_id,
        resource_id,
        token_lineage_id,
    );
    row.declared_summary = json!({
        "registration": {
            "status": "active",
            "authority_kind": "registrar"
        },
        "resolver": {
            "chain_id": "ethereum-mainnet",
            "address": "0x0000000000000000000000000000000000000abc",
            "latest_event_kind": "ResolverChanged"
        },
        "history": {
            "surface_head": null,
            "resource_head": null
        }
    });
    database.insert_name_current_row(row).await?;

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
        .context("name request failed")?;

    assert_eq!(explain_response.status(), StatusCode::OK);
    assert_eq!(name_response.status(), StatusCode::OK);

    let explain_payload: NameResponse = read_json(explain_response).await?;
    let name_payload: NameResponse = read_json(name_response).await?;

    assert_eq!(explain_payload.data, name_payload.data);
    assert_eq!(explain_payload.coverage, name_payload.coverage);
    assert_eq!(explain_payload.provenance, name_payload.provenance);
    assert_eq!(
        explain_payload.chain_positions,
        name_payload.chain_positions
    );
    assert_eq!(explain_payload.consistency, name_payload.consistency);
    assert_eq!(explain_payload.last_updated, name_payload.last_updated);
    assert_eq!(explain_payload.verified_state, None);
    assert_eq!(
        explain_payload.declared_state.get("history"),
        name_payload.declared_state.get("history")
    );
    assert_eq!(
        explain_payload.declared_state,
        json!({
            "surface_binding": {
                "surface_binding_id": surface_binding_id.to_string(),
                "binding_kind": "declared_registry_path"
            },
            "history": {
                "surface_head": null,
                "resource_head": null
            }
        })
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_authority_control_explain_reuses_exact_name_envelope_fields() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let registrant = "0x0000000000000000000000000000000000000abc";
    let registry_owner = "0x0000000000000000000000000000000000000def";

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

    let mut row = exact_name_row(
        logical_name_id,
        surface_binding_id,
        resource_id,
        token_lineage_id,
    );
    row.declared_summary = json!({
        "registration": {
            "status": "active",
            "authority_kind": "registrar"
        },
        "control": {
            "registrant": registrant,
            "registry_owner": registry_owner,
            "latest_event_kind": "NameWrapped"
        },
        "resolver": {
            "chain_id": "ethereum-mainnet",
            "address": "0x0000000000000000000000000000000000000abc",
            "latest_event_kind": "ResolverChanged"
        }
    });
    database.insert_name_current_row(row).await?;

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
        .context("name request failed")?;

    assert_eq!(explain_response.status(), StatusCode::OK);
    assert_eq!(name_response.status(), StatusCode::OK);

    let explain_payload: NameResponse = read_json(explain_response).await?;
    let name_payload: NameResponse = read_json(name_response).await?;

    assert_eq!(explain_payload.data, name_payload.data);
    assert_eq!(explain_payload.coverage, name_payload.coverage);
    assert_eq!(explain_payload.provenance, name_payload.provenance);
    assert_eq!(
        explain_payload.chain_positions,
        name_payload.chain_positions
    );
    assert_eq!(explain_payload.consistency, name_payload.consistency);
    assert_eq!(explain_payload.last_updated, name_payload.last_updated);
    assert_eq!(explain_payload.verified_state, None);
    assert_eq!(
        explain_payload.declared_state.get("authority"),
        name_payload.declared_state.get("authority")
    );
    assert_eq!(
        explain_payload.declared_state.get("control"),
        name_payload.declared_state.get("control")
    );
    assert_eq!(
        explain_payload.declared_state,
        json!({
            "authority": {
                "resource_id": resource_id.to_string(),
                "token_lineage_id": token_lineage_id.to_string(),
                "binding_kind": "declared_registry_path"
            },
            "control": {
                "registrant": registrant,
                "registry_owner": registry_owner,
                "latest_event_kind": "NameWrapped"
            }
        })
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_name_reads_rebuilt_basenames_exact_name_projection() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "basenames:alice.base.eth";
    let resource_id = Uuid::from_u128(0x9200);
    let token_lineage_id = Uuid::from_u128(0x9201);
    let surface_binding_id = Uuid::from_u128(0x9202);

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
    let history = payload
        .declared_state
        .get("history")
        .cloned()
        .expect("history summary must be present");
    assert_eq!(payload.data["logical_name_id"], json!(logical_name_id));
    assert_eq!(payload.data["namespace"], json!("basenames"));
    assert_eq!(
        payload.data["binding_kind"],
        json!("declared_registry_path")
    );
    assert_eq!(
        payload.declared_state.get("control"),
        Some(&basenames_exact_name_control_summary())
    );
    assert_eq!(
        payload.declared_state.get("resolver"),
        Some(&basenames_exact_name_resolver_summary())
    );
    assert_eq!(
        history
            .get("surface_head")
            .and_then(|value| value.get("event_kind")),
        Some(&json!("ResolverChanged"))
    );
    assert_eq!(
        history
            .get("resource_head")
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
async fn get_coverage_reads_shared_basenames_exact_name_coverage() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "basenames:alice.base.eth";
    let resource_id = Uuid::from_u128(0x9210);
    let token_lineage_id = Uuid::from_u128(0x9211);
    let surface_binding_id = Uuid::from_u128(0x9212);

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
    assert_eq!(
        coverage_payload.chain_positions,
        name_payload.chain_positions
    );
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
async fn get_basenames_exact_name_explains_reuse_projection_envelope_fields() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "basenames:alice.base.eth";
    let resource_id = Uuid::from_u128(0x9220);
    let token_lineage_id = Uuid::from_u128(0x9221);
    let surface_binding_id = Uuid::from_u128(0x9222);

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
    assert_eq!(
        surface_payload.chain_positions,
        name_payload.chain_positions
    );
    assert_eq!(surface_payload.consistency, name_payload.consistency);
    assert_eq!(surface_payload.last_updated, name_payload.last_updated);
    assert_eq!(surface_payload.verified_state, None);
    assert_eq!(
        surface_payload.declared_state,
        json!({
            "surface_binding": {
                "surface_binding_id": surface_binding_id.to_string(),
                "binding_kind": "declared_registry_path"
            },
            "history": history.clone(),
        })
    );

    assert_eq!(authority_payload.data, name_payload.data);
    assert_eq!(authority_payload.coverage, name_payload.coverage);
    assert_eq!(authority_payload.provenance, name_payload.provenance);
    assert_eq!(
        authority_payload.chain_positions,
        name_payload.chain_positions
    );
    assert_eq!(authority_payload.consistency, name_payload.consistency);
    assert_eq!(authority_payload.last_updated, name_payload.last_updated);
    assert_eq!(authority_payload.verified_state, None);
    assert_eq!(
        authority_payload.declared_state,
        json!({
            "authority": {
                "resource_id": resource_id.to_string(),
                "token_lineage_id": token_lineage_id.to_string(),
                "binding_kind": "declared_registry_path"
            },
            "control": basenames_exact_name_control_summary(),
        })
    );

    database.cleanup().await?;
    Ok(())
}

