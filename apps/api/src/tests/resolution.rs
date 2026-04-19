#[tokio::test]
async fn get_resolution_execution_explain_returns_persisted_verified_state_and_reuses_resolution_envelope_fields()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000021);
    let request_key = resolution_execution_request_key(&["text:com.twitter", "addr:60"]);
    let persisted_verified_queries = json!([
        {
            "record_key": "text:com.twitter",
            "status": "success",
            "value": {
                "value": "@alice"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000aa"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);

    database
        .seed_name_current_binding_migrated(
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
    database
        .insert_record_inventory_current_row(record_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;

    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["text:com.twitter", "addr:60"],
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        logical_name_id,
        resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let explain_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/resolutions/ens/alice.eth/execution?records=text:com.twitter,addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution execution explain request failed")?;
    let resolution_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=verified&records=text:com.twitter,addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution request failed")?;

    assert_eq!(explain_response.status(), StatusCode::OK);
    assert_eq!(resolution_response.status(), StatusCode::OK);

    let explain_payload: ResolutionResponse = read_json(explain_response).await?;
    let resolution_payload: ResolutionResponse = read_json(resolution_response).await?;
    let expected_resolution_verified_state = json!({
        "verified_queries": [
            {
                "record_key": "text:com.twitter",
                "status": "success",
                "value": {
                    "value": "@alice"
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            },
            {
                "record_key": "addr:60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x00000000000000000000000000000000000000aa"
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            }
        ]
    });

    assert_eq!(explain_payload.data, resolution_payload.data);
    assert_eq!(explain_payload.coverage, resolution_payload.coverage);
    assert_eq!(explain_payload.provenance, resolution_payload.provenance);
    assert_eq!(
        explain_payload.chain_positions,
        resolution_payload.chain_positions
    );
    assert_eq!(explain_payload.consistency, resolution_payload.consistency);
    assert_eq!(
        explain_payload.last_updated,
        resolution_payload.last_updated
    );
    assert_eq!(explain_payload.declared_state, None);
    assert_eq!(
        resolution_payload.verified_state,
        Some(expected_resolution_verified_state)
    );
    assert_eq!(
        explain_payload.verified_state,
        Some(json!({
            "execution": {
                "execution_trace_id": execution_trace_id.to_string(),
                "selected_entrypoint": {
                    "source_family": "ens_execution",
                    "role": "universal_resolver",
                    "chain_id": "ethereum-mainnet",
                    "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe"
                },
                "resolver_discovery_path": [
                    {
                        "logical_name_id": "ens:alice.eth",
                        "namespace": "ens",
                        "normalized_name": "alice.eth",
                        "canonical_display_name": "Alice.eth",
                        "resource_id": resource_id.to_string(),
                        "chain_id": "ethereum-mainnet",
                        "address": "0x0000000000000000000000000000000000000abc",
                        "latest_event_kind": "ResolverChanged"
                    }
                ],
                "wildcard": {
                    "source": null,
                    "matched_labels": []
                },
                "alias": {
                    "final_target": null,
                    "hops": []
                },
                "steps": [
                    {
                        "step_index": 0,
                        "step_kind": "load_declared_topology",
                        "input_digest": "sha256:topology-input",
                        "output_digest": "sha256:topology-output",
                        "latency": 4,
                        "canonicality_dependency": {
                            "ethereum-mainnet": {
                                "block_hash": "0xbinding",
                                "block_number": 21_000_003,
                                "state": "finalized"
                            }
                        }
                    },
                    {
                        "step_index": 1,
                        "step_kind": "call_universal_resolver",
                        "input_digest": "sha256:resolver-input",
                        "output_digest": "sha256:resolver-output",
                        "latency": 28,
                        "canonicality_dependency": {
                            "ethereum-mainnet": {
                                "block_hash": "0xbinding",
                                "block_number": 21_000_003,
                                "state": "finalized"
                            }
                        }
                    }
                ],
                "finished_at": format_timestamp(timestamp(1_717_171_900))
            },
            "verified_queries": [
                {
                    "record_key": "text:com.twitter",
                    "status": "success",
                    "value": {
                        "value": "@alice"
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string()
                    }
                },
                {
                    "record_key": "addr:60",
                    "status": "success",
                    "value": {
                        "coin_type": "60",
                        "value": "0x00000000000000000000000000000000000000aa"
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string()
                    }
                }
            ]
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_execution_explain_reads_persisted_alias_only_avatar_answers_for_ens_alias_binding()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000025);
    let request_key = resolution_execution_request_key(&["text:com.twitter"]);
    let persisted_verified_queries = json!([
        {
            "record_key": "avatar",
            "status": "success",
            "value": {
                "value": "https://cdn.example.test/alice-via-alias.png"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "text:com.twitter",
            "status": "success",
            "value": {
                "value": "@alice-via-alias"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);
    let alias_target = json!({
        "logical_name_id": "ens:profile.alice.eth",
        "namespace": "ens",
        "normalized_name": "profile.alice.eth",
        "canonical_display_name": "Profile.alice.eth",
        "namehash": "namehash:profile.alice.eth",
        "resource_id": resource_id.to_string(),
        "binding_kind": "resolver_alias_path"
    });

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
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
    row.binding_kind = Some(bigname_storage::SurfaceBindingKind::ResolverAliasPath);
    database.insert_name_current_row(row).await?;
    database
        .insert_record_inventory_current_row(record_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;

    let mut trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["avatar", "text:com.twitter"],
        persisted_verified_queries.clone(),
    );
    trace.request_metadata = json!({
        "surface": "alice.eth",
        "record_keys": ["avatar", "text:com.twitter"],
        "entrypoint": "universal_resolver",
        "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe",
        "alias": {
            "final_target": alias_target.clone(),
            "hops": [alias_target.clone()]
        }
    });
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        logical_name_id,
        resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let explain_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/resolutions/ens/alice.eth/execution?records=avatar,text:com.twitter")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution execution explain alias request failed")?;
    let resolution_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=verified&records=avatar,text:com.twitter")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution alias request failed")?;

    assert_eq!(explain_response.status(), StatusCode::OK);
    assert_eq!(resolution_response.status(), StatusCode::OK);

    let explain_payload: ResolutionResponse = read_json(explain_response).await?;
    let resolution_payload: ResolutionResponse = read_json(resolution_response).await?;
    let expected_resolution_verified_state = json!({
        "verified_queries": [
            {
                "record_key": "avatar",
                "status": "success",
                "value": {
                    "value": "https://cdn.example.test/alice-via-alias.png"
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            },
            {
                "record_key": "text:com.twitter",
                "status": "success",
                "value": {
                    "value": "@alice-via-alias"
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            }
        ]
    });

    assert_eq!(explain_payload.data, resolution_payload.data);
    assert_eq!(explain_payload.coverage, resolution_payload.coverage);
    assert_eq!(explain_payload.provenance, resolution_payload.provenance);
    assert_eq!(
        explain_payload.chain_positions,
        resolution_payload.chain_positions
    );
    assert_eq!(explain_payload.consistency, resolution_payload.consistency);
    assert_eq!(
        explain_payload.last_updated,
        resolution_payload.last_updated
    );
    assert_eq!(explain_payload.declared_state, None);
    assert_eq!(
        resolution_payload.verified_state,
        Some(expected_resolution_verified_state)
    );
    assert_eq!(
        explain_payload.verified_state,
        Some(json!({
            "execution": {
                "execution_trace_id": execution_trace_id.to_string(),
                "selected_entrypoint": {
                    "source_family": "ens_execution",
                    "role": "universal_resolver",
                    "chain_id": "ethereum-mainnet",
                    "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe"
                },
                "resolver_discovery_path": [
                    {
                        "logical_name_id": "ens:alice.eth",
                        "namespace": "ens",
                        "normalized_name": "alice.eth",
                        "canonical_display_name": "Alice.eth",
                        "resource_id": resource_id.to_string(),
                        "chain_id": "ethereum-mainnet",
                        "address": "0x0000000000000000000000000000000000000abc",
                        "latest_event_kind": "ResolverChanged"
                    }
                ],
                "wildcard": {
                    "source": null,
                    "matched_labels": []
                },
                "alias": {
                    "final_target": alias_target.clone(),
                    "hops": [alias_target.clone()]
                },
                "steps": [
                    {
                        "step_index": 0,
                        "step_kind": "load_declared_topology",
                        "input_digest": "sha256:topology-input",
                        "output_digest": "sha256:topology-output",
                        "latency": 4,
                        "canonicality_dependency": {
                            "ethereum-mainnet": {
                                "block_hash": "0xbinding",
                                "block_number": 21_000_003,
                                "state": "finalized"
                            }
                        }
                    },
                    {
                        "step_index": 1,
                        "step_kind": "call_universal_resolver",
                        "input_digest": "sha256:resolver-input",
                        "output_digest": "sha256:resolver-output",
                        "latency": 28,
                        "canonicality_dependency": {
                            "ethereum-mainnet": {
                                "block_hash": "0xbinding",
                                "block_number": 21_000_003,
                                "state": "finalized"
                            }
                        }
                    }
                ],
                "finished_at": format_timestamp(timestamp(1_717_171_900))
            },
            "verified_queries": [
                {
                    "record_key": "avatar",
                    "status": "success",
                    "value": {
                        "value": "https://cdn.example.test/alice-via-alias.png"
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string()
                    }
                },
                {
                    "record_key": "text:com.twitter",
                    "status": "success",
                    "value": {
                        "value": "@alice-via-alias"
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string()
                    }
                }
            ]
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_both_mode_returns_basenames_declared_transport_inventory_and_cache()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "basenames:alice.base.eth";
    let resource_id = Uuid::from_u128(0x6200);
    let token_lineage_id = Uuid::from_u128(0x6100);
    let surface_binding_id = Uuid::from_u128(0x6300);

    database
        .seed_basenames_resolution_rebuild_inputs(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database.rebuild_name_current(logical_name_id).await?;
    database
        .rebuild_record_inventory_current(resource_id)
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(
                    "/v1/resolutions/basenames/alice.base.eth?mode=both&records=addr:60,text",
                )
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("basenames mixed resolution request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    let declared_state = payload
        .declared_state
        .as_ref()
        .context("basenames mixed resolution must include declared_state")?;
    let record_inventory_boundary = declared_state
        .get("record_inventory")
        .and_then(|value| value.get("record_version_boundary"))
        .cloned()
        .context("basenames mixed resolution must include record_inventory boundary")?;
    let worker_row = bigname_storage::load_record_inventory_current(
        &database.pool,
        resource_id,
        &record_inventory_boundary,
    )
    .await?
    .context("worker-produced basenames record_inventory_current row must exist")?;

    assert_eq!(
        declared_state.get("topology"),
        Some(&json!({
            "registry_path": [
                {
                    "logical_name_id": logical_name_id,
                    "namespace": "basenames",
                    "normalized_name": "alice.base.eth",
                    "canonical_display_name": "Alice.base.eth",
                    "namehash": "namehash:alice.base.eth",
                    "resource_id": resource_id.to_string(),
                    "binding_kind": "declared_registry_path",
                }
            ],
            "subregistry_path": [],
            "resolver_path": [
                {
                    "logical_name_id": logical_name_id,
                    "namespace": "basenames",
                    "normalized_name": "alice.base.eth",
                    "canonical_display_name": "Alice.base.eth",
                    "resource_id": resource_id.to_string(),
                    "chain_id": "base-mainnet",
                    "address": "0x0000000000000000000000000000000000000abc",
                    "latest_event_kind": "ResolverChanged",
                }
            ],
            "wildcard": {
                "source": null,
                "matched_labels": [],
            },
            "alias": {
                "final_target": null,
                "hops": [],
            },
            "version_boundaries": {
                "topology_version_boundary": worker_row.record_version_boundary.clone(),
                "record_version_boundary": worker_row.record_version_boundary.clone(),
            },
            "transport": {
                "source_chain_id": "base-mainnet",
                "target_chain_id": "ethereum-mainnet",
                "contract_address": "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31",
                "latest_event_kind": null,
            },
        }))
    );
    assert_eq!(
        declared_state.get("record_inventory"),
        Some(&json!({
            "record_version_boundary": worker_row.record_version_boundary.clone(),
            "enumeration_basis": worker_row.enumeration_basis.clone(),
            "selectors": worker_row.selectors.clone(),
            "explicit_gaps": worker_row.explicit_gaps.clone(),
            "unsupported_families": worker_row.unsupported_families.clone(),
            "last_change": worker_row.last_change.clone().unwrap_or(Value::Null),
        }))
    );
    assert_eq!(
        declared_state.get("record_cache"),
        Some(&json!({
            "record_version_boundary": worker_row.record_version_boundary.clone(),
            "entries": worker_row.entries.clone(),
        }))
    );
    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_queries": [
                {
                    "record_key": "addr:60",
                    "status": "unsupported",
                    "unsupported_reason": "verified resolution entrypoint is not yet supported",
                },
                {
                    "record_key": "text",
                    "status": "unsupported",
                    "unsupported_reason": "verified resolution entrypoint is not yet supported",
                }
            ]
        }))
    );
    assert_eq!(
        payload.provenance.get("execution_trace_id"),
        Some(&Value::Null)
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_execution_explain_returns_not_found_for_basenames_while_execution_is_shadow()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "basenames:alice.base.eth";
    let resource_id = Uuid::from_u128(0x6400);
    let token_lineage_id = Uuid::from_u128(0x6500);
    let surface_binding_id = Uuid::from_u128(0x6600);

    database
        .seed_basenames_resolution_rebuild_inputs(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database.rebuild_name_current(logical_name_id).await?;
    database
        .rebuild_record_inventory_current(resource_id)
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/resolutions/basenames/alice.base.eth/execution?records=addr:60,text")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("basenames resolution execution explain request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(
        payload.error.message,
        "persisted resolution execution explain was not found for name alice.base.eth in namespace basenames"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_verified_state_uses_supported_persisted_answers_and_preserves_request_order()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000022);
    let request_key = resolution_execution_request_key(&["text:com.twitter", "addr:60"]);
    let persisted_verified_queries = json!([
        {
            "record_key": "text:com.twitter",
            "status": "success",
            "value": {
                "value": "@alice"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000aa"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);

    database
        .seed_name_current_binding_migrated(
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
    database
        .insert_record_inventory_current_row(record_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;

    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["text:com.twitter", "addr:60"],
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        logical_name_id,
        resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let verified_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=verified&records=avatar,text:com.twitter,addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("verified resolution request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=both&records=avatar,text:com.twitter,addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed resolution request failed")?;

    assert_eq!(verified_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let verified_payload: ResolutionResponse = read_json(verified_response).await?;
    let both_payload: ResolutionResponse = read_json(both_response).await?;
    let expected_verified_state = json!({
        "verified_queries": [
            {
                "record_key": "avatar",
                "status": "unsupported",
                "unsupported_reason": "verified resolution entrypoint is not yet supported"
            },
            {
                "record_key": "text:com.twitter",
                "status": "success",
                "value": {
                    "value": "@alice"
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            },
            {
                "record_key": "addr:60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x00000000000000000000000000000000000000aa"
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            }
        ]
    });

    assert_eq!(
        verified_payload.provenance.get("execution_trace_id"),
        Some(&Value::String(execution_trace_id.to_string()))
    );
    assert_eq!(
        verified_payload.verified_state,
        Some(expected_verified_state.clone())
    );
    assert_eq!(
        both_payload.provenance.get("execution_trace_id"),
        Some(&Value::String(execution_trace_id.to_string()))
    );
    assert!(both_payload.declared_state.is_some());
    assert_eq!(both_payload.verified_state, Some(expected_verified_state));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_both_mode_reads_persisted_alias_only_avatar_answers_for_ens_alias_binding()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000026);
    let request_key = resolution_execution_request_key(&["text:com.twitter"]);
    let persisted_verified_queries = json!([
        {
            "record_key": "avatar",
            "status": "success",
            "value": {
                "value": "https://cdn.example.test/alice-via-alias.png"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "text:com.twitter",
            "status": "success",
            "value": {
                "value": "@alice-via-alias"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
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
    row.binding_kind = Some(bigname_storage::SurfaceBindingKind::ResolverAliasPath);
    database.insert_name_current_row(row).await?;
    database
        .insert_record_inventory_current_row(record_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;

    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["avatar", "text:com.twitter"],
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        logical_name_id,
        resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=both&records=avatar,text:com.twitter")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed resolution alias request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    let declared_state = payload
        .declared_state
        .as_ref()
        .expect("declared_state must be present");

    assert_eq!(
        payload.provenance.get("execution_trace_id"),
        Some(&Value::String(execution_trace_id.to_string()))
    );
    assert_eq!(
        declared_state.get("topology"),
        Some(&json!({
            "status": "unsupported",
            "unsupported_reason": "declared resolution topology is not yet projected",
        }))
    );
    assert!(
        declared_state
            .get("record_inventory")
            .and_then(|value| value.get("record_version_boundary"))
            .is_some(),
        "record inventory should still load through the persisted readback lane"
    );
    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_queries": [
                {
                    "record_key": "avatar",
                    "status": "success",
                    "value": {
                        "value": "https://cdn.example.test/alice-via-alias.png"
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string()
                    }
                },
                {
                    "record_key": "text:com.twitter",
                    "status": "success",
                    "value": {
                        "value": "@alice-via-alias"
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string()
                    }
                }
            ]
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_verified_state_surfaces_persisted_avatar_answers_and_preserves_request_order()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000023);
    let contenthash = "ipfs://bafybeigdyrzt5sfp7udm7hu76fx4f2jv4jvgxk5csodx4d6vshv3zysn7u";
    let request_key =
        resolution_execution_request_key(&["text:com.twitter", "contenthash", "addr:60"]);
    let persisted_verified_queries = json!([
        {
            "record_key": "avatar",
            "status": "success",
            "value": {
                "value": "https://cdn.example.test/alice.png"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "text:com.twitter",
            "status": "not_found",
            "failure_reason": "no_text_record"
        },
        {
            "record_key": "contenthash",
            "status": "success",
            "value": {
                "value": contenthash
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000aa"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);

    database
        .seed_name_current_binding_migrated(
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
    database
        .insert_record_inventory_current_row(record_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;

    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["avatar", "text:com.twitter", "contenthash", "addr:60"],
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        logical_name_id,
        resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let verified_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=verified&records=avatar,text:com.twitter,contenthash,addr:60")
                .body(Body::empty())
                .expect("verified request must build"),
        )
        .await
        .context("verified resolution request with contenthash failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=both&records=avatar,text:com.twitter,contenthash,addr:60")
                .body(Body::empty())
                .expect("mixed request must build"),
        )
        .await
        .context("mixed resolution request with contenthash failed")?;

    assert_eq!(verified_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let verified_payload: ResolutionResponse = read_json(verified_response).await?;
    let both_payload: ResolutionResponse = read_json(both_response).await?;
    let expected_verified_state = json!({
        "verified_queries": [
            {
                "record_key": "avatar",
                "status": "success",
                "value": {
                    "value": "https://cdn.example.test/alice.png"
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            },
            {
                "record_key": "text:com.twitter",
                "status": "not_found",
                "failure_reason": "no_text_record"
            },
            {
                "record_key": "contenthash",
                "status": "success",
                "value": {
                    "value": contenthash
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            },
            {
                "record_key": "addr:60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x00000000000000000000000000000000000000aa"
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            }
        ]
    });

    assert_eq!(
        verified_payload.provenance.get("execution_trace_id"),
        Some(&Value::String(execution_trace_id.to_string()))
    );
    assert_eq!(
        verified_payload.verified_state,
        Some(expected_verified_state.clone())
    );
    assert_eq!(
        both_payload.provenance.get("execution_trace_id"),
        Some(&Value::String(execution_trace_id.to_string()))
    );
    assert!(both_payload.declared_state.is_some());
    assert_eq!(both_payload.verified_state, Some(expected_verified_state));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_execution_explain_returns_not_found_when_persisted_answer_is_missing()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);

    database
        .seed_name_current_binding_migrated(
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
                .uri("/v1/explain/resolutions/ens/alice.eth/execution?records=addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution execution explain request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(
        payload.error.message,
        "persisted resolution execution explain was not found for name alice.eth in namespace ens"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_execution_explain_surfaces_persisted_avatar_answers_and_reuses_resolution_envelope_fields()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000024);
    let contenthash = "ipfs://bafybeigdyrzt5sfp7udm7hu76fx4f2jv4jvgxk5csodx4d6vshv3zysn7u";
    let request_key =
        resolution_execution_request_key(&["text:com.twitter", "contenthash", "addr:60"]);
    let persisted_verified_queries = json!([
        {
            "record_key": "avatar",
            "status": "success",
            "value": {
                "value": "https://cdn.example.test/alice.png"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "text:com.twitter",
            "status": "not_found",
            "failure_reason": "no_text_record"
        },
        {
            "record_key": "contenthash",
            "status": "success",
            "value": {
                "value": contenthash
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        },
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000aa"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);

    database
        .seed_name_current_binding_migrated(
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
    database
        .insert_record_inventory_current_row(record_inventory_current_row(
            logical_name_id,
            resource_id,
        ))
        .await?;

    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["avatar", "text:com.twitter", "contenthash", "addr:60"],
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        logical_name_id,
        resource_id,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let explain_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/resolutions/ens/alice.eth/execution?records=avatar,text:com.twitter,contenthash,addr:60")
                .body(Body::empty())
                .expect("explain request must build"),
        )
        .await
        .context("resolution execution explain request with contenthash failed")?;
    let resolution_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=verified&records=avatar,text:com.twitter,contenthash,addr:60")
                .body(Body::empty())
                .expect("resolution request must build"),
        )
        .await
        .context("resolution request with contenthash failed")?;

    assert_eq!(explain_response.status(), StatusCode::OK);
    assert_eq!(resolution_response.status(), StatusCode::OK);

    let explain_payload: ResolutionResponse = read_json(explain_response).await?;
    let resolution_payload: ResolutionResponse = read_json(resolution_response).await?;
    let expected_resolution_verified_state = json!({
        "verified_queries": [
            {
                "record_key": "avatar",
                "status": "success",
                "value": {
                    "value": "https://cdn.example.test/alice.png"
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            },
            {
                "record_key": "text:com.twitter",
                "status": "not_found",
                "failure_reason": "no_text_record"
            },
            {
                "record_key": "contenthash",
                "status": "success",
                "value": {
                    "value": contenthash
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            },
            {
                "record_key": "addr:60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x00000000000000000000000000000000000000aa"
                },
                "provenance": {
                    "execution_trace_id": execution_trace_id.to_string()
                }
            }
        ]
    });

    assert_eq!(explain_payload.data, resolution_payload.data);
    assert_eq!(explain_payload.coverage, resolution_payload.coverage);
    assert_eq!(explain_payload.provenance, resolution_payload.provenance);
    assert_eq!(
        explain_payload.chain_positions,
        resolution_payload.chain_positions
    );
    assert_eq!(explain_payload.consistency, resolution_payload.consistency);
    assert_eq!(
        explain_payload.last_updated,
        resolution_payload.last_updated
    );
    assert_eq!(explain_payload.declared_state, None);
    assert_eq!(
        resolution_payload.verified_state,
        Some(expected_resolution_verified_state)
    );
    assert_eq!(
        explain_payload.verified_state,
        Some(json!({
            "execution": {
                "execution_trace_id": execution_trace_id.to_string(),
                "selected_entrypoint": {
                    "source_family": "ens_execution",
                    "role": "universal_resolver",
                    "chain_id": "ethereum-mainnet",
                    "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe"
                },
                "resolver_discovery_path": [
                    {
                        "logical_name_id": "ens:alice.eth",
                        "namespace": "ens",
                        "normalized_name": "alice.eth",
                        "canonical_display_name": "Alice.eth",
                        "resource_id": resource_id.to_string(),
                        "chain_id": "ethereum-mainnet",
                        "address": "0x0000000000000000000000000000000000000abc",
                        "latest_event_kind": "ResolverChanged"
                    }
                ],
                "wildcard": {
                    "source": null,
                    "matched_labels": []
                },
                "alias": {
                    "final_target": null,
                    "hops": []
                },
                "steps": [
                    {
                        "step_index": 0,
                        "step_kind": "load_declared_topology",
                        "input_digest": "sha256:topology-input",
                        "output_digest": "sha256:topology-output",
                        "latency": 4,
                        "canonicality_dependency": {
                            "ethereum-mainnet": {
                                "block_hash": "0xbinding",
                                "block_number": 21_000_003,
                                "state": "finalized"
                            }
                        }
                    },
                    {
                        "step_index": 1,
                        "step_kind": "call_universal_resolver",
                        "input_digest": "sha256:resolver-input",
                        "output_digest": "sha256:resolver-output",
                        "latency": 28,
                        "canonicality_dependency": {
                            "ethereum-mainnet": {
                                "block_hash": "0xbinding",
                                "block_number": 21_000_003,
                                "state": "finalized"
                            }
                        }
                    }
                ],
                "finished_at": format_timestamp(timestamp(1_717_171_900))
            },
            "verified_queries": [
                {
                    "record_key": "avatar",
                    "status": "success",
                    "value": {
                        "value": "https://cdn.example.test/alice.png"
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string()
                    }
                },
                {
                    "record_key": "text:com.twitter",
                    "status": "not_found",
                    "failure_reason": "no_text_record"
                },
                {
                    "record_key": "contenthash",
                    "status": "success",
                    "value": {
                        "value": contenthash
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string()
                    }
                },
                {
                    "record_key": "addr:60",
                    "status": "success",
                    "value": {
                        "coin_type": "60",
                        "value": "0x00000000000000000000000000000000000000aa"
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string()
                    }
                }
            ]
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_execution_explain_rejects_duplicate_records() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/resolutions/ens/alice.eth/execution?records=text,text")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("duplicate resolution execution explain request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "invalid_input");
    assert_eq!(
        payload.error.message,
        "records must not contain duplicate selectors"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_execution_explain_rejects_malformed_records() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/resolutions/ens/alice.eth/execution?records=:avatar")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("malformed resolution execution explain request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "invalid_input");
    assert_eq!(
        payload.error.message,
        "records must contain only valid record selectors"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_mode_parsing_populates_expected_sections() -> Result<()> {
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

    let default_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("default resolution request failed")?;
    let declared_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=declared&records=text")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("declared resolution request failed")?;
    let verified_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=verified&records=text,addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("verified resolution request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=both&records=text")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed resolution request failed")?;

    assert_eq!(default_response.status(), StatusCode::OK);
    assert_eq!(declared_response.status(), StatusCode::OK);
    assert_eq!(verified_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let default_payload: ResolutionResponse = read_json(default_response).await?;
    let declared_payload: ResolutionResponse = read_json(declared_response).await?;
    let verified_payload: ResolutionResponse = read_json(verified_response).await?;
    let both_payload: ResolutionResponse = read_json(both_response).await?;

    assert!(default_payload.declared_state.is_some());
    assert_eq!(default_payload.verified_state, None);
    assert!(declared_payload.declared_state.is_some());
    assert_eq!(declared_payload.verified_state, None);
    assert_eq!(verified_payload.declared_state, None);
    assert_eq!(
        verified_payload.verified_state,
        Some(json!({
            "verified_queries": [
                {
                    "record_key": "text",
                    "status": "unsupported",
                    "unsupported_reason": "verified resolution entrypoint is not yet supported",
                },
                {
                    "record_key": "addr:60",
                    "status": "unsupported",
                    "unsupported_reason": "verified resolution entrypoint is not yet supported",
                }
            ]
        }))
    );
    assert!(both_payload.declared_state.is_some());
    assert_eq!(
        both_payload.verified_state,
        Some(json!({
            "verified_queries": [
                {
                    "record_key": "text",
                    "status": "unsupported",
                    "unsupported_reason": "verified resolution entrypoint is not yet supported",
                }
            ]
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_execution_explain_supports_projected_wildcard_topology() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let wildcard_resource_id = Uuid::from_u128(0x4400);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000027);
    let request_key = resolution_execution_request_key(&["addr:60"]);
    let wildcard_source = json!({
        "logical_name_id": "ens:eth",
        "namespace": "ens",
        "normalized_name": "eth",
        "canonical_display_name": "Eth",
        "namehash": "namehash:eth",
        "resource_id": wildcard_resource_id.to_string(),
        "binding_kind": "observed_wildcard_path"
    });
    let wildcard_boundary = record_inventory_boundary("ens:eth", wildcard_resource_id);
    let projected_topology = json!({
        "registry_path": [
            {
                "logical_name_id": logical_name_id,
                "namespace": "ens",
                "normalized_name": "alice.eth",
                "canonical_display_name": "Alice.eth",
                "namehash": "namehash:alice.eth",
                "resource_id": resource_id.to_string(),
                "binding_kind": "observed_wildcard_path"
            }
        ],
        "subregistry_path": [],
        "resolver_path": [
            {
                "logical_name_id": "ens:eth",
                "namespace": "ens",
                "normalized_name": "eth",
                "canonical_display_name": "Eth",
                "resource_id": wildcard_resource_id.to_string(),
                "chain_id": "ethereum-mainnet",
                "address": "0x0000000000000000000000000000000000000def",
                "latest_event_kind": "ResolverChanged"
            }
        ],
        "wildcard": {
            "source": wildcard_source.clone(),
            "matched_labels": ["alice"]
        },
        "alias": {
            "final_target": null,
            "hops": []
        },
        "version_boundaries": {
            "topology_version_boundary": wildcard_boundary.clone(),
            "record_version_boundary": wildcard_boundary.clone()
        },
        "transport": {
            "source_chain_id": null,
            "target_chain_id": null,
            "contract_address": null,
            "latest_event_kind": null
        }
    });
    let persisted_verified_queries = json!([
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000aa"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
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
    row.binding_kind = Some(bigname_storage::SurfaceBindingKind::ObservedWildcardPath);
    row.declared_summary = json!({
        "topology": projected_topology.clone()
    });
    database.insert_name_current_row(row).await?;

    let mut trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["addr:60"],
        persisted_verified_queries.clone(),
    );
    trace.request_metadata = json!({
        "surface": "alice.eth",
        "record_keys": ["addr:60"],
        "entrypoint": "universal_resolver",
        "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe",
        "wildcard": {
            "source": wildcard_source.clone(),
            "matched_labels": ["alice"]
        }
    });
    let outcome = resolution_execution_outcome_with_boundaries(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        wildcard_boundary.clone(),
        wildcard_boundary,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let explain_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/resolutions/ens/alice.eth/execution?records=addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("wildcard resolution execution explain request failed")?;
    let resolution_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=verified&records=addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("wildcard mixed resolution request failed")?;

    assert_eq!(explain_response.status(), StatusCode::OK);
    assert_eq!(resolution_response.status(), StatusCode::OK);

    let explain_payload: ResolutionResponse = read_json(explain_response).await?;
    let resolution_payload: ResolutionResponse = read_json(resolution_response).await?;

    assert_eq!(
        resolution_payload.verified_state,
        Some(json!({
            "verified_queries": [
                {
                    "record_key": "addr:60",
                    "status": "success",
                    "value": {
                        "coin_type": "60",
                        "value": "0x00000000000000000000000000000000000000aa"
                    },
                    "provenance": {
                        "execution_trace_id": execution_trace_id.to_string()
                    }
                }
            ]
        }))
    );
    assert_eq!(
        explain_payload
            .verified_state
            .as_ref()
            .and_then(|state| state.get("execution"))
            .and_then(|execution| execution.get("resolver_discovery_path")),
        projected_topology.get("resolver_path")
    );
    assert_eq!(
        explain_payload
            .verified_state
            .as_ref()
            .and_then(|state| state.get("execution"))
            .and_then(|execution| execution.get("wildcard")),
        Some(&json!({
            "source": wildcard_source,
            "matched_labels": ["alice"]
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_both_mode_preserves_projected_topology_for_deferred_ancestor_selected_path()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let ancestor_resource_id = Uuid::from_u128(0x5500);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000028);
    let request_key = resolution_execution_request_key(&["text:com.twitter"]);
    let ancestor_boundary = record_inventory_boundary("ens:eth", ancestor_resource_id);
    let projected_topology = json!({
        "registry_path": [
            {
                "logical_name_id": logical_name_id,
                "namespace": "ens",
                "normalized_name": "alice.eth",
                "canonical_display_name": "Alice.eth",
                "namehash": "namehash:alice.eth",
                "resource_id": resource_id.to_string(),
                "binding_kind": "declared_registry_path"
            }
        ],
        "subregistry_path": [],
        "resolver_path": [
            {
                "logical_name_id": "ens:eth",
                "namespace": "ens",
                "normalized_name": "eth",
                "canonical_display_name": "Eth",
                "resource_id": ancestor_resource_id.to_string(),
                "chain_id": "ethereum-mainnet",
                "address": "0x0000000000000000000000000000000000000def",
                "latest_event_kind": "ResolverChanged"
            }
        ],
        "wildcard": {
            "source": null,
            "matched_labels": []
        },
        "alias": {
            "final_target": null,
            "hops": []
        },
        "version_boundaries": {
            "topology_version_boundary": ancestor_boundary.clone(),
            "record_version_boundary": ancestor_boundary.clone()
        },
        "transport": {
            "source_chain_id": null,
            "target_chain_id": null,
            "contract_address": null,
            "latest_event_kind": null
        }
    });
    let persisted_verified_queries = json!([
        {
            "record_key": "text:com.twitter",
            "status": "success",
            "value": {
                "value": "@ancestor"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
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
        "topology": projected_topology.clone()
    });
    database.insert_name_current_row(row).await?;

    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["text:com.twitter"],
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome_with_boundaries(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        ancestor_boundary.clone(),
        ancestor_boundary,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=both&records=text:com.twitter")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("deferred ancestor-selected mixed resolution request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    assert_eq!(
        payload
            .declared_state
            .as_ref()
            .and_then(|state| state.get("topology")),
        Some(&projected_topology)
    );
    assert_eq!(
        payload.provenance.get("execution_trace_id"),
        Some(&Value::Null)
    );
    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_queries": [
                {
                    "record_key": "text:com.twitter",
                    "status": "unsupported",
                    "unsupported_reason": "verified resolution entrypoint is not yet supported"
                }
            ]
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_both_mode_preserves_projected_transport_for_deferred_transport_path()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000029);
    let request_key = resolution_execution_request_key(&["addr:60"]);
    let route_boundary = record_inventory_boundary(logical_name_id, resource_id);
    let projected_topology = json!({
        "registry_path": [
            {
                "logical_name_id": logical_name_id,
                "namespace": "ens",
                "normalized_name": "alice.eth",
                "canonical_display_name": "Alice.eth",
                "namehash": "namehash:alice.eth",
                "resource_id": resource_id.to_string(),
                "binding_kind": "declared_registry_path"
            }
        ],
        "subregistry_path": [],
        "resolver_path": [
            {
                "logical_name_id": logical_name_id,
                "namespace": "ens",
                "normalized_name": "alice.eth",
                "canonical_display_name": "Alice.eth",
                "resource_id": resource_id.to_string(),
                "chain_id": "ethereum-mainnet",
                "address": "0x0000000000000000000000000000000000000abc",
                "latest_event_kind": "ResolverChanged"
            }
        ],
        "wildcard": {
            "source": null,
            "matched_labels": []
        },
        "alias": {
            "final_target": null,
            "hops": []
        },
        "version_boundaries": {
            "topology_version_boundary": route_boundary.clone(),
            "record_version_boundary": route_boundary.clone()
        },
        "transport": {
            "source_chain_id": "ethereum-mainnet",
            "target_chain_id": "base-mainnet",
            "contract_address": "0x000000000000000000000000000000000000beef",
            "latest_event_kind": "TransportResolved"
        }
    });
    let persisted_verified_queries = json!([
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000aa"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
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
        "topology": projected_topology.clone()
    });
    database.insert_name_current_row(row).await?;

    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["addr:60"],
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome_with_boundaries(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        route_boundary.clone(),
        route_boundary,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=both&records=addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("transport-assisted mixed resolution request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    assert_eq!(
        payload
            .declared_state
            .as_ref()
            .and_then(|state| state.get("topology")),
        Some(&projected_topology)
    );
    assert_eq!(
        payload.provenance.get("execution_trace_id"),
        Some(&Value::Null)
    );
    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_queries": [
                {
                    "record_key": "addr:60",
                    "status": "unsupported",
                    "unsupported_reason": "verified resolution entrypoint is not yet supported"
                }
            ]
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_execution_explain_returns_not_found_for_deferred_ancestor_selected_path()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let ancestor_resource_id = Uuid::from_u128(0x5500);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000002a);
    let request_key = resolution_execution_request_key(&["text:com.twitter"]);
    let ancestor_boundary = record_inventory_boundary("ens:eth", ancestor_resource_id);

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
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
        "topology": {
            "registry_path": [],
            "subregistry_path": [],
            "resolver_path": [
                {
                    "logical_name_id": "ens:eth",
                    "namespace": "ens",
                    "normalized_name": "eth",
                    "canonical_display_name": "Eth",
                    "resource_id": ancestor_resource_id.to_string(),
                    "chain_id": "ethereum-mainnet",
                    "address": "0x0000000000000000000000000000000000000def",
                    "latest_event_kind": "ResolverChanged"
                }
            ],
            "wildcard": {
                "source": null,
                "matched_labels": []
            },
            "alias": {
                "final_target": null,
                "hops": []
            },
            "version_boundaries": {
                "topology_version_boundary": ancestor_boundary.clone(),
                "record_version_boundary": ancestor_boundary.clone()
            },
            "transport": {
                "source_chain_id": null,
                "target_chain_id": null,
                "contract_address": null,
                "latest_event_kind": null
            }
        }
    });
    database.insert_name_current_row(row).await?;

    let persisted_verified_queries = json!([
        {
            "record_key": "text:com.twitter",
            "status": "success",
            "value": {
                "value": "@ancestor"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);
    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["text:com.twitter"],
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome_with_boundaries(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        ancestor_boundary.clone(),
        ancestor_boundary,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/resolutions/ens/alice.eth/execution?records=text:com.twitter")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("deferred ancestor-selected resolution execution explain request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(
        payload.error.message,
        "persisted resolution execution explain was not found for name alice.eth in namespace ens"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_execution_explain_returns_not_found_for_deferred_transport_path()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000002b);
    let request_key = resolution_execution_request_key(&["addr:60"]);
    let route_boundary = record_inventory_boundary(logical_name_id, resource_id);

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
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
        "topology": {
            "registry_path": [],
            "subregistry_path": [],
            "resolver_path": [
                {
                    "logical_name_id": logical_name_id,
                    "namespace": "ens",
                    "normalized_name": "alice.eth",
                    "canonical_display_name": "Alice.eth",
                    "resource_id": resource_id.to_string(),
                    "chain_id": "ethereum-mainnet",
                    "address": "0x0000000000000000000000000000000000000abc",
                    "latest_event_kind": "ResolverChanged"
                }
            ],
            "wildcard": {
                "source": null,
                "matched_labels": []
            },
            "alias": {
                "final_target": null,
                "hops": []
            },
            "version_boundaries": {
                "topology_version_boundary": route_boundary.clone(),
                "record_version_boundary": route_boundary.clone()
            },
            "transport": {
                "source_chain_id": "ethereum-mainnet",
                "target_chain_id": "base-mainnet",
                "contract_address": "0x000000000000000000000000000000000000beef",
                "latest_event_kind": "TransportResolved"
            }
        }
    });
    database.insert_name_current_row(row).await?;

    let persisted_verified_queries = json!([
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000aa"
            },
            "provenance": {
                "execution_trace_id": execution_trace_id.to_string()
            }
        }
    ]);
    let trace = resolution_execution_trace(
        execution_trace_id,
        &request_key,
        &["addr:60"],
        persisted_verified_queries.clone(),
    );
    let outcome = resolution_execution_outcome_with_boundaries(
        execution_trace_id,
        &request_key,
        persisted_verified_queries,
        route_boundary.clone(),
        route_boundary,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/explain/resolutions/ens/alice.eth/execution?records=addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("deferred transport resolution execution explain request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(
        payload.error.message,
        "persisted resolution execution explain was not found for name alice.eth in namespace ens"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_requires_records_for_verified_modes() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;

    let verified_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("verified resolution request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed resolution request failed")?;

    assert_eq!(verified_response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(both_response.status(), StatusCode::BAD_REQUEST);

    let verified_payload: ErrorResponse = read_json(verified_response).await?;
    let both_payload: ErrorResponse = read_json(both_response).await?;
    assert_eq!(verified_payload.error.code, "invalid_input");
    assert_eq!(both_payload.error.code, "invalid_input");
    assert_eq!(
        verified_payload.error.message,
        "records is required when mode is verified or both"
    );
    assert_eq!(both_payload.error.message, verified_payload.error.message);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_rejects_duplicate_records_for_verified_modes() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=verified&records=text,text")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("duplicate resolution request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "invalid_input");
    assert_eq!(
        payload.error.message,
        "records must not contain duplicate selectors"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_rejects_malformed_records() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=declared&records=:avatar")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("malformed resolution request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "invalid_input");
    assert_eq!(
        payload.error.message,
        "records must contain only valid record selectors"
    );
    assert!(payload.error.details.is_empty());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_returns_supported_topology_for_direct_ens_binding() -> Result<()> {
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
                .uri("/v1/resolutions/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    assert_eq!(payload.verified_state, None);
    assert_eq!(
        payload.declared_state,
        Some(json!({
            "topology": {
                "registry_path": [
                    {
                        "logical_name_id": "ens:alice.eth",
                        "namespace": "ens",
                        "normalized_name": "alice.eth",
                        "canonical_display_name": "Alice.eth",
                        "namehash": "namehash:alice.eth",
                        "resource_id": resource_id.to_string(),
                        "binding_kind": "declared_registry_path",
                    }
                ],
                "subregistry_path": [],
                "resolver_path": [
                    {
                        "logical_name_id": "ens:alice.eth",
                        "namespace": "ens",
                        "normalized_name": "alice.eth",
                        "canonical_display_name": "Alice.eth",
                        "resource_id": resource_id.to_string(),
                        "chain_id": "ethereum-mainnet",
                        "address": "0x0000000000000000000000000000000000000abc",
                        "latest_event_kind": "ResolverChanged",
                    }
                ],
                "wildcard": {
                    "source": null,
                    "matched_labels": [],
                },
                "alias": {
                    "final_target": null,
                    "hops": [],
                },
                "version_boundaries": {
                    "topology_version_boundary": {
                        "logical_name_id": "ens:alice.eth",
                        "resource_id": resource_id.to_string(),
                        "normalized_event_id": null,
                        "event_kind": null,
                        "chain_position": {
                            "chain_id": "ethereum-mainnet",
                            "block_number": 21_000_003,
                            "block_hash": "0xbinding",
                            "timestamp": "2026-04-17T00:00:03Z",
                        },
                    },
                    "record_version_boundary": {
                        "logical_name_id": "ens:alice.eth",
                        "resource_id": resource_id.to_string(),
                        "normalized_event_id": null,
                        "event_kind": null,
                        "chain_position": {
                            "chain_id": "ethereum-mainnet",
                            "block_number": 21_000_003,
                            "block_hash": "0xbinding",
                            "timestamp": "2026-04-17T00:00:03Z",
                        },
                    },
                },
                "transport": {
                    "source_chain_id": null,
                    "target_chain_id": null,
                    "contract_address": null,
                    "latest_event_kind": null,
                },
            },
            "record_inventory": {
                "record_version_boundary": record_inventory_boundary(logical_name_id, resource_id),
                "enumeration_basis": {
                    "observed_selectors": true,
                    "capability_declared_families": true,
                    "globally_enumerable": false,
                },
                "selectors": [
                    {
                        "record_key": "addr:60",
                        "record_family": "addr",
                        "selector_key": "60",
                        "cacheable": true,
                    },
                    {
                        "record_key": "avatar",
                        "record_family": "avatar",
                        "selector_key": null,
                        "cacheable": true,
                    },
                    {
                        "record_key": "text:com.twitter",
                        "record_family": "text",
                        "selector_key": "com.twitter",
                        "cacheable": false,
                    }
                ],
                "explicit_gaps": [
                    {
                        "record_key": "contenthash",
                        "record_family": "contenthash",
                        "selector_key": null,
                        "gap_reason": "not_observed_on_current_resolver",
                    }
                ],
                "unsupported_families": [
                    {
                        "record_family": "abi",
                        "unsupported_reason": "resolver_family_pending",
                    },
                    {
                        "record_family": "pubkey",
                        "unsupported_reason": "resolver_family_pending",
                    }
                ],
                "last_change": {
                    "normalized_event_id": 1200,
                    "event_kind": "RecordsChanged",
                    "chain_position": {
                        "chain_id": "ethereum-mainnet",
                        "block_number": 21_000_003,
                        "block_hash": "0xlastchange",
                        "timestamp": "2026-04-17T00:00:04Z",
                    }
                }
            },
            "record_cache": {
                "record_version_boundary": record_inventory_boundary(logical_name_id, resource_id),
                "entries": [
                    {
                        "record_key": "addr:60",
                        "record_family": "addr",
                        "selector_key": "60",
                        "status": "success",
                        "value": {
                            "coin_type": "60",
                            "value": "0x0000000000000000000000000000000000000abc",
                        }
                    },
                    {
                        "record_key": "avatar",
                        "record_family": "avatar",
                        "selector_key": null,
                        "status": "unsupported",
                        "unsupported_reason": "resolver_family_pending",
                    }
                ]
            }
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_preserves_worker_record_inventory_boundary_pointer() -> Result<()> {
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
                .uri("/v1/resolutions/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution request with worker-shaped record inventory projection failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    let declared_state = payload
        .declared_state
        .as_ref()
        .expect("declared_state must be present");
    let topology = declared_state
        .get("topology")
        .and_then(Value::as_object)
        .expect("topology must be supported");
    let version_boundaries = topology
        .get("version_boundaries")
        .and_then(Value::as_object)
        .expect("version_boundaries must be present");

    assert_eq!(
        version_boundaries.get("topology_version_boundary"),
        Some(&worker_boundary)
    );
    assert_eq!(
        version_boundaries.get("record_version_boundary"),
        Some(&worker_boundary)
    );
    assert_eq!(
        declared_state
            .get("record_inventory")
            .and_then(|value| value.get("record_version_boundary")),
        Some(&worker_boundary)
    );
    assert_eq!(
        declared_state
            .get("record_cache")
            .and_then(|value| value.get("record_version_boundary")),
        Some(&worker_boundary)
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_returns_unsupported_record_inventory_sections_when_projection_row_is_missing()
-> Result<()> {
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
                .uri("/v1/resolutions/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution request without record inventory projection failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    let declared_state = payload
        .declared_state
        .as_ref()
        .expect("declared_state must be present");
    assert_eq!(
        declared_state.get("record_inventory"),
        Some(&json!({
            "status": "unsupported",
            "unsupported_reason": "declared resolution record inventory is not yet projected",
        }))
    );
    assert_eq!(
        declared_state.get("record_cache"),
        Some(&json!({
            "status": "unsupported",
            "unsupported_reason": "declared resolution record cache is not yet projected",
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_declared_records_narrow_record_cache_in_request_order() -> Result<()> {
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
                .uri("/v1/resolutions/ens/alice.eth?mode=declared&records=text:com.twitter,addr:60,avatar")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("declared resolution request with narrowed records failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    assert_eq!(
        payload
            .declared_state
            .as_ref()
            .and_then(|state| state.get("record_cache")),
        Some(&json!({
            "record_version_boundary": record_inventory_boundary(logical_name_id, resource_id),
            "entries": [
                {
                    "record_key": "text:com.twitter",
                    "record_family": "text",
                    "selector_key": "com.twitter",
                    "status": "not_found",
                },
                {
                    "record_key": "addr:60",
                    "record_family": "addr",
                    "selector_key": "60",
                    "status": "success",
                    "value": {
                        "coin_type": "60",
                        "value": "0x0000000000000000000000000000000000000abc",
                    }
                },
                {
                    "record_key": "avatar",
                    "record_family": "avatar",
                    "selector_key": null,
                    "status": "unsupported",
                    "unsupported_reason": "resolver_family_pending",
                }
            ]
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_declared_records_return_not_found_cache_entry_for_explicit_gap()
-> Result<()> {
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
                .uri("/v1/resolutions/ens/alice.eth?mode=declared&records=contenthash")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("declared resolution request with explicit-gap selector failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    let declared_state = payload
        .declared_state
        .as_ref()
        .expect("declared_state must be present");
    assert_eq!(
        declared_state
            .get("record_inventory")
            .and_then(|value| value.get("explicit_gaps")),
        Some(&json!([
            {
                "record_key": "contenthash",
                "record_family": "contenthash",
                "selector_key": null,
                "gap_reason": "not_observed_on_current_resolver",
            }
        ]))
    );
    assert_eq!(
        declared_state.get("record_cache"),
        Some(&json!({
            "record_version_boundary": record_inventory_boundary(logical_name_id, resource_id),
            "entries": [
                {
                    "record_key": "contenthash",
                    "record_family": "contenthash",
                    "selector_key": null,
                    "status": "not_found",
                }
            ]
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_declared_records_synthesize_unsupported_family_entries() -> Result<()> {
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
                .uri("/v1/resolutions/ens/alice.eth?mode=declared&records=abi:json,addr:60,pubkey,text")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("declared resolution request with unsupported-family selectors failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    assert_eq!(
        payload
            .declared_state
            .as_ref()
            .and_then(|state| state.get("record_inventory"))
            .and_then(|inventory| inventory.get("unsupported_families")),
        Some(&json!([]))
    );
    assert_eq!(
        payload
            .declared_state
            .as_ref()
            .and_then(|state| state.get("record_cache")),
        Some(&json!({
            "record_version_boundary": worker_boundary,
            "entries": [
                {
                    "record_key": "abi:json",
                    "record_family": "abi",
                    "selector_key": "json",
                    "status": "unsupported",
                    "unsupported_reason": "record_family_not_supported_in_phase6_projection",
                },
                {
                    "record_key": "addr:60",
                    "record_family": "addr",
                    "selector_key": "60",
                    "status": "unsupported",
                    "unsupported_reason": "value_not_retained_in_normalized_events",
                },
                {
                    "record_key": "pubkey",
                    "record_family": "pubkey",
                    "selector_key": null,
                    "status": "unsupported",
                    "unsupported_reason": "record_family_not_supported_in_phase6_projection",
                },
                {
                    "record_key": "text",
                    "record_family": "text",
                    "selector_key": null,
                    "status": "unsupported",
                    "unsupported_reason": "value_not_retained_in_normalized_events",
                }
            ]
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_returns_unsupported_topology_for_non_direct_bindings() -> Result<()> {
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
    row.binding_kind = Some(bigname_storage::SurfaceBindingKind::ResolverAliasPath);
    database.insert_name_current_row(row).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    assert_eq!(payload.verified_state, None);
    assert_eq!(
        payload
            .declared_state
            .as_ref()
            .and_then(|state| state.get("topology")),
        Some(&json!({
            "status": "unsupported",
            "unsupported_reason": "declared resolution topology is not yet projected",
        }))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_supported_topology_uses_terminal_null_hop_when_no_resolver_is_declared()
-> Result<()> {
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
            "chain_id": null,
            "address": null,
            "latest_event_kind": null
        }
    });
    database.insert_name_current_row(row).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: ResolutionResponse = read_json(response).await?;
    let topology = payload
        .declared_state
        .as_ref()
        .and_then(|state| state.get("topology"))
        .and_then(Value::as_object)
        .expect("topology must be supported");
    let resolver_path = topology
        .get("resolver_path")
        .and_then(Value::as_array)
        .expect("resolver_path must be an array");
    assert_eq!(resolver_path.len(), 1);
    assert_eq!(
        resolver_path.first(),
        Some(&json!({
            "logical_name_id": "ens:alice.eth",
            "namespace": "ens",
            "normalized_name": "alice.eth",
            "canonical_display_name": "Alice.eth",
            "resource_id": resource_id.to_string(),
            "chain_id": null,
            "address": null,
            "latest_event_kind": null,
        }))
    );
    assert_eq!(
        topology
            .get("version_boundaries")
            .and_then(Value::as_object)
            .and_then(|value| value.get("topology_version_boundary")),
        topology
            .get("version_boundaries")
            .and_then(Value::as_object)
            .and_then(|value| value.get("record_version_boundary"))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_resolution_reuses_exact_name_envelope_fields() -> Result<()> {
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

    let resolution_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/resolutions/ens/alice.eth?mode=both&records=text,addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("resolution request failed")?;
    let name_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/names/ens/alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("name request failed")?;

    assert_eq!(resolution_response.status(), StatusCode::OK);
    assert_eq!(name_response.status(), StatusCode::OK);

    let resolution_payload: ResolutionResponse = read_json(resolution_response).await?;
    let name_payload: NameResponse = read_json(name_response).await?;

    assert_eq!(resolution_payload.data, name_payload.data);
    assert_eq!(resolution_payload.provenance, name_payload.provenance);
    assert_eq!(resolution_payload.coverage, name_payload.coverage);
    assert_eq!(
        resolution_payload.chain_positions,
        name_payload.chain_positions
    );
    assert_eq!(resolution_payload.consistency, name_payload.consistency);
    assert_eq!(resolution_payload.last_updated, name_payload.last_updated);
    assert_eq!(
        resolution_payload.verified_state,
        Some(json!({
            "verified_queries": [
                {
                    "record_key": "text",
                    "status": "unsupported",
                    "unsupported_reason": "verified resolution entrypoint is not yet supported",
                },
                {
                    "record_key": "addr:60",
                    "status": "unsupported",
                    "unsupported_reason": "verified resolution entrypoint is not yet supported",
                }
            ]
        }))
    );

    database.cleanup().await?;
    Ok(())
}
