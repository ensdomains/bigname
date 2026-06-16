#[tokio::test]
async fn v2_get_name_returns_flat_name_record_envelope() -> Result<()> {
    let payload = v2_name_record_payload("/v2/names/Alice.eth").await?;

    assert!(payload.get("page").is_none());
    assert_eq!(payload["meta"]["source"], json!("indexed"));
    assert_eq!(
        payload["meta"]["as_of"]["1"],
        json!({
            "block_number": 21_000_003,
            "block_hash": "0xbinding",
            "timestamp": "2026-04-17T00:00:03Z"
        })
    );

    let data = payload["data"].as_object().expect("data must be an object");
    assert_eq!(data.get("name"), Some(&json!("alice.eth")));
    assert_eq!(data.get("display_name"), Some(&json!("Alice.eth")));
    assert_eq!(data.get("namespace"), Some(&json!("ens")));
    assert_eq!(data.get("namehash"), Some(&json!("namehash:alice.eth")));
    assert_eq!(data.get("registration_status"), Some(&json!("active")));
    assert_eq!(data.get("status"), Some(&json!("ok")));
    assert_eq!(data.get("chain_id"), Some(&json!(1)));
    assert_eq!(data.get("network"), Some(&json!("ethereum")));
    assert_eq!(
        data.get("resolver"),
        Some(&json!({
            "chain_id": 1,
            "address": "0x0000000000000000000000000000000000000abc"
        }))
    );
    assert_eq!(
        data.get("registration_id"),
        Some(&json!(Uuid::from_u128(0x2200).to_string()))
    );
    assert_eq!(data.get("token_id"), Some(&Value::Null));
    assert_eq!(
        data.get("owner"),
        Some(&json!("0x00000000000000000000000000000000000000bb"))
    );
    assert_eq!(data.get("manager"), Some(&Value::Null));
    assert_eq!(
        data.get("registrant"),
        Some(&json!("0x00000000000000000000000000000000000000aa"))
    );
    assert_eq!(data.get("registered_at"), Some(&json!("2024-01-02T03:04:05Z")));
    assert_eq!(data.get("created_at"), Some(&json!("2023-01-02T03:04:05Z")));
    assert_eq!(data.get("expires_at"), Some(&json!("2027-01-02T03:04:05Z")));
    assert_eq!(
        data.get("addresses"),
        Some(&json!({
            "60": "0x0000000000000000000000000000000000000def"
        }))
    );
    assert_eq!(
        data.get("text_records"),
        Some(&json!({
            "avatar": "https://example.test/avatar.png",
            "description": "Alice profile"
        }))
    );
    assert_eq!(data.get("content_hash"), Some(&json!("ipfs://alice")));
    assert_eq!(data.get("primary_name"), Some(&json!("alice.eth")));
    assert_eq!(
        data.get("primary_address"),
        Some(&json!("0x0000000000000000000000000000000000000def"))
    );
    assert!(data.get("unsupported_fields").is_none());

    Ok(())
}

#[tokio::test]
async fn v2_get_name_response_omits_banned_v1_spellings() -> Result<()> {
    let payload = v2_name_record_payload("/v2/names/Alice.eth").await?;
    assert_no_banned_v1_spellings(&payload);
    Ok(())
}

#[tokio::test]
async fn v2_get_name_verified_source_uses_in_record_failed_status() -> Result<()> {
    let payload = v2_name_record_payload("/v2/names/Alice.eth?source=verified").await?;

    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(payload["data"]["status"], json!("failed"));
    assert_eq!(
        payload["data"]["unsupported_fields"],
        json!(["addresses", "content_hash", "primary_address", "text_records"])
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_classifies_ens_v2_registry_as_registered() -> Result<()> {
    let payload = v2_name_record_payload_with_row("/v2/names/Alice.eth", |row| {
        row.declared_summary["registration"] = json!({
            "status": "active",
            "authority_kind": "ens_v2_registry",
            "authority_key": "registry:ens-v2:alice",
            "released_at": null,
            "registrant": null,
            "latest_event_kind": "NameTransferred"
        });
    })
    .await?;

    assert_eq!(payload["data"]["registration_status"], json!("registered"));

    Ok(())
}

#[tokio::test]
async fn v2_get_name_classifies_released_as_released() -> Result<()> {
    let payload = v2_name_record_payload_with_row("/v2/names/Alice.eth", |row| {
        row.declared_summary["registration"] = json!({
            "status": "released",
            "authority_kind": "registrar",
            "authority_key": "registrar:ethereum-mainnet:alice",
            "released_at": "2026-06-14T00:00:00Z",
            "registrant": "0x00000000000000000000000000000000000000aa",
            "expiry": "2026-03-01T00:00:00Z",
            "latest_event_kind": "RegistrationReleased"
        });
    })
    .await?;

    assert_eq!(payload["data"]["registration_status"], json!("released"));

    Ok(())
}

#[tokio::test]
async fn v2_get_name_classifies_no_binding_as_unregistered() -> Result<()> {
    let payload = v2_name_record_payload_with_row("/v2/names/Alice.eth", |row| {
        row.surface_binding_id = None;
        row.resource_id = None;
        row.token_lineage_id = None;
        row.binding_kind = None;
    })
    .await?;

    assert_eq!(payload["data"]["registration_status"], json!("unregistered"));
    assert_eq!(payload["data"]["registration_id"], Value::Null);

    Ok(())
}

#[tokio::test]
async fn v2_get_name_rejects_source_auto() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v2/names/alice.eth?source=auto")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 source=auto request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));
    assert_eq!(
        payload["error"]["message"],
        json!("source must be one of: indexed, verified")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_reads_basenames_record_with_base_network() -> Result<()> {
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
                .uri("/v2/names/alice.base.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 basenames name record request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["data"]["namespace"], json!("basenames"));
    assert_eq!(payload["data"]["network"], json!("base"));
    assert_eq!(payload["data"]["chain_id"], json!(8453));
    assert_eq!(payload["data"]["registration_status"], json!("active"));
    assert_eq!(payload["data"]["resolver"]["chain_id"], json!(8453));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_returns_indexed_values() -> Result<()> {
    let payload = v2_name_records_payload("/v2/names/Alice.eth/records").await?;

    assert!(payload["data"].get("records").is_none());
    assert_eq!(payload["meta"]["source"], json!("indexed"));
    assert_eq!(
        payload["data"]["resolver"],
        json!({
            "chain_id": 1,
            "address": "0x0000000000000000000000000000000000000abc"
        })
    );
    assert_eq!(
        payload["data"]["addresses"],
        json!({
            "60": "0x0000000000000000000000000000000000000def"
        })
    );
    assert_eq!(
        payload["data"]["text_records"],
        json!({
            "avatar": "https://example.test/avatar.png",
            "description": "Alice profile"
        })
    );
    assert_eq!(payload["data"]["content_hash"], json!("ipfs://alice"));

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_keys_filter_values_and_per_key_answers() -> Result<()> {
    let payload =
        v2_name_records_payload("/v2/names/Alice.eth/records?keys=addr:60,text:description")
            .await?;

    assert_eq!(
        payload["data"]["addresses"],
        json!({
            "60": "0x0000000000000000000000000000000000000def"
        })
    );
    assert_eq!(
        payload["data"]["text_records"],
        json!({
            "description": "Alice profile"
        })
    );
    assert_eq!(payload["data"]["content_hash"], Value::Null);
    assert_eq!(
        payload["data"]["records"],
        json!({
            "addr:60": {
                "status": "ok",
                "value": "0x0000000000000000000000000000000000000def"
            },
            "text:description": {
                "status": "ok",
                "value": "Alice profile"
            }
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_rejects_too_many_keys() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let keys = (0..=200)
        .map(|index| format!("text:key{index}"))
        .collect::<Vec<_>>()
        .join(",");
    let uri = format!("/v2/names/alice.eth/records?keys={keys}");

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 oversized records keys request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));
    assert_eq!(
        payload["error"]["message"],
        json!("keys must contain at most 200 record keys")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_reports_unset_and_unsupported_per_key() -> Result<()> {
    let payload = v2_name_records_payload_with_setup(
        "/v2/names/Alice.eth/records?keys=contenthash,text:email",
        |_, _, inventory| {
            inventory.selectors = json!([
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
                }
            ]);
            inventory.entries = json!([
                {
                    "record_key": "addr:60",
                    "record_family": "addr",
                    "selector_key": "60",
                    "status": "success",
                    "value": {
                        "coin_type": "60",
                        "value": "0x0000000000000000000000000000000000000def"
                    }
                },
                {
                    "record_key": "avatar",
                    "record_family": "avatar",
                    "selector_key": null,
                    "status": "success",
                    "value": {
                        "value": "https://example.test/avatar.png"
                    }
                }
            ]);
            inventory.explicit_gaps = json!([
                {
                    "record_key": "contenthash",
                    "record_family": "contenthash",
                    "selector_key": null,
                    "gap_reason": "not_observed_on_current_resolver"
                }
            ]);
            inventory.unsupported_families = json!([
                {
                    "record_family": "text",
                    "unsupported_reason": "resolver_family_pending"
                }
            ]);
        },
        None,
    )
    .await?;

    assert_eq!(payload["data"]["content_hash"], Value::Null);
    assert_eq!(
        payload["data"]["records"],
        json!({
            "contenthash": {
                "status": "not_found",
                "failure_reason": "not_observed_on_current_resolver"
            },
            "text:email": {
                "status": "unsupported",
                "unsupported_reason": "resolver_family_pending"
            }
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_include_inventory_uses_product_key_lists() -> Result<()> {
    let payload = v2_name_records_payload_with_setup(
        "/v2/names/Alice.eth/records?keys=contenthash,text:email&include=inventory",
        |_, _, inventory| {
            inventory.selectors = json!([
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
                }
            ]);
            inventory.entries = json!([
                {
                    "record_key": "addr:60",
                    "record_family": "addr",
                    "selector_key": "60",
                    "status": "success",
                    "value": {
                        "coin_type": "60",
                        "value": "0x0000000000000000000000000000000000000def"
                    }
                },
                {
                    "record_key": "avatar",
                    "record_family": "avatar",
                    "selector_key": null,
                    "status": "success",
                    "value": {
                        "value": "https://example.test/avatar.png"
                    }
                }
            ]);
            inventory.explicit_gaps = json!([
                {
                    "record_key": "contenthash",
                    "record_family": "contenthash",
                    "selector_key": null,
                    "gap_reason": "not_observed_on_current_resolver"
                }
            ]);
            inventory.unsupported_families = json!([
                {
                    "record_family": "text",
                    "unsupported_reason": "resolver_family_pending"
                }
            ]);
        },
        None,
    )
    .await?;

    assert_eq!(
        payload["data"]["inventory"],
        json!({
            "known_keys": ["addr:60", "avatar"],
            "unset_keys": ["contenthash"],
            "unsupported_keys": ["text:email"]
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_inventory_partitions_unsupported_entries() -> Result<()> {
    let payload = v2_name_records_payload_with_setup(
        "/v2/names/Alice.eth/records?keys=addr:60,avatar&include=inventory",
        |_, _, inventory| {
            inventory.selectors = json!([
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
                }
            ]);
            inventory.entries = json!([
                {
                    "record_key": "addr:60",
                    "record_family": "addr",
                    "selector_key": "60",
                    "status": "success",
                    "value": {
                        "coin_type": "60",
                        "value": "0x0000000000000000000000000000000000000def"
                    }
                },
                {
                    "record_key": "avatar",
                    "record_family": "avatar",
                    "selector_key": null,
                    "status": "unsupported",
                    "unsupported_reason": "resolver_family_pending"
                }
            ]);
            inventory.explicit_gaps = json!([]);
            inventory.unsupported_families = json!([]);
        },
        None,
    )
    .await?;

    assert_eq!(
        payload["data"]["inventory"],
        json!({
            "known_keys": ["addr:60"],
            "unset_keys": [],
            "unsupported_keys": ["avatar"]
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_inventory_absence_is_unknown_not_unsupported() -> Result<()> {
    let payload =
        v2_name_records_payload_without_inventory("/v2/names/Alice.eth/records?keys=addr:60&include=inventory")
            .await?;

    assert_eq!(
        payload["data"]["inventory"],
        json!({
            "known_keys": [],
            "unset_keys": [],
            "unsupported_keys": []
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_source_verified_reads_persisted_verified_values() -> Result<()> {
    let verified_queries = json!([
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x0000000000000000000000000000000000000fed"
            },
            "provenance": {
                "execution_trace_id": Uuid::from_u128(0x0e7ec7ace00000000000000000000072).to_string()
            }
        }
    ]);
    let payload = v2_name_records_payload_with_setup(
        "/v2/names/Alice.eth/records?source=verified&keys=addr:60",
        |_, _, _| {},
        Some((&["addr:60"], verified_queries)),
    )
    .await?;

    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(
        payload["data"]["addresses"],
        json!({
            "60": "0x0000000000000000000000000000000000000fed"
        })
    );
    assert_eq!(
        payload["data"]["records"]["addr:60"],
        json!({
            "status": "ok",
            "value": "0x0000000000000000000000000000000000000fed"
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_source_verified_executes_on_demand_for_cache_miss() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0x2200);
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = Uuid::from_u128(0x3300);
    let execution_block_hash =
        "0x1111111111111111111111111111111111111111111111111111111111111111";

    database
        .seed_name_current_binding_migrated(
            logical_name_id,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row({
            let mut row = exact_name_row(
                logical_name_id,
                surface_binding_id,
                resource_id,
                token_lineage_id,
            );
            row.chain_positions = json!({
                "ethereum": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_000_003,
                    "block_hash": execution_block_hash,
                    "timestamp": "2026-04-17T00:00:03Z"
                }
            });
            row
        })
        .await?;
    database
        .insert_record_inventory_current_row({
            let mut inventory = record_inventory_current_row(logical_name_id, resource_id);
            inventory.record_version_boundary["chain_position"]["block_hash"] =
                json!(execution_block_hash);
            inventory.chain_positions = json!({
                "ethereum-mainnet": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_000_003,
                    "block_hash": execution_block_hash,
                    "timestamp": "2026-04-17T00:00:03Z"
                }
            });
            inventory
        })
        .await?;
    let executed_address = "0x0000000000000000000000000000000000000e0e";
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        resolution_universal_resolver_addr60_response(executed_address),
    ])
    .await?;
    let chain_rpc_urls =
        bigname_execution::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;

    let response = app_router(database.app_state_with_chain_rpc_urls(chain_rpc_urls))
        .oneshot(
            Request::builder()
                .uri("/v2/names/Alice.eth/records?source=verified&keys=addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 on-demand verified name records request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(
        payload["data"]["addresses"],
        json!({
            "60": executed_address
        })
    );
    assert_eq!(
        payload["data"]["records"]["addr:60"],
        json!({
            "status": "ok",
            "value": executed_address
        })
    );

    let rpc_requests = join_primary_name_mock_rpc_requests(rpc_handle).await?;
    assert_eq!(rpc_requests.len(), 1);
    assert_eq!(rpc_requests[0]["method"], json!("eth_call"));
    assert_eq!(
        rpc_requests[0]["params"][0]["to"],
        json!(bigname_execution::ENS_UNIVERSAL_RESOLVER_ADDRESS)
    );
    assert_eq!(
        rpc_requests[0]["params"][1],
        json!({
            "blockHash": execution_block_hash,
            "requireCanonical": true
        })
    );

    let cached_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v2/names/Alice.eth/records?source=verified&keys=addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 cached verified name records request failed")?;
    assert_eq!(cached_response.status(), StatusCode::OK);
    let cached_payload: Value = read_json(cached_response).await?;
    assert_eq!(
        cached_payload["data"]["records"]["addr:60"],
        json!({
            "status": "ok",
            "value": executed_address
        })
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_source_verified_cache_miss_reports_stale_when_execution_cannot_run(
) -> Result<()> {
    let payload = v2_name_records_payload_with_setup(
        "/v2/names/Alice.eth/records?source=verified&keys=addr:60",
        |_, _, _| {},
        None,
    )
    .await?;

    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(
        payload["data"]["records"]["addr:60"],
        json!({
            "status": "stale",
            "failure_reason": "verified resolution RPC provider for ethereum-mainnet is not configured; set BIGNAME_API_CHAIN_RPC_URLS=ethereum-mainnet=<url>"
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_source_verified_reports_stale_for_non_ens_on_demand_miss(
) -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "basenames:alice.base.eth";
    let resource_id = Uuid::from_u128(0x9220);
    let token_lineage_id = Uuid::from_u128(0x9221);
    let surface_binding_id = Uuid::from_u128(0x9222);

    database
        .seed_name_current_binding(
            logical_name_id,
            "basenames",
            "alice.base.eth",
            "Alice.base.eth",
            "namehash:alice.base.eth",
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )
        .await?;
    database
        .insert_name_current_row({
            let mut row = exact_name_row(
                logical_name_id,
                surface_binding_id,
                resource_id,
                token_lineage_id,
            );
            row.namespace = "basenames".to_owned();
            row.canonical_display_name = "Alice.base.eth".to_owned();
            row.normalized_name = "alice.base.eth".to_owned();
            row.namehash = "namehash:alice.base.eth".to_owned();
            row.declared_summary = json!({
                "registration": {
                    "status": "active",
                    "authority_kind": "registrar"
                },
                "resolver": {
                    "chain_id": "base-mainnet",
                    "address": "0x0000000000000000000000000000000000000abc",
                    "latest_event_kind": "ResolverChanged"
                }
            });
            row.provenance = json!({
                "manifest_versions": [
                    {
                        "manifest_version": 2,
                        "source_family": "basenames_execution",
                        "chain": "ethereum-mainnet",
                        "deployment_epoch": "basenames_v1"
                    }
                ]
            });
            row.chain_positions = json!({
                "base": {
                    "chain_id": "base-mainnet",
                    "block_number": 21_000_003,
                    "block_hash": "0xbase-binding",
                    "timestamp": "2026-04-17T00:00:03Z"
                },
                "ethereum": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_000_003,
                    "block_hash": "0xbinding",
                    "timestamp": "2026-04-17T00:00:03Z"
                }
            });
            row.canonicality_summary = json!({
                "status": "finalized",
                "chains": {
                    "base-mainnet": "finalized",
                    "ethereum-mainnet": "finalized"
                }
            });
            row
        })
        .await?;
    database
        .insert_record_inventory_current_row({
            let mut inventory =
                basenames_l2resolver_record_inventory_current_row(logical_name_id, resource_id);
            inventory.chain_positions = json!({
                "base": {
                    "chain_id": "base-mainnet",
                    "block_number": 21_000_003,
                    "block_hash": "0xbase-binding",
                    "timestamp": "2026-04-17T00:00:03Z"
                },
                "ethereum": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_000_003,
                    "block_hash": "0xbinding",
                    "timestamp": "2026-04-17T00:00:03Z"
                }
            });
            inventory.canonicality_summary = json!({
                "status": "finalized",
                "chains": {
                    "base-mainnet": "finalized",
                    "ethereum-mainnet": "finalized"
                }
            });
            inventory
        })
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v2/names/alice.base.eth/records?source=verified&keys=addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 non-ENS on-demand verified name records request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(
        payload["data"]["records"]["addr:60"],
        json!({
            "status": "stale",
            "failure_reason": "persisted verified resolution output is not available for the selected snapshot"
        })
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_source_verified_reports_unsupported_without_verified_boundary(
) -> Result<()> {
    let payload = v2_name_records_payload_with_row_and_setup(
        "/v2/names/Alice.eth/records?source=verified&keys=avatar",
        |row| {
            row.binding_kind = Some(bigname_storage::SurfaceBindingKind::ObservedOnly);
        },
        |_, _, _| {},
        None,
    )
    .await?;

    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(
        payload["data"]["records"]["avatar"],
        json!({
            "status": "unsupported",
            "unsupported_reason": "verified_records_not_supported"
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_source_auto_falls_back_to_verified_when_indexed_does_not_satisfy(
) -> Result<()> {
    let verified_queries = json!([
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000ee"
            },
            "provenance": {
                "execution_trace_id": Uuid::from_u128(0x0e7ec7ace00000000000000000000073).to_string()
            }
        }
    ]);
    let payload = v2_name_records_payload_with_setup(
        "/v2/names/Alice.eth/records?source=auto&keys=addr:60",
        |logical_name_id, resource_id, inventory| {
            *inventory = dynamic_resolver_unsupported_profile_record_inventory_current_row(
                logical_name_id,
                resource_id,
            );
        },
        Some((&["addr:60"], verified_queries)),
    )
    .await?;

    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(
        payload["data"]["addresses"],
        json!({
            "60": "0x00000000000000000000000000000000000000ee"
        })
    );
    assert_eq!(
        payload["data"]["records"]["addr:60"],
        json!({
            "status": "ok",
            "value": "0x00000000000000000000000000000000000000ee"
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_source_auto_blends_indexed_and_verified_per_key() -> Result<()> {
    let payload = v2_name_records_payload_with_setup(
        "/v2/names/Alice.eth/records?source=auto&keys=addr:60,text:email",
        |_, _, inventory| {
            inventory.unsupported_families = json!([
                {
                    "record_family": "text",
                    "unsupported_reason": "resolver_family_pending"
                }
            ]);
        },
        None,
    )
    .await?;

    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(
        payload["data"]["addresses"],
        json!({
            "60": "0x0000000000000000000000000000000000000def"
        })
    );
    assert_eq!(
        payload["data"]["records"],
        json!({
            "addr:60": {
                "status": "ok",
                "value": "0x0000000000000000000000000000000000000def"
            },
            "text:email": {
                "status": "stale",
                "failure_reason": "verified resolution RPC provider for ethereum-mainnet is not configured; set BIGNAME_API_CHAIN_RPC_URLS=ethereum-mainnet=<url>"
            }
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_missing_name_returns_not_found() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_alice_name_records_fixture(&database, |_, _, _| {}, None).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v2/names/missing.eth/records")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 missing name records request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("not_found"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_response_omits_banned_v1_spellings() -> Result<()> {
    let payload =
        v2_name_records_payload("/v2/names/Alice.eth/records?keys=addr:60&include=inventory")
            .await?;
    assert_no_banned_v1_spellings(&payload);
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_uses_envelope_shape() -> Result<()> {
    let payload = v2_name_records_payload("/v2/names/Alice.eth/records?keys=addr:60").await?;

    assert!(payload.get("page").is_none());
    assert!(payload["data"].is_object());
    assert_eq!(payload["meta"]["source"], json!("indexed"));
    assert_eq!(
        payload["meta"]["as_of"]["1"],
        json!({
            "block_number": 21_000_003,
            "block_hash": "0xbinding",
            "timestamp": "2026-04-17T00:00:03Z"
        })
    );

    Ok(())
}

async fn v2_name_record_payload(uri: &str) -> Result<Value> {
    v2_name_record_payload_with_row(uri, |_| {}).await
}

async fn v2_name_record_payload_with_row(
    uri: &str,
    configure_row: impl FnOnce(&mut bigname_storage::NameCurrentRow),
) -> Result<Value> {
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
    row.declared_summary["registration"] = json!({
        "status": "active",
        "authority_kind": "registrar",
        "authority_key": "registrar:ethereum-mainnet:alice",
        "released_at": null,
        "registrant": "0x00000000000000000000000000000000000000aa",
        "expiry": "2027-01-02T03:04:05Z",
        "registered_at": "2024-01-02T03:04:05Z",
        "created_at": "2023-01-02T03:04:05Z",
        "latest_event_kind": "NameRegistered"
    });
    row.declared_summary["control"] = json!({
        "status": "active",
        "expiry": "2027-01-02T03:04:05Z",
        "registry_owner": "0x00000000000000000000000000000000000000bb",
        "registrant": "0x00000000000000000000000000000000000000aa",
        "latest_event_kind": "NameTransferred"
    });
    row.declared_summary["primary_name"] = json!("alice.eth");
    configure_row(&mut row);
    database.insert_name_current_row(row).await?;

    let mut inventory = record_inventory_current_row(logical_name_id, resource_id);
    inventory.selectors = json!([
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
            "record_key": "contenthash",
            "record_family": "contenthash",
            "selector_key": null,
            "cacheable": true
        },
        {
            "record_key": "text:description",
            "record_family": "text",
            "selector_key": "description",
            "cacheable": true
        }
    ]);
    inventory.explicit_gaps = json!([]);
    inventory.unsupported_families = json!([]);
    inventory.entries = json!([
        {
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x0000000000000000000000000000000000000def"
            }
        },
        {
            "record_key": "avatar",
            "record_family": "avatar",
            "selector_key": null,
            "status": "success",
            "value": {
                "value": "https://example.test/avatar.png"
            }
        },
        {
            "record_key": "contenthash",
            "record_family": "contenthash",
            "selector_key": null,
            "status": "success",
            "value": {
                "value": "ipfs://alice"
            }
        },
        {
            "record_key": "text:description",
            "record_family": "text",
            "selector_key": "description",
            "status": "success",
            "value": {
                "key": "description",
                "value": "Alice profile"
            }
        }
    ]);
    database.insert_record_inventory_current_row(inventory).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 name record request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;

    database.cleanup().await?;
    Ok(payload)
}

async fn v2_name_records_payload(uri: &str) -> Result<Value> {
    v2_name_records_payload_with_setup(uri, |_, _, _| {}, None).await
}

async fn v2_name_records_payload_with_setup(
    uri: &str,
    configure_inventory: impl FnOnce(&str, Uuid, &mut bigname_storage::RecordInventoryCurrentRow),
    verified: Option<(&[&str], Value)>,
) -> Result<Value> {
    v2_name_records_payload_with_row_and_setup(uri, |_| {}, configure_inventory, verified).await
}

async fn v2_name_records_payload_with_row_and_setup(
    uri: &str,
    configure_row: impl FnOnce(&mut bigname_storage::NameCurrentRow),
    configure_inventory: impl FnOnce(&str, Uuid, &mut bigname_storage::RecordInventoryCurrentRow),
    verified: Option<(&[&str], Value)>,
) -> Result<Value> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_alice_name_records_fixture_with_row(
        &database,
        configure_row,
        configure_inventory,
        verified,
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 name records request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;

    database.cleanup().await?;
    Ok(payload)
}

async fn v2_name_records_payload_without_inventory(uri: &str) -> Result<Value> {
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
                .uri(uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 name records request without inventory failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;

    database.cleanup().await?;
    Ok(payload)
}

async fn seed_v2_alice_name_records_fixture(
    database: &TestDatabase,
    configure_inventory: impl FnOnce(&str, Uuid, &mut bigname_storage::RecordInventoryCurrentRow),
    verified: Option<(&[&str], Value)>,
) -> Result<()> {
    seed_v2_alice_name_records_fixture_with_row(database, |_| {}, configure_inventory, verified)
        .await
}

async fn seed_v2_alice_name_records_fixture_with_row(
    database: &TestDatabase,
    configure_row: impl FnOnce(&mut bigname_storage::NameCurrentRow),
    configure_inventory: impl FnOnce(&str, Uuid, &mut bigname_storage::RecordInventoryCurrentRow),
    verified: Option<(&[&str], Value)>,
) -> Result<()> {
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
        .insert_name_current_row({
            let mut row = exact_name_row(
                logical_name_id,
                surface_binding_id,
                resource_id,
                token_lineage_id,
            );
            configure_row(&mut row);
            row
        })
        .await?;

    let mut inventory = record_inventory_current_row(logical_name_id, resource_id);
    inventory.selectors = json!([
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
            "record_key": "contenthash",
            "record_family": "contenthash",
            "selector_key": null,
            "cacheable": true
        },
        {
            "record_key": "text:description",
            "record_family": "text",
            "selector_key": "description",
            "cacheable": true
        }
    ]);
    inventory.explicit_gaps = json!([]);
    inventory.unsupported_families = json!([]);
    inventory.entries = json!([
        {
            "record_key": "addr:60",
            "record_family": "addr",
            "selector_key": "60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x0000000000000000000000000000000000000def"
            }
        },
        {
            "record_key": "avatar",
            "record_family": "avatar",
            "selector_key": null,
            "status": "success",
            "value": {
                "value": "https://example.test/avatar.png"
            }
        },
        {
            "record_key": "contenthash",
            "record_family": "contenthash",
            "selector_key": null,
            "status": "success",
            "value": {
                "value": "ipfs://alice"
            }
        },
        {
            "record_key": "text:description",
            "record_family": "text",
            "selector_key": "description",
            "status": "success",
            "value": {
                "key": "description",
                "value": "Alice profile"
            }
        }
    ]);
    configure_inventory(logical_name_id, resource_id, &mut inventory);
    database.insert_record_inventory_current_row(inventory).await?;

    if let Some((record_keys, verified_queries)) = verified {
        let request_key = resolution_execution_request_key(record_keys);
        let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000070);
        let trace = resolution_execution_trace(
            execution_trace_id,
            &request_key,
            record_keys,
            verified_queries.clone(),
        );
        let outcome = resolution_execution_outcome(
            execution_trace_id,
            &request_key,
            verified_queries,
            logical_name_id,
            resource_id,
        );
        upsert_execution_trace(&database.pool, &trace).await?;
        upsert_execution_outcome(&database.pool, &outcome).await?;
    }

    Ok(())
}

fn resolution_universal_resolver_addr60_response(address: &str) -> Value {
    json!(format!(
        "0x{}{}{}{}",
        resolution_left_pad_hex("40", 64),
        resolution_padded_address_hex(bigname_execution::ENS_UNIVERSAL_RESOLVER_ADDRESS),
        resolution_left_pad_hex("20", 64),
        resolution_padded_address_hex(address),
    ))
}

fn resolution_padded_address_hex(address: &str) -> String {
    let stripped = address
        .strip_prefix("0x")
        .expect("test address must be 0x-prefixed");
    assert_eq!(stripped.len(), 40, "test address must be 20 bytes");
    resolution_left_pad_hex(stripped, 64)
}

fn resolution_left_pad_hex(value: &str, width: usize) -> String {
    assert!(value.len() <= width, "test hex value must fit padded width");
    format!("{value:0>width$}")
}

fn assert_no_banned_v1_spellings(value: &Value) {
    const BANNED: &[&str] = &[
        "as_of_timestamp",
        "canonical_display_name",
        "chain_positions",
        "coin_addresses",
        "coin_type_addresses",
        "consistency",
        "coverage",
        "declared_state",
        "expiration",
        "expiry",
        "expiry_date",
        "last_updated",
        "logical_name_id",
        "manager_address",
        "normalized_name",
        "owner_address",
        "provenance",
        "resolver_address",
        "resource_id",
        "surface_binding_id",
        "token_lineage_id",
        "verified_state",
    ];

    match value {
        Value::Object(object) => {
            for (key, value) in object {
                assert!(
                    !BANNED.contains(&key.as_str()),
                    "v2 response leaked banned v1 field {key}"
                );
                assert_no_banned_v1_spellings(value);
            }
        }
        Value::Array(values) => {
            for value in values {
                assert_no_banned_v1_spellings(value);
            }
        }
        _ => {}
    }
}
