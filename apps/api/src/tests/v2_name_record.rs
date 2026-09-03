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
    assert_eq!(data.get("display_name"), Some(&json!("alice.eth")));
    assert_eq!(data.get("namespace"), Some(&json!("ens")));
    assert_eq!(
        data.get("namehash"),
        Some(&json!(
            "0x787192fc5378cc32aa956ddfdedbf26b24e8d78e40109add0eea2c1a012c3dec"
        ))
    );
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
    assert_eq!(
        data.get("token_id"),
        Some(&json!(
            "70564938991660933374592024341600875602376452319261984317470407481576058979585"
        ))
    );
    assert_eq!(
        data.get("owner"),
        Some(&json!("0x00000000000000000000000000000000000000bb"))
    );
    assert!(data.get("manager").is_none());
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
async fn storage_name_surface_reads_preserve_stored_ensip15_normalized_name_bytes() -> Result<()> {
    const NORMALIZED_NAME: &str = "ᏣᎳᎩ.eth";
    const INPUT_LOGICAL_NAME_ID: &str = "ens:ᏣᎳᎩ.eth";

    let database = TestDatabase::new_migrated().await?;
    seed_identity_name(
        &database,
        INPUT_LOGICAL_NAME_ID,
        NORMALIZED_NAME,
        NORMALIZED_NAME,
        "node:ᏣᎳᎩ.eth",
        Uuid::from_u128(0x349_6001),
        Uuid::from_u128(0x349_6002),
        Uuid::from_u128(0x349_6003),
        "0x0000000000000000000000000000000000000349",
        bigname_storage::AddressNameRelation::TokenHolder,
        349,
    )
    .await?;

    let logical_name_id: String = sqlx::query_scalar(
        "SELECT logical_name_id FROM bigname_phase.name_surfaces WHERE raw_name = $1",
    )
    .bind(NORMALIZED_NAME)
    .fetch_one(&database.pool)
    .await?;
    let one = bigname_storage::load_name_surface(&database.pool, &logical_name_id)
        .await?
        .expect("seeded surface");
    assert_eq!(one.normalized_name, NORMALIZED_NAME);

    let many = bigname_storage::load_name_surfaces_by_logical_name_ids(
        &database.pool,
        std::slice::from_ref(&logical_name_id),
    )
    .await?;
    assert_eq!(many[&logical_name_id].normalized_name, NORMALIZED_NAME);

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_subnames_preserves_stored_ensip15_normalized_name_bytes() -> Result<()> {
    const NORMALIZED_NAME: &str = "ᏣᎳᎩ.parent.eth";

    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_fixture(&database).await?;
    seed_v2_subnames_bound_child(
        &database,
        "ens:ᏣᎳᎩ.parent.eth",
        NORMALIZED_NAME,
        "node:ᏣᎳᎩ.parent.eth",
        85,
        Uuid::from_u128(0x349_2001),
        Uuid::from_u128(0x349_2002),
        Uuid::from_u128(0x349_2003),
        json!({
            "registration": {"status": "active", "authority_kind": "registrar"},
            "control": {
                "registry_owner": "0x0000000000000000000000000000000000034920"
            }
        }),
    )
    .await?;
    upsert_phase_children_current_rows(
        &database.pool,
        &[v2_subnames_declared_child_row(
            "ens:parent.eth",
            "ens:ᏣᎳᎩ.parent.eth",
            NORMALIZED_NAME,
            "node:ᏣᎳᎩ.parent.eth",
            906,
            85,
        )],
    )
    .await?;
    let stored_raw_name: String = sqlx::query_scalar(
        "SELECT raw_name FROM bigname_phase.name_current WHERE raw_name = $1",
    )
    .bind(NORMALIZED_NAME)
    .fetch_one(&database.pool)
    .await?;

    let payload = v2_subnames_payload_for_database(
        &database,
        "/v2/names/parent.eth/subnames?page_size=20",
    )
    .await?;
    let row = payload["data"]
        .as_array()
        .expect("subnames data must be an array")
        .iter()
        .find(|row| row["display_name"] == json!(NORMALIZED_NAME))
        .expect("Cherokee subname must be served");
    assert_eq!(row["name"], json!(stored_raw_name));

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_name_preserves_stored_ensip15_normalized_name_bytes() -> Result<()> {
    const NORMALIZED_NAME: &str = "ᏣᎳᎩ.eth";

    let database = TestDatabase::new_migrated().await?;
    seed_identity_name(
        &database,
        "ens:ᏣᎳᎩ.eth",
        NORMALIZED_NAME,
        NORMALIZED_NAME,
        "namehash:ᏣᎳᎩ.eth",
        Uuid::from_u128(0x349_4001),
        Uuid::from_u128(0x349_4002),
        Uuid::from_u128(0x349_4003),
        "0x0000000000000000000000000000000000034940",
        bigname_storage::AddressNameRelation::TokenHolder,
        43,
    )
    .await?;
    let stored_raw_name: String = sqlx::query_scalar(
        "SELECT raw_name FROM bigname_phase.name_current WHERE raw_name = $1",
    )
    .bind(NORMALIZED_NAME)
    .fetch_one(&database.pool)
    .await?;

    let payload = v2_name_record_payload_for_database(
        &database,
        "/v2/names/%E1%8F%A3%E1%8E%B3%E1%8E%A9.eth",
    )
    .await?;
    assert_eq!(payload["data"]["name"], json!(stored_raw_name));

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_name_exposes_authority_unsupported_shape() -> Result<()> {
    for reason in [
        "conflicting_current_ens_authority",
        "independent_ens_deployments_overlap",
    ] {
        let payload = v2_name_record_payload_with_row("/v2/names/Alice.eth", |row| {
            row.coverage = json!({
                "status": "unsupported",
                "exhaustiveness": "not_asserted",
                "unsupported_reason": reason
            });
        })
        .await?;
        let data = payload["data"].as_object().expect("data must be an object");
        assert_eq!(data.get("status"), Some(&json!("unsupported")));
        assert_eq!(data.get("unsupported_reason"), Some(&json!(reason)));
        assert_eq!(
            data.keys().cloned().collect::<Vec<_>>(),
            vec![
                "display_name",
                "name",
                "namehash",
                "namespace",
                "status",
                "unsupported_reason",
            ]
        );
    }
    Ok(())
}

// An unsupported projection row never serves `status=ok`, whatever its reason. The rule is keyed
// on the unsupported status with one named exception, so a reason this build has never seen fails
// closed instead of serving a registration the projection declined to support.
#[tokio::test]
async fn v2_get_name_downgrades_every_unsupported_reason() -> Result<()> {
    for (reason, expected) in [
        (
            "ensv2_exact_name_profile_shadow",
            "exact_name_profile_not_supported",
        ),
        (
            "a_reason_this_build_has_never_seen",
            "a_reason_this_build_has_never_seen",
        ),
    ] {
        let payload = v2_name_record_payload_with_row("/v2/names/Alice.eth", |row| {
            row.coverage = json!({
                "status": "unsupported",
                "exhaustiveness": "not_asserted",
                "unsupported_reason": reason
            });
        })
        .await?;
        let data = payload["data"].as_object().expect("data must be an object");
        assert_eq!(data.get("status"), Some(&json!("unsupported")), "{reason}");
        assert_eq!(data.get("unsupported_reason"), Some(&json!(expected)));
        assert_eq!(
            data.keys().cloned().collect::<Vec<_>>(),
            vec![
                "display_name",
                "name",
                "namehash",
                "namespace",
                "status",
                "unsupported_reason",
            ],
            "{reason} served fields beyond the identity-only object"
        );
    }
    Ok(())
}

#[tokio::test]
async fn v2_get_name_does_not_serve_a_resolver_without_projected_authority() -> Result<()> {
    let payload = v2_name_record_payload_with_row("/v2/names/Alice.eth", |row| {
        row.coverage = json!({
            "status": "unsupported",
            "exhaustiveness": "not_asserted",
            "unsupported_reason": "current_authority_not_projected"
        });
    })
    .await?;
    let data = payload["data"].as_object().expect("data must be an object");
    assert_eq!(data.get("status"), Some(&json!("ok")));
    assert!(data.get("resolver").is_none());
    Ok(())
}

#[tokio::test]
async fn v2_get_name_exposes_projected_wrapper_state_and_fuses() -> Result<()> {
    let payload = v2_name_record_payload_with_row("/v2/names/Alice.eth", |row| {
        row.declared_summary["wrapper_state"] = json!("locked");
        row.declared_summary["wrapper_fuses"] = json!({
            "fuses": 196_609,
            "cannot_unwrap": true,
            "cannot_burn_fuses": false,
            "cannot_transfer": false,
            "cannot_set_resolver": false,
            "cannot_set_ttl": false,
            "cannot_create_subdomain": false,
            "cannot_approve": false,
            "parent_cannot_control": true,
            "is_dot_eth": true,
            "can_extend_expiry": false
        });
    })
    .await?;

    assert_eq!(payload["data"]["wrapper_state"], json!("locked"));
    assert_eq!(payload["data"]["wrapper_fuses"]["fuses"], json!(196_609));
    assert_eq!(payload["data"]["wrapper_fuses"]["cannot_unwrap"], json!(true));
    assert_eq!(
        payload["data"]["wrapper_fuses"]["parent_cannot_control"],
        json!(true)
    );
    Ok(())
}

#[tokio::test]
async fn v2_get_name_rejects_unknown_wrapper_fuse_fields() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_alice_name_record_fixture(
        &database,
        |row| {
            row.declared_summary["wrapper_state"] = json!("locked");
            row.declared_summary["wrapper_fuses"] = json!({
                "fuses": 65_537,
                "cannot_unwrap": true,
                "cannot_burn_fuses": false,
                "cannot_transfer": false,
                "cannot_set_resolver": false,
                "cannot_set_ttl": false,
                "cannot_create_subdomain": false,
                "cannot_approve": false,
                "parent_cannot_control": true,
                "is_dot_eth": false,
                "can_extend_expiry": false,
                "unknown_future_fuse": true
            });
        },
        |_, _, _| {},
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v2/names/Alice.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{payload}");
    assert_eq!(payload["error"]["code"], json!("internal_error"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_response_omits_banned_v1_spellings() -> Result<()> {
    let payload = v2_name_record_payload("/v2/names/Alice.eth").await?;
    assert_no_banned_v1_spellings(&payload);
    Ok(())
}

#[tokio::test]
async fn v2_get_name_verified_source_basenames_keeps_stale_inventory_before_lookup(
) -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "basenames:alice.base.eth";
    let resource_id = Uuid::from_u128(0x9240);
    let token_lineage_id = Uuid::from_u128(0x9241);
    let surface_binding_id = Uuid::from_u128(0x9242);
    let chain_positions = json!({
        "base": {
            "chain_id": "base-mainnet",
            "block_number": 21_000_003,
            "block_hash": "0xbase-binding",
            "timestamp": "2026-04-17T00:00:03Z"
        },
        "ethereum": {
            "chain_id": "ethereum-mainnet",
            "block_number": 21_000_100,
            "block_hash": "0xbasenamesl1",
            "timestamp": "2026-04-17T00:00:03Z"
        }
    });

    database
        .seed_snapshot_selector_chain_positions(&chain_positions)
        .await?;
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
        "manifest_versions": [basenames_execution_manifest_version()]
    });
    row.chain_positions = chain_positions;
    row.canonicality_summary = json!({
        "status": "finalized",
        "chains": {
            "base-mainnet": "finalized",
            "ethereum-mainnet": "finalized"
        }
    });
    database.insert_name_current_row(row).await?;

    let mut inventory =
        basenames_l2resolver_record_inventory_current_row(logical_name_id, resource_id);
    inventory.record_version_boundary =
        basenames_dynamic_resolver_record_inventory_boundary(logical_name_id, resource_id, None, None);
    inventory.chain_positions = json!({
        "base": {
            "chain_id": "base-mainnet",
            "block_number": 21_000_004,
            "block_hash": "0xbase-stale",
            "timestamp": "2026-04-17T00:00:04Z"
        }
    });
    inventory.canonicality_summary = json!({
        "status": "finalized",
        "chains": {
            "base-mainnet": "finalized"
        }
    });
    database
        .insert_record_inventory_current_row(inventory)
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v2/names/alice.base.eth?source=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 stale basenames verified name profile request failed")?;
    let status = response.status();
    let payload: Value = read_json(response).await?;

    assert_eq!(status, StatusCode::CONFLICT, "{payload}");
    assert_eq!(payload["error"]["code"], json!("stale"));
    assert_eq!(
        payload["error"]["message"],
        json!("requested snapshot is not available for name")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_verified_source_reports_stale_when_lookup_state_is_unavailable(
) -> Result<()> {
    let payload = v2_name_record_payload("/v2/names/Alice.eth?source=verified").await?;

    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(payload["data"]["status"], json!("stale"));
    assert_eq!(
        payload["data"]["failure_reason"],
        json!("verified_answer_stale_for_snapshot")
    );
    assert_eq!(
        payload["data"]["unsupported_fields"],
        json!(["addresses", "content_hash", "primary_address", "text_records"])
    );
    assert!(payload["data"].get("addresses").is_none());
    assert!(payload["data"].get("text_records").is_none());
    assert!(payload["data"].get("content_hash").is_none());
    assert!(payload["data"].get("primary_address").is_none());
    assert_ne!(
        payload["data"].get("addresses"),
        Some(&json!({
            "60": "0x0000000000000000000000000000000000000def"
        }))
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_verified_source_reports_unsupported_without_verified_boundary() -> Result<()> {
    let payload = v2_name_record_payload_with_row("/v2/names/Alice.eth?source=verified", |row| {
        row.binding_kind = None;
        row.surface_binding_id = None;
        row.resource_id = None;
        row.token_lineage_id = None;
    })
    .await?;

    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(payload["data"]["status"], json!("unsupported"));
    assert_eq!(
        payload["data"]["unsupported_reason"],
        json!("verified_records_not_supported")
    );
    assert_eq!(
        payload["data"]["unsupported_fields"],
        json!(["addresses", "content_hash", "primary_address", "text_records"])
    );
    assert!(payload["data"].get("addresses").is_none());
    assert!(payload["data"].get("text_records").is_none());
    assert!(payload["data"].get("content_hash").is_none());
    assert!(payload["data"].get("primary_address").is_none());

    Ok(())
}

#[tokio::test]
async fn v2_get_name_verified_source_accepts_event_linked_ownerless_registry_serving() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let execution_block_hash =
        "0x1111111111111111111111111111111111111111111111111111111111111111";
    let lookup_pool = database.lookup_pool().await?;
    seed_schema_v2_ens_lookup_head(
        &lookup_pool,
        21_000_003,
        execution_block_hash,
        "2026-04-17T00:00:03Z",
    )
    .await?;
    let namehash = bigname_lookup::ens_namehash_hex("alice.eth")?;

    seed_v2_alice_name_record_fixture(
        &database,
        |row| {
            row.namehash = namehash;
            row.serving_resource_id = row.resource_id.take();
            row.surface_binding_id = None;
            row.token_lineage_id = None;
            row.binding_kind = None;
            row.declared_summary["registration"]["status"] = json!("unregistered");
            row.declared_summary["control"]["status"] = json!("unregistered");
            row.provenance["read_reachability"] = json!({
                "basis": "retained_registry_resolver_pointer"
            });
            row.chain_positions = json!({
                "ethereum": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_000_003,
                    "block_hash": execution_block_hash,
                    "timestamp": "2026-04-17T00:00:03Z"
                }
            });
        },
        |_, _, inventory| {
            inventory.selectors = json!([{
                "record_key": "addr:2147483648",
                "record_family": "addr",
                "selector_key": "2147483648",
                "cacheable": true
            }]);
            inventory.entries = json!([{
                "record_key": "addr:2147483648",
                "record_family": "addr",
                "selector_key": "2147483648",
                "status": "success",
                "value": {
                    "coin_type": "2147483648",
                    "value": "0x0000000000000000000000000000000000000def"
                }
            }]);
            inventory.provenance["read_rules"] = json!([{
                "kind": "ensip19_default_address",
                "source_record_key": "addr:2147483648"
            }]);
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
        },
    )
    .await?;
    sqlx::query(
        "UPDATE bigname_phase.resources SET token_lineage_id = NULL
         WHERE resource_id = $1",
    )
    .bind(Uuid::from_u128(0x2200))
    .execute(&database.pool)
    .await?;
    let logical_name_id = bigname_storage::logical_name_id_for_name("ens", "alice.eth");
    let projected = bigname_storage::load_name_current(&database.pool, &logical_name_id)
        .await
        .context("ownerless fixture must remain readable through name_current storage")?;
    assert!(projected.is_some());
    let executed_address = "0x0000000000000000000000000000000000000e0e";
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        resolution_universal_resolver_multicoin_response(executed_address),
        resolution_universal_resolver_addr60_response(executed_address),
    ])
    .await?;
    let chain_rpc_urls =
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(chain_rpc_urls)
        .await?;

    let response = app_router(state)
        .oneshot(
            Request::builder()
                .uri("/v2/names/Alice.eth?source=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("ownerless verified name profile request failed")?;

    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "unexpected response: {payload}");
    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(payload["data"]["status"], json!("ok"));
    assert_eq!(payload["data"]["addresses"]["60"], json!(executed_address));
    assert_eq!(join_primary_name_mock_rpc_requests(rpc_handle).await?.len(), 2);

    lookup_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_verified_source_executes_without_legacy_persistence_and_aborts_transport_failure(
) -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.initialize_lookup_schema().await?;
    let execution_block_hash =
        "0x1111111111111111111111111111111111111111111111111111111111111111";
    let lookup_pool = database.lookup_pool().await?;
    let namehash = seed_schema_v2_ens_record_lookup(
        &lookup_pool,
        21_000_003,
        execution_block_hash,
        "2026-04-17T00:00:03Z",
        "0x0000000000000000000000000000000000000def",
    )
    .await?;

    seed_v2_alice_name_record_fixture_migrated(
        &database,
        |row| {
            row.namehash = namehash;
            row.chain_positions = json!({
                "ethereum": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_000_003,
                    "block_hash": execution_block_hash,
                    "timestamp": "2026-04-17T00:00:03Z"
                }
            });
        },
        |_, _, inventory| {
            inventory.selectors = json!([
                {
                    "record_key": "addr:2147483648",
                    "record_family": "addr",
                    "selector_key": "2147483648",
                    "cacheable": true
                }
            ]);
            inventory.entries = json!([
                {
                    "record_key": "addr:2147483648",
                    "record_family": "addr",
                    "selector_key": "2147483648",
                    "status": "success",
                    "value": {
                        "coin_type": "2147483648",
                        "value": "0x0000000000000000000000000000000000000def"
                    }
                }
            ]);
            inventory.provenance["read_rules"] = json!([{
                "kind": "ensip19_default_address",
                "source_record_key": "addr:2147483648"
            }]);
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
        },
    )
    .await?;
    let executed_address = "0x0000000000000000000000000000000000000e0e";
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        resolution_universal_resolver_multicoin_response(executed_address),
        resolution_universal_resolver_addr60_response(executed_address),
        resolution_universal_resolver_multicoin_response(executed_address),
        resolution_universal_resolver_addr60_response(executed_address),
    ])
    .await?;
    let chain_rpc_urls =
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(chain_rpc_urls)
        .await?;

    let response = app_router(state.clone())
        .oneshot(
            Request::builder()
                .uri("/v2/names/Alice.eth?source=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 on-demand verified name profile request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_v2_name_snapshot_meta(&payload);
    assert_eq!(payload["data"]["status"], json!("ok"));
    assert_eq!(
        payload["data"]["addresses"],
        json!({
            "2147483648": executed_address,
            "60": executed_address
        })
    );
    assert_eq!(payload["data"]["primary_address"], json!(executed_address));
    assert_eq!(
        payload["data"]["unsupported_fields"],
        json!(["content_hash", "text_records"])
    );
    assert_ne!(
        payload["data"]["addresses"]["60"],
        json!("0x0000000000000000000000000000000000000def")
    );

    let repeated_response = app_router(state.clone())
        .oneshot(
            Request::builder()
                .uri("/v2/names/Alice.eth?source=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 repeated verified name profile request failed")?;
    assert_eq!(repeated_response.status(), StatusCode::OK);
    let repeated_payload: Value = read_json(repeated_response).await?;
    assert_eq!(repeated_payload["data"]["addresses"]["60"], json!(executed_address));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let unavailable_rpc_url = format!("http://{}", listener.local_addr()?);
    drop(listener);
    let mut transport_failure_state = state;
    transport_failure_state.lookup_chain_rpc_urls = bigname_lookup::ChainRpcUrls::from_entries(&[
        format!("ethereum-mainnet={unavailable_rpc_url}"),
    ])?;
    let transport_failure_response = app_router(transport_failure_state)
        .oneshot(
            Request::builder()
                .uri("/v2/names/Alice.eth?source=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 transport-failed verified name profile request failed")?;
    let transport_failure_status = transport_failure_response.status();
    let transport_failure_payload: Value = read_json(transport_failure_response).await?;
    assert_eq!(
        transport_failure_status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "unexpected response: {transport_failure_payload}"
    );
    assert_eq!(
        transport_failure_payload["error"]["code"],
        json!("internal_error")
    );

    let rpc_requests = join_primary_name_mock_rpc_requests(rpc_handle).await?;
    assert_eq!(rpc_requests.len(), 4, "v2 must not reuse a verified cache outcome");
    for request in &rpc_requests {
        assert_eq!(request["method"], json!("eth_call"));
        assert_eq!(
            request["params"][0]["to"],
            json!("0xeeeeeeee14d718c2b47d9923deab1335e144eeee")
        );
        assert_eq!(
            request["params"][1],
            json!({
                "blockHash": execution_block_hash,
                "requireCanonical": true
            })
        );
    }
    let ledger_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM resolution_divergences WHERE cleared_at IS NULL",
    )
    .fetch_one(&lookup_pool)
    .await?;
    assert_eq!(ledger_count, 2);

    lookup_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_verified_records_return_conflict_when_project_generation_changes_during_rpc()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.initialize_lookup_schema().await?;
    let execution_block_hash =
        "0x1111111111111111111111111111111111111111111111111111111111111111";
    let lookup_pool = database.lookup_pool().await?;
    let namehash = seed_schema_v2_ens_record_lookup(
        &lookup_pool,
        21_000_003,
        execution_block_hash,
        "2026-04-17T00:00:03Z",
        "0x0000000000000000000000000000000000000def",
    )
    .await?;
    seed_v2_alice_name_record_fixture_migrated(
        &database,
        |row| {
            row.namehash = namehash;
            row.chain_positions = json!({
                "ethereum": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_000_003,
                    "block_hash": execution_block_hash,
                    "timestamp": "2026-04-17T00:00:03Z"
                }
            });
        },
        |_, _, inventory| {
            inventory.selectors = json!([{
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "cacheable": true
            }]);
            inventory.entries = json!([{
                "record_key": "addr:60",
                "record_family": "addr",
                "selector_key": "60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x0000000000000000000000000000000000000def"
                }
            }]);
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
        },
    )
    .await?;

    let executed_address = "0x0000000000000000000000000000000000000e0e";
    let (rpc_url, request_reached, release_response, rpc_handle) =
        spawn_primary_name_mock_rpc_with_last_response_gate(vec![
            resolution_universal_resolver_addr60_response(executed_address),
        ])
        .await?;
    let chain_rpc_urls =
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(chain_rpc_urls)
        .await?;
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .uri("/v2/names/Alice.eth/records?source=verified&keys=addr:60")
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
    });

    request_reached
        .await
        .context("verified lookup did not reach its provider call")?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET input_content_hash = 'manifest-authority:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:api-concurrency-test'
         WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'",
    )
    .execute(&lookup_pool)
    .await?;
    release_response
        .send(())
        .map_err(|()| anyhow::anyhow!("verified lookup dropped its provider response gate"))?;

    let response = request_task
        .await
        .context("v2 concurrent verified records task panicked")?
        .context("v2 concurrent verified records request failed")?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected response: {payload}");
    assert_eq!(payload["error"]["code"], json!("stale"));
    assert!(payload.get("data").is_none());

    let rpc_requests = join_primary_name_mock_rpc_requests(rpc_handle).await?;
    assert_eq!(rpc_requests.len(), 1);
    lookup_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_null_exact_resolver_auto_and_verified_execute_universal_resolver() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.initialize_lookup_schema().await?;
    let execution_block_hash =
        "0x1111111111111111111111111111111111111111111111111111111111111111";
    let lookup_pool = database.lookup_pool().await?;
    let namehash = seed_schema_v2_ens_record_lookup(
        &lookup_pool,
        21_000_003,
        execution_block_hash,
        "2026-04-17T00:00:03Z",
        "0x0000000000000000000000000000000000000def",
    )
    .await?;
    let logical_name_id = format!("ens:{namehash}");
    seed_v2_alice_name_record_fixture_migrated(
        &database,
        |row| {
            row.namehash = namehash.clone();
            row.declared_summary["resolver"] = json!({"chain_id":null,"address":null});
            row.declared_summary["topology"]["resolver_path"][0]["address"] = Value::Null;
            row.chain_positions = json!({
                "ethereum": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_000_003,
                    "block_hash": execution_block_hash,
                    "timestamp": "2026-04-17T00:00:03Z"
                }
            });
        },
        |_, _, _| {},
    )
    .await?;
    sqlx::query(
        "UPDATE name_current
         SET declared_summary = jsonb_set(
             declared_summary #- '{topology}', '{resolver}',
             '{\"chain_id\":null,\"address\":null}'::jsonb
         )
         WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .execute(&lookup_pool)
    .await?;
    sqlx::query("DELETE FROM record_inventory_current")
        .execute(&lookup_pool)
        .await?;

    let executed_address = "0x0000000000000000000000000000000000000e0e";
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        resolution_universal_resolver_text_response("https://alice.example"),
        resolution_universal_resolver_text_response(""),
        resolution_universal_resolver_addr60_response(executed_address),
        resolution_resolver_not_found_error(b"\x05alice\x03eth\0"),
    ])
    .await?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(bigname_lookup::ChainRpcUrls::from_entries(&[
            format!("ethereum-mainnet={rpc_url}"),
        ])?)
        .await?;

    let auto_response = app_router(state.clone())
        .oneshot(
            Request::builder()
                .uri("/v2/names/Alice.eth/records?source=auto&keys=text:url,avatar")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 null-resolver auto records request failed")?;
    assert_eq!(auto_response.status(), StatusCode::OK);
    let auto_payload: Value = read_json(auto_response).await?;
    assert_eq!(auto_payload["meta"]["source"], json!("verified"));
    assert_eq!(auto_payload["data"]["resolver"], Value::Null);
    let mut mixed_statuses = [
        auto_payload["data"]["records"]["text:url"]["status"]
            .as_str()
            .expect("text:url status"),
        auto_payload["data"]["records"]["avatar"]["status"]
            .as_str()
            .expect("avatar status"),
    ];
    mixed_statuses.sort_unstable();
    assert_eq!(mixed_statuses, ["not_found", "ok"]);

    let verified_response = app_router(state.clone())
        .oneshot(
            Request::builder()
                .uri("/v2/names/Alice.eth/records?source=verified&keys=addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 null-resolver verified records request failed")?;
    assert_eq!(verified_response.status(), StatusCode::OK);
    let verified_payload: Value = read_json(verified_response).await?;
    assert_eq!(verified_payload["meta"]["source"], json!("verified"));
    assert_eq!(verified_payload["data"]["resolver"], Value::Null);
    assert_eq!(
        verified_payload["data"]["addresses"]["60"],
        json!(executed_address)
    );
    assert_eq!(
        verified_payload["data"]["records"]["addr:60"]["status"],
        json!("ok")
    );
    let missing_response = app_router(state.clone())
        .oneshot(
            Request::builder()
                .uri("/v2/names/Alice.eth/records?source=verified&keys=text:url")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 null-resolver missing-resolver request failed")?;
    assert_eq!(missing_response.status(), StatusCode::OK);
    let missing_payload: Value = read_json(missing_response).await?;
    assert_eq!(missing_payload["meta"]["source"], json!("verified"));
    assert_eq!(missing_payload["data"]["resolver"], Value::Null);
    assert_eq!(
        missing_payload["data"]["records"]["text:url"]["status"],
        json!("not_found")
    );
    assert_eq!(
        missing_payload["data"]["records"]["text:url"]["failure_reason"],
        json!("resolver_not_found")
    );
    let summary_response = app_router(state.clone())
        .oneshot(
            Request::builder()
                .uri("/v2/names/Alice.eth/records?source=auto")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 null-resolver summary request failed")?;
    assert_eq!(summary_response.status(), StatusCode::OK);
    let summary_payload: Value = read_json(summary_response).await?;
    assert_eq!(summary_payload["meta"]["source"], json!("indexed"));
    assert!(summary_payload["data"].get("records").is_none());

    sqlx::query(
        "UPDATE manifest_versions
         SET rollout_status = 'deprecated'
         WHERE namespace = 'ens' AND source_family = 'ens_execution'",
    )
    .execute(&lookup_pool)
    .await?;
    let no_entrypoint_response = app_router(state)
        .oneshot(
            Request::builder()
                .uri("/v2/names/Alice.eth/records?source=auto&keys=addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 null-resolver request without an admitted entrypoint failed")?;
    assert_eq!(no_entrypoint_response.status(), StatusCode::OK);
    let no_entrypoint_payload: Value = read_json(no_entrypoint_response).await?;
    assert_eq!(no_entrypoint_payload["meta"]["source"], json!("verified"));
    assert_eq!(
        no_entrypoint_payload["data"]["records"]["addr:60"]["status"],
        json!("unsupported")
    );
    assert_eq!(
        no_entrypoint_payload["data"]["records"]["addr:60"]["unsupported_reason"],
        json!("verified_records_not_supported")
    );
    assert_eq!(join_primary_name_mock_rpc_requests(rpc_handle).await?.len(), 4);
    let ledger_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM resolution_divergences")
            .fetch_one(&lookup_pool)
            .await?;
    assert_eq!(ledger_count, 0);

    lookup_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_null_resolver_auto_rejects_direct_route_selected_after_admission() -> Result<()> {
    assert_null_resolver_route_flip_is_stale("auto").await
}

#[tokio::test]
async fn v2_null_resolver_verified_rejects_direct_route_selected_after_admission() -> Result<()> {
    assert_null_resolver_route_flip_is_stale("verified").await
}

async fn assert_null_resolver_route_flip_is_stale(source: &str) -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.initialize_lookup_schema().await?;
    let execution_block_hash =
        "0x1111111111111111111111111111111111111111111111111111111111111111";
    let lookup_pool = database.lookup_pool().await?;
    let namehash = seed_schema_v2_ens_record_lookup(
        &lookup_pool,
        21_000_003,
        execution_block_hash,
        "2026-04-17T00:00:03Z",
        "0x0000000000000000000000000000000000000def",
    )
    .await?;
    let logical_name_id = format!("ens:{namehash}");
    seed_v2_alice_name_record_fixture_migrated(
        &database,
        |row| {
            row.namehash = namehash;
            row.declared_summary["resolver"] = json!({"chain_id":null,"address":null});
            row.declared_summary["topology"]["resolver_path"][0]["address"] = Value::Null;
            row.chain_positions = json!({
                "ethereum": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_000_003,
                    "block_hash": execution_block_hash,
                    "timestamp": "2026-04-17T00:00:03Z"
                }
            });
        },
        |_, _, _| {},
    )
    .await?;

    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        resolution_basenames_l1_addr60_response(
            "0x0000000000000000000000000000000000000e0e",
        ),
    ])
    .await?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(bigname_lookup::ChainRpcUrls::from_entries(&[
            format!("ethereum-mainnet={rpc_url}"),
        ])?)
        .await?;
    let (_guard, control) =
        crate::v2::name_records_auto_fallback_test_hooks::install(&database.pool).await?;
    let uri = format!("/v2/names/Alice.eth/records?source={source}&keys=addr:60");
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
    });

    control.wait_until_reached().await;
    let direct_resolver = "0x1000000000000000000000000000000000000001";
    sqlx::query(
        "UPDATE name_current
         SET declared_summary = jsonb_set(
             jsonb_set(declared_summary, '{resolver}', $2),
             '{topology,resolver_path,0,address}', $3
         )
         WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .bind(json!({"chain_id":"ethereum-mainnet","address":direct_resolver}))
    .bind(json!(direct_resolver))
    .execute(&lookup_pool)
    .await?;
    control.resume().await;

    let response = request_task
        .await
        .context("v2 discovery-route mismatch request task panicked")?
        .context("v2 discovery-route mismatch request failed")?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected response: {payload}");
    assert_eq!(payload["error"]["code"], json!("stale"));
    assert!(payload.get("data").is_none());
    assert_eq!(join_primary_name_mock_rpc_requests(rpc_handle).await?.len(), 1);

    lookup_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_default_source_matches_explicit_indexed() -> Result<()> {
    let default_payload = v2_name_record_payload("/v2/names/Alice.eth").await?;
    let indexed_payload = v2_name_record_payload("/v2/names/Alice.eth?source=indexed").await?;

    assert_eq!(default_payload, indexed_payload);

    Ok(())
}

#[tokio::test]
async fn v2_get_name_omits_record_maps_when_inventory_is_absent() -> Result<()> {
    let payload = v2_name_payload_without_inventory("/v2/names/Alice.eth").await?;

    assert_eq!(
        payload["data"]["unsupported_fields"],
        json!(["addresses", "content_hash", "primary_address", "text_records"])
    );
    assert!(payload["data"].get("addresses").is_none());
    assert!(payload["data"].get("text_records").is_none());
    assert!(payload["data"].get("content_hash").is_none());
    assert!(payload["data"].get("primary_address").is_none());

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
async fn v2_get_name_withholds_retained_inventory_for_released_tombstone() -> Result<()> {
    // The fixture's inventory row and declared resolver stay attached: a
    // released tombstone must not serve them even if projection state loss
    // retains them.
    let payload = v2_name_record_payload_with_row("/v2/names/Alice.eth", |row| {
        row.declared_summary["registration"]["status"] = json!("released");
        row.declared_summary["registration"]["released_at"] = json!("2026-06-14T00:00:00Z");
    })
    .await?;

    let data = payload["data"].as_object().expect("data must be an object");
    assert_eq!(data.get("status"), Some(&json!("ok")));
    assert_eq!(data.get("registration_status"), Some(&json!("released")));
    assert!(data.get("resolver").is_none());
    assert!(data.get("addresses").is_none());
    assert!(data.get("text_records").is_none());
    assert!(data.get("content_hash").is_none());
    assert!(data.get("primary_address").is_none());
    assert_eq!(
        data.get("unsupported_fields"),
        Some(&json!([
            "addresses",
            "content_hash",
            "primary_address",
            "text_records"
        ]))
    );
    Ok(())
}

#[tokio::test]
async fn v2_get_name_skips_stale_inventory_for_released_tombstone() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_alice_name_record_fixture(
        &database,
        |row| {
            row.declared_summary["registration"]["status"] = json!("released");
            row.declared_summary["registration"]["released_at"] =
                json!("2026-06-14T00:00:00Z");
        },
        |_, _, _| {},
    )
    .await?;
    sqlx::query(
        "UPDATE bigname_phase.record_inventory_current
         SET chain_positions = jsonb_build_object(
                 'block_number', 21000003,
                 'block_hash', '0xrejected-inventory-target',
                 'target_block_number', 21000003,
                 'target_block_hash', '0xrejected-inventory-target'
             ),
             canonicality_summary = jsonb_build_object(
                 'state', 'canonical_lineage',
                 'target_block_number', 21000003,
                 'target_block_hash', '0xrejected-inventory-target'
             )
         WHERE resource_id = $1",
    )
    .bind(Uuid::from_u128(0x2200))
    .execute(&database.pool)
    .await?;

    let payload = v2_name_record_payload_for_database(&database, "/v2/names/Alice.eth").await?;
    let data = payload["data"].as_object().expect("data must be an object");
    assert_eq!(data.get("registration_status"), Some(&json!("released")));
    assert!(data.get("resolver").is_none());
    assert!(data.get("addresses").is_none());
    assert!(data.get("text_records").is_none());
    assert!(data.get("content_hash").is_none());
    assert!(data.get("primary_address").is_none());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_withholds_expired_resource_identity_and_inventory_for_reservation() -> Result<()>
{
    // Model a reservation-selected row with inventory retained for the expired
    // resource. A reservation has no current registration.
    let payload = v2_name_record_payload_with_row("/v2/names/Alice.eth", |row| {
        row.declared_summary["registration"] = json!({
            "status": "reserved",
            "expiry": 4_000_000_000_u64,
            "latest_event_kind": "RegistrationReserved"
        });
        row.declared_summary["control"] = json!({"status": "reserved"});
    })
    .await?;

    let data = payload["data"].as_object().expect("data must be an object");
    assert_eq!(data.get("registration_status"), Some(&json!("unregistered")));
    assert!(data.get("registration_id").is_none());
    assert!(data.get("resolver").is_none());
    assert!(data.get("addresses").is_none());
    assert!(data.get("text_records").is_none());
    assert!(data.get("content_hash").is_none());
    assert!(data.get("primary_address").is_none());
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_withholds_retained_inventory_for_reservation() -> Result<()> {
    let payload = v2_name_records_payload_with_row_and_setup(
        "/v2/names/Alice.eth/records?include=inventory",
        |row| {
            row.declared_summary["registration"] = json!({
                "status": "reserved",
                "expiry": 4_000_000_000_u64,
                "latest_event_kind": "RegistrationReserved"
            });
            row.declared_summary["control"] = json!({"status": "reserved"});
        },
        |_, _, _| {},
    )
    .await?;

    let data = payload["data"].as_object().expect("data must be an object");
    assert_eq!(data.get("resolver"), Some(&Value::Null));
    assert_eq!(data.get("addresses"), Some(&json!({})));
    assert_eq!(data.get("text_records"), Some(&json!({})));
    assert_eq!(data.get("content_hash"), Some(&Value::Null));
    assert!(data.get("inventory").is_none());
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_verified_ignores_reservation_audit_selectors() -> Result<()> {
    let payload = v2_name_records_payload_with_row_and_setup(
        "/v2/names/Alice.eth/records?source=verified&include=inventory",
        |row| {
            row.declared_summary["registration"] = json!({
                "status": "reserved",
                "expiry": 4_000_000_000_u64,
                "latest_event_kind": "RegistrationReserved"
            });
            row.declared_summary["control"] = json!({"status": "reserved"});
        },
        |_, _, inventory| {
            inventory.selectors = Value::Array(
                (0..=200)
                    .map(|index| {
                        json!({
                            "record_key": format!("text:audit-{index}"),
                            "record_family": "text",
                            "selector_key": format!("audit-{index}"),
                            "cacheable": true
                        })
                    })
                    .collect(),
            );
        },
    )
    .await?;

    let data = payload["data"].as_object().expect("data must be an object");
    assert_eq!(data.get("resolver"), Some(&Value::Null));
    assert_eq!(data.get("addresses"), Some(&json!({})));
    assert_eq!(data.get("text_records"), Some(&json!({})));
    assert_eq!(data.get("content_hash"), Some(&Value::Null));
    assert!(data.get("records").is_none());
    assert!(data.get("inventory").is_none());
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_verified_keeps_empty_records_for_active_name() -> Result<()> {
    let payload = v2_name_records_payload_with_row_and_setup(
        "/v2/names/Alice.eth/records?source=verified",
        |_| {},
        |_, _, inventory| {
            inventory.selectors = json!([]);
            inventory.entries = json!([]);
            inventory.explicit_gaps = json!([]);
            inventory.unsupported_families = json!([]);
        },
    )
    .await?;

    assert_eq!(payload["data"]["records"], json!({}));
    Ok(())
}

#[tokio::test]
async fn v2_get_name_verified_source_withholds_retained_inventory_for_released_tombstone(
) -> Result<()> {
    // The fixture retains the inventory row, declared resolver, and a live
    // lookup topology for a released name: verified execution must not
    // dispatch a provider call against the former resolver, and the response
    // must match the canonical released path where projection dropped the
    // inventory row.
    let database = TestDatabase::new_migrated().await?;
    database.initialize_lookup_schema().await?;
    let execution_block_hash =
        "0x1111111111111111111111111111111111111111111111111111111111111111";
    let lookup_pool = database.lookup_pool().await?;
    let namehash = seed_schema_v2_ens_record_lookup(
        &lookup_pool,
        21_000_003,
        execution_block_hash,
        "2026-04-17T00:00:03Z",
        "0x0000000000000000000000000000000000000def",
    )
    .await?;

    seed_v2_alice_name_record_fixture_migrated(
        &database,
        |row| {
            row.namehash = namehash;
            row.chain_positions = json!({
                "ethereum": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_000_003,
                    "block_hash": execution_block_hash,
                    "timestamp": "2026-04-17T00:00:03Z"
                }
            });
            row.declared_summary["registration"]["status"] = json!("released");
            row.declared_summary["registration"]["released_at"] =
                json!("2026-06-14T00:00:00Z");
        },
        |_, _, inventory| {
            inventory.selectors = json!([
                {
                    "record_key": "addr:60",
                    "record_family": "addr",
                    "selector_key": "60",
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
                }
            ]);
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
        },
    )
    .await?;
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        resolution_universal_resolver_addr60_response(
            "0x0000000000000000000000000000000000000e0e",
        ),
    ])
    .await?;
    let chain_rpc_urls =
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(chain_rpc_urls)
        .await?;

    let response = app_router(state.clone())
        .oneshot(
            Request::builder()
                .uri("/v2/names/Alice.eth?source=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 verified released-tombstone name profile request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    let retained_payload: Value = read_json(response).await?;

    assert_eq!(retained_payload["meta"]["source"], json!("verified"));
    let data = retained_payload["data"]
        .as_object()
        .expect("data must be an object");
    assert_eq!(data.get("status"), Some(&json!("unsupported")));
    assert_eq!(
        data.get("unsupported_reason"),
        Some(&json!("verified_records_not_supported"))
    );
    assert_eq!(data.get("registration_status"), Some(&json!("released")));
    assert_eq!(
        data.get("unsupported_fields"),
        Some(&json!([
            "addresses",
            "content_hash",
            "primary_address",
            "text_records"
        ]))
    );
    assert!(data.get("resolver").is_none());
    assert!(data.get("addresses").is_none());
    assert!(data.get("text_records").is_none());
    assert!(data.get("content_hash").is_none());
    assert!(data.get("primary_address").is_none());

    // Canonical released path: projection dropped the inventory row. The
    // retained-state response must be byte-equivalent.
    sqlx::query("DELETE FROM bigname_phase.record_inventory_current WHERE resource_id = $1")
        .bind(Uuid::from_u128(0xc200_0000_0000_0000_0000_0000_0000_0101))
        .execute(&database.pool)
        .await?;
    let canonical_response = app_router(state)
        .oneshot(
            Request::builder()
                .uri("/v2/names/Alice.eth?source=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 verified canonical released name profile request failed")?;
    assert_eq!(canonical_response.status(), StatusCode::OK);
    let canonical_payload: Value = read_json(canonical_response).await?;
    assert_eq!(retained_payload, canonical_payload);

    // The mock queue still holds its one response: any dispatch would have
    // consumed it and finished the task with a recorded request.
    rpc_handle.abort();
    let dispatched = match rpc_handle.await {
        Err(join_error) if join_error.is_cancelled() => Vec::new(),
        other => other.context("mock primary-name RPC task failed")??,
    };
    assert!(
        dispatched.is_empty(),
        "released tombstone must not dispatch a verified lookup: {dispatched:?}"
    );

    lookup_pool.close().await;
    database.cleanup().await?;
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
async fn v2_get_name_infers_exact_base_eth_as_ens() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "ens:base.eth";
    let resource_id = Uuid::from_u128(0x9180);
    let token_lineage_id = Uuid::from_u128(0x9181);
    let surface_binding_id = Uuid::from_u128(0x9182);

    database
        .seed_name_current_binding(
            logical_name_id,
            "ens",
            "base.eth",
            "base.eth",
            "namehash:base.eth",
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
            row.normalized_name = "base.eth".to_owned();
            row.canonical_display_name = "base.eth".to_owned();
            row.namehash = "namehash:base.eth".to_owned();
            row
        })
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v2/names/base.eth")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 base.eth name record request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["data"]["name"], json!("base.eth"));
    assert_eq!(payload["data"]["namespace"], json!("ens"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_rejects_trailing_dot() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_alice_name_record_fixture(&database, |_| {}, |_, _, _| {}).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v2/names/alice.eth.")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 trailing-dot name record request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_uses_sepolia_positioned_at_token_on_mixed_phase_heads() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_mixed_phase_head_names(&database).await?;

    let at = v2_sepolia_snapshot_token();
    let payload = v2_name_record_payload_for_database(
        &database,
        &format!("/v2/names/{V2_SEPOLIA_SNAPSHOT_NAME}?at={at}"),
    )
    .await?;

    assert_eq!(
        payload["meta"]["as_of"]["11155111"],
        json!({
            "block_number": V2_SEPOLIA_SNAPSHOT_BLOCK,
            "block_hash": V2_SEPOLIA_SNAPSHOT_HASH,
            "timestamp": V2_SEPOLIA_SNAPSHOT_TIMESTAMP
        })
    );
    assert!(payload["meta"]["as_of"].get("1").is_none());
    assert_eq!(payload["data"]["network"], json!("ethereum-sepolia"));
    assert_eq!(payload["data"]["chain_id"], json!(11155111));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_at_tokens_round_trip_mainnet_and_sepolia_profiles() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_mixed_phase_head_names(&database).await?;

    let mainnet = v2_name_record_payload_for_database(
        &database,
        &format!("/v2/names/{V2_MAINNET_SNAPSHOT_NAME}"),
    )
    .await?;
    let mainnet_at =
        v2_at_token_from_meta_as_of(&mainnet, "1", "ethereum", "ethereum-mainnet")?;
    let mainnet_replay = v2_name_record_payload_for_database(
        &database,
        &format!("/v2/names/{V2_MAINNET_SNAPSHOT_NAME}?at={mainnet_at}"),
    )
    .await?;
    assert_eq!(mainnet_replay["meta"]["as_of"], mainnet["meta"]["as_of"]);
    assert_eq!(mainnet_replay["data"], mainnet["data"]);

    let sepolia_at = v2_sepolia_snapshot_token();
    let sepolia = v2_name_record_payload_for_database(
        &database,
        &format!("/v2/names/{V2_SEPOLIA_SNAPSHOT_NAME}?at={sepolia_at}"),
    )
    .await?;
    let sepolia_replay_at = v2_at_token_from_meta_as_of(
        &sepolia,
        "11155111",
        "ethereum-sepolia",
        "ethereum-sepolia",
    )?;
    let sepolia_replay = v2_name_record_payload_for_database(
        &database,
        &format!("/v2/names/{V2_SEPOLIA_SNAPSHOT_NAME}?at={sepolia_replay_at}"),
    )
    .await?;
    assert_eq!(sepolia_replay["meta"]["as_of"], sepolia["meta"]["as_of"]);
    assert_eq!(sepolia_replay["data"], sepolia["data"]);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_without_at_keeps_mainnet_preference_on_mixed_phase_heads() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_mixed_phase_head_names(&database).await?;

    let payload = v2_name_record_payload_for_database(
        &database,
        &format!("/v2/names/{V2_MAINNET_SNAPSHOT_NAME}"),
    )
    .await?;

    assert_eq!(
        payload["meta"]["as_of"]["1"],
        json!({
            "block_number": V2_MAINNET_SNAPSHOT_BLOCK,
            "block_hash": V2_MAINNET_SNAPSHOT_HASH,
            "timestamp": V2_MAINNET_SNAPSHOT_TIMESTAMP
        })
    );
    assert!(payload["meta"]["as_of"].get("11155111").is_none());
    assert_eq!(payload["data"]["network"], json!("ethereum"));
    assert_eq!(payload["data"]["chain_id"], json!(1));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_uses_phase_snapshot() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_mixed_phase_head_names(&database).await?;

    let payload = v2_name_record_payload_for_database(
        &database,
        &format!("/v2/names/{V2_MAINNET_SNAPSHOT_NAME}"),
    )
    .await?;
    assert_eq!(payload["meta"]["as_of"]["1"]["block_hash"], V2_MAINNET_SNAPSHOT_HASH);

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_name_timestamp_at_uses_sepolia_when_only_sepolia_phase_head_exists() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_sepolia_only_phase_head_name(&database).await?;

    let payload = v2_name_record_payload_for_database(
        &database,
        &format!("/v2/names/{V2_SEPOLIA_ONLY_SNAPSHOT_NAME}?at=2026-04-17T00:10:30Z"),
    )
    .await?;

    assert_eq!(
        payload["meta"]["as_of"]["11155111"],
        json!({
            "block_number": V2_SEPOLIA_ONLY_SNAPSHOT_BLOCK,
            "block_hash": V2_SEPOLIA_ONLY_SNAPSHOT_HASH,
            "timestamp": V2_SEPOLIA_ONLY_SNAPSHOT_TIMESTAMP
        })
    );
    assert!(payload["meta"]["as_of"].get("1").is_none());
    assert_eq!(payload["data"]["network"], json!("ethereum-sepolia"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_returns_indexed_values() -> Result<()> {
    let payload = v2_name_records_payload("/v2/names/Alice.eth/records").await?;

    assert!(payload["data"].get("records").is_none());
    assert_eq!(payload["meta"]["source"], json!("indexed"));
    assert_eq!(payload["data"]["namespace"], json!("ens"));
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
async fn v2_get_name_records_flattens_projected_byte_address_values() -> Result<()> {
    let payload = v2_name_records_payload_with_setup(
        "/v2/names/Alice.eth/records?keys=addr:0",
        |_, _, inventory| {
            inventory.selectors = json!([{
                "record_key": "addr:0",
                "record_family": "addr",
                "selector_key": "0",
                "cacheable": true
            }]);
            inventory.entries = json!([{
                "record_key": "addr:0",
                "record_family": "addr",
                "selector_key": "0",
                "status": "success",
                "value": {"encoding": "hex", "bytes": "0x001122"}
            }]);
        },
    )
    .await?;

    assert_eq!(payload["data"]["addresses"]["0"], json!("0x001122"));
    assert_eq!(
        payload["data"]["records"]["addr:0"],
        json!({
            "status": "ok",
            "value": "0x001122"
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_records_and_name_detail_derive_ensip19_default_addresses() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_alice_name_records_fixture(&database, |_, _, inventory| {
        inventory.selectors = json!([
            {
                "record_key": "addr:2147483648",
                "record_family": "addr",
                "selector_key": "2147483648",
                "cacheable": true
            },
            {
                "record_key": "addr:2147483649",
                "record_family": "addr",
                "selector_key": "2147483649",
                "cacheable": true
            }
        ]);
        inventory.entries = json!([
            {
                "record_key": "addr:2147483648",
                "record_family": "addr",
                "selector_key": "2147483648",
                "status": "success",
                "value": "0x0000000000000000000000000000000000000DeF"
            },
            {
                "record_key": "addr:2147483649",
                "record_family": "addr",
                "selector_key": "2147483649",
                "status": "not_found"
            }
        ]);
        inventory.provenance["read_rules"] = json!([{
            "kind": "ensip19_default_address",
            "source_record_key": "addr:2147483648"
        }]);
        inventory.explicit_gaps = json!([]);
        inventory.unsupported_families = json!([]);
    })
    .await?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let unavailable_rpc_url = format!("http://{}", listener.local_addr()?);
    drop(listener);
    let state = database
        .app_state_with_lookup_chain_rpc_urls(bigname_lookup::ChainRpcUrls::from_entries(&[
            format!("ethereum-mainnet={unavailable_rpc_url}"),
        ])?)
        .await?;

    for uri in [
        "/v2/names/Alice.eth/records?source=indexed&keys=addr:2147483649",
        "/v2/names/Alice.eth/records?source=auto&keys=addr:2147483649",
    ] {
        let response = app_router(state.clone())
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let payload: Value = read_json(response).await?;
        assert_eq!(payload["meta"]["source"], "indexed");
        assert_eq!(
            payload["data"]["records"]["addr:2147483649"],
            json!({
                "status": "ok",
                "value": "0x0000000000000000000000000000000000000def",
                "meta": {
                    "basis": "derived",
                    "rule": "ensip19_default_address",
                    "source_record_key": "addr:2147483648"
                }
            })
        );
    }

    let response = app_router(state)
        .oneshot(
            Request::builder()
                .uri("/v2/names/Alice.eth?source=indexed")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await?;
    let payload: Value = read_json(response).await?;
    assert_eq!(
        payload["data"]["addresses"]["60"],
        "0x0000000000000000000000000000000000000def"
    );
    assert_eq!(
        payload["data"]["primary_address"],
        "0x0000000000000000000000000000000000000def"
    );

    let lookup = v2_lookup_json(
        &database,
        json!({"profile": "detail", "inputs": [{"id": "alice", "name": "Alice.eth"}]}),
    )
    .await?;
    assert_eq!(
        lookup["data"][0]["record"]["addresses"]["60"],
        "0x0000000000000000000000000000000000000def"
    );
    assert_eq!(
        lookup["data"][0]["record"]["primary_address"],
        "0x0000000000000000000000000000000000000def"
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_ensip19_zero_default_matches_each_requested_getter() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_alice_name_records_fixture(&database, |_, _, inventory| {
        inventory.selectors = json!([{
            "record_key": "addr:2147483648",
            "record_family": "addr",
            "selector_key": "2147483648",
            "cacheable": true
        }]);
        inventory.entries = json!([{
            "record_key": "addr:2147483648",
            "record_family": "addr",
            "selector_key": "2147483648",
            "status": "success",
            "value": "0x0000000000000000000000000000000000000000"
        }]);
        inventory.provenance["read_rules"] = json!([{
            "kind": "ensip19_default_address",
            "source_record_key": "addr:2147483648"
        }]);
        inventory.explicit_gaps = json!([]);
        inventory.unsupported_families = json!([]);
    })
    .await?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let unavailable_rpc_url = format!("http://{}", listener.local_addr()?);
    drop(listener);
    let state = database
        .app_state_with_lookup_chain_rpc_urls(bigname_lookup::ChainRpcUrls::from_entries(&[
            format!("ethereum-mainnet={unavailable_rpc_url}"),
        ])?)
        .await?;

    let expected_records = json!({
        "addr:60": {
            "status": "not_found",
            "meta": {
                "basis": "derived",
                "rule": "ensip19_default_address",
                "source_record_key": "addr:2147483648"
            }
        },
        "addr:2147483649": {
            "status": "ok",
            "value": "0x0000000000000000000000000000000000000000",
            "meta": {
                "basis": "derived",
                "rule": "ensip19_default_address",
                "source_record_key": "addr:2147483648"
            }
        }
    });
    for source in ["indexed", "auto"] {
        let response = app_router(state.clone())
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v2/names/Alice.eth/records?source={source}&keys=addr:60,addr:2147483649"
                    ))
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let payload: Value = read_json(response).await?;
        assert_eq!(payload["meta"]["source"], "indexed");
        assert_eq!(payload["data"]["records"], expected_records);
        assert!(payload["data"]["addresses"].get("60").is_none());
        assert_eq!(
            payload["data"]["addresses"]["2147483649"],
            "0x0000000000000000000000000000000000000000"
        );
    }

    let response = app_router(state)
        .oneshot(
            Request::builder()
                .uri("/v2/names/Alice.eth?source=indexed")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await?;
    let payload: Value = read_json(response).await?;
    assert!(payload["data"]["addresses"].get("60").is_none());
    assert!(payload["data"].get("primary_address").is_none());

    let lookup = v2_lookup_json(
        &database,
        json!({"profile": "detail", "inputs": [{"id": "alice", "name": "Alice.eth"}]}),
    )
    .await?;
    assert!(lookup["data"][0]["record"]["addresses"]
        .get("60")
        .is_none());
    assert!(lookup["data"][0]["record"]
        .get("primary_address")
        .is_none());

    database.cleanup().await
}

#[tokio::test]
async fn v2_indexed_records_do_not_derive_for_unflagged_resolvers() -> Result<()> {
    let payload = v2_name_records_payload_with_setup(
        "/v2/names/Alice.eth/records?source=indexed&keys=addr:2147483649",
        |_, _, inventory| {
            inventory.selectors = json!([{
                "record_key": "addr:2147483648",
                "record_family": "addr",
                "selector_key": "2147483648",
                "cacheable": true
            }]);
            inventory.entries = json!([{
                "record_key": "addr:2147483648",
                "record_family": "addr",
                "selector_key": "2147483648",
                "status": "success",
                "value": "0x0000000000000000000000000000000000000def"
            }]);
            inventory.provenance["read_rules"] = json!([]);
            inventory.explicit_gaps = json!([]);
            inventory.unsupported_families = json!([]);
        },
    )
    .await?;
    assert_eq!(
        payload["data"]["records"]["addr:2147483649"],
        json!({"status":"not_found"})
    );
    Ok(())
}

#[tokio::test]
async fn v2_pre_surface_recovered_record_is_authoritative_for_profile_and_records() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_alice_name_records_fixture(&database, |_, _, inventory| {
        inventory.selectors = json!([{
            "record_key": "text:pre-surface",
            "record_family": "text",
            "selector_key": "pre-surface",
            "cacheable": true
        }]);
        inventory.entries = json!([{
            "record_key": "text:pre-surface",
            "record_family": "text",
            "selector_key": "pre-surface",
            "status": "success",
            "value": {
                "key": "pre-surface",
                "value": "recovered before the name surface"
            }
        }]);
        inventory.explicit_gaps = json!([]);
        inventory.unsupported_families = json!([]);
    })
    .await?;

    // A closed RPC endpoint makes any accidental verified fallback fail the request.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let unavailable_rpc_url = format!("http://{}", listener.local_addr()?);
    drop(listener);
    let state = database
        .app_state_with_lookup_chain_rpc_urls(bigname_lookup::ChainRpcUrls::from_entries(&[
            format!("ethereum-mainnet={unavailable_rpc_url}"),
        ])?)
        .await?;

    let mut payloads = Vec::new();
    for uri in [
        "/v2/names/Alice.eth",
        "/v2/names/Alice.eth/records?source=indexed&keys=text:pre-surface",
        "/v2/names/Alice.eth/records?source=auto&keys=text:pre-surface&include=inventory",
    ] {
        let response = app_router(state.clone())
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .context("pre-surface recovered record request failed")?;
        let status = response.status();
        let payload: Value = read_json(response).await?;
        assert_eq!(status, StatusCode::OK, "unexpected response: {payload}");
        payloads.push(payload);
    }

    assert_eq!(payloads[0]["meta"]["source"], json!("indexed"));
    assert_eq!(
        payloads[0]["data"]["text_records"]["pre-surface"],
        json!("recovered before the name surface")
    );
    assert!(payloads[0]["data"].get("unsupported_fields").is_none());
    for payload in &payloads[1..] {
        assert_eq!(payload["meta"]["source"], json!("indexed"));
        assert_eq!(
            payload["data"]["records"]["text:pre-surface"],
            json!({
                "status": "ok",
                "value": "recovered before the name surface"
            })
        );
    }
    assert_eq!(
        payloads[2]["data"]["inventory"],
        json!({
            "known_keys": ["text:pre-surface"],
            "unset_keys": [],
            "unsupported_keys": []
        })
    );
    let divergence_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM resolution_divergences")
            .fetch_one(&database.lookup_pool)
            .await?;
    assert_eq!(divergence_count, 0);

    database.cleanup().await
}

#[tokio::test]
async fn v2_ownerless_event_linked_resolver_serves_indexed_records() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_alice_name_records_fixture_with_row(
        &database,
        |row| {
            let serving_resource_id = row.resource_id.expect("fixture control resource");
            row.surface_binding_id = None;
            row.resource_id = None;
            row.serving_resource_id = Some(serving_resource_id);
            row.token_lineage_id = None;
            row.binding_kind = None;
            row.declared_summary["registration"] = json!({"status":"unregistered"});
            row.declared_summary["control"] = json!({"status":"unregistered"});
            row.declared_summary["coverage"] = json!({
                "status":"projected",
                "exhaustiveness":"not_asserted",
                "enumeration_basis":"event_linked_registry_resolver",
                "unsupported_reason":null
            });
            row.provenance["read_reachability"] = json!({
                "serving_resource_id":serving_resource_id,
                "basis":"retained_registry_resolver_pointer",
                "owner_getter_reason":"registry_self",
                "pointer_event_id":102
            });
            row.coverage = json!({
                "status":"projected",
                "exhaustiveness":"not_asserted",
                "enumeration_basis":"event_linked_registry_resolver",
                "unsupported_reason":null
            });
        },
        |_, _, _| {},
    )
    .await?;
    sqlx::query(
        "UPDATE bigname_phase.resources SET token_lineage_id = NULL
         WHERE resource_id = $1",
    )
    .bind(Uuid::from_u128(0x2200))
    .execute(&database.pool)
    .await?;

    for uri in [
        "/v2/names/Alice.eth",
        "/v2/names/Alice.eth/records?source=indexed&keys=text:description",
        "/v2/names/Alice.eth/records?source=auto&keys=text:description&include=inventory",
    ] {
        let response = app_router(database.app_state())
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .context("ownerless resolver request failed")?;
        assert_eq!(response.status(), StatusCode::OK);
        let payload: Value = read_json(response).await?;
        assert_eq!(
            payload["data"]["resolver"]["address"],
            json!("0x0000000000000000000000000000000000000abc")
        );
        assert_eq!(payload["data"]["registration_id"], Value::Null);
        if uri.contains("/records") {
            assert_eq!(payload["meta"]["source"], json!("indexed"));
            assert_eq!(
                payload["data"]["records"]["text:description"],
                json!({"status":"ok","value":"Alice profile"})
            );
            assert_ne!(
                payload["data"]["records"]["text:description"]["unsupported_reason"],
                json!("inventory_not_available")
            );
        } else {
            assert_eq!(payload["data"]["registration_status"], json!("unregistered"));
            assert!(
                payload["data"].get("token_id").is_none(),
                "ownerless exact-name payload must not imply token control: {payload}"
            );
            assert_eq!(
                payload["data"]["text_records"]["description"],
                json!("Alice profile"),
                "ownerless exact-name payload: {payload}"
            );
        }
    }

    let lookup = v2_lookup_json(
        &database,
        json!({"profile":"detail","inputs":[{"id":"ownerless","name":"alice.eth"}]}),
    )
    .await?;
    let lookup_record = &lookup["data"][0]["record"];
    assert_eq!(lookup_record["registration_status"], json!("unregistered"));
    assert!(
        lookup_record.get("token_id").is_none(),
        "ownerless batch lookup must not imply token control: {lookup}"
    );
    assert_eq!(
        lookup_record["resolver"]["address"],
        json!("0x0000000000000000000000000000000000000abc")
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_unclassified_serving_resource_does_not_expose_retained_records() -> Result<()> {
    let payload = v2_name_record_payload_with_row("/v2/names/Alice.eth", |row| {
        let serving_resource_id = row.resource_id.expect("fixture resource");
        row.surface_binding_id = None;
        row.resource_id = None;
        row.serving_resource_id = Some(serving_resource_id);
        row.token_lineage_id = None;
        row.binding_kind = None;
        row.declared_summary["registration"] = json!({
            "status":"reserved",
            "authority_kind":"ens_v2_registry"
        });
        row.declared_summary["control"] = json!({"status":"unregistered"});
        row.provenance = json!({});
    })
    .await?;

    assert_eq!(payload["data"]["registration_status"], json!("unregistered"));
    assert!(payload["data"].get("resolver").is_none());
    assert!(payload["data"].get("addresses").is_none());
    assert!(payload["data"].get("text_records").is_none());

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
    )
    .await?;

    assert_eq!(payload["data"]["content_hash"], Value::Null);
    assert_eq!(
        payload["data"]["records"],
        json!({
            "contenthash": {
                "status": "not_found"
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
    )
    .await?;

    assert_eq!(
        payload["data"]["inventory"],
        json!({
            "known_keys": ["addr:60", "avatar"],
            "unset_keys": [],
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
        v2_name_payload_without_inventory("/v2/names/Alice.eth/records?keys=addr:60&include=inventory")
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
async fn v2_get_name_records_source_verified_reports_unsupported_without_lookup_topology(
) -> Result<()> {
    let payload = v2_name_records_payload_with_setup(
        "/v2/names/Alice.eth/records?source=verified&keys=addr:60",
        |_, _, _| {},
    )
    .await?;

    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(
        payload["data"]["records"]["addr:60"],
        json!({
            "status": "unsupported",
            "unsupported_reason": "verified_records_not_supported"
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_withholds_unproven_authority_without_verified_lookup() -> Result<()> {
    // Every unsupported reason short-circuits the records response under its public name; only
    // `current_authority_not_projected` keeps its own documented inventory reason.
    for (reason, product_reason) in [
        (
            "conflicting_current_ens_authority",
            "conflicting_current_ens_authority",
        ),
        (
            "ensv2_exact_name_profile_shadow",
            "exact_name_profile_not_supported",
        ),
        (
            "a_reason_this_build_has_never_seen",
            "a_reason_this_build_has_never_seen",
        ),
        (
            "current_authority_not_projected",
            "inventory_not_available",
        ),
    ] {
        for source in ["indexed", "verified", "auto"] {
            let payload = v2_name_records_payload_with_row_and_setup(
                &format!("/v2/names/Alice.eth/records?source={source}&keys=addr:60"),
                |row| {
                    row.coverage = json!({
                        "status":"unsupported",
                        "unsupported_reason":reason
                    });
                },
                |_, _, _| {},
            )
            .await?;

            assert_eq!(payload["data"]["resolver"], Value::Null);
            assert_eq!(payload["data"]["addresses"], json!({}));
            assert_eq!(
                payload["data"]["records"]["addr:60"],
                json!({
                    "status":"unsupported",
                    "unsupported_reason":product_reason
                })
            );
            assert_eq!(
                payload["meta"]["source"],
                json!(if source == "verified" {
                    "verified"
                } else {
                    "indexed"
                })
            );
        }
    }

    Ok(())
}

#[tokio::test]
async fn v2_unknown_pipeline_unsupported_reason_stays_in_band() -> Result<()> {
    let records = v2_name_records_payload_with_row_and_setup(
        "/v2/names/Alice.eth/records?keys=addr:60",
        |row| {
            row.coverage = json!({
                "status": "unsupported",
                "unsupported_reason": "future_projection_gap"
            });
        },
        |_, _, _| {},
    )
    .await?;

    assert_eq!(records["data"]["resolver"], Value::Null);
    assert_eq!(records["data"]["addresses"], json!({}));
    assert_eq!(records["data"]["text_records"], json!({}));
    assert_eq!(records["data"]["content_hash"], Value::Null);
    assert_eq!(
        records["data"]["records"]["addr:60"],
        json!({
            "status": "unsupported",
            "unsupported_reason": "unsupported_reason_unrecognized"
        })
    );

    let verified =
        v2_name_record_payload_with_row("/v2/names/Alice.eth?source=verified", |row| {
            row.coverage = json!({
                "status": "unsupported",
                "unsupported_reason": "future_projection_gap"
            });
        })
        .await?;
    let data = verified["data"].as_object().expect("data must be an object");
    assert_eq!(data.get("status"), Some(&json!("unsupported")));
    assert_eq!(
        data.get("unsupported_reason"),
        Some(&json!("unsupported_reason_unrecognized"))
    );
    assert_eq!(
        data.keys().cloned().collect::<Vec<_>>(),
        vec![
            "display_name",
            "name",
            "namehash",
            "namespace",
            "status",
            "unsupported_reason",
        ]
    );

    Ok(())
}

#[tokio::test]
async fn v2_verified_name_reads_reject_oversized_inventory_derived_selector_sets() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_alice_name_records_fixture(&database, |_, _, inventory| {
        let mut selectors = (0..=crate::v2::MAX_PAGE_SIZE)
            .map(|index| {
                json!({
                    "record_key": format!("text:key-{index}"),
                    "record_family": "text",
                    "selector_key": format!("key-{index}"),
                    "cacheable": false
                })
            })
            .collect::<Vec<_>>();
        selectors.sort_by(|left, right| {
            left["record_key"]
                .as_str()
                .cmp(&right["record_key"].as_str())
        });
        inventory.selectors = Value::Array(selectors);
        inventory.entries = json!([]);
        inventory.explicit_gaps = json!([]);
    })
    .await?;
    let state = database.app_state();

    for uri in [
        "/v2/names/Alice.eth/records?source=verified",
        "/v2/names/Alice.eth?source=verified",
    ] {
        let response = app_router(state.clone())
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .context("oversized inventory-derived verified request failed")?;
        let status = response.status();
        let payload: Value = read_json(response).await?;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{payload}");
        assert_eq!(payload["error"]["code"], json!("unsupported"));
        assert_eq!(
            payload["error"]["message"],
            json!("verified record reads support at most 200 record keys")
        );
    }

    let narrowed = app_router(state)
        .oneshot(
            Request::builder()
                .uri("/v2/names/Alice.eth/records?source=verified&keys=text:key-0")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("narrowed verified records request failed")?;
    assert_eq!(narrowed.status(), StatusCode::OK);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_basenames_records_source_auto_stays_base_scoped_without_fallback() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let logical_name_id = "basenames:alice.base.eth";
    let resource_id = Uuid::from_u128(0x9250);
    let token_lineage_id = Uuid::from_u128(0x9251);
    let surface_binding_id = Uuid::from_u128(0x9252);
    let chain_positions = json!({
        "base": {
            "chain_id": "base-mainnet",
            "block_number": 21_000_003,
            "block_hash": "0xbase-binding",
            "timestamp": "2026-04-17T00:00:03Z"
        }
    });
    database
        .seed_snapshot_selector_chain_positions(&chain_positions)
        .await?;
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
    row.declared_summary["resolver"] = json!({
        "chain_id": "base-mainnet",
        "address": "0x0000000000000000000000000000000000000abc",
        "latest_event_kind": "ResolverChanged"
    });
    row.chain_positions = chain_positions;
    row.canonicality_summary = json!({
        "status": "finalized",
        "chains": { "base-mainnet": "finalized" }
    });
    database.insert_name_current_row(row).await?;

    let indexed_address = "0x0000000000000000000000000000000000000def";
    let mut inventory =
        basenames_l2resolver_record_inventory_current_row(logical_name_id, resource_id);
    inventory.selectors = json!([{
        "record_key": "addr:60",
        "record_family": "addr",
        "selector_key": "60",
        "cacheable": true
    }]);
    inventory.entries = json!([{
        "record_key": "addr:60",
        "record_family": "addr",
        "selector_key": "60",
        "status": "success",
        "value": { "coin_type": "60", "value": indexed_address }
    }]);
    database
        .insert_record_inventory_current_row(inventory)
        .await?;

    for uri in [
        "/v2/names/alice.base.eth/records?source=auto",
        "/v2/names/alice.base.eth/records?source=auto&keys=addr:60",
    ] {
        let response = app_router(database.app_state())
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .context("v2 Base-only auto records request failed")?;
        let status = response.status();
        let payload: Value = read_json(response).await?;
        assert_eq!(status, StatusCode::OK, "unexpected response for {uri}: {payload}");
        assert_eq!(payload["meta"]["source"], json!("indexed"));
        assert!(payload["meta"]["as_of"].get("1").is_none());
        assert_eq!(payload["meta"]["as_of"]["8453"]["block_hash"], json!("0xbase-binding"));
        assert_eq!(payload["data"]["addresses"]["60"], json!(indexed_address));
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_basenames_records_source_auto_retries_when_fallback_disappears_during_reselection()
-> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    let resource_id = Uuid::from_u128(0x9260);
    seed_v2_basenames_auto_transition_fixture(&database, resource_id).await?;
    let (_guard, control) =
        crate::v2::name_records_auto_fallback_test_hooks::install(&database.pool).await?;
    let state = database.app_state();
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .uri(
                        "/v2/names/alice.base.eth/records?source=auto&keys=addr:60",
                    )
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
    });

    control.wait_until_reached().await;
    let indexed_address = "0x0000000000000000000000000000000000000def";
    sqlx::query(
        "UPDATE record_inventory_current
         SET entries = $2
         WHERE resource_id = $1",
    )
    .bind(resource_id)
    .bind(json!([{
        "record_key": "addr:60",
        "record_family": "addr",
        "selector_key": "60",
        "status": "success",
        "value": { "coin_type": "60", "value": indexed_address }
    }]))
    .execute(&database.pool)
    .await?;
    control.resume().await;

    let response = request_task
        .await
        .context("v2 auto fallback transition request task panicked")?
        .context("v2 auto fallback transition request failed")?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected response: {payload}");
    assert_eq!(payload["error"]["code"], json!("stale"));

    database.cleanup().await?;
    Ok(())
}

async fn seed_v2_basenames_auto_transition_fixture(
    database: &TestDatabase,
    resource_id: Uuid,
) -> Result<()> {
    let logical_name_id = "basenames:alice.base.eth";
    let token_lineage_id = Uuid::from_u128(0x9261);
    let surface_binding_id = Uuid::from_u128(0x9262);
    database
        .seed_snapshot_selector_chain_positions(&json!({
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
        }))
        .await?;
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
    row.declared_summary["resolver"] = json!({
        "chain_id": "base-mainnet",
        "address": "0x0000000000000000000000000000000000000abc",
        "latest_event_kind": "ResolverChanged"
    });
    row.chain_positions = json!({
        "base": {
            "chain_id": "base-mainnet",
            "block_number": 21_000_003,
            "block_hash": "0xbase-binding",
            "timestamp": "2026-04-17T00:00:03Z"
        }
    });
    row.canonicality_summary = json!({
        "status": "finalized",
        "chains": { "base-mainnet": "finalized" }
    });
    database.insert_name_current_row(row).await?;

    let mut inventory =
        basenames_l2resolver_record_inventory_current_row(logical_name_id, resource_id);
    inventory.selectors = json!([{
        "record_key": "addr:60",
        "record_family": "addr",
        "selector_key": "60",
        "cacheable": true
    }]);
    inventory.entries = json!([{
        "record_key": "addr:60",
        "record_family": "addr",
        "selector_key": "60",
        "status": "unsupported",
        "unsupported_reason": "value_not_retained_in_normalized_events"
    }]);
    database
        .insert_record_inventory_current_row(inventory)
        .await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_basenames_records_source_auto_retries_when_authority_reclassifies_during_reselection()
-> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    database.initialize_lookup_schema().await?;
    let lookup_pool = database.lookup_pool().await?;
    let _namehash = seed_schema_v2_basenames_record_lookup(
        &lookup_pool,
        21_000_003,
        "0xbase-binding",
        "0xbinding",
        "2026-04-17T00:00:03Z",
        "0x0000000000000000000000000000000000000def",
    )
    .await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({
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
        }))
        .await?;
    seed_basenames_auto_fallback_requiring_inventory(&database).await?;

    let (_guard, control) =
        crate::v2::name_records_auto_fallback_test_hooks::install(&database.pool).await?;
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        resolution_basenames_l1_addr60_response("0x0000000000000000000000000000000000000e0e"),
    ])
    .await?;
    let chain_rpc_urls =
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(chain_rpc_urls)
        .await?;
    let request_task = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .uri(
                        "/v2/names/alice.base.eth/records?source=auto&keys=addr:60",
                    )
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
    });

    control.wait_until_reached().await;
    sqlx::query(
        "UPDATE bigname_phase.name_current
         SET support_status = 'unsupported',
             unsupported_reason = 'current_authority_not_projected'
         WHERE namespace = 'basenames' AND raw_name = 'alice.base.eth'",
    )
    .execute(&database.pool)
    .await?;
    control.resume().await;

    let response = request_task
        .await
        .context("v2 auto fallback authority transition request task panicked")?
        .context("v2 auto fallback authority transition request failed")?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::CONFLICT, "unexpected response: {payload}");
    assert_eq!(payload["error"]["code"], json!("stale"));
    assert_eq!(
        payload["error"]["message"],
        json!("name records changed while preparing verified fallback; retry the request")
    );

    // The mock queue still holds its one response: any dispatch would have
    // consumed it and finished the task with a recorded request.
    rpc_handle.abort();
    let dispatched = match rpc_handle.await {
        Err(join_error) if join_error.is_cancelled() => Vec::new(),
        other => other.context("mock primary-name RPC task failed")??,
    };
    assert!(
        dispatched.is_empty(),
        "authority reclassification during auto-fallback reselection must not dispatch a verified lookup: {dispatched:?}"
    );

    lookup_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_basenames_records_source_auto_executes_verified_fallback_after_reselection()
-> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    database.initialize_lookup_schema().await?;
    let lookup_pool = database.lookup_pool().await?;
    let _namehash = seed_schema_v2_basenames_record_lookup(
        &lookup_pool,
        21_000_003,
        "0xbase-binding",
        "0xbinding",
        "2026-04-17T00:00:03Z",
        "0x0000000000000000000000000000000000000def",
    )
    .await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({
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
        }))
        .await?;
    seed_basenames_auto_fallback_requiring_inventory(&database).await?;

    let executed_address = "0x0000000000000000000000000000000000000e0e";
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        resolution_basenames_l1_addr60_response(executed_address),
    ])
    .await?;
    let chain_rpc_urls =
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(chain_rpc_urls)
        .await?;
    let response = app_router(state)
        .oneshot(
            Request::builder()
                .uri("/v2/names/alice.base.eth/records?source=auto&keys=addr:60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 auto fallback verified execution request failed")?;

    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "unexpected response: {payload}");
    assert_eq!(payload["meta"]["source"], json!("verified"));
    assert_eq!(
        payload["data"]["records"]["addr:60"],
        json!({
            "status": "ok",
            "value": executed_address
        })
    );

    let rpc_requests = join_primary_name_mock_rpc_requests(rpc_handle).await?;
    assert_eq!(rpc_requests.len(), 1);

    lookup_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

/// Rewrite the seeded Basenames inventory entry so `addr:60` stops being
/// indexed-satisfying and `source=auto` must take the verified fallback path.
async fn seed_basenames_auto_fallback_requiring_inventory(database: &TestDatabase) -> Result<()> {
    sqlx::query(
        "UPDATE bigname_phase.record_inventory_current
         SET entries = $1
         WHERE resource_id = (
             SELECT resource_id FROM bigname_phase.name_current
             WHERE namespace = 'basenames' AND raw_name = 'alice.base.eth'
         )",
    )
    .bind(json!([{
        "record_key": "addr:60",
        "record_family": "addr",
        "selector_key": "60",
        "status": "unsupported",
        "unsupported_reason": "value_not_retained_in_normalized_events"
    }]))
    .execute(&database.pool)
    .await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_source_verified_executes_basenames_with_auxiliary_position(
) -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    database.initialize_lookup_schema().await?;
    let lookup_pool = database.lookup_pool().await?;
    let _namehash = seed_schema_v2_basenames_record_lookup(
        &lookup_pool,
        21_000_003,
        "0xbase-binding",
        "0xbinding",
        "2026-04-17T00:00:03Z",
        "0x0000000000000000000000000000000000000def",
    )
    .await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "base": {
                "chain_id": "base-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbase-binding",
                "timestamp": "2026-04-17T00:00:03Z"
            },
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_004,
                "block_hash": "0xnewer-binding",
                "timestamp": "2026-04-17T00:00:04Z"
            }
        }))
        .await?;

    let executed_address = "0x0000000000000000000000000000000000000e0e";
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        resolution_basenames_l1_addr60_response(executed_address),
    ])
    .await?;
    let chain_rpc_urls =
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(chain_rpc_urls)
        .await?;
    let response = app_router(state)
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
        payload["meta"]["as_of"]["1"]["block_hash"],
        json!("0xbinding"),
        "Basenames verified response metadata must expose the row's actual execution position"
    );
    assert_eq!(payload["meta"]["as_of"]["1"]["block_number"], json!(21_000_003));
    assert_eq!(
        payload["meta"]["as_of"]["8453"]["block_hash"],
        json!("0xbase-binding")
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
    assert_eq!(
        rpc_requests[0]["params"][1],
        json!({
            "blockHash": "0xbinding",
            "requireCanonical": true
        })
    );
    let ledger_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM resolution_divergences WHERE cleared_at IS NULL",
    )
    .fetch_one(&lookup_pool)
    .await?;
    assert_eq!(ledger_count, 1);

    lookup_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_source_verified_reports_unsupported_without_verified_boundary(
) -> Result<()> {
    let payload = v2_name_records_payload_with_row_and_setup(
        "/v2/names/Alice.eth/records?source=verified&keys=avatar",
        |row| {
            row.binding_kind = None;
            row.surface_binding_id = None;
            row.resource_id = None;
            row.token_lineage_id = None;
        },
        |_, _, _| {},
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
                "status": "unsupported",
                "unsupported_reason": "verified_records_not_supported"
            }
        })
    );

    Ok(())
}

#[tokio::test]
async fn v2_get_name_records_missing_name_returns_not_found() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_alice_name_records_fixture(&database, |_, _, _| {}).await?;

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

#[tokio::test]
async fn v2_get_subnames_returns_record_shaped_rows_in_display_name_order() -> Result<()> {
    let (database, payload) =
        v2_subnames_payload("/v2/names/Parent.eth/subnames?page_size=3").await?;
    let stored_owner: Option<String> = sqlx::query_scalar(
        "SELECT owner FROM bigname_phase.children_current
         WHERE decoded_name = 'gamma.parent.eth'",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(
        stored_owner.as_deref(),
        Some("0x00000000000000000000000000000000000000cc")
    );

    assert_eq!(payload["page"]["page_size"], json!(3));
    assert_eq!(payload["page"]["total_count"], Value::Null);
    assert_eq!(payload["page"]["has_more"], json!(false));
    assert_eq!(payload["meta"], json!({}));

    let data = payload["data"]
        .as_array()
        .expect("subnames data must be an array");
    assert_eq!(data.len(), 3);
    assert_eq!(data[0]["name"], json!("alpha.parent.eth"));
    assert_eq!(data[1]["name"], json!("beta.parent.eth"));
    assert_eq!(data[2]["name"], json!("gamma.parent.eth"));
    assert_eq!(data[0]["display_name"], json!("alpha.parent.eth"));
    assert_eq!(data[0]["namespace"], json!("ens"));
    assert_eq!(
        data[0]["namehash"],
        json!(bigname_lookup::ens_namehash_hex("alpha.parent.eth")?)
    );
    assert_eq!(
        data[0]["labelhash"],
        json!(labelhash_for_display_name("alpha.parent.eth"))
    );
    assert_eq!(
        data[0]["owner"],
        json!("0x00000000000000000000000000000000000000aa")
    );
    assert_eq!(
        data[0]["registrant"],
        json!("0x00000000000000000000000000000000000000ab")
    );
    assert_eq!(data[0]["registration_status"], json!("active"));
    assert_eq!(data[0]["registered_at"], json!("2024-01-02T03:04:05Z"));
    assert_eq!(data[0]["created_at"], json!("2023-01-02T03:04:05Z"));
    assert_eq!(data[0]["expires_at"], json!("2027-01-02T03:04:05Z"));
    assert_eq!(data[1]["registration_status"], json!("released"));
    assert_eq!(data[2]["registration_status"], json!("unregistered"));
    assert!(
        data[2].get("owner").is_none(),
        "a generic no-registration row must not inherit the children projection owner"
    );
    assert!(data[0].get("subname_count").is_none());
    assert!(data[0].get("resolver").is_none());
    assert!(data[0].get("addresses").is_none());
    assert!(data[0].get("text_records").is_none());
    assert!(data[0].get("content_hash").is_none());
    assert_no_banned_v1_spellings(&payload);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_subnames_keeps_zero_owner_for_ownerless_resolver_child() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_fixture(&database).await?;
    let child_updated = sqlx::query(
        "UPDATE bigname_phase.children_current
         SET owner = '0x0000000000000000000000000000000000000000', registrant = NULL
         WHERE decoded_name = 'gamma.parent.eth'",
    )
    .execute(&database.pool)
    .await?;
    assert_eq!(child_updated.rows_affected(), 1);
    let name_updated = sqlx::query(
        "UPDATE bigname_phase.name_current
         SET surface_binding_id = NULL, resource_id = NULL, token_lineage_id = NULL,
             binding_kind = NULL,
             serving_resource_id = (SELECT resource_id FROM bigname_phase.name_current
                                    WHERE raw_name = 'alpha.parent.eth'),
             declared_summary = jsonb_build_object(
                 'registration', jsonb_build_object('status', 'unregistered'),
                 'control', jsonb_build_object('status', 'unregistered'),
                 'coverage', jsonb_build_object(
                     'status', 'projected',
                     'exhaustiveness', 'not_asserted',
                     'enumeration_basis', 'event_linked_registry_resolver',
                     'unsupported_reason', NULL))
         WHERE raw_name = 'gamma.parent.eth'",
    )
    .execute(&database.pool)
    .await?;
    assert_eq!(name_updated.rows_affected(), 1);

    let payload = v2_subnames_payload_for_database(
        &database,
        "/v2/names/parent.eth/subnames?page_size=10",
    )
    .await?;
    let child = payload["data"]
        .as_array()
        .and_then(|rows| rows.iter().find(|row| row["name"] == "gamma.parent.eth"))
        .expect("ownerless child must remain enumerable");
    assert_eq!(
        child["owner"],
        json!("0x0000000000000000000000000000000000000000")
    );
    assert_eq!(child["registrant"], Value::Null);
    assert_eq!(child["registration_status"], json!("unregistered"));

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_subnames_paginates_with_opaque_cursor_without_overlap() -> Result<()> {
    let (database, first_page) =
        v2_subnames_payload("/v2/names/parent.eth/subnames?page_size=2").await?;
    let next_cursor = first_page["page"]["next_cursor"]
        .as_str()
        .expect("first page must include a next cursor")
        .to_owned();
    assert_eq!(first_page["page"]["has_more"], json!(true));

    let second_page = v2_subnames_payload_for_database(
        &database,
        &format!("/v2/names/parent.eth/subnames?page_size=2&cursor={next_cursor}"),
    )
    .await?;

    assert_eq!(second_page["page"]["cursor"], json!(next_cursor));
    assert_eq!(second_page["page"]["next_cursor"], Value::Null);
    assert_eq!(second_page["page"]["has_more"], json!(false));
    assert_eq!(
        first_page["data"]
            .as_array()
            .expect("first page data")
            .iter()
            .map(|row| row["name"].as_str().expect("row name"))
            .collect::<Vec<_>>(),
        vec!["alpha.parent.eth", "beta.parent.eth"]
    );
    assert_eq!(
        second_page["data"]
            .as_array()
            .expect("second page data")
            .iter()
            .map(|row| row["name"].as_str().expect("row name"))
            .collect::<Vec<_>>(),
        vec!["gamma.parent.eth"]
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_subnames_uses_current_sepolia_anchor_on_mixed_phase_heads() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_mixed_phase_head_names(&database).await?;
    let child_logical_name_id = format!("ens:child.{V2_SEPOLIA_SNAPSHOT_NAME}");
    seed_v2_subnames_bound_child(
        &database,
        &child_logical_name_id,
        "Child.Sepolia-Pin.eth",
        "namehash:child.sepolia-pin.eth",
        91,
        Uuid::from_u128(0x7e23),
        Uuid::from_u128(0x7e24),
        Uuid::from_u128(0x7e25),
        json!({
            "registration": {
                "status": "active",
                "authority_kind": "ens_v2_registry"
            }
        }),
    )
    .await?;
    upsert_phase_children_current_rows(
        &database.pool,
        &[v2_subnames_declared_child_row(
            &format!("ens:{V2_SEPOLIA_SNAPSHOT_NAME}"),
            &child_logical_name_id,
            "Child.Sepolia-Pin.eth",
            "namehash:child.sepolia-pin.eth",
            905,
            91,
        )],
    )
    .await?;

    let payload = v2_subnames_payload_for_database(
        &database,
        &format!("/v2/names/{V2_SEPOLIA_SNAPSHOT_NAME}/subnames"),
    )
    .await?;
    assert_eq!(payload["meta"], json!({}));
    assert_eq!(payload["data"][0]["name"], json!("child.sepolia-pin.eth"));

    database.cleanup().await
}

#[tokio::test]
async fn v2_get_subnames_rejects_cursor_reused_for_different_parent() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_fixture(&database).await?;
    seed_v2_subnames_bound_child(
        &database,
        "ens:other.eth",
        "other.eth",
        "node:other.eth",
        79,
        Uuid::from_u128(0x4030),
        Uuid::from_u128(0x5030),
        Uuid::from_u128(0x6030),
        json!({
            "registration": {
                "status": "active",
                "authority_kind": "registrar"
            },
            "control": {
                "registry_owner": "0x0000000000000000000000000000000000000002"
            }
        }),
    )
    .await?;
    seed_v2_subnames_bound_child(
        &database,
        "ens:one.other.eth",
        "one.other.eth",
        "node:one.other.eth",
        80,
        Uuid::from_u128(0x4040),
        Uuid::from_u128(0x5040),
        Uuid::from_u128(0x6040),
        json!({
            "registration": {
                "status": "active",
                "authority_kind": "registrar"
            },
            "control": {
                "registry_owner": "0x0000000000000000000000000000000000000003"
            }
        }),
    )
    .await?;
    upsert_phase_children_current_rows(
        &database.pool,
        &[v2_subnames_declared_child_row(
            "ens:other.eth",
            "ens:one.other.eth",
            "one.other.eth",
            "node:one.other.eth",
            905,
            80,
        )],
    )
    .await?;

    let first_page =
        v2_subnames_payload_for_database(&database, "/v2/names/parent.eth/subnames?page_size=2")
            .await?;
    let next_cursor = first_page["page"]["next_cursor"]
        .as_str()
        .expect("first page must include a next cursor");

    let response = v2_subnames_response_for_database(
        &database,
        &format!("/v2/names/other.eth/subnames?page_size=2&cursor={next_cursor}"),
    )
    .await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_subnames_include_counts_adds_child_subname_count_only_when_requested()
-> Result<()> {
    let (database, without_counts) =
        v2_subnames_payload("/v2/names/parent.eth/subnames?page_size=3").await?;
    assert!(
        without_counts["data"][0].get("subname_count").is_none(),
        "subname_count must be omitted by default"
    );

    let with_counts =
        v2_subnames_payload_for_database(&database, "/v2/names/parent.eth/subnames?include=counts")
            .await?;
    assert_eq!(with_counts["data"][0]["subname_count"], json!(1));
    assert_eq!(with_counts["data"][1]["subname_count"], json!(0));
    assert_eq!(with_counts["data"][2]["subname_count"], json!(0));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_subname_collections_filter_orphaned_phase_lineage_and_keep_preimage_rows()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_fixture(&database).await?;

    let parent_logical_name_id: String = sqlx::query_scalar(
        "SELECT logical_name_id FROM bigname_phase.name_surfaces WHERE raw_name = 'parent.eth'",
    )
    .fetch_one(&database.pool)
    .await?;

    sqlx::raw_sql(
        r#"
        INSERT INTO bigname_phase.chain_lineage (
            chain_id, block_hash, block_number, block_timestamp, canonicality_state
        ) VALUES
            ('ethereum-mainnet', '0xreorg-beta-child', 1003, '2026-04-17T01:00:03Z',
             'canonical'::bigname_phase.canonicality_state),
            ('ethereum-mainnet', '0xreorg-gamma-child', 1004, '2026-04-17T01:00:04Z',
             'canonical'::bigname_phase.canonicality_state);
        UPDATE bigname_phase.name_surfaces
        SET block_hash = CASE raw_name
                WHEN 'beta.parent.eth' THEN '0xreorg-beta-child'
                ELSE '0xreorg-gamma-child'
            END,
            block_number = CASE raw_name
                WHEN 'beta.parent.eth' THEN 1003
                ELSE 1004
            END,
            canonicality_state = 'canonical'::bigname_phase.canonicality_state
        WHERE raw_name IN ('beta.parent.eth', 'gamma.parent.eth');
        UPDATE bigname_phase.chain_lineage lineage
        SET canonicality_state = 'orphaned'::bigname_phase.canonicality_state
        FROM bigname_phase.name_surfaces surface
        WHERE surface.raw_name IN ('beta.parent.eth', 'gamma.parent.eth')
          AND lineage.chain_id = surface.chain_id
          AND lineage.block_hash = surface.block_hash
        "#,
    )
    .execute(&database.pool)
    .await?;
    sqlx::query(
        r#"
        UPDATE bigname_phase.children_current
        SET provenance = jsonb_set(provenance, '{label}',
            '{"source":"label_preimage"}'::jsonb)
        WHERE decoded_name = 'gamma.parent.eth'
        "#,
    )
    .execute(&database.pool)
    .await?;

    let page = bigname_storage::load_children_current_page(
        &database.pool,
        &parent_logical_name_id,
        None,
        10,
    )
    .await?;
    assert_eq!(
        page.rows
            .iter()
            .map(|row| row.normalized_name.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha.parent.eth", "gamma.parent.eth"]
    );
    assert_eq!(page.summary.child_count, 2);

    let audit_rows = bigname_storage::load_children_current_including_noncanonical(
        &database.pool,
        &parent_logical_name_id,
    )
    .await?;
    assert_eq!(audit_rows.len(), 3);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_subname_collections_exclude_orphaned_project_target_before_redo() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_fixture(&database).await?;
    let parent_logical_name_id: String = sqlx::query_scalar(
        "SELECT logical_name_id FROM bigname_phase.name_surfaces WHERE raw_name = 'parent.eth'",
    )
    .fetch_one(&database.pool)
    .await?;

    sqlx::raw_sql(
        r#"
        INSERT INTO bigname_phase.chain_lineage (
            chain_id, block_hash, block_number, block_timestamp, canonicality_state
        ) VALUES (
            'ethereum-mainnet', '0xproject-children-target', 2010,
            '2026-04-17T02:00:10Z', 'canonical'
        );
        UPDATE bigname_phase.children_current
        SET chain_positions = jsonb_build_object(
                'block_number', 2010,
                'block_hash', '0xproject-children-target',
                'target_block_number', 2010,
                'target_block_hash', '0xproject-children-target'
            ),
            canonicality_summary = jsonb_build_object(
                'state', 'canonical',
                'target_block_number', 2010,
                'target_block_hash', '0xproject-children-target'
            );
        "#,
    )
    .execute(&database.pool)
    .await?;

    let target_is_not_an_identity_anchor: bool = sqlx::query_scalar(
        "SELECT NOT EXISTS ( \
             SELECT 1 FROM bigname_phase.name_surfaces \
             WHERE block_hash = '0xproject-children-target' \
         )",
    )
    .fetch_one(&database.pool)
    .await?;
    assert!(target_is_not_an_identity_anchor);
    assert_eq!(
        bigname_storage::load_children_current(&database.pool, &parent_logical_name_id)
            .await?
            .len(),
        3
    );

    sqlx::query(
        "UPDATE bigname_phase.chain_lineage \
         SET canonicality_state = 'orphaned' \
         WHERE chain_id = 'ethereum-mainnet' \
           AND block_hash = '0xproject-children-target'",
    )
    .execute(&database.pool)
    .await?;

    assert!(
        bigname_storage::load_children_current(&database.pool, &parent_logical_name_id)
            .await?
            .is_empty()
    );
    assert_eq!(
        bigname_storage::load_children_current_including_noncanonical(
            &database.pool,
            &parent_logical_name_id,
        )
        .await?
        .len(),
        3
    );

    // The counted summary must fail closed on the same orphaned projection target as the page it
    // annotates, even though both identity anchors are still canonical.
    let summaries = bigname_storage::load_children_current_summaries(
        &database.pool,
        std::slice::from_ref(&parent_logical_name_id),
    )
    .await?;
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].child_count, 0);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_subnames_paginates_across_a_child_with_no_observed_label() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_fixture(&database).await?;
    // A registry edge proves the child node and its labelhash but not the label. Project writes
    // that row with every name column null and no child name surface — the shape most historical
    // labels have — and the page must name it by the documented placeholder rather than decoding
    // a null into a mandatory field.
    seed_v2_subnames_topology_only_child(&database, "parent.eth", "0xfeed0001").await?;

    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..5 {
        let uri = match cursor.as_deref() {
            Some(cursor) => format!(
                "/v2/names/parent.eth/subnames?page_size=2&cursor={cursor}"
            ),
            None => "/v2/names/parent.eth/subnames?page_size=2".to_owned(),
        };
        let payload = v2_subnames_payload_for_database(&database, &uri).await?;
        for row in payload["data"].as_array().expect("subnames data") {
            seen.push(row["name"].as_str().expect("row name must be a string").to_owned());
        }
        match payload["page"]["next_cursor"].as_str() {
            Some(next) => cursor = Some(next.to_owned()),
            None => break,
        }
    }

    let mut deduped = seen.clone();
    deduped.sort();
    deduped.dedup();
    assert_eq!(deduped.len(), seen.len(), "no row may be served twice: {seen:?}");
    assert!(
        seen.contains(&"[feed0001].parent.eth".to_owned()),
        "the unobserved-label child must be named by its placeholder: {seen:?}"
    );
    assert_eq!(seen.len(), 4, "every child must be paged exactly once: {seen:?}");

    // A preimage whose label bytes do not decode is stored raw with no decoded form; the read
    // escape-encodes it. It is equally not an addressable name, and equally must not fail the page.
    seed_v2_subnames_undecodable_child(&database, "parent.eth", "0xfeed0002").await?;
    let with_undecodable =
        v2_subnames_payload_for_database(&database, "/v2/names/parent.eth/subnames?page_size=20")
            .await?;
    let names = with_undecodable["data"]
        .as_array()
        .expect("subnames data")
        .iter()
        .map(|row| row["name"].as_str().expect("row name").to_owned())
        .collect::<Vec<_>>();
    let escaped_row = with_undecodable["data"]
        .as_array()
        .expect("subnames data")
        .iter()
        .find(|row| row["labelhash"] == "0xfeed0002")
        .unwrap_or_else(|| panic!("an undecodable label must be served, not dropped: {names:?}"));
    assert_eq!(escaped_row["name"], "\\377\tBad.parent.eth");
    assert_eq!(escaped_row["display_name"], "\\377\tBad.parent.eth");

    // The audit read has no keyset to drop the row, so it decodes the name directly.
    let parent_logical_name_id: String = sqlx::query_scalar(
        "SELECT logical_name_id FROM bigname_phase.name_surfaces WHERE raw_name = 'parent.eth'",
    )
    .fetch_one(&database.pool)
    .await?;
    let audited = bigname_storage::load_children_current_including_noncanonical(
        &database.pool,
        &parent_logical_name_id,
    )
    .await?;
    assert!(
        audited
            .iter()
            .any(|row| row.canonical_display_name == "[feed0001].parent.eth"),
        "the audit read must name the unobserved-label child too"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_subnames_gates_decoded_text_on_the_normalization_verdict() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_fixture(&database).await?;
    // Project composes the name columns only under a true normalization verdict; a
    // proof-checked label whose text fails keeps its raw bytes in the projection but is
    // written with both name columns null, whether the text normalizes to different bytes
    // ("Alice") or the normalizer errors outright (the ZWJ label).
    seed_v2_subnames_preimage_child(&database, "parent.eth", "alice", true).await?;
    seed_v2_subnames_preimage_child(&database, "parent.eth", "Alice", false).await?;
    seed_v2_subnames_preimage_child(&database, "parent.eth", "Ni\u{200d}ck", false).await?;

    let payload =
        v2_subnames_payload_for_database(&database, "/v2/names/parent.eth/subnames?page_size=20")
            .await?;
    let rows = payload["data"].as_array().expect("subnames data").clone();
    let row_for_label = |label: &str| {
        let labelhash = format!("{:#x}", alloy_primitives::keccak256(label.as_bytes()));
        rows.iter()
            .find(|row| row["labelhash"] == labelhash)
            .unwrap_or_else(|| panic!("child for label {label:?} must be served: {rows:?}"))
    };

    // Verdict true: the decoded name serves, and the served name re-hashes to the served node.
    let decoded = row_for_label("alice");
    assert_eq!(decoded["name"], json!("alice.parent.eth"));
    assert_eq!(decoded["display_name"], json!("alice.parent.eth"));
    assert_eq!(decoded["namehash"], json!(namehash_of("alice.parent.eth")));

    // Verdict false with decodable text: the placeholder serves against the raw-byte node —
    // never the text, which would re-hash to a different node than the one proven on chain.
    let unnormalized = row_for_label("Alice");
    assert_eq!(
        unnormalized["name"],
        json!(format!(
            "[{}].parent.eth",
            &format!("{:#x}", alloy_primitives::keccak256(b"Alice"))[2..]
        ))
    );
    assert_eq!(unnormalized["name"], unnormalized["display_name"]);
    assert_eq!(unnormalized["namehash"], json!(namehash_of("Alice.parent.eth")));
    assert_ne!(unnormalized["namehash"], json!(namehash_of("alice.parent.eth")));

    // A normalizer error gates the same way.
    let errored = row_for_label("Ni\u{200d}ck");
    assert_eq!(
        errored["name"],
        json!(format!(
            "[{}].parent.eth",
            &format!("{:#x}", alloy_primitives::keccak256("Ni\u{200d}ck".as_bytes()))[2..]
        ))
    );
    assert_eq!(
        errored["namehash"],
        json!(namehash_of("Ni\u{200d}ck.parent.eth"))
    );

    database.cleanup().await
}

fn namehash_of(name: &str) -> String {
    let labels = name.split('.').map(str::as_bytes).collect::<Vec<_>>();
    format!("{:#x}", bigname_storage::ens_namehash_label_bytes(&labels))
}

/// Seeds the shape Project writes for a proof-checked preimage: the raw label bytes plus, only
/// when the label's text passes normalization, the composed name columns. A label whose decoded
/// text fails normalization keeps its raw bytes but is written with both name columns null, so
/// serving falls to the placeholder.
async fn seed_v2_subnames_preimage_child(
    database: &TestDatabase,
    parent_name: &str,
    label: &str,
    verdict_true: bool,
) -> Result<()> {
    let parent_logical_name_id: String = sqlx::query_scalar(
        "SELECT logical_name_id FROM bigname_phase.name_surfaces WHERE raw_name = $1",
    )
    .bind(parent_name)
    .fetch_one(&database.pool)
    .await?;
    let (chain_positions, canonicality_summary): (Value, Value) = sqlx::query_as(
        "SELECT chain_positions, canonicality_summary FROM bigname_phase.children_current \
         WHERE parent_logical_name_id = $1 LIMIT 1",
    )
    .bind(&parent_logical_name_id)
    .fetch_one(&database.pool)
    .await?;
    let mut labels = vec![label.as_bytes()];
    labels.extend(parent_name.split('.').map(str::as_bytes));
    let namehash = format!("{:#x}", bigname_storage::ens_namehash_label_bytes(&labels));
    let labelhash = format!("{:#x}", alloy_primitives::keccak256(label.as_bytes()));
    let raw_name = format!("{label}.{parent_name}");
    sqlx::query(
        r#"
        INSERT INTO bigname_phase.children_current (
            parent_logical_name_id, child_logical_name_id, surface_class, namespace,
            raw_name, decoded_name, raw_label, decoded_label, namehash, labelhash,
            provenance, chain_positions, canonicality_summary, manifest_version
        ) VALUES ($1, 'ens:' || $2, 'declared', 'ens', $3, $4, $5, $6, $2, $7,
                  jsonb_build_object('chain_id', 'ethereum-mainnet',
                                     'derivation_kind', 'children_current_rebuild'),
                  $8, $9, 1)
        "#,
    )
    .bind(&parent_logical_name_id)
    .bind(&namehash)
    .bind(verdict_true.then_some(raw_name.as_bytes()))
    .bind(verdict_true.then_some(raw_name.as_str()))
    .bind(label.as_bytes())
    .bind(verdict_true.then_some(label))
    .bind(&labelhash)
    .bind(chain_positions)
    .bind(canonicality_summary)
    .execute(&database.pool)
    .await?;
    Ok(())
}

#[tokio::test]
async fn v2_subname_counts_agree_with_the_page_when_a_child_target_is_orphaned() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_fixture(&database).await?;

    let counted = v2_subnames_payload_for_database(
        &database,
        "/v2/names/parent.eth/subnames?include=counts",
    )
    .await?;
    assert_eq!(counted["data"][0]["name"], json!("alpha.parent.eth"));
    assert_eq!(counted["data"][0]["subname_count"], json!(1));

    // Move only the grandchild row onto an orphaned projection target. Its parent and child
    // identity anchors stay canonical, so nothing but the target fence can exclude it.
    let child_logical_name_id: String = sqlx::query_scalar(
        "SELECT logical_name_id FROM bigname_phase.name_surfaces \
         WHERE raw_name = 'delta.alpha.parent.eth'",
    )
    .fetch_one(&database.pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO bigname_phase.chain_lineage (
            chain_id, block_hash, block_number, block_timestamp, canonicality_state
        ) VALUES (
            'ethereum-mainnet', '0xorphaned-grandchild-target', 2011,
            '2026-04-17T02:00:11Z', 'orphaned'::bigname_phase.canonicality_state
        );
        "#,
    )
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "UPDATE bigname_phase.children_current \
         SET chain_positions = jsonb_build_object( \
                 'block_number', 2011, \
                 'block_hash', '0xorphaned-grandchild-target', \
                 'target_block_number', 2011, \
                 'target_block_hash', '0xorphaned-grandchild-target' \
             ), \
             canonicality_summary = jsonb_build_object( \
                 'state', 'canonical', \
                 'target_block_number', 2011, \
                 'target_block_hash', '0xorphaned-grandchild-target' \
             ) \
         WHERE child_logical_name_id = $1",
    )
    .bind(&child_logical_name_id)
    .execute(&database.pool)
    .await?;

    let page = v2_subnames_payload_for_database(
        &database,
        "/v2/names/alpha.parent.eth/subnames?page_size=10",
    )
    .await?;
    assert_eq!(page["data"], json!([]));

    let recounted = v2_subnames_payload_for_database(
        &database,
        "/v2/names/parent.eth/subnames?include=counts",
    )
    .await?;
    assert_eq!(recounted["data"][0]["name"], json!("alpha.parent.eth"));
    assert_eq!(recounted["data"][0]["subname_count"], json!(0));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_record_inventory_reads_exclude_orphaned_phase_resource_lineage() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_alice_name_record_fixture(&database, |_| {}, |_, _, _| {}).await?;
    let resource_id = Uuid::from_u128(0x2200);
    let boundary: Value = sqlx::query_scalar(
        "SELECT record_version_boundary FROM bigname_phase.record_inventory_current \
         WHERE resource_id = $1",
    )
    .bind(resource_id)
    .fetch_one(&database.pool)
    .await?;
    let mut pointerless_boundary = boundary.clone();
    let pointerless_boundary_object = pointerless_boundary
        .as_object_mut()
        .context("record inventory boundary must be an object")?;
    pointerless_boundary_object.insert("normalized_event_id".to_owned(), Value::Null);
    pointerless_boundary_object.insert("event_kind".to_owned(), Value::Null);
    assert!(
        bigname_storage::load_record_inventory_current_with_anchor_fallback(
            &database.pool,
            resource_id,
            &pointerless_boundary,
        )
        .await?
        .is_some()
    );

    sqlx::raw_sql(
        r#"
        INSERT INTO bigname_phase.chain_lineage (
            chain_id, block_hash, block_number, block_timestamp, canonicality_state
        ) VALUES (
            'ethereum-mainnet', '0xreorg-record-resource', 1005,
            '2026-04-17T01:00:05Z', 'canonical'::bigname_phase.canonicality_state
        );
        UPDATE bigname_phase.resources
        SET block_hash = '0xreorg-record-resource', block_number = 1005,
            canonicality_state = 'canonical'::bigname_phase.canonicality_state
        WHERE resource_id = '00000000-0000-0000-0000-000000002200'::uuid;
        UPDATE bigname_phase.chain_lineage
        SET canonicality_state = 'orphaned'::bigname_phase.canonicality_state
        WHERE chain_id = 'ethereum-mainnet' AND block_hash = '0xreorg-record-resource'
        "#,
    )
    .execute(&database.pool)
    .await?;

    assert!(
        bigname_storage::load_record_inventory_current(&database.pool, resource_id, &boundary)
            .await?
            .is_none()
    );
    assert!(
        bigname_storage::load_record_inventory_current_with_anchor_fallback(
            &database.pool,
            resource_id,
            &pointerless_boundary,
        )
        .await?
        .is_none()
    );
    assert_eq!(
        bigname_storage::count_record_inventory_selectors_by_lookup_keys(
            &database.pool,
            &[(resource_id, boundary)],
        )
        .await?,
        vec![None]
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_record_inventory_reads_exclude_orphaned_project_target_before_redo() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_alice_name_record_fixture(&database, |_| {}, |_, _, _| {}).await?;
    let resource_id = Uuid::from_u128(0x2200);
    let boundary: Value = sqlx::query_scalar(
        "SELECT record_version_boundary FROM bigname_phase.record_inventory_current \
         WHERE resource_id = $1",
    )
    .bind(resource_id)
    .fetch_one(&database.pool)
    .await?;
    let mut pointerless_boundary = boundary.clone();
    let pointerless_boundary_object = pointerless_boundary
        .as_object_mut()
        .context("record inventory boundary must be an object")?;
    pointerless_boundary_object.insert("normalized_event_id".to_owned(), Value::Null);
    pointerless_boundary_object.insert("event_kind".to_owned(), Value::Null);

    sqlx::raw_sql(
        r#"
        INSERT INTO bigname_phase.chain_lineage (
            chain_id, block_hash, block_number, block_timestamp, canonicality_state
        ) VALUES (
            'ethereum-mainnet', '0xproject-record-target', 2020,
            '2026-04-17T02:00:20Z', 'canonical'
        );
        UPDATE bigname_phase.record_inventory_current
        SET chain_positions = jsonb_build_object(
                'block_number', 2020,
                'block_hash', '0xproject-record-target',
                'target_block_number', 2020,
                'target_block_hash', '0xproject-record-target'
            ),
            canonicality_summary = jsonb_build_object(
                'state', 'canonical_lineage',
                'target_block_number', 2020,
                'target_block_hash', '0xproject-record-target'
            )
        WHERE resource_id = '00000000-0000-0000-0000-000000002200'::uuid;
        "#,
    )
    .execute(&database.pool)
    .await?;

    let target_is_not_the_resource_anchor: bool = sqlx::query_scalar(
        "SELECT NOT EXISTS ( \
             SELECT 1 FROM bigname_phase.resources \
             WHERE resource_id = $1 AND block_hash = '0xproject-record-target' \
         )",
    )
    .bind(resource_id)
    .fetch_one(&database.pool)
    .await?;
    assert!(target_is_not_the_resource_anchor);
    assert!(
        bigname_storage::load_record_inventory_current(&database.pool, resource_id, &boundary)
            .await?
            .is_some()
    );
    assert!(
        bigname_storage::load_record_inventory_current_with_anchor_fallback(
            &database.pool,
            resource_id,
            &pointerless_boundary,
        )
        .await?
        .is_some()
    );

    sqlx::query(
        "UPDATE bigname_phase.chain_lineage \
         SET canonicality_state = 'orphaned' \
         WHERE chain_id = 'ethereum-mainnet' \
           AND block_hash = '0xproject-record-target'",
    )
    .execute(&database.pool)
    .await?;

    assert!(
        bigname_storage::load_record_inventory_current(&database.pool, resource_id, &boundary)
            .await?
            .is_none()
    );
    assert!(
        bigname_storage::load_record_inventory_current_with_anchor_fallback(
            &database.pool,
            resource_id,
            &pointerless_boundary,
        )
        .await?
        .is_none()
    );
    assert_eq!(
        bigname_storage::count_record_inventory_selectors_by_lookup_keys(
            &database.pool,
            &[(resource_id, boundary)],
        )
        .await?,
        vec![None]
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_subnames_parent_with_zero_children_returns_empty_page() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_parent(&database, "ens:empty.eth", "empty.eth", "node:empty.eth", 80).await?;

    let payload =
        v2_subnames_payload_for_database(&database, "/v2/names/empty.eth/subnames").await?;

    assert_eq!(payload["data"], json!([]));
    assert_eq!(payload["page"]["has_more"], json!(false));
    assert_eq!(payload["page"]["next_cursor"], Value::Null);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_subnames_missing_parent_returns_not_found() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.seed_default_ens_snapshot_selector_position().await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v2/names/missing.eth/subnames")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 missing parent subnames request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("not_found"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_subnames_rejects_malformed_cursor() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_fixture(&database).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v2/names/parent.eth/subnames?cursor=not-a-cursor")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 malformed subnames cursor request failed")?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["error"]["code"], json!("invalid_input"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_subnames_rejects_wrong_sort_but_ignores_legacy_snapshot_component() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_fixture(&database).await?;

    let wrong_sort = crate::v2::encode(&crate::v2::CursorPayload::new(
        "wrong",
        BTreeMap::from([
            ("namespace".to_owned(), "ens".to_owned()),
            ("parent".to_owned(), "ens:parent.eth".to_owned()),
        ]),
        BTreeMap::from([
            ("display_name".to_owned(), "alpha.parent.eth".to_owned()),
            (
                "child_logical_name_id".to_owned(),
                "ens:alpha.parent.eth".to_owned(),
            ),
        ]),
        Some("wrong-snapshot".to_owned()),
    ));
    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v2/names/parent.eth/subnames?cursor={wrong_sort}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 wrong-sort subnames cursor request failed")?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let legacy_snapshot = crate::v2::encode(&crate::v2::CursorPayload::new(
        "display_name_asc",
        BTreeMap::from([
            ("namespace".to_owned(), "ens".to_owned()),
            (
                "parent".to_owned(),
                bigname_storage::logical_name_id_for_name("ens", "parent.eth"),
            ),
        ]),
        BTreeMap::from([
            ("display_name".to_owned(), "alpha.parent.eth".to_owned()),
            (
                "child_logical_name_id".to_owned(),
                bigname_storage::logical_name_id_for_name("ens", "alpha.parent.eth"),
            ),
        ]),
        Some("legacy-snapshot".to_owned()),
    ));
    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v2/names/parent.eth/subnames?cursor={legacy_snapshot}"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 legacy-snapshot subnames cursor request failed")?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["data"][0]["name"], json!("beta.parent.eth"));
    assert_eq!(payload["meta"], json!({}));

    database.cleanup().await?;
    Ok(())
}

const V2_MAINNET_SNAPSHOT_NAME: &str = "mainnet-pin.eth";
const V2_MAINNET_SNAPSHOT_HASH: &str = "0xv2-mainnet-pin";
const V2_MAINNET_SNAPSHOT_BLOCK: i64 = 21_000_011;
const V2_MAINNET_SNAPSHOT_TIMESTAMP: &str = "2026-04-17T00:00:11Z";
const V2_SEPOLIA_SNAPSHOT_NAME: &str = "sepolia-pin.eth";
const V2_SEPOLIA_SNAPSHOT_HASH: &str = "0xv2-sepolia-pin";
const V2_SEPOLIA_SNAPSHOT_BLOCK: i64 = 111_551_110;
const V2_SEPOLIA_SNAPSHOT_TIMESTAMP: &str = "2026-04-17T00:10:10Z";
const V2_SEPOLIA_ONLY_SNAPSHOT_NAME: &str = "sepolia-only.eth";
const V2_SEPOLIA_ONLY_SNAPSHOT_HASH: &str = "0xv2-sepolia-only";
const V2_SEPOLIA_ONLY_SNAPSHOT_BLOCK: i64 = 111_551_120;
const V2_SEPOLIA_ONLY_SNAPSHOT_TIMESTAMP: &str = "2026-04-17T00:10:20Z";

async fn seed_v2_mixed_phase_head_names(database: &TestDatabase) -> Result<()> {
    seed_v2_snapshot_profile_name(
        database,
        V2_SEPOLIA_SNAPSHOT_NAME,
        "SepoliaPin.eth",
        "namehash:sepolia-pin.eth",
        Uuid::from_u128(0x7e20),
        Uuid::from_u128(0x7e21),
        Uuid::from_u128(0x7e22),
        "ethereum-sepolia",
        "ethereum-sepolia",
        V2_SEPOLIA_SNAPSHOT_BLOCK,
        V2_SEPOLIA_SNAPSHOT_HASH,
        V2_SEPOLIA_SNAPSHOT_TIMESTAMP,
    )
    .await?;
    seed_v2_snapshot_profile_name(
        database,
        V2_MAINNET_SNAPSHOT_NAME,
        "MainnetPin.eth",
        "namehash:mainnet-pin.eth",
        Uuid::from_u128(0x7e10),
        Uuid::from_u128(0x7e11),
        Uuid::from_u128(0x7e12),
        "ethereum",
        "ethereum-mainnet",
        V2_MAINNET_SNAPSHOT_BLOCK,
        V2_MAINNET_SNAPSHOT_HASH,
        V2_MAINNET_SNAPSHOT_TIMESTAMP,
    )
    .await
}

async fn seed_v2_sepolia_only_phase_head_name(database: &TestDatabase) -> Result<()> {
    seed_v2_snapshot_profile_name(
        database,
        V2_SEPOLIA_ONLY_SNAPSHOT_NAME,
        "SepoliaOnly.eth",
        "namehash:sepolia-only.eth",
        Uuid::from_u128(0x7e30),
        Uuid::from_u128(0x7e31),
        Uuid::from_u128(0x7e32),
        "ethereum-sepolia",
        "ethereum-sepolia",
        V2_SEPOLIA_ONLY_SNAPSHOT_BLOCK,
        V2_SEPOLIA_ONLY_SNAPSHOT_HASH,
        V2_SEPOLIA_ONLY_SNAPSHOT_TIMESTAMP,
    )
    .await?;
    sqlx::query("DELETE FROM chain_phase_state WHERE chain_id = 'ethereum-mainnet'")
        .execute(&database.lookup_pool)
        .await
        .context("failed to remove mainnet project state for sepolia-only v2 snapshot test")?;
    sqlx::query("DELETE FROM chain_heads WHERE chain_id = 'ethereum-mainnet'")
        .execute(&database.lookup_pool)
        .await
        .context("failed to remove mainnet phase head for sepolia-only v2 snapshot test")?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn seed_v2_snapshot_profile_name(
    database: &TestDatabase,
    normalized_name: &str,
    display_name: &str,
    namehash: &str,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
    slot: &str,
    chain_id: &str,
    block_number: i64,
    block_hash: &str,
    timestamp: &str,
) -> Result<()> {
    let fixture_logical_name_id = format!("ens:{normalized_name}");
    let identity_block_number = block_number.rem_euclid(100);
    let mut surface = collection_name_surface(
        &fixture_logical_name_id,
        display_name,
        namehash,
        identity_block_number,
    );
    surface.normalized_name = normalized_name.to_owned();
    surface.dns_encoded_name = normalized_name.as_bytes().to_vec();
    surface.labelhashes = labelhash_for_display_name(normalized_name)
        .into_iter()
        .collect();
    surface.chain_id = chain_id.to_owned();
    surface.block_hash = format!("0xsurface{identity_block_number:02x}");
    upsert_test_name_surfaces(&database.pool, &[surface]).await?;

    let mut token_lineage = address_name_token_lineage(
        token_lineage_id,
        &format!("0xtoken{identity_block_number:02x}"),
        identity_block_number,
    );
    token_lineage.chain_id = chain_id.to_owned();
    upsert_test_token_lineages(&database.pool, &[token_lineage]).await?;

    let mut resource = address_name_resource(
        resource_id,
        Some(token_lineage_id),
        &format!("0xresource{identity_block_number:02x}"),
        identity_block_number,
    );
    resource.chain_id = chain_id.to_owned();
    upsert_test_resources(&database.pool, &[resource]).await?;

    let mut binding = address_name_surface_binding(
        surface_binding_id,
        &fixture_logical_name_id,
        resource_id,
        &format!("0xbinding{identity_block_number:02x}"),
        identity_block_number,
        1_717_176_000 + identity_block_number,
    );
    binding.chain_id = chain_id.to_owned();
    upsert_test_surface_bindings(&database.pool, &[binding]).await?;

    upsert_phase_name_current_rows(
        &database.pool,
        &[v2_subnames_name_current_row(
            &fixture_logical_name_id,
            display_name,
            namehash,
            identity_block_number,
            Some(surface_binding_id),
            Some(resource_id),
            Some(token_lineage_id),
            json!({
            "registration": {
                "status": "active",
                "authority_kind": "ens_v2_registry"
            },
            "control": {
                "registry_owner": "0x0000000000000000000000000000000000000001"
            },
            "resolver": {
                "chain_id": chain_id,
                "address": "0x0000000000000000000000000000000000000abc",
                "latest_event_kind": "ResolverChanged"
            }
            }),
        )],
    )
    .await?;

    let logical_name_id = bigname_storage::logical_name_id_for_name("ens", normalized_name);
    let mut row = bigname_storage::load_name_current(&database.pool, &logical_name_id)
        .await
        .with_context(|| format!("failed to load v2 snapshot fixture row {logical_name_id}"))?
        .with_context(|| format!("v2 snapshot fixture row {logical_name_id} was not inserted"))?;
    row.chain_positions = v2_snapshot_chain_positions(slot, chain_id, block_number, block_hash, timestamp);
    row.canonicality_summary = json!({
        "status": "finalized",
        "chains": {
            chain_id: "finalized"
        }
    });
    row.declared_summary["resolver"]["chain_id"] = json!(chain_id);
    row.provenance["manifest_versions"] = json!([
        {
            "manifest_version": 3,
            "source_family": "ens_v2_registry_l1",
            "chain": chain_id,
            "deployment_epoch": "ens_v2"
        }
    ]);
    database.insert_name_current_row(row).await
}

fn v2_snapshot_chain_positions(
    slot: &str,
    chain_id: &str,
    block_number: i64,
    block_hash: &str,
    timestamp: &str,
) -> Value {
    json!({
        slot: {
            "chain_id": chain_id,
            "block_number": block_number,
            "block_hash": block_hash,
            "timestamp": timestamp
        }
    })
}

fn v2_sepolia_snapshot_token() -> String {
    v2_at_token(
        "ethereum-sepolia",
        "ethereum-sepolia",
        V2_SEPOLIA_SNAPSHOT_BLOCK,
        V2_SEPOLIA_SNAPSHOT_HASH,
        V2_SEPOLIA_SNAPSHOT_TIMESTAMP,
    )
    .expect("sepolia snapshot token fixture must encode")
}

fn v2_at_token_from_meta_as_of(
    payload: &Value,
    numeric_chain_id: &str,
    slot: &str,
    chain_id: &str,
) -> Result<String> {
    let as_of = payload
        .pointer(&format!("/meta/as_of/{numeric_chain_id}"))
        .with_context(|| format!("response must include meta.as_of[{numeric_chain_id}]"))?;
    let block_number = as_of
        .get("block_number")
        .and_then(Value::as_i64)
        .context("meta.as_of block_number must be an i64")?;
    let block_hash = as_of
        .get("block_hash")
        .and_then(Value::as_str)
        .context("meta.as_of block_hash must be a string")?;
    let timestamp = as_of
        .get("timestamp")
        .and_then(Value::as_str)
        .context("meta.as_of timestamp must be a string")?;

    v2_at_token(slot, chain_id, block_number, block_hash, timestamp)
}

fn v2_at_token(
    slot: &str,
    chain_id: &str,
    block_number: i64,
    block_hash: &str,
    timestamp: &str,
) -> Result<String> {
    let position = bigname_storage::ChainPosition {
        slot: slot.to_owned(),
        chain_id: chain_id.to_owned(),
        block_number,
        block_hash: block_hash.to_owned(),
        timestamp: bigname_storage::parse_rfc3339_utc_timestamp(timestamp)
            .map_err(|error| anyhow::anyhow!("{error}"))?,
    };
    let selected = bigname_storage::SelectedSnapshot {
        chain_positions: bigname_storage::ChainPositions::new(std::collections::BTreeMap::from([
            (slot.to_owned(), position),
        ])),
        consistency: bigname_storage::SnapshotConsistency::Head,
    };
    Ok(crate::v2::encode_at_token(&selected))
}

async fn v2_name_record_payload(uri: &str) -> Result<Value> {
    v2_name_record_payload_with_row(uri, |_| {}).await
}

async fn v2_name_record_payload_for_database(
    database: &TestDatabase,
    uri: &str,
) -> Result<Value> {
    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 name record request failed")?;

    let status = response.status();
    let payload = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "unexpected response: {payload}");
    Ok(payload)
}

async fn v2_name_record_payload_with_row(
    uri: &str,
    configure_row: impl FnOnce(&mut bigname_storage::NameCurrentRow),
) -> Result<Value> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_alice_name_record_fixture(&database, configure_row, |_, _, _| {}).await?;

    let payload = v2_name_record_payload_for_database(&database, uri).await?;

    database.cleanup().await?;
    Ok(payload)
}

async fn seed_v2_alice_name_record_fixture(
    database: &TestDatabase,
    configure_row: impl FnOnce(&mut bigname_storage::NameCurrentRow),
    configure_inventory: impl FnOnce(&str, Uuid, &mut bigname_storage::RecordInventoryCurrentRow),
) -> Result<()> {
    seed_v2_alice_name_record_fixture_with_binding_mode(
        database,
        configure_row,
        configure_inventory,
        false,
    )
    .await
}

async fn seed_v2_alice_name_record_fixture_migrated(
    database: &TestDatabase,
    configure_row: impl FnOnce(&mut bigname_storage::NameCurrentRow),
    configure_inventory: impl FnOnce(&str, Uuid, &mut bigname_storage::RecordInventoryCurrentRow),
) -> Result<()> {
    seed_v2_alice_name_record_fixture_with_binding_mode(
        database,
        configure_row,
        configure_inventory,
        true,
    )
    .await
}

async fn seed_v2_alice_name_record_fixture_with_binding_mode(
    database: &TestDatabase,
    configure_row: impl FnOnce(&mut bigname_storage::NameCurrentRow),
    configure_inventory: impl FnOnce(&str, Uuid, &mut bigname_storage::RecordInventoryCurrentRow),
    migrated_binding: bool,
) -> Result<()> {
    let logical_name_id = "ens:alice.eth";
    let resource_id = if migrated_binding {
        Uuid::from_u128(0xc200_0000_0000_0000_0000_0000_0000_0101)
    } else {
        Uuid::from_u128(0x2200)
    };
    let token_lineage_id = Uuid::from_u128(0x1100);
    let surface_binding_id = if migrated_binding {
        Uuid::from_u128(0xc200_0000_0000_0000_0000_0000_0000_0102)
    } else {
        Uuid::from_u128(0x3300)
    };

    if !migrated_binding {
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
    }

    let mut row = exact_name_row(
        logical_name_id,
        surface_binding_id,
        resource_id,
        token_lineage_id,
    );
    if migrated_binding {
        let (declared_summary, provenance): (Value, Value) = sqlx::query_as(
            "SELECT declared_summary, provenance FROM bigname_phase.name_current
             WHERE namespace = 'ens' AND lower(raw_name) = 'alice.eth'",
        )
        .fetch_one(&database.pool)
        .await?;
        row.declared_summary = declared_summary;
        row.provenance = provenance;
        row.token_lineage_id = None;
    }
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

    if migrated_binding {
        sqlx::query("DELETE FROM bigname_phase.record_inventory_current WHERE resource_id = $1")
            .bind(resource_id)
            .execute(&database.pool)
            .await?;
    } else {
        let phase_logical_name_id =
            bigname_storage::logical_name_id_for_name("ens", "alice.eth");
        let chain_position: Value = sqlx::query_scalar(
            "SELECT chain_positions -> 'ethereum' FROM bigname_phase.name_current
             WHERE logical_name_id = $1",
        )
        .bind(&phase_logical_name_id)
        .fetch_one(&database.pool)
        .await?;
        let boundary = json!({
            "logical_name_id": phase_logical_name_id.clone(),
            "resource_id": resource_id,
            "normalized_event_id": null,
            "event_kind": null,
            "chain_position": chain_position,
        });
        let topology = json!({
            "registry_path": [],
            "subregistry_path": [],
            "resolver_path": [{
                "logical_name_id": phase_logical_name_id.clone(),
                "resource_id": resource_id,
                "chain_id": "ethereum-mainnet",
                "address": "0x0000000000000000000000000000000000000abc"
            }],
            "wildcard": { "source": null, "matched_labels": [] },
            "alias": { "final_target": null, "hops": [] },
            "version_boundaries": { "record_version_boundary": boundary },
            "transport": {
                "source_chain_id": null,
                "target_chain_id": null,
                "contract_address": null,
                "latest_event_kind": null
            }
        });
        sqlx::query(
            "UPDATE bigname_phase.name_current
             SET declared_summary = jsonb_set(declared_summary, '{topology}', $2)
             WHERE logical_name_id = $1",
        )
        .bind(&phase_logical_name_id)
        .bind(topology)
        .execute(&database.pool)
        .await?;
        seed_schema_v2_ens_manifest(
            &database.pool,
            "ens_execution",
            "universal_resolver",
            "0xeeeeeeee14d718c2b47d9923deab1335e144eeee",
            Uuid::from_u128(0xc200_0000_0000_0000_0000_0000_0000_0103),
            true,
        )
        .await?;
    }

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
    if migrated_binding {
        inventory.record_version_boundary = sqlx::query_scalar(
            "SELECT declared_summary #> '{topology,version_boundaries,record_version_boundary}'
             FROM bigname_phase.name_current
             WHERE namespace = 'ens' AND lower(raw_name) = 'alice.eth'",
        )
        .fetch_one(&database.pool)
        .await?;
    }
    database.insert_record_inventory_current_row(inventory).await?;

    Ok(())
}

async fn v2_name_records_payload(uri: &str) -> Result<Value> {
    v2_name_records_payload_with_setup(uri, |_, _, _| {}).await
}

async fn v2_name_records_payload_with_setup(
    uri: &str,
    configure_inventory: impl FnOnce(&str, Uuid, &mut bigname_storage::RecordInventoryCurrentRow),
) -> Result<Value> {
    v2_name_records_payload_with_row_and_setup(uri, |_| {}, configure_inventory).await
}

async fn v2_name_records_payload_with_row_and_setup(
    uri: &str,
    configure_row: impl FnOnce(&mut bigname_storage::NameCurrentRow),
    configure_inventory: impl FnOnce(&str, Uuid, &mut bigname_storage::RecordInventoryCurrentRow),
) -> Result<Value> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_alice_name_records_fixture_with_row(
        &database,
        configure_row,
        configure_inventory,
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

    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "unexpected response: {payload:#}");

    database.cleanup().await?;
    Ok(payload)
}

async fn v2_name_payload_without_inventory(uri: &str) -> Result<Value> {
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
) -> Result<()> {
    seed_v2_alice_name_records_fixture_with_row(database, |_| {}, configure_inventory)
        .await
}

async fn seed_v2_alice_name_records_fixture_with_row(
    database: &TestDatabase,
    configure_row: impl FnOnce(&mut bigname_storage::NameCurrentRow),
    configure_inventory: impl FnOnce(&str, Uuid, &mut bigname_storage::RecordInventoryCurrentRow),
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

    Ok(())
}

async fn v2_subnames_payload(uri: &str) -> Result<(TestDatabase, Value)> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_fixture(&database).await?;
    let payload = v2_subnames_payload_for_database(&database, uri).await?;
    Ok((database, payload))
}

async fn v2_subnames_payload_for_database(database: &TestDatabase, uri: &str) -> Result<Value> {
    let response = v2_subnames_response_for_database(database, uri).await?;

    assert_eq!(response.status(), StatusCode::OK);
    read_json(response).await
}

async fn v2_subnames_response_for_database(
    database: &TestDatabase,
    uri: &str,
) -> Result<Response> {
    app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("v2 subnames request failed")
}

async fn seed_v2_subnames_fixture(database: &TestDatabase) -> Result<()> {
    seed_v2_subnames_parent(database, "ens:parent.eth", "parent.eth", "node:parent.eth", 80)
        .await?;
    seed_v2_subnames_bound_child(
        database,
        "ens:alpha.parent.eth",
        "Alpha.Parent.eth",
        "node:alpha.parent.eth",
        81,
        Uuid::from_u128(0x4010),
        Uuid::from_u128(0x5010),
        Uuid::from_u128(0x6010),
        json!({
            "registration": {
                "status": "active",
                "authority_kind": "registrar",
                "registrant": "0x00000000000000000000000000000000000000aB",
                "registered_at": "2024-01-02T03:04:05Z",
                "created_at": "2023-01-02T03:04:05Z",
                "expiry": "2027-01-02T03:04:05Z"
            },
            "control": {
                "registry_owner": "0x00000000000000000000000000000000000000aA"
            }
        }),
    )
    .await?;
    seed_v2_subnames_bound_child(
        database,
        "ens:beta.parent.eth",
        "beta.parent.eth",
        "node:beta.parent.eth",
        82,
        Uuid::from_u128(0x4020),
        Uuid::from_u128(0x5020),
        Uuid::from_u128(0x6020),
        json!({
            "registration": {
                "status": "released",
                "authority_kind": "registrar",
                "released_at": "2026-02-03T04:05:06Z",
                "registrant": "0x00000000000000000000000000000000000000bB"
            },
            "control": {
                "registry_owner": "0x00000000000000000000000000000000000000bA"
            }
        }),
    )
    .await?;

    upsert_test_name_surfaces(
        &database.pool,
        &[collection_name_surface(
            "ens:gamma.parent.eth",
            "gamma.parent.eth",
            "node:gamma.parent.eth",
            83,
        )],
    )
    .await?;
    database
        .insert_name_current_row(v2_subnames_name_current_row(
            "ens:gamma.parent.eth",
            "gamma.parent.eth",
            "node:gamma.parent.eth",
            83,
            None,
            None,
            None,
            json!({}),
        ))
        .await?;

    upsert_test_name_surfaces(
        &database.pool,
        &[collection_name_surface(
            "ens:delta.alpha.parent.eth",
            "delta.alpha.parent.eth",
            "node:delta.alpha.parent.eth",
            84,
        )],
    )
    .await?;

    upsert_phase_children_current_rows(
        &database.pool,
        &[
            v2_subnames_declared_child_row(
                "ens:parent.eth",
                "ens:gamma.parent.eth",
                "gamma.parent.eth",
                "node:gamma.parent.eth",
                903,
                83,
            ),
            v2_subnames_declared_child_row(
                "ens:parent.eth",
                "ens:beta.parent.eth",
                "beta.parent.eth",
                "node:beta.parent.eth",
                902,
                82,
            ),
            v2_subnames_declared_child_row(
                "ens:parent.eth",
                "ens:alpha.parent.eth",
                "Alpha.Parent.eth",
                "node:alpha.parent.eth",
                901,
                81,
            ),
            v2_subnames_declared_child_row(
                "ens:alpha.parent.eth",
                "ens:delta.alpha.parent.eth",
                "delta.alpha.parent.eth",
                "node:delta.alpha.parent.eth",
                904,
                84,
            ),
        ],
    )
    .await?;
    sqlx::query(
        "UPDATE bigname_phase.children_current
         SET owner = '0x00000000000000000000000000000000000000cc'
         WHERE decoded_name = 'gamma.parent.eth'",
    )
    .execute(&database.pool)
    .await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 80,
                "block_hash": "0xname50",
                "timestamp": "2026-04-17T00:00:20Z"
            }
        }))
        .await?;

    Ok(())
}

/// Seeds the shape Project writes for a preimage whose label bytes do not decode: raw bytes
/// stored with no decoded form.
async fn seed_v2_subnames_undecodable_child(
    database: &TestDatabase,
    parent_name: &str,
    labelhash: &str,
) -> Result<()> {
    let parent_logical_name_id: String = sqlx::query_scalar(
        "SELECT logical_name_id FROM bigname_phase.name_surfaces WHERE raw_name = $1",
    )
    .bind(parent_name)
    .fetch_one(&database.pool)
    .await?;
    let (chain_positions, canonicality_summary): (Value, Value) = sqlx::query_as(
        "SELECT chain_positions, canonicality_summary FROM bigname_phase.children_current \
         WHERE parent_logical_name_id = $1 AND raw_name IS NOT NULL LIMIT 1",
    )
    .bind(&parent_logical_name_id)
    .fetch_one(&database.pool)
    .await?;
    let namehash = format!("node:undecodable-{}", labelhash.trim_start_matches("0x"));
    // A high-bit byte and a control byte: PostgreSQL's `escape` encoding octal-escapes the first
    // and passes the second through, which is the half of the documented rule easiest to get wrong.
    let raw_name = [&[0xffu8, 0x09][..], b"Bad.", parent_name.as_bytes()].concat();
    sqlx::query(
        r#"
        INSERT INTO bigname_phase.children_current (
            parent_logical_name_id, child_logical_name_id, surface_class, namespace,
            raw_name, namehash, labelhash, provenance, chain_positions,
            canonicality_summary, manifest_version
        ) VALUES ($1, 'ens:' || $2, 'declared', 'ens', $3, $2, $4,
                  jsonb_build_object('chain_id', 'ethereum-mainnet',
                                     'derivation_kind', 'children_current_rebuild'),
                  $5, $6, 1)
        "#,
    )
    .bind(&parent_logical_name_id)
    .bind(&namehash)
    .bind(&raw_name)
    .bind(labelhash)
    .bind(chain_positions)
    .bind(canonicality_summary)
    .execute(&database.pool)
    .await?;
    Ok(())
}

/// Seeds the shape Project writes for a registry edge whose label was never observed: every name
/// column null and no child `name_surfaces` row.
async fn seed_v2_subnames_topology_only_child(
    database: &TestDatabase,
    parent_name: &str,
    labelhash: &str,
) -> Result<()> {
    let parent_logical_name_id: String = sqlx::query_scalar(
        "SELECT logical_name_id FROM bigname_phase.name_surfaces WHERE raw_name = $1",
    )
    .bind(parent_name)
    .fetch_one(&database.pool)
    .await?;
    let (chain_positions, canonicality_summary): (Value, Value) = sqlx::query_as(
        "SELECT chain_positions, canonicality_summary FROM bigname_phase.children_current \
         WHERE parent_logical_name_id = $1 LIMIT 1",
    )
    .bind(&parent_logical_name_id)
    .fetch_one(&database.pool)
    .await?;
    let namehash = format!("node:unobserved-{}", labelhash.trim_start_matches("0x"));
    sqlx::query(
        r#"
        INSERT INTO bigname_phase.children_current (
            parent_logical_name_id, child_logical_name_id, surface_class, namespace,
            namehash, labelhash, provenance, chain_positions, canonicality_summary,
            manifest_version
        ) VALUES ($1, 'ens:' || $2, 'declared', 'ens', $2, $3,
                  jsonb_build_object('chain_id', 'ethereum-mainnet',
                                     'derivation_kind', 'children_current_rebuild',
                                     'coverage', jsonb_build_object(
                                         'status', 'projected',
                                         'exhaustiveness', 'not_asserted')),
                  $4, $5, 1)
        "#,
    )
    .bind(&parent_logical_name_id)
    .bind(&namehash)
    .bind(labelhash)
    .bind(chain_positions)
    .bind(canonicality_summary)
    .execute(&database.pool)
    .await?;
    Ok(())
}

async fn seed_v2_subnames_parent(
    database: &TestDatabase,
    logical_name_id: &str,
    display_name: &str,
    namehash: &str,
    block_number: i64,
) -> Result<()> {
    seed_v2_subnames_bound_child(
        database,
        logical_name_id,
        display_name,
        namehash,
        block_number,
        Uuid::from_u128(0x4000),
        Uuid::from_u128(0x5000),
        Uuid::from_u128(0x6000),
        json!({
            "registration": {
                "status": "active",
                "authority_kind": "registrar"
            },
            "control": {
                "registry_owner": "0x0000000000000000000000000000000000000001"
            }
        }),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn seed_v2_subnames_bound_child(
    database: &TestDatabase,
    logical_name_id: &str,
    display_name: &str,
    namehash: &str,
    block_number: i64,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
    declared_summary: Value,
) -> Result<()> {
    let normalized_name = normalized_name_from_logical_name_id(logical_name_id);
    let surface_block_number = block_number.saturating_sub(3);
    let token_block_number = block_number.saturating_sub(2);
    let resource_block_number = block_number.saturating_sub(1);
    let mut surface = collection_name_surface(
        logical_name_id,
        display_name,
        namehash,
        surface_block_number,
    );
    surface.normalized_name = normalized_name.to_owned();
    surface.dns_encoded_name = normalized_name.as_bytes().to_vec();
    surface.labelhashes = labelhash_for_display_name(normalized_name)
        .into_iter()
        .collect();

    upsert_test_name_surfaces(
        &database.pool,
        &[surface],
    )
    .await?;
    upsert_test_token_lineages(
        &database.pool,
        &[address_name_token_lineage(
            token_lineage_id,
            &format!("0xtoken{token_block_number:02x}"),
            token_block_number,
        )],
    )
    .await?;
    upsert_test_resources(
        &database.pool,
        &[address_name_resource(
            resource_id,
            Some(token_lineage_id),
            &format!("0xresource{resource_block_number:02x}"),
            resource_block_number,
        )],
    )
    .await?;
    upsert_test_surface_bindings(
        &database.pool,
        &[address_name_surface_binding(
            surface_binding_id,
            logical_name_id,
            resource_id,
            &format!("0xname{block_number:02x}"),
            block_number,
            1_717_176_000 + block_number,
        )],
    )
    .await?;
    database
        .insert_name_current_row(v2_subnames_name_current_row(
            logical_name_id,
            display_name,
            namehash,
            block_number,
            Some(surface_binding_id),
            Some(resource_id),
            Some(token_lineage_id),
            declared_summary,
        ))
        .await
}

fn v2_subnames_declared_child_row(
    parent_logical_name_id: &str,
    child_logical_name_id: &str,
    display_name: &str,
    namehash: &str,
    normalized_event_id: i64,
    block_number: i64,
) -> bigname_storage::ChildrenCurrentRow {
    let mut row = declared_child_row(
        parent_logical_name_id,
        child_logical_name_id,
        display_name,
        namehash,
        normalized_event_id,
        block_number,
    );
    row.normalized_name = normalized_name_from_logical_name_id(child_logical_name_id).to_owned();
    row.labelhash = labelhash_for_display_name(&row.normalized_name);
    row
}

#[allow(clippy::too_many_arguments)]
fn v2_subnames_name_current_row(
    logical_name_id: &str,
    display_name: &str,
    namehash: &str,
    block_number: i64,
    surface_binding_id: Option<Uuid>,
    resource_id: Option<Uuid>,
    token_lineage_id: Option<Uuid>,
    declared_summary: Value,
) -> bigname_storage::NameCurrentRow {
    let (namespace, normalized_name) = split_logical_name_id(logical_name_id);
    let chain_id = chain_id_for_namespace(namespace);
    let chain_slot = chain_slot_for_namespace(namespace);

    bigname_storage::NameCurrentRow {
        logical_name_id: logical_name_id.to_owned(),
        namespace: namespace.to_owned(),
        canonical_display_name: display_name.to_owned(),
        normalized_name: normalized_name.to_owned(),
        namehash: namehash.to_owned(),
        surface_binding_id,
        resource_id,
        serving_resource_id: None,
        token_lineage_id,
        binding_kind: surface_binding_id
            .is_some()
            .then_some(bigname_storage::SurfaceBindingKind::DeclaredRegistryPath),
        declared_summary,
        provenance: json!({
            "normalized_event_ids": [block_number],
            "raw_fact_refs": [{
                "kind": "raw_log",
                "block_number": block_number,
            }],
            "manifest_versions": [{
                "manifest_version": 1,
                "source_family": source_family_for_namespace(namespace),
                "source_manifest_id": null,
            }],
            "derivation_kind": "name_current_rebuild",
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": [source_family_for_namespace(namespace)],
            "enumeration_basis": "exact_name",
            "unsupported_reason": null,
        }),
        chain_positions: json!({
            chain_slot: {
                "chain_id": chain_id,
                "block_number": block_number,
                "block_hash": format!("0xname{block_number:02x}"),
                "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60),
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                chain_id: "finalized"
            }
        }),
        manifest_version: 1,
        last_recomputed_at: timestamp(1_717_176_000 + block_number),
    }
}

fn split_logical_name_id(logical_name_id: &str) -> (&str, &str) {
    logical_name_id
        .split_once(':')
        .expect("logical_name_id must include namespace")
}

fn normalized_name_from_logical_name_id(logical_name_id: &str) -> &str {
    split_logical_name_id(logical_name_id).1
}

fn resolution_universal_resolver_addr60_response(address: &str) -> Value {
    json!(format!(
        "0x{}{}{}{}",
        resolution_left_pad_hex("40", 64),
        resolution_padded_address_hex("0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe"),
        resolution_left_pad_hex("20", 64),
        resolution_padded_address_hex(address),
    ))
}

fn resolution_universal_resolver_text_response(text: &str) -> Value {
    let text_hex = hex::encode(text);
    let padded_text_len = text_hex.len().div_ceil(64) * 64;
    let inner_length = 64 + padded_text_len / 2;
    json!(format!(
        "0x{}{}{}{}{}{:0<padded_text_len$}",
        resolution_left_pad_hex("40", 64),
        resolution_padded_address_hex("0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe"),
        resolution_left_pad_hex(&format!("{inner_length:x}"), 64),
        resolution_left_pad_hex("20", 64),
        resolution_left_pad_hex(&format!("{:x}", text.len()), 64),
        text_hex,
    ))
}

fn resolution_resolver_not_found_error(name: &[u8]) -> Value {
    let selector = format!(
        "{:#x}",
        alloy_primitives::keccak256("ResolverNotFound(bytes)")
    );
    let name_hex = hex::encode(name);
    let padded_name_len = name_hex.len().div_ceil(64) * 64;
    json!({
        "__rpc_error": {
            "code": -32000,
            "message": "execution reverted",
            "data": {
                "originalError": {
                    "data": format!(
                        "0x{}{}{}{:0<padded_name_len$}",
                        &selector[2..10],
                        resolution_left_pad_hex("20", 64),
                        resolution_left_pad_hex(&format!("{:x}", name.len()), 64),
                        name_hex,
                    )
                }
            }
        }
    })
}

fn resolution_universal_resolver_multicoin_response(address: &str) -> Value {
    let stripped = address
        .strip_prefix("0x")
        .expect("test address must be 0x-prefixed");
    assert_eq!(stripped.len(), 40, "test address must be 20 bytes");
    json!(format!(
        "0x{}{}{}{}{}{}",
        resolution_left_pad_hex("40", 64),
        resolution_padded_address_hex("0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe"),
        resolution_left_pad_hex("60", 64),
        resolution_left_pad_hex("20", 64),
        resolution_left_pad_hex("14", 64),
        format!("{stripped:0<64}"),
    ))
}

fn resolution_basenames_l1_addr60_response(address: &str) -> Value {
    json!(format!(
        "0x{}{}{}",
        resolution_left_pad_hex("20", 64),
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

fn assert_v2_name_snapshot_meta(payload: &Value) {
    assert!(
        payload["meta"]["as_of"].is_object(),
        "name response must include meta.as_of"
    );
    let token = payload["meta"]["as_of_token"]
        .as_str()
        .expect("name response must include meta.as_of_token");
    assert!(!token.is_empty(), "meta.as_of_token must not be empty");
    assert!(
        token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~')),
        "meta.as_of_token must be URL-safe"
    );
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
