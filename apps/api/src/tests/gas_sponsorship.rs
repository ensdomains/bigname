fn gas_sponsorship_name_row_fixture() -> GasSponsorshipCurrentRow {
    GasSponsorshipCurrentRow {
        logical_name_id: "ens:alice.eth".to_owned(),
        namespace: "ens".to_owned(),
        normalized_name: "alice.eth".to_owned(),
        namehash: "0x787192fc5378cc32aa956ddfdedbf26b24e8d78e40109add0eea2c1a012c3dec".to_owned(),
        lease_start_at: Some(
            OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid test timestamp"),
        ),
        registered_seconds_total: 63_072_000,
        earned_updates: 10,
        spent_updates: 3,
        last_sponsored_write_at: Some(
            OffsetDateTime::from_unix_timestamp(1_752_000_000).expect("valid test timestamp"),
        ),
        provenance: json!({"derivation_kind": "gas_sponsorship", "normalized_event_ids": [1, 2]}),
        coverage: json!({
            "status": "partial",
            "exhaustiveness": "not_applicable",
            "source_classes_considered": [
                "ens_gas_sponsorship_l1",
                "ens_v1_registrar_l1",
                "ens_v2_registrar_l1",
            ],
            "enumeration_basis": "gas_sponsorship_lookup",
            "unsupported_reason": null,
        }),
        chain_positions: json!({
            "ethereum-sepolia": {
                "chain_id": "ethereum-sepolia",
                "block_number": 5_500_000,
                "block_hash": "0xblock",
                "timestamp": 1_752_000_000,
            }
        }),
        canonicality_summary: json!({"status": "canonical", "chains": {}}),
        manifest_version: 1,
        last_recomputed_at: OffsetDateTime::from_unix_timestamp(1_752_000_100)
            .expect("valid test timestamp"),
    }
}

fn gas_sponsorship_global_row_fixture() -> GasSponsorshipGlobalCurrentRow {
    GasSponsorshipGlobalCurrentRow {
        namespace: "ens".to_owned(),
        sponsored_op_count: 12,
        attributed_op_count: 10,
        failed_op_count: 2,
        gas_wei_total: "1500000000000000000".to_owned(),
        failed_gas_wei_total: "500000000000000000".to_owned(),
        usd_e8_total: "125000000000".to_owned(),
        unpriced_wei_total: "0".to_owned(),
        provenance: json!({"derivation_kind": "gas_sponsorship", "normalized_event_count": 13}),
        coverage: json!({"status": "partial"}),
        chain_positions: json!({}),
        canonicality_summary: json!({"status": "canonical", "chains": {}}),
        manifest_version: 1,
        last_recomputed_at: OffsetDateTime::from_unix_timestamp(1_752_000_200)
            .expect("valid test timestamp"),
    }
}

#[tokio::test]
async fn gas_sponsorship_returns_name_and_global_accounting() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let pool = database.app_state().pool;
    bigname_storage::upsert_gas_sponsorship_current_rows(
        &pool,
        std::slice::from_ref(&gas_sponsorship_name_row_fixture()),
    )
    .await?;
    bigname_storage::upsert_gas_sponsorship_global_current_row(
        &pool,
        &gas_sponsorship_global_row_fixture(),
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/gas-sponsorship/ens/alice.eth")
                .body(Body::empty())
                .unwrap(),
        )
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(
        payload["data"],
        json!({"namespace": "ens", "name": "alice.eth"})
    );
    assert_eq!(
        payload["name_accounting"],
        json!({
            "logical_name_id": "ens:alice.eth",
            "namehash": "0x787192fc5378cc32aa956ddfdedbf26b24e8d78e40109add0eea2c1a012c3dec",
            "lease_start_at": "2023-11-14T22:13:20Z",
            "registered_seconds_total": 63_072_000,
            "earned_updates": 10,
            "spent_updates": 3,
            "last_sponsored_write_at": "2025-07-08T18:40:00Z",
        })
    );
    assert_eq!(
        payload["global_accounting"],
        json!({
            "sponsored_op_count": 12,
            "attributed_op_count": 10,
            "failed_op_count": 2,
            "gas_wei_total": "1500000000000000000",
            "failed_gas_wei_total": "500000000000000000",
            "usd_e8_total": "125000000000",
            "unpriced_wei_total": "0",
        })
    );
    assert_eq!(payload["consistency"], json!("head"));
    assert_eq!(
        payload["coverage"]["enumeration_basis"],
        json!("gas_sponsorship_lookup")
    );
    assert_eq!(payload["last_updated"], json!("2025-07-08T18:43:20Z"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn gas_sponsorship_zero_fills_unknown_names_and_rejects_bad_input() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/gas-sponsorship/ens/unregistered.eth")
                .body(Body::empty())
                .unwrap(),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(payload["name_accounting"]["earned_updates"], json!(0));
    assert_eq!(payload["name_accounting"]["spent_updates"], json!(0));
    assert_eq!(payload["name_accounting"]["namehash"], Value::Null);
    assert_eq!(payload["global_accounting"]["sponsored_op_count"], json!(0));
    assert_eq!(payload["global_accounting"]["usd_e8_total"], json!("0"));

    let unsupported_namespace = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/gas-sponsorship/unknownns/alice.eth")
                .body(Body::empty())
                .unwrap(),
        )
        .await?;
    assert_eq!(unsupported_namespace.status(), StatusCode::NOT_FOUND);

    let unnormalized = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/gas-sponsorship/ens/Alice.eth")
                .body(Body::empty())
                .unwrap(),
        )
        .await?;
    assert_eq!(unnormalized.status(), StatusCode::BAD_REQUEST);

    database.cleanup().await?;
    Ok(())
}
