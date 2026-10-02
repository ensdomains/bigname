// Permission history rows separate the roles a change granted and revoked from the resulting set
// when the log states the previous roles (TYR-65). Uses the registry role fixture of
// v2_address_names_roles.rs.

/// ROLE_HOLDER's registry roles on beta.eth change from `old` to `new`, with the log's old
/// bitmap retained in the before state as the registry adapter stores it.
async fn change_role_holder_powers(database: &TestDatabase, old: Value, new: Value) -> Result<()> {
    let (block, hash) = address_fixture_head(database).await?;
    let name = bigname_storage::logical_name_id_for_name("ens", "beta.eth");
    let mut event = address_role_event(
        Some(&name),
        Uuid::from_u128(ROLE_RESOURCE),
        ROLE_HOLDER,
        false,
        new,
        block,
        &hash,
    );
    event.before_state = json!({"subject":ROLE_HOLDER, "role_bitmap":"0x01",
        "effective_powers":old});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[event]).await?;
    rebuild_address_fixture(database).await
}

#[tokio::test]
async fn v2_name_history_separates_granted_and_revoked_registry_roles() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    bind_address_name_ens_v2(&database, "beta.eth", ROLE_RESOURCE, false).await?;
    change_role_holder_powers(&database, json!([]), json!(["set_resolver"])).await?;
    change_role_holder_powers(
        &database,
        json!(["set_resolver"]),
        json!(["set_resolver", "set_subregistry"]),
    )
    .await?;
    change_role_holder_powers(
        &database,
        json!(["set_resolver", "set_subregistry"]),
        json!(["set_subregistry"]),
    )
    .await?;
    change_role_holder_powers(&database, json!(["set_subregistry"]), json!([])).await?;
    let payload = v2_history_payload_for_database(
        &database,
        "/v1/names/beta.eth/history?type=permission&include=data&order=asc",
    )
    .await?;
    let data = payload["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["data"].clone())
        .collect::<Vec<_>>();
    let change = |powers: Value, added: Value, removed: Value| {
        json!({"address":ROLE_HOLDER, "grant_scope":{"kind":"registry", "detail":{}},
            "powers":powers, "added_powers":added, "removed_powers":removed})
    };
    assert_eq!(
        data,
        vec![
            change(json!(["set_resolver"]), json!(["set_resolver"]), json!([])),
            change(
                json!(["set_resolver", "set_subregistry"]),
                json!(["set_subregistry"]),
                json!([])
            ),
            change(
                json!(["set_subregistry"]),
                json!([]),
                json!(["set_resolver"])
            ),
            change(json!([]), json!([]), json!(["set_subregistry"])),
        ],
        "{payload}"
    );
    database.cleanup().await
}

#[tokio::test]
async fn v2_events_describe_admitted_registrar_controller_changes() -> Result<()> {
    use alloy_sol_types::{SolEvent, sol};
    use bigname_adapters::schema_v2::{
        AddressAdmissionInput, BatchInput, ManifestInput, RawBlockInput, RawLogInput,
        StateCacheCapacity, prepare_schema_v2_batch_incremental,
    };
    sol! {
        event ControllerAdded(address indexed controller);
        event ControllerRemoved(address indexed controller);
    }
    const CHAIN: &str = "ethereum-sepolia";
    const BLOCK: i64 = 11_820_500;
    const HASH: &str = "0xcontroller-history";
    let repository = bigname_manifests::load_repository(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia"),
    )?;
    let mut manifests = Vec::new();
    let mut admissions = Vec::new();
    let mut registrar = String::new();
    let mut controller = String::new();
    for (id, family) in [(971, "ens_v1_registrar_l1"), (972, "ens_v2_migration_l1")] {
        let manifest = &repository
            .manifests()
            .iter()
            .find(|loaded| {
                loaded.manifest.source_family == family
                    && loaded.manifest.rollout_status == bigname_manifests::RolloutStatus::Active
            })
            .context("checked-in controller manifest")?
            .manifest;
        if family == "ens_v2_migration_l1" {
            controller = manifest.correlation_addresses["ens_v1_name_wrapper"].to_ascii_lowercase();
        }
        for (index, contract) in manifest.contracts.iter().enumerate() {
            if family == "ens_v1_registrar_l1" && contract.role != "registrar" {
                continue;
            }
            if contract.role == "registrar" {
                registrar = contract.address.to_ascii_lowercase();
            }
            admissions.push(AddressAdmissionInput {
                address: contract.address.clone(),
                contract_instance_id: Uuid::from_u128(id as u128 * 100 + index as u128),
                source_manifest_id: Some(id),
                role: Some(contract.role.clone()),
                discovery_edge_kind: None,
                discovery_from_contract_instance_id: None,
                discovery_observation_key: None,
                active_from_block: contract.start_block.map(|block| block as i64),
                active_to_block: None,
            });
        }
        manifests.push(ManifestInput {
            manifest_id: id,
            manifest_version: manifest.manifest_version as i64,
            namespace: manifest.namespace.clone(),
            source_family: family.into(),
            chain_id: CHAIN.into(),
            deployment_label: manifest.deployment_epoch.clone(),
            normalizer_version: manifest.normalizer_version.clone(),
            payload_json: serde_json::to_string(manifest)?,
        });
    }
    let raw = |log: alloy_primitives::LogData, index| RawLogInput {
        chain_id: CHAIN.into(),
        block_hash: HASH.into(),
        block_number: BLOCK,
        block_timestamp: timestamp(1_800_000_000),
        canonicality_state: "canonical".into(),
        transaction_hash: "0xcontroller-transaction".into(),
        transaction_index: 0,
        log_index: index,
        emitting_address: registrar.clone(),
        topics: log
            .topics()
            .iter()
            .map(|topic| format!("{topic:#x}"))
            .collect(),
        data: log.data.to_vec(),
    };
    let stored_manifests = manifests.clone();
    let (output, _) = prepare_schema_v2_batch_incremental(
        BatchInput {
            chain_id: CHAIN.into(),
            manifests,
            admissions,
            discovery_rules: vec![],
            prior_events: vec![],
            blocks: vec![RawBlockInput {
                chain_id: CHAIN.into(),
                block_hash: HASH.into(),
                block_number: BLOCK,
                block_timestamp: timestamp(1_800_000_000),
                canonicality_state: "canonical".into(),
            }],
            raw_logs: vec![
                raw(
                    ControllerAdded {
                        controller: controller.parse()?,
                    }
                    .encode_log_data(),
                    0,
                ),
                raw(
                    ControllerRemoved {
                        controller: controller.parse()?,
                    }
                    .encode_log_data(),
                    1,
                ),
            ],
        },
        None,
        StateCacheCapacity::Unlimited,
    )?
    .finish(vec![])?;
    assert!(output.decode_skips.is_empty(), "{:?}", output.decode_skips);
    assert_eq!(
        output.normalized_events.len(),
        2,
        "{:?}",
        output.normalized_events
    );
    let database = TestDatabase::new_migrated().await?;
    for manifest in stored_manifests {
        let id = database
            .insert_manifest(
                "ens",
                &manifest.source_family,
                CHAIN,
                &manifest.deployment_label,
                manifest.manifest_version as u64,
                "active",
                &manifest.normalizer_version,
            )
            .await?;
        sqlx::query("UPDATE bigname_phase.manifest_versions SET manifest_payload = $2 WHERE manifest_id = $1")
            .bind(id).bind(serde_json::from_str::<Value>(&manifest.payload_json)?)
            .execute(&database.pool).await?;
    }
    database
        .seed_snapshot_selector_chain_positions(&json!({CHAIN:{"chain_id":CHAIN,
        "block_number":BLOCK,"block_hash":HASH,"timestamp":"2027-01-15T08:00:00Z"}}))
        .await?;
    let events = output
        .normalized_events
        .iter()
        .map(|event| {
            assert_eq!(event.event_kind, "PermissionChanged");
            assert_eq!(event.source_family, "ens_v2_migration_l1");
            assert_eq!(event.consumer_visibility, "activated");
            assert!(!event.migration_correlation_ids.is_empty());
            assert!(event.resource_id.is_none() && event.logical_name_id.is_none());
            NormalizedEvent {
                event_identity: event.event_identity.clone(),
                namespace: event.namespace.clone(),
                logical_name_id: event.logical_name_id.clone(),
                resource_id: event.resource_id,
                event_kind: event.event_kind.clone(),
                source_family: event.source_family.clone(),
                manifest_version: event.manifest_version,
                source_manifest_id: None,
                chain_id: Some(event.chain_id.clone()),
                block_number: event.block_number,
                block_hash: event.block_hash.clone(),
                transaction_hash: event.transaction_hash.clone(),
                log_index: event.log_index,
                raw_fact_ref: event.raw_fact_ref.clone(),
                derivation_kind: event.derivation_kind.clone(),
                canonicality_state: CanonicalityState::Canonical,
                before_state: event.before_state.clone(),
                after_state: event.after_state.clone(),
            }
        })
        .collect::<Vec<_>>();
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    rebuild_fixture_families(&database.pool, CHAIN, BLOCK, HASH).await?;
    // Derive the served chain from the real Sepolia declarations; the generic fixture helper
    // deliberately overrides ENS to Mainnet and would never admit this publication.
    let response = app_router(AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    ))
    .oneshot(
        Request::builder()
            .uri("/v1/events?namespace=ens&type=permission&include=data&order=asc")
            .body(Body::empty())?,
    )
    .await?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "{payload}");
    let rows = payload["data"]
        .as_array()
        .context("controller history rows")?;
    assert_eq!(rows.len(), 2, "{payload}");
    for (row, approved) in rows.iter().zip([true, false]) {
        assert_eq!(row["type"], "permission");
        assert_eq!(
            row["data"],
            json!({"address":controller,
            "grant_scope":{"kind":"registrar_controller","detail":{"registrar":{
                "chain_id":11155111,"address":registrar}}}, "approved":approved}),
            "{payload}"
        );
    }
    database.cleanup().await
}
