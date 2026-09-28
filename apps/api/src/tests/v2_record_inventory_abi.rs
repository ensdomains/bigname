// ABI content types on `include=inventory` (docs/api-v1-routes.md, records route). Selection
// through Project is covered in crates/project/tests/record_inventory_abi.rs and
// record_id_resolver.rs; these cases pin the public shape, availability, and batch cost.

/// The declared ENSv1 resolver `seed_identity_name` points each name at.
const ABI_RESOLVER: &str = "0x0000000000000000000000000000000000000abc";
const ABI_CHAIN: &str = "ethereum-mainnet";

struct AbiWrite<'a> {
    identity: &'a str,
    family: &'a str,
    selector: &'a str,
}

fn abi_write<'a>(identity: &'a str, selector: &'a str) -> AbiWrite<'a> {
    AbiWrite {
        identity,
        family: "abi",
        selector,
    }
}

async fn abi_head(database: &TestDatabase) -> Result<(String, i64)> {
    Ok(sqlx::query_as(
        "SELECT latest_block_hash, latest_block_number FROM chain_heads WHERE chain_id = $1",
    )
    .bind(ABI_CHAIN)
    .fetch_one(&database.lookup_pool)
    .await?)
}

/// `name`'s writes on `resolver` as the ENSv1 resolver adapter stores them. An ABI write keeps
/// its content type as both selector and value.
fn abi_write_events(
    name: &str,
    resolver: &str,
    manifest: i64,
    block: (i64, &str),
    writes: &[AbiWrite<'_>],
) -> Result<Vec<NormalizedEvent>> {
    let node = bigname_lookup::ens_namehash_hex(name)?;
    writes
        .iter()
        .map(|write| {
            let ordinal = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as i64 + 100;
            let mut event = history_event(
                &format!("abi-fixture:{name}:{}", write.identity),
                None,
                None,
                Some(ABI_CHAIN),
                Some(block.0),
                Some(block.1),
                Some(&format!("0x{:064x}", 0xab1_u64)),
                Some(ordinal),
                CanonicalityState::Canonical,
            );
            event.event_kind = "RecordChanged".into();
            event.source_family = "ens_v1_resolver_l1".into();
            event.derivation_kind = "ens_v1_unwrapped_authority".into();
            event.manifest_version = 1;
            event.source_manifest_id = Some(manifest);
            event.raw_fact_ref =
                json!({"kind":"raw_log", "emitting_address":resolver, "transaction_index":0});
            event.before_state = json!({});
            event.after_state = json!({
                "source_event": if write.family == "abi" { "ABIChanged" } else { "TextChanged" },
                "node": node,
                "resolver": resolver,
                "record_key": format!("{}:{}", write.family, write.selector),
                "record_family": write.family,
                "selector_key": write.selector,
                "value_retained": true,
                "value": if write.family == "abi" { json!(write.selector) } else { json!("https://example.test") },
            });
            Ok(event)
        })
        .collect()
}

/// Record writes on `name`'s resolver at the head block, published, returned as normalized
/// event ids in order.
async fn seed_abi_writes(
    database: &TestDatabase,
    name: &str,
    writes: &[AbiWrite<'_>],
) -> Result<Vec<i64>> {
    let (hash, number) = abi_head(database).await?;
    let manifest = declare_family_fixture_resolver(
        &database.pool,
        "ens",
        ABI_CHAIN,
        "ens_v1_resolver_l1",
        ABI_RESOLVER,
    )
    .await?;
    let events = abi_write_events(name, ABI_RESOLVER, manifest, (number, &hash), writes)?;
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    rebuild_fixture_families(&database.pool, ABI_CHAIN, number, &hash).await?;
    let mut ids = Vec::new();
    for event in &events {
        ids.push(
            sqlx::query_scalar(
                "SELECT normalized_event_id FROM normalized_events WHERE event_identity = $1",
            )
            .bind(&event.event_identity)
            .fetch_one(&database.pool)
            .await?,
        );
    }
    Ok(ids)
}

async fn seed_abi_name(database: &TestDatabase, name: &str, id: u128) -> Result<()> {
    seed_identity_name(
        database,
        &format!("ens:{name}"),
        name,
        name,
        &format!("namehash:{name}"),
        Uuid::from_u128(id),
        Uuid::from_u128(id + 1),
        Uuid::from_u128(id + 2),
        ABI_RESOLVER,
        bigname_storage::AddressNameRelation::TokenHolder,
        38,
    )
    .await
}

/// An ENSv2 name whose registry points at a manifest-declared PublicResolverV2 holding one text
/// record: a supported inventory on resolver storage with no admitted ABI event.
async fn seed_abi_public_resolver_v2_name(database: &TestDatabase, name: &str) -> Result<()> {
    const RESOLVER_V2: &str = "0x00000000000000000000000000000000000a0b2c";
    let (hash, number) = abi_head(database).await?;
    let resource = Uuid::from_u128(0x5ab900);
    let logical = seed_family_identity_inputs(
        &database.pool,
        "ens",
        name,
        ABI_CHAIN,
        number,
        &hash,
        resource,
        Uuid::from_u128(0x5ab901),
        Uuid::from_u128(0x5ab902),
        "ens_v2",
    )
    .await?;
    let payload = json!({"contracts":[{"role":"public_resolver_v2", "address":RESOLVER_V2,
        "proxy_kind":"none", "start_block":0, "read_features":[]}]});
    let manifest: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions (manifest_version, namespace, source_family, chain_id,
            deployment_label, rollout_status, normalizer_version, file_path, manifest_payload)
         VALUES (1, 'ens', 'ens_v2_resolver_l1', $1, 'fixture', 'active', $2,
            'fixture/abi-public-resolver-v2.toml', $3) RETURNING manifest_id",
    )
    .bind(ABI_CHAIN)
    .bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION)
    .bind(&payload)
    .fetch_one(&database.pool)
    .await?;
    let instance = Uuid::new_v4();
    sqlx::query("INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind) VALUES ($1, $2, 'contract')")
        .bind(instance).bind(ABI_CHAIN).execute(&database.pool).await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances (manifest_id, chain_id, declaration_kind,
         declaration_name, contract_instance_id, declared_address, role, proxy_kind)
         VALUES ($1, $2, 'contract', $3, $4, $3, 'public_resolver_v2', 'none')",
    )
    .bind(manifest)
    .bind(ABI_CHAIN)
    .bind(RESOLVER_V2)
    .bind(instance)
    .execute(&database.pool)
    .await?;
    seed_fixture_manifest_update(&database.pool, manifest, ABI_CHAIN, "ens", "ens_v2_resolver_l1", &payload)
        .await?;
    let registry_event = |identity: &str, kind: &str, log: i64, after: Value| {
        let mut event = history_event(
            &format!("abi-v2-{identity}"),
            Some(&logical),
            Some(resource),
            Some(ABI_CHAIN),
            Some(number),
            Some(&hash),
            Some("0xabi-v2"),
            Some(log),
            CanonicalityState::Canonical,
        );
        event.event_kind = kind.into();
        event.source_family = "ens_v2_registry_l1".into();
        event.derivation_kind = "ens_v2_registry_resource_surface".into();
        event.before_state = json!({});
        event.after_state = after;
        event
    };
    let node = bigname_lookup::ens_namehash_hex(name)?;
    let mut write = history_event(
        "abi-v2-text",
        None,
        None,
        Some(ABI_CHAIN),
        Some(number),
        Some(&hash),
        Some("0xabi-v2"),
        Some(2),
        CanonicalityState::Canonical,
    );
    write.event_kind = "RecordChanged".into();
    write.source_family = "ens_v2_resolver_l1".into();
    write.derivation_kind = "ens_v2_resolver".into();
    write.manifest_version = 1;
    write.source_manifest_id = Some(manifest);
    write.raw_fact_ref = json!({"kind":"raw_log", "emitting_address":RESOLVER_V2, "transaction_index":0});
    write.before_state = json!({});
    write.after_state = json!({"source_event":"TextChanged", "node":node, "resolver":RESOLVER_V2,
        "record_key":"text:url", "record_family":"text", "selector_key":"url",
        "value_retained":true, "value":"https://example.test"});
    let events = [
        registry_event("grant", "RegistrationGranted", 0,
            json!({"authority_kind":"ens_v2_registry", "status":"registered",
                "registrant":ABI_RESOLVER, "expiry":1_900_000_000_i64})),
        registry_event("pointer", "ResolverChanged", 1, json!({"node":node, "resolver":RESOLVER_V2})),
        write,
    ];
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    rebuild_fixture_families(&database.pool, ABI_CHAIN, number, &hash).await
}

/// The lookup container and the records route container for one name, asserted equal.
async fn abi_inventory_on_both_routes(database: &TestDatabase, name: &str) -> Result<Value> {
    let payload = v2_lookup_json(
        database,
        json!({"profile": "detail", "include": "inventory", "inputs": [{"name": name}]}),
    )
    .await?;
    let lookup = payload["data"][0]["record"]["inventory"].clone();
    let records = v2_get_json(database, &format!("/v1/names/{name}/records?include=inventory")).await?;
    assert_eq!(records["data"]["inventory"], lookup, "{records:#}");
    Ok(lookup)
}

#[tokio::test]
async fn abi_content_types_are_served_identically_on_lookup_and_records() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_abi_name(&database, "abi-types.eth", 0x5ab100).await?;
    seed_abi_writes(
        &database,
        "abi-types.eth",
        &[AbiWrite { identity: "text", family: "text", selector: "url" }],
    )
    .await?;
    let before = v2_get_json(&database, "/v1/names/abi-types.eth/records?include=inventory").await?;
    let wide = (1_u128 << 70).to_string();
    seed_abi_writes(
        &database,
        "abi-types.eth",
        &[
            abi_write("wide", &wide),
            abi_write("json", "1"),
            abi_write("json-again", "1"),
            abi_write("cbor", "4"),
        ],
    )
    .await?;

    let inventory = abi_inventory_on_both_routes(&database, "abi-types.eth").await?;
    // Decimal strings, deduplicated, numerically ordered, wider than 64 bits intact.
    assert_eq!(inventory["abi_content_types"], json!(["1", "4", wide]), "{inventory}");
    assert!(inventory.get("abi_unsupported_reason").is_none(), "{inventory}");
    // The ordinary keys are unchanged: ABI stays outside the grammar and the counts.
    for field in ["known_keys", "unset_keys", "unsupported_keys"] {
        assert_eq!(inventory[field], before["data"]["inventory"][field], "{field}");
    }
    assert!(
        !inventory["known_keys"].to_string().contains("abi"),
        "{inventory}"
    );
    let response = v2_get_response(&database, "/v1/names/abi-types.eth/records?keys=abi:1").await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let rejected: Value = read_json(response).await?;
    assert_eq!(rejected["error"]["code"], json!("invalid_input"));
    database.cleanup().await
}

#[tokio::test]
async fn abi_content_types_distinguish_unavailable_from_observed_empty() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_abi_name(&database, "abi-empty.eth", 0x5ab200).await?;
    seed_abi_writes(
        &database,
        "abi-empty.eth",
        &[AbiWrite { identity: "empty-text", family: "text", selector: "url" }],
    )
    .await?;
    // An ENSv1 resolver with no selected ABI write: an eligible path, observed empty.
    let inventory = abi_inventory_on_both_routes(&database, "abi-empty.eth").await?;
    assert_eq!(inventory["abi_content_types"], json!([]), "{inventory}");
    assert!(inventory.get("abi_unsupported_reason").is_none());

    // A supported inventory behind the direct PublicResolverV2 classification (role
    // public_resolver_v2) has no admitted ABI event: unavailable, not empty.
    seed_abi_public_resolver_v2_name(&database, "abi-v2.eth").await?;
    let inventory = abi_inventory_on_both_routes(&database, "abi-v2.eth").await?;
    assert_eq!(inventory["abi_content_types"], Value::Null, "{inventory}");
    assert_eq!(
        inventory["abi_unsupported_reason"],
        json!("abi_observations_not_supported")
    );
    database.cleanup().await
}

// Without an inventory row the records route keeps its container and says why; the lookup route
// omits the container. An unsupported inventory is covered with the default key set.
#[tokio::test]
async fn abi_content_types_are_withheld_without_an_inventory() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.seed_default_ens_snapshot_selector_position().await?;
    let (hash, number) = abi_head(&database).await?;
    let resource = Uuid::from_u128(0x5ab330);
    let logical = seed_family_identity_inputs(
        &database.pool,
        "ens",
        "abi-missing.eth",
        ABI_CHAIN,
        number,
        &hash,
        resource,
        Uuid::from_u128(0x5ab331),
        Uuid::from_u128(0x5ab332),
        "ens_v1",
    )
    .await?;
    let mut grant = history_event(
        "abi-missing-grant",
        Some(&logical),
        Some(resource),
        Some(ABI_CHAIN),
        Some(number),
        Some(&hash),
        Some("0xabi-missing"),
        Some(0),
        CanonicalityState::Canonical,
    );
    grant.event_kind = "RegistrationGranted".into();
    grant.source_family = "ens_v1_registrar_l1".into();
    grant.before_state = json!({});
    grant.after_state = json!({"authority_kind":"registrar", "registrant":ABI_RESOLVER,
        "expiry":1_900_000_000_i64});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[grant]).await?;
    rebuild_fixture_families(&database.pool, ABI_CHAIN, number, &hash).await?;

    let records = v2_get_json(&database, "/v1/names/abi-missing.eth/records?include=inventory").await?;
    let inventory = &records["data"]["inventory"];
    assert_eq!(inventory["abi_content_types"], Value::Null, "{records:#}");
    assert_eq!(inventory["abi_unsupported_reason"], json!("inventory_not_available"));
    let lookup = v2_lookup_json(
        &database,
        json!({"profile": "detail", "include": "inventory", "inputs": [{"name": "abi-missing.eth"}]}),
    )
    .await?;
    assert!(lookup["data"][0]["record"].get("inventory").is_none(), "{lookup:#}");
    database.cleanup().await
}

#[tokio::test]
async fn abi_content_types_for_a_full_lookup_batch_use_one_batched_read() -> Result<()> {
    const NAMES: usize = 1_000;
    let database = TestDatabase::new_migrated().await?;
    let specs = (0..NAMES)
        .map(|index| V2AddressNameSpec {
            logical_name_id: Box::leak(format!("ens:abi-batch-{index}.eth").into_boxed_str()),
            name: Box::leak(format!("abi-batch-{index}.eth").into_boxed_str()),
            resource_id: Uuid::from_u128(0x5b_0000 + 0x10 * index as u128),
            token_lineage_id: Uuid::from_u128(0x5b_0001 + 0x10 * index as u128),
            surface_binding_id: Uuid::from_u128(0x5b_0002 + 0x10 * index as u128),
            block_hash: Box::leak(format!("0xabi-batch-{index}").into_boxed_str()),
            block_number: 3_000 + index as i64,
            owner: ABI_RESOLVER,
            registrant: ABI_RESOLVER,
            registered_at: "2024-01-02T00:00:00Z",
            created_at: "2023-01-02T00:00:00Z",
            expires_at: "2027-01-02T00:00:00Z",
            relations: &[],
        })
        .collect::<Vec<_>>();
    seed_v2_address_name_identities(&database, &specs).await?;
    publish_v2_address_name_inputs(&database, &specs).await?;
    let names = specs.iter().map(|spec| spec.name.to_owned()).collect::<Vec<_>>();
    let content_type = |index: usize| (1_u128 << (index % 100)).to_string();
    let (block, hash) = address_fixture_head(&database).await?;
    let manifest = declare_family_fixture_resolver(
        &database.pool,
        "ens",
        ABI_CHAIN,
        "ens_v1_resolver_l1",
        ABI_RESOLVER,
    )
    .await?;
    let mut events = Vec::new();
    let mut identities = Vec::new();
    for (index, spec) in specs.iter().enumerate() {
        let node = bigname_lookup::ens_namehash_hex(spec.name)?;
        events.push(address_fixture_event(
            &format!("abi-batch-pointer-{index}"),
            Some(&bigname_storage::logical_name_id_for_name("ens", spec.name)),
            Some(spec.resource_id),
            "ResolverChanged",
            "ens_v1_registry_l1",
            block,
            &hash,
            NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as i64 + 100,
            json!({"source_event":"NewResolver", "node":node, "resolver":ABI_RESOLVER}),
        ));
        let selector = content_type(index);
        let write = abi_write_events(
            spec.name,
            ABI_RESOLVER,
            manifest,
            (block, &hash),
            &[abi_write("batch", &selector)],
        )?;
        identities.push(write[0].event_identity.clone());
        events.extend(write);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    rebuild_address_fixture(&database).await?;
    let ids: Vec<i64> = sqlx::query_scalar(
        "SELECT normalized_event_id FROM normalized_events WHERE event_identity = ANY($1)",
    )
    .bind(&identities)
    .fetch_all(&database.pool)
    .await?;

    let (_guard, calls) = crate::v2::abi_content_types_test_hooks::install(
        &database.lookup_pool,
    )
    .await?;
    let payload = v2_lookup_json(
        &database,
        json!({
            "profile": "detail",
            "include": "inventory",
            "inputs": names.iter().map(|name| json!({"name": name})).collect::<Vec<_>>()
        }),
    )
    .await?;
    let results = payload["data"].as_array().context("lookup results")?;
    assert_eq!(results.len(), NAMES);
    for (index, result) in results.iter().enumerate() {
        assert_eq!(
            result["record"]["inventory"]["abi_content_types"],
            json!([content_type(index)]),
            "{index}: {result}"
        );
    }
    // One batched read for the whole request, never one per name.
    assert_eq!(calls.lock().expect("calls").as_slice(), &[NAMES]);

    let plan = bigname_storage::explain_record_inventory_abi_evidence_for_test(
        &database.lookup_pool,
        &ids,
        ABI_CHAIN,
    )
    .await?;
    // The referenced events are fetched by primary key, never by scanning other events.
    let event_scans = plan
        .lines()
        .filter(|line| line.contains(" on normalized_events"))
        .collect::<Vec<_>>();
    assert!(!event_scans.is_empty(), "{plan}");
    assert!(
        event_scans
            .iter()
            .all(|line| line.contains("Index Scan using normalized_events_pkey")),
        "{plan}"
    );
    // `requested` is the statement's alias for the unnested id list, so renaming it changes this
    // pinned text as well as the plan shape.
    assert!(
        plan.contains("Index Cond: (normalized_event_id = requested.normalized_event_id)"),
        "{plan}"
    );
    // Each event's block is checked by its own lineage key, not by scanning the chain.
    assert!(
        plan.lines().any(|line| line.contains("Index Cond")
            && (line.contains("block_number = candidate.block_number")
                || line.contains("block_hash = candidate.block_hash"))),
        "{plan}"
    );
    // The plan is taken with sequential scans enabled, so this is the planner's own choice.
    assert!(!plan.contains("Seq Scan"), "{plan}");
    database.cleanup().await
}
