// ABI content types on `include=inventory` (docs/api-v2-routes.md, records route). Selection
// through Project is covered in crates/project/tests/record_inventory_abi.rs and
// record_id_resolver.rs; these cases pin the public shape, availability, and batch cost.

const ABI_RESOLVER: &str = "0x00000000000000000000000000000000000a0b11";
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

async fn seed_abi_resolver(database: &TestDatabase, source_family: &str, role: &str) -> Result<()> {
    let (hash, _) = abi_head(database).await?;
    sqlx::query(
        r#"
        INSERT INTO bigname_phase.resolver_current (
            chain_id, resolver_address, declared_summary, support_status, provenance,
            chain_positions, canonicality_summary, manifest_version
        ) VALUES (
            $1, $2, jsonb_build_object('classification',
                jsonb_build_object('source_family', $3::text, 'role', $4::text)),
            'supported', '{}'::jsonb, jsonb_build_object('target_block_hash', $5::text),
            '{"state": "canonical_lineage"}'::jsonb, 1
        )
        ON CONFLICT (chain_id, resolver_address) DO UPDATE
        SET declared_summary = EXCLUDED.declared_summary
        "#,
    )
    .bind(ABI_CHAIN)
    .bind(ABI_RESOLVER)
    .bind(source_family)
    .bind(role)
    .bind(hash)
    .execute(&database.lookup_pool)
    .await?;
    Ok(())
}

/// Record writes on the shared resolver, returned as normalized event ids in order.
async fn seed_abi_writes(database: &TestDatabase, writes: &[AbiWrite<'_>]) -> Result<Vec<i64>> {
    let (hash, number) = abi_head(database).await?;
    let mut ids = Vec::new();
    for (log_index, write) in writes.iter().enumerate() {
        let key = match write.family {
            "abi" => format!("abi:{}", write.selector),
            family => format!("{family}:{}", write.selector),
        };
        let id: i64 = sqlx::query_scalar(
            r#"
            INSERT INTO bigname_phase.normalized_events (
                event_identity, namespace, event_kind, source_family, manifest_version,
                chain_id, block_number, block_hash, transaction_hash, transaction_index,
                log_index, derivation_kind, canonicality_state, after_state, raw_fact_ref
            ) VALUES (
                $1, 'ens', 'RecordChanged', 'ens_v1_resolver_l1', 1, $2, $3, $4, $5, 0, $6,
                'ens_v1_unwrapped_authority', 'canonical', $7,
                jsonb_build_object('emitting_address', $8::text)
            )
            RETURNING normalized_event_id
            "#,
        )
        .bind(format!("abi-fixture:{}", write.identity))
        .bind(ABI_CHAIN)
        .bind(number)
        .bind(&hash)
        .bind(format!("0x{:064x}", 0xab1_u64))
        .bind(log_index as i64)
        .bind(json!({
            "source_event": if write.family == "abi" { "ABIChanged" } else { "TextChanged" },
            "resolver": ABI_RESOLVER,
            "record_key": key,
            "record_family": write.family,
            "selector_key": write.selector,
            "value_retained": true,
            // The ENSv1 adapter stores the content type as `value`; the read must ignore it.
            "value": "0"
        }))
        .bind(ABI_RESOLVER)
        .fetch_one(&database.lookup_pool)
        .await?;
        ids.push(id);
    }
    Ok(ids)
}

async fn point_inventory_at_abi_writes(
    database: &TestDatabase,
    raw_name: &str,
    record_event_ids: &[i64],
) -> Result<()> {
    let updated = sqlx::query(
        r#"
        UPDATE bigname_phase.record_inventory_current inventory
        SET provenance = inventory.provenance || jsonb_build_object(
                'chain_id', $2::text,
                'resolver_address', $3::text,
                'record_event_ids', to_jsonb($4::BIGINT[]),
                'record_link_event_ids', '[]'::jsonb
            )
        FROM bigname_phase.name_current name
        WHERE name.resource_id = inventory.resource_id
          AND name.raw_name = $1
        "#,
    )
    .bind(raw_name)
    .bind(ABI_CHAIN)
    .bind(ABI_RESOLVER)
    .bind(record_event_ids)
    .execute(&database.lookup_pool)
    .await?
    .rows_affected();
    anyhow::ensure!(updated == 1, "expected one inventory row for {raw_name}");
    Ok(())
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
        "0x0000000000000000000000000000000000000abc",
        bigname_storage::AddressNameRelation::TokenHolder,
        38,
    )
    .await
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
    seed_abi_resolver(&database, "ens_v1_resolver_l1", "public_resolver").await?;
    let before = v2_get_json(&database, "/v1/names/abi-types.eth/records?include=inventory").await?;
    let wide = (1_u128 << 70).to_string();
    let ids = seed_abi_writes(
        &database,
        &[
            abi_write("wide", &wide),
            abi_write("json", "1"),
            abi_write("json-again", "1"),
            AbiWrite { identity: "text", family: "text", selector: "url" },
            abi_write("cbor", "4"),
        ],
    )
    .await?;
    point_inventory_at_abi_writes(&database, "abi-types.eth", &ids).await?;

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
    seed_abi_resolver(&database, "ens_v1_resolver_l1", "public_resolver").await?;
    let text = seed_abi_writes(
        &database,
        &[AbiWrite { identity: "empty-text", family: "text", selector: "url" }],
    )
    .await?;
    point_inventory_at_abi_writes(&database, "abi-empty.eth", &text).await?;
    // An ENSv1 resolver with no selected ABI write: an eligible path, observed empty.
    let inventory = abi_inventory_on_both_routes(&database, "abi-empty.eth").await?;
    assert_eq!(inventory["abi_content_types"], json!([]), "{inventory}");
    assert!(inventory.get("abi_unsupported_reason").is_none());

    // The same supported inventory behind the direct PublicResolverV2 profile has no admitted
    // ABI event: unavailable, not empty.
    seed_abi_resolver(&database, "ens_v2_resolver_l1", "public_resolver_v2").await?;
    let inventory = abi_inventory_on_both_routes(&database, "abi-empty.eth").await?;
    assert_eq!(inventory["abi_content_types"], Value::Null, "{inventory}");
    assert_eq!(
        inventory["abi_unsupported_reason"],
        json!("abi_observations_not_supported")
    );
    database.cleanup().await
}

#[tokio::test]
async fn abi_content_types_are_withheld_without_usable_evidence() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    for (offset, name) in ["abi-stale.eth", "abi-mask.eth", "abi-unsupported.eth", "abi-missing.eth"]
        .into_iter()
        .enumerate()
    {
        seed_abi_name(&database, name, 0x5ab300 + 0x10 * offset as u128).await?;
    }
    seed_abi_resolver(&database, "ens_v1_resolver_l1", "public_resolver").await?;
    let ids = seed_abi_writes(
        &database,
        &[abi_write("stale-one", "1"), abi_write("mask", "3"), abi_write("fine", "2")],
    )
    .await?;
    // A referenced write that is no longer retained.
    point_inventory_at_abi_writes(&database, "abi-stale.eth", &[ids[0], ids[2] + 1_000]).await?;
    // A nonstandard multi-bit content type.
    point_inventory_at_abi_writes(&database, "abi-mask.eth", &[ids[1], ids[2]]).await?;
    // A supported name whose inventory row is unsupported.
    point_inventory_at_abi_writes(&database, "abi-unsupported.eth", &[ids[2]]).await?;
    sqlx::query(
        r#"
        UPDATE bigname_phase.record_inventory_current inventory
        SET support_status = 'unsupported', unsupported_reason = 'resolver_implementation_unknown'
        FROM bigname_phase.name_current name
        WHERE name.resource_id = inventory.resource_id AND name.raw_name = 'abi-unsupported.eth'
        "#,
    )
    .execute(&database.lookup_pool)
    .await?;

    for (name, reason) in [
        ("abi-stale.eth", "abi_observations_stale"),
        ("abi-mask.eth", "abi_content_type_not_single_bit"),
        ("abi-unsupported.eth", "inventory_not_authoritative"),
    ] {
        let inventory = abi_inventory_on_both_routes(&database, name).await?;
        assert_eq!(inventory["abi_content_types"], Value::Null, "{name}: {inventory}");
        assert_eq!(inventory["abi_unsupported_reason"], json!(reason), "{name}");
    }
    let lookup = v2_lookup_json(
        &database,
        json!({"profile": "detail", "include": "inventory", "inputs": [{"name": "abi-unsupported.eth"}]}),
    )
    .await?;
    assert_eq!(lookup["data"][0]["status"], json!("ok"));

    // Without an inventory row the records route keeps its container and says why; the lookup
    // route omits the container as before.
    sqlx::query(
        r#"
        DELETE FROM bigname_phase.record_inventory_current inventory
        USING bigname_phase.name_current name
        WHERE name.resource_id = inventory.resource_id AND name.raw_name = 'abi-missing.eth'
        "#,
    )
    .execute(&database.lookup_pool)
    .await?;
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
    let names = (0..NAMES).map(|index| format!("abi-batch-{index}.eth")).collect::<Vec<_>>();
    for (index, name) in names.iter().enumerate() {
        seed_abi_name(&database, name, 0x5b_0000 + 0x10 * index as u128).await?;
    }
    seed_abi_resolver(&database, "ens_v1_resolver_l1", "public_resolver").await?;
    let content_type = |index: usize| (1_u128 << (index % 100)).to_string();
    let writes = (0..NAMES)
        .map(|index| (format!("batch-{index}"), content_type(index)))
        .collect::<Vec<_>>();
    let ids = seed_abi_writes(
        &database,
        &writes
            .iter()
            .map(|(identity, selector)| abi_write(identity, selector))
            .collect::<Vec<_>>(),
    )
    .await?;
    for (name, id) in names.iter().zip(&ids) {
        point_inventory_at_abi_writes(&database, name, &[*id]).await?;
    }

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
