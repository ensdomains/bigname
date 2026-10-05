use super::*;

pub(super) const RESOLVER: &str = "0x0000000000000000000000000000000000000def";

pub(super) async fn database_at(block: i64) -> Result<TestDatabase> {
    let (manifest, _, mut logs) = fixture(false)?;
    logs.push(raw(
        ResolverUpdated {
            tokenId: token(0),
            resolver: RESOLVER.parse()?,
            sender: HOLDER.parse()?,
        }
        .encode_log_data(),
        120,
        4,
    ));
    logs.sort_by_key(|log| (log.block_number, log.transaction_index, log.log_index));
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_blocks(&database, 119..=134).await?;
    seed_interpret(&database, &manifest, &logs).await?;
    let engine = bigname_interpret::Engine::new(database.pool.clone());
    for block in 119..=block {
        engine
            .run_batch(bigname_interpret::BatchRequest {
                chain_id: CHAIN.into(),
                from_block: block,
                to_block: block,
                resume_current: None,
                mode: bigname_interpret::RunMode::Normal,
            })
            .await?;
    }
    database.seed_snapshot_selector_chain_positions(&json!({CHAIN:{"chain_id":CHAIN,"block_number":block,"block_hash":format!("0xhistory{block}"),"timestamp":format!("2023-11-14T22:15:{:02}Z",block-100)}})).await?;
    // The token lifecycle above comes entirely from the real decoder and writer. Existing
    // resolver-record fixture helpers supply an independent serving resource for route coverage.
    let (manifest, _) = declare_family_fixture_contract(
        &database.pool,
        "ens",
        CHAIN,
        "ens_v2_resolver_l1",
        "public_resolver_v2",
        RESOLVER,
    )
    .await?;
    let mut event = history_event(
        "token-route-resolver-record",
        None,
        None,
        Some(CHAIN),
        Some(120),
        Some("0xhistory120"),
        Some("0xrecords"),
        Some(5),
        CanonicalityState::Canonical,
    );
    event.event_kind = "RecordChanged".into();
    event.source_family = "ens_v2_resolver_l1".into();
    event.source_manifest_id = Some(manifest);
    event.manifest_version = 1;
    event.raw_fact_ref =
        json!({"kind":"raw_log","emitting_address":RESOLVER,"transaction_index":0});
    event.after_state = json!({"source_event":"AddressChanged","node":bigname_lookup::ens_namehash_hex(NAME)?,"resolver":RESOLVER,"record_key":"addr:60","record_family":"addr","selector_key":"60","value":GRANTEE});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[event]).await?;
    publish(&database, block).await?;
    Ok(database)
}

pub(super) async fn publish(database: &TestDatabase, block: i64) -> Result<()> {
    publish_test_families_on(&database.pool, CHAIN, block).await?;
    database.seed_snapshot_selector_chain_positions(&json!({CHAIN:{"chain_id":CHAIN,"block_number":block,"block_hash":format!("0xhistory{block}"),"timestamp":format!("2023-11-14T22:15:{:02}Z",block-100)}})).await
}

pub(super) async fn request(database: &TestDatabase, route: &str) -> Result<(StatusCode, Value)> {
    if route == "lookup" {
        let response=v2_lookup_response_for_database(database,"/v1/lookup",json!({"profile":"detail","inputs":[{"name":NAME},{"address":HOLDER,"relation":"owner"},{"address":GRANTEE,"relation":"resolves_to"},{"address":HOLDER,"relation":"any"}]})).await?;
        let status = response.status();
        Ok((status, read_json(response).await?))
    } else {
        read_family_response(database, route).await
    }
}

pub(super) fn token_rows<'a>(route: &str, body: &'a Value) -> Vec<&'a Value> {
    if route == "lookup" {
        body["data"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|r| {
                r.get("record")
                    .into_iter()
                    .chain(r["records"].as_array().into_iter().flatten())
            })
            .filter(|r| r["name"] == NAME)
            .collect()
    } else if route.starts_with("/v1/resolvers/") {
        body["data"]["bound_names"]["data"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|r| r["name"] == NAME)
            .collect()
    } else {
        vec![&body["data"]]
    }
}

#[tokio::test]
async fn registry_token_ids_agree_on_all_detail_routes_and_omit_without_evidence() -> Result<()> {
    let database = database_at(121).await?;
    let routes = [
        format!("/v1/names/{NAME}"),
        format!("/v1/names/{NAME}?source=verified"),
        "lookup".into(),
        format!("/v1/resolvers/1/{RESOLVER}"),
    ];
    let mut resource = None;
    for route in &routes {
        let (status, body) = request(&database, route).await?;
        assert_eq!(status, StatusCode::OK, "{route}: {body}");
        let rows = token_rows(route, &body);
        assert!(!rows.is_empty(), "{route}: {body}");
        if route == "lookup" {
            assert_eq!(rows.len(), 4, "all four forward/reverse branches: {body}");
        }
        for row in rows {
            assert_eq!(row["token_id"], token(1).to_string(), "{route}: {body}");
            let id = row["registration_id"].as_str().unwrap();
            assert_eq!(id, resource.get_or_insert_with(|| id.to_owned()));
        }
    }
    // Simulate retained current family state whose token evidence is absent: never fall back
    // to the old raw labelhash. No endpoint creates a guessed ID or a JSON null.
    sqlx::query("DELETE FROM normalized_events WHERE event_kind IN ('TokenResourceLinked','TokenRegenerated')").execute(&database.pool).await?;
    for route in &routes {
        let (status, body) = request(&database, route).await?;
        assert_eq!(status, StatusCode::OK, "{route}: {body}");
        for row in token_rows(route, &body) {
            assert!(row.get("token_id").is_none(), "{route}: {body}");
        }
    }
    database.cleanup().await
}
