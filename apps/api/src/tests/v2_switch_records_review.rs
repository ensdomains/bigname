// Review regressions for namespace-scoped reads and a family reset between address membership
// selection and the later record-count read.

async fn with_record_families(database: &TestDatabase, uri: &str) -> Result<(StatusCode, Value)> {
    let response = bigname_storage::publication_source::with_serve_from_families(
        true, v2_get_response(database, uri),
    ).await?;
    Ok((response.status(), read_json(response).await?))
}

#[tokio::test]
async fn v2_empty_ens_address_reads_ignore_an_unrelated_family_rebuild() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_records_fixture(&database).await?;
    sqlx::query(
        "INSERT INTO bigname_phase.project_family_marker (chain_id, state)
         VALUES ('base-mainnet', 'bootstrap_pending')",
    )
    .execute(&database.pool)
    .await?;
    let address = "0x0000000000000000000000000000000000000fff";
    for uri in [
        format!("/v1/addresses/{address}/names?namespace=ens"),
        format!("/v1/addresses/{address}/names?namespace=ens&relation=resolves_to&coin_type=60"),
        format!("/v1/addresses/{address}/names?namespace=ens&relation=resolves_to&coin_type=evm"),
        format!("/v1/addresses/{address}/primary-name?namespace=ens&source=indexed"),
    ] {
        let (status, body) = assert_switch_differential(&database, &uri).await?;
        assert_eq!(status, StatusCode::OK, "{uri}: {body:#}");
    }
    database.cleanup().await
}

/// Real family replay has populated both address indexes but has not reached its target.
async fn seed_partial_base_address_families(database: &TestDatabase) -> Result<()> {
    let chain = "base-mainnet";
    let blocks = (200..=241).map(|number| {
        raw_block(chain, &format!("0xhistory{number}"),
            (number > 200).then(|| format!("0xhistory{}", number - 1)).as_deref(),
            number, 1_700_000_000 + number)
    }).collect::<Vec<_>>();
    upsert_phase_raw_blocks(&database.pool, &blocks).await?;
    seed_schema_v2_lookup_head(&database.pool, chain, 240, "0xhistory240",
        &crate::v2::format_timestamp(OffsetDateTime::from_unix_timestamp(1_700_000_240)?)).await?;
    let (name, resource) = seed_switch_name_on(database, "alpha.base.eth", 0x5f1_0000,
        "basenames", "basenames", chain).await?;
    let node = name.strip_prefix("basenames:").expect("Basenames id");
    let mut events = vec![
        switch_event("switch-base-grant", Some(&name), Some(resource), "RegistrationGranted",
            "basenames_base_registrar", 201, 0,
            json!({"authority_kind": "registrar", "registrant": SWITCH_ALICE,
                   "expiry": 1_900_000_000})),
        switch_event("switch-base-resolver", Some(&name), Some(resource), "ResolverChanged",
            "basenames_base_registry", 202, 0,
            json!({"node": node, "resolver": SWITCH_RESOLVER})),
        switch_event("switch-base-addr", None, None, "RecordChanged",
            "basenames_base_resolver", 203, 0,
            json!({"source_event": "AddressChanged", "node": node, "resolver": SWITCH_RESOLVER,
                   "record_key": "addr:60", "record_family": "addr", "selector_key": "60",
                   "value": SWITCH_ALICE})),
    ];
    for event in &mut events {
        event.chain_id = Some(chain.to_owned());
        event.namespace = "basenames".to_owned();
    }
    events[2].raw_fact_ref["emitting_address"] = json!(SWITCH_RESOLVER);
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    let token = bigname_project::families::input_token(&database.pool, chain).await?;
    let mut options = bigname_project::families::FamilyOptions::new(
        bigname_content_hash::INTERPRETER_CONTENT_HASH);
    // Identity at 200 and events at 201..203; leave the target block for the next run.
    options.max_blocks_per_run = 4;
    options.rebuild_ranges = bigname_project::families::RebuildRanges::Off;
    let outcome = bigname_project::families::apply(&database.pool, chain,
        &bigname_project::Marker { number: 240, hash: "0xhistory240".to_owned() },
        bigname_project::families::FamilyMode::Rebuild, &token, &options).await;
    anyhow::ensure!(outcome.reset && outcome.budget_exhausted && outcome.skipped.is_none(),
        "partial rebuild: {outcome:?}");
    let state: String = sqlx::query_scalar(
        "SELECT state FROM bigname_phase.project_family_marker WHERE chain_id = $1")
        .bind(chain).fetch_one(&database.pool).await?;
    assert_eq!(state, "bootstrap_pending");
    let indexed: (bool, bool) = sqlx::query_as(
        "SELECT EXISTS (SELECT 1 FROM bigname_phase.project_address_name_index
                        WHERE chain_id = $1 AND address = $2),
                EXISTS (SELECT 1 FROM bigname_phase.project_address_record_node_index index
                        JOIN bigname_phase.project_resource_pointer pointer
                          ON pointer.chain_id = index.chain_id AND pointer.namehash = index.node
                         AND pointer.resolver_address = index.resolver_address
                        WHERE index.chain_id = $1 AND index.address = $2)")
        .bind(chain).bind(SWITCH_ALICE).fetch_one(&database.pool).await?;
    assert_eq!(indexed, (true, true), "both nonempty address candidate paths must reach Base");
    Ok(())
}

#[tokio::test]
async fn v2_nonempty_ens_address_reads_ignore_an_unrelated_family_rebuild() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_records_fixture(&database).await?;
    let relations = ["", "&relation=resolves_to&coin_type=60", "&relation=resolves_to&coin_type=evm"];
    let mut expected = Vec::new();
    for relation in relations {
        let uri = format!("/v1/addresses/{SWITCH_ALICE}/names?namespace=ens{relation}");
        let (status, body) = with_record_families(&database, &uri).await?;
        assert_eq!(status, StatusCode::OK, "{uri}: {body:#}");
        assert!(!body["data"].as_array().expect("name list").is_empty());
        expected.push(body);
    }
    seed_partial_base_address_families(&database).await?;
    let mut actual = Vec::new();
    for relation in relations {
        actual.push(with_record_families(&database,
            &format!("/v1/addresses/{SWITCH_ALICE}/names?namespace=ens{relation}")).await?);
    }
    assert_eq!(actual.iter().map(|(status, _)| *status).collect::<Vec<_>>(),
        vec![StatusCode::OK; relations.len()], "ENS scope must ignore unavailable Base");
    for ((_, actual), expected) in actual.into_iter().zip(expected) {
        assert_eq!(actual, expected, "the ENS collection must be unchanged");
    }
    for relation in relations {
        for namespace in ["", "namespace=basenames"] {
            let uri = format!("/v1/addresses/{SWITCH_ALICE}/names?{namespace}{relation}");
            let (status, body) = with_record_families(&database, &uri).await?;
            assert_eq!((status, &body["error"]["code"]),
                (StatusCode::CONFLICT, &json!("stale")), "{uri}: {body:#}");
        }
    }
    database.cleanup().await
}

async fn reset_records_families(database: &TestDatabase) -> Result<()> {
    let token = bigname_project::families::input_token(&database.pool, SWITCH_CHAIN).await?;
    let mut options = bigname_project::families::FamilyOptions::new(
        bigname_content_hash::INTERPRETER_CONTENT_HASH,
    );
    options.max_blocks_per_run = 0;
    let outcome = bigname_project::families::apply(
        &database.pool,
        SWITCH_CHAIN,
        &bigname_project::Marker { number: 240, hash: "0xhistory240".to_owned() },
        bigname_project::families::FamilyMode::Rebuild,
        &token,
        &options,
    ).await;
    anyhow::ensure!(outcome.reset && outcome.marker.is_none() && outcome.skipped.is_none(),
        "actual reset before replay: {outcome:?}");
    Ok(())
}

async fn assert_address_counts_reset_is_stale(relation: &str) -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_records_fixture(&database).await?;
    let uri = format!("/v1/addresses/{SWITCH_ALICE}/names?namespace=ens&q=alpha\
                      &include=counts,role_summary{relation}");
    let (status, before) = with_record_families(&database, &uri).await?;
    assert_eq!(status, StatusCode::OK, "{before:#}");
    assert_eq!(before["data"].as_array().map(Vec::len), Some(1));
    let (_guard, control) =
        crate::v2::address_names_grant_read_test_hooks::install(&database.lookup_pool).await?;
    let (status, body) = {
        let request = with_record_families(&database, &uri);
        tokio::pin!(request);
        tokio::select! {
            response = &mut request => anyhow::bail!("request did not pause: {:?}", response?),
            () = control.wait_until_reached() => {}
        }
        reset_records_families(&database).await?;
        let ((), response) = tokio::join!(control.resume(), &mut request);
        response?
    };
    assert_eq!((status, &body["error"]["code"]),
        (StatusCode::CONFLICT, &json!("stale")), "{body:#}");
    database.cleanup().await
}

#[tokio::test]
async fn v2_address_name_counts_after_family_reset_are_stale() -> Result<()> {
    assert_address_counts_reset_is_stale("").await
}

#[tokio::test]
async fn v2_resolves_to_counts_after_family_reset_are_stale() -> Result<()> {
    assert_address_counts_reset_is_stale("&relation=resolves_to&coin_type=60").await
}

#[tokio::test]
async fn v2_empty_address_collections_revalidate_the_requested_family_publication() -> Result<()> {
    for relation in ["", "&relation=resolves_to&coin_type=60", "&relation=resolves_to&coin_type=evm"] {
        let database = TestDatabase::new_migrated().await?;
        seed_switch_records_fixture(&database).await?;
        let uri = format!("/v1/addresses/0x0000000000000000000000000000000000000fff/names?namespace=ens{relation}");
        let (_guard, control) =
            crate::v2::collection_snapshot::finish_test_hooks::install(&database.lookup_pool).await?;
        let (status, body) = {
            let request = with_record_families(&database, &uri);
            tokio::pin!(request);
            tokio::select! {
                response = &mut request => anyhow::bail!("empty page did not reach its fence: {:?}", response?),
                () = control.wait_until_reached() => {}
            }
            reset_records_families(&database).await?;
            let ((), response) = tokio::join!(control.resume(), &mut request);
            response?
        };
        assert_eq!((status, &body["error"]["code"]),
            (StatusCode::CONFLICT, &json!("stale")), "{uri}: {body:#}");
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn v2_missing_primary_claim_revalidates_the_requested_family_publication() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_records_fixture(&database).await?;
    let uri = "/v1/addresses/0x0000000000000000000000000000000000000fff/primary-name?namespace=ens&source=indexed";
    let (_guard, control) =
        crate::v2::support::indexed_read_test_hooks::install(&database.lookup_pool).await?;
    let (status, body) = {
        let request = with_record_families(&database, uri);
        tokio::pin!(request);
        tokio::select! {
            response = &mut request => anyhow::bail!("missing claim did not reach its fence: {:?}", response?),
            () = control.wait_until_reached() => {}
        }
        reset_records_families(&database).await?;
        let ((), response) = tokio::join!(control.resume(), &mut request);
        response?
    };
    assert_eq!((status, &body["error"]["code"]),
        (StatusCode::CONFLICT, &json!("stale")), "{body:#}");
    database.cleanup().await
}
