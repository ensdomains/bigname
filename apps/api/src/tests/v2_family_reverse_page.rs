// Actual family event publications exercise the public reverse page and count path.
async fn family_reverse_body(database: &TestDatabase, input: Value) -> Result<Value> {
    let response = v2_lookup_response_for_database_with_public_namespaces(database, "/v1/lookup", input, &["ens"]).await?;
    let status = response.status();
    let mut body: Value = read_json(response).await?;
    anyhow::ensure!(status == StatusCode::OK, "{status}: {body:#}");
    if let Some(meta) = body["meta"].as_object_mut() { meta.remove("as_of"); }
    Ok(body)
}

async fn seed_family_reverse_page_fixture(database: &TestDatabase) -> Result<()> {
    // beta sorts after alpha lexically; the reverse claim must move it ahead on page one.
    let mut events = family_primary_claim_events();
    let claim = events.last_mut().expect("the name record");
    claim.after_state["raw_name"] = json!("beta.eth");
    seed_family_routes_fixture_with(database, events).await
}

#[tokio::test]
async fn v2_family_reverse_pages_counts_and_follow_primary_order() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_family_reverse_page_fixture(&database).await?;
    let mut requests = Vec::new();
    for profile in ["feed", "detail"] {
        for relation in [None, Some("manager"), Some("owner"), Some("resolves_to")] {
            let mut cursor = None;
            let mut names = Vec::new();
            for _ in 0..5 {
                let mut input = json!({"id": "address", "address": FAMILY_ALICE, "page_size": 1});
                if let Some(relation) = relation { input["relation"] = json!(relation); }
                if let Some(cursor) = cursor { input["cursor"] = cursor; }
                let request = json!({"profile": profile, "inputs": [input]});

                let family = family_reverse_body(&database, request.clone()).await?;

                requests.push((request, family.clone()));
                let records = family["data"][0]["records"].as_array().expect("reverse records");
                assert!(!records.is_empty(), "{family:#}");
                names.extend(records.iter().map(|record| record["name"].as_str().unwrap().to_owned()));
                cursor = family["data"][0]["page"]["next_cursor"].as_str().map(|v| json!(v));
                if cursor.is_none() { break; }
            }
            let expected = if relation == Some("resolves_to") { vec!["alpha.eth"] } else { vec!["beta.eth", "alpha.eth"] };
            assert_eq!(names, expected, "profile={profile} relation={relation:?}");
        }
    }
    for address in [FAMILY_ALICE, "0x0000000000000000000000000000000000000fff"] {
        let request = json!({"inputs": [
            {"id": "eth", "address": address, "coin_type": 60, "page_size": 1},
            {"id": "other", "address": address, "coin_type": 0, "page_size": 1}
        ]});

        let family = family_reverse_body(&database, request.clone()).await?;

        requests.push((request, family));
    }
    // A chain outside the admitted ENS scope must not gate its missing tuples or counts.
    sqlx::query("INSERT INTO bigname_phase.project_family_marker (chain_id, state) VALUES ('base-mainnet', 'bootstrap_pending')")
        .execute(&database.pool).await?;
    for (request, expected) in requests {
        assert_eq!(family_reverse_body(&database, request.clone()).await?, expected, "{request:#}");
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_family_reverse_publication_loss_after_fence_is_stale() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_family_reverse_page_fixture(&database).await?;
    for relation in [None, Some("owner"), Some("resolves_to")] {
        let mut input = json!({"address": FAMILY_ALICE, "page_size": 1});
        if let Some(relation) = relation { input["relation"] = json!(relation); }
        let request = json!({"inputs": [input]});
        let live = family_reverse_body(&database, request.clone()).await?;
        assert_eq!(live["data"][0]["records"].as_array().unwrap().len(), 1);
        let reached = std::sync::Arc::new(tokio::sync::Notify::new());
        let resume = std::sync::Arc::new(tokio::sync::Notify::new());
        let request = bigname_storage::families::name::seams::with_pause_before_snapshot(
            reached.clone(), resume.clone(),
            v2_lookup_response_for_database_with_public_namespaces(&database, "/v1/lookup", request, &["ens"]),
        );
        tokio::pin!(request);
        let mut paused = 0;
        let response = loop {
            tokio::select! {
                result = &mut request => break result?,
                () = reached.notified() => {
                    paused += 1;
                    sqlx::query("UPDATE bigname_phase.project_family_marker SET state = 'bootstrap_pending'")
                        .execute(&database.pool).await?;
                    resume.notify_one();
                }
            }
        };
        assert!(paused > 0);
        let status = response.status();
        let body: Value = read_json(response).await?;
        assert_eq!((status, &body["error"]["code"]), (StatusCode::CONFLICT, &json!("stale")), "{relation:?}: {body:#}");
        sqlx::query("UPDATE bigname_phase.project_family_marker SET state = 'live'").execute(&database.pool).await?;
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_family_reverse_count_crosses_candidate_batches_and_ignores_cursor() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bounded_membership_blocks(&database, 240).await?;
    let mut events = Vec::new();
    for index in 0..67 {
        let (id, resource) = seed_family_name(&database, &format!("reverse{index:03}.eth"), 0x770_0000 + index * 4, "ens_v1").await?;
        events.push(family_event(&format!("reverse-grant-{index}"), Some(&id), Some(resource),
            "RegistrationGranted", "ens_v1_registrar_l1", 201, index as i64,
            json!({"authority_kind": "registrar", "registrant": FAMILY_ALICE, "expiry": 1_900_000_000})));
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    publish_test_families(&database, 240).await?;
    let mut input = bigname_storage::ReverseIdentityStorageInput {
        address: FAMILY_ALICE.to_owned(), coin_type: "60".to_owned(),
        roles: bigname_storage::ReverseIdentityRoles::Both, page_size: 33, cursor: None,
    };
    let namespaces = vec!["ens".to_owned()];
    let chains = vec![FAMILY_CHAIN.to_owned()];
    let mut names = Vec::new();
    for _ in 0..3 {
        let mut groups = bigname_storage::families::records::load_family_reverse_identity_groups(
            &database.pool, &[input.clone()], &namespaces, Some(&chains), true).await?;
        let group = groups.pop().unwrap();
        assert_eq!(group.total_count, Some(67));
        let mut page_only = bigname_storage::families::records::load_family_reverse_identity_groups(
            &database.pool, &[input.clone()], &namespaces, Some(&chains), false).await?;
        let page_only = page_only.pop().unwrap();
        assert_eq!(page_only.entries, group.entries);
        assert_eq!(page_only.has_more, group.has_more);
        assert_eq!(page_only.total_count, None);
        names.extend(group.entries.iter().map(|e| e.name_record.row.normalized_name.clone()));
        let last = group.entries.last().unwrap();
        input.cursor = Some(bigname_storage::ReverseIdentityCursor {
            is_primary: false, role_rank: 0, normalized_name: last.name_record.row.normalized_name.clone(),
            namespace: "ens".to_owned(), namehash: last.name_record.row.namehash.clone(),
        });
        if !group.has_more { break; }
    }
    assert_eq!(names, (0..67).map(|index| format!("reverse{index:03}.eth")).collect::<Vec<_>>());
    database.cleanup().await
}

#[tokio::test]
async fn v2_family_reverse_page_count_and_claim_hold_one_publication() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bounded_membership_blocks(&database, 240).await?;
    let (gamma, resource) = seed_family_name(&database, "gamma.eth", 0x771_0000, "ens_v1").await?;
    seed_family_reverse_page_fixture(&database).await?;
    let input = bigname_storage::ReverseIdentityStorageInput {
        address: FAMILY_ALICE.to_owned(), coin_type: "60".to_owned(),
        roles: bigname_storage::ReverseIdentityRoles::Both, page_size: 10, cursor: None,
    };
    let inputs = [input];
    let namespaces = vec!["ens".to_owned()];
    let chains = vec![FAMILY_CHAIN.to_owned()];
    let reached = std::sync::Arc::new(tokio::sync::Notify::new());
    let resume = std::sync::Arc::new(tokio::sync::Notify::new());
    let read_pool = database.pool.clone();
    let read = bigname_storage::families::name::seams::with_pause_after_publication(
        reached.clone(), resume.clone(),
        bigname_storage::families::records::load_family_reverse_identity_groups(
            &read_pool, &inputs, &namespaces, Some(&chains), true),
    );
    tokio::pin!(read);
    let mut advanced = false;
    let groups = loop {
        tokio::select! {
            result = &mut read => break result?,
            () = reached.notified() => {
                if !advanced {
                    let mut claim = family_primary_claim_events().pop().unwrap();
                    claim.event_identity = "family-next-claim".to_owned();
                    claim.block_number = Some(241);
                    claim.block_hash = Some("0xhistory241".to_owned());
                    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[
                        claim,
                        family_event("family-gamma-grant", Some(&gamma), Some(resource),
                            "RegistrationGranted", "ens_v1_registrar_l1", 241, 1,
                            json!({"authority_kind": "registrar", "registrant": FAMILY_ALICE, "expiry": 1_900_000_000})),
                    ]).await?;
                    publish_test_families(&database, 241).await?;
                    advanced = true;
                }
                resume.notify_one();
            }
        }
    };
    assert!(advanced, "the test must interrupt an actual composed batch");
    assert_eq!(groups[0].total_count, Some(2));
    assert_eq!(groups[0].entries.iter().map(|e| e.name_record.row.normalized_name.as_str()).collect::<Vec<_>>(), ["beta.eth", "alpha.eth"]);
    assert_eq!(groups[0].entries[0].primary_name.as_ref().unwrap().normalized_claim_name.as_deref(), Some("beta.eth"));
    let next = bigname_storage::families::records::load_family_reverse_identity_groups(
        &database.pool, &inputs, &namespaces, Some(&chains), true).await?;
    assert_eq!(next[0].total_count, Some(3));
    assert_eq!(next[0].entries[0].name_record.row.normalized_name, "alpha.eth");
    assert_eq!(next[0].entries[0].primary_name.as_ref().unwrap().normalized_claim_name.as_deref(), Some("alpha.eth"));
    database.cleanup().await
}

#[tokio::test]
async fn v2_family_reverse_primary_manager_precedes_owned_names() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bounded_membership_blocks(&database, 240).await?;
    let (gamma, resource) = seed_family_name(&database, "gamma.eth", 0x772_0000, "ens_v1").await?;
    seed_family_reverse_page_fixture(&database).await?;
    let mut claim = family_primary_claim_events().pop().unwrap();
    claim.event_identity = "family-gamma-claim".to_owned();
    claim.block_number = Some(241);
    claim.block_hash = Some("0xhistory241".to_owned());
    claim.after_state["raw_name"] = json!("gamma.eth");
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[
        claim,
        family_event("family-gamma-owned", Some(&gamma), Some(resource),
            "RegistrationGranted", "ens_v1_registrar_l1", 241, 1,
            json!({"authority_kind": "registrar", "registrant": FAMILY_BOB, "expiry": 1_900_000_000})),
        family_event("family-gamma-controller", Some(&gamma), Some(resource),
            "AuthorityTransferred", "ens_v1_registry_l1", 241, 2,
            json!({"authority_kind": "registry", "owner": FAMILY_ALICE, "owner_getter": FAMILY_ALICE, "node": gamma.strip_prefix("ens:").unwrap()})),
    ]).await?;
    publish_test_families(&database, 241).await?;
    for relation in [None, Some("manager"), Some("owner")] {
        let mut cursor = None;
        let mut names = Vec::new();
        for _ in 0..4 {
            let mut input = json!({"address": FAMILY_ALICE, "page_size": 1});
            if let Some(relation) = relation { input["relation"] = json!(relation); }
            if let Some(cursor) = cursor { input["cursor"] = cursor; }
            let request = json!({"inputs": [input]});

            let family = family_reverse_body(&database, request.clone()).await?;

            names.extend(family["data"][0]["records"].as_array().unwrap().iter().map(|r| r["name"].as_str().unwrap().to_owned()));
            cursor = family["data"][0]["page"]["next_cursor"].as_str().map(|v| json!(v));
            if cursor.is_none() { break; }
        }
        let expected = if relation == Some("owner") { vec!["alpha.eth", "beta.eth"] } else { vec!["gamma.eth", "alpha.eth", "beta.eth"] };
        assert_eq!(names, expected, "{relation:?}");
    }
    database.cleanup().await
}
