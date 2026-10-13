// An address page's role summary reads the grants of all its names in a number of statements
// that does not depend on how many names the page has. `v2_address_names_response_for_database`
// compares every role-summary answer with the read that took one resource at a time.

const RESOURCE_CANDIDATES: &str = "families.control.permissions.resource_candidate_page";
const PAGE_GRANTS: &str = "families.control.permissions.grants";

/// Keep the shared name/sort fixture unchanged; only this permission fixture adds roles.
async fn seed_role_summary_bulk_names(database: &TestDatabase) -> Result<()> {
    seed_bulk_address_names(database, 260).await?;
    let manifest = address_budget_resolver_manifest(database).await?;
    let (block, hash) = address_fixture_head(database).await?;
    let subject = address_budget_subject(1);
    let events = (0..260)
        .map(|index| {
            let resource = Uuid::from_u128(0x7000_0000 + index * 4);
            address_budget_role_event(resource, &subject, true, manifest, block, &hash)
        })
        .collect::<Vec<_>>();
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    rebuild_address_fixture(database).await
}

/// Independent public contract for the one direct resolver grant seeded on each bulk name.
fn assert_bulk_role_summary(row: &Value) {
    assert_eq!(
        row["role_summary"],
        json!([{"address":address_budget_subject(1), "grants":[{
            "grant_scope":{"kind":"resolver", "detail":{"resolver":{
                "chain_id":1, "address":ADDRESS_BUDGET_RESOLVER}}},
            "powers":["set_addr"]
        }]}]),
        "{row}"
    );
}

fn assert_budget_page(payload: &Value) -> Result<()> {
    let rows = payload["data"].as_array().context("budget data")?;
    assert_eq!(rows.len(), 2, "{payload}");
    for (resource, count) in [(0xd100, 600), (0xd200, 400)] {
        let id = Uuid::from_u128(resource).to_string();
        let matched = rows
            .iter()
            .filter(|row| row["permission_resource_id"] == json!(id))
            .collect::<Vec<_>>();
        assert_eq!(matched.len(), 1, "{payload}");
        assert_eq!(address_name_inline_grants(matched[0]).len(), count);
    }
    Ok(())
}

/// The payload of `uri` and the statements the request sent, on connections of its own.
async fn counted_address_names(
    database: &TestDatabase,
    uri: &str,
) -> Result<(Value, bigname_test_support::StatementCounts)> {
    let counter = bigname_test_support::StatementCounter::connect(&database.lookup_pool).await?;
    let state = AppState::new_with_rpc_urls(
        counter.pool().clone(),
        bigname_lookup::ChainRpcUrls::default(),
    )
    .with_public_namespaces_for_test(["ens", "basenames"]);
    let request = Request::builder()
        .uri(uri)
        .body(Body::empty())
        .expect("request must build");
    let (response, sent) = counter.count(app_router(state).oneshot(request)).await;
    let response = response.context("counted address names request failed")?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    counter.close().await;
    assert_eq!(status, StatusCode::OK, "{payload}");
    Ok((payload, sent))
}

#[tokio::test]
async fn address_names_role_summary_statement_count_is_flat_from_5_to_200_names() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_role_summary_bulk_names(&database).await?;
    // What the role summary adds to the page's statements, at each page size.
    let mut added = Vec::new();
    for page_size in [5, 50, 200] {
        let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?page_size={page_size}");
        let (_, plain) = counted_address_names(&database, &uri).await?;
        let (payload, summarised) =
            counted_address_names(&database, &format!("{uri}&include=role_summary")).await?;
        let rows = payload["data"].as_array().context("data")?;
        assert_eq!(rows.len(), page_size, "{payload}");
        for row in rows {
            assert_bulk_role_summary(row);
        }
        assert_eq!(
            summarised.tagged(RESOURCE_CANDIDATES),
            1,
            "page_size {page_size}: {:?}",
            summarised.by_tag()
        );
        assert_eq!(
            summarised.tagged(PAGE_GRANTS),
            1,
            "page_size {page_size}: {:?}",
            summarised.by_tag()
        );
        added.push((
            page_size,
            summarised.without_transaction_control() as i64
                - plain.without_transaction_control() as i64,
        ));
    }
    assert!(
        added
            .iter()
            .all(|(_, count)| *count > 0 && *count == added[0].1),
        "the role summary must add the same statements at every page size: {added:?}"
    );
    database.cleanup().await
}

/// The registry binding of a resource reads each observed name's current resource and authority
/// arm from the name's composed row before its name is rendered and its topology attached. Both
/// compositions must give the same two fields for every name of the fixture.
async fn assert_name_attributions_agree(database: &TestDatabase) -> Result<usize> {
    let names: Vec<String> =
        sqlx::query_scalar("SELECT logical_name_id FROM bigname_phase.name_surfaces")
            .fetch_all(&database.pool)
            .await?;
    let [full, base] =
        bigname_storage::families::control::permissions::name_attributions_by_both_compositions(
            &database.lookup_pool,
            &names,
        )
        .await?;
    assert_eq!(full, base);
    Ok(base.len())
}

#[tokio::test]
async fn address_names_attributions_agree_between_the_two_compositions() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    assert!(assert_name_attributions_agree(&database).await? > 3);
    database.cleanup().await
}

#[tokio::test]
async fn address_names_role_summary_bodies_match_the_per_resource_loop() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_role_summary_bulk_names(&database).await?;
    assert_eq!(assert_name_attributions_agree(&database).await?, 260);
    // The helper fails when the two reads answer differently.
    for query in [
        "page_size=5&include=role_summary",
        "page_size=50&include=role_summary",
        "page_size=100&include=role_summary",
        "page_size=200&include=role_summary",
        "page_size=200&include=role_summary,counts",
        "page_size=200&include=role_summary&namespace=ens",
        "page_size=200&include=role_summary&namespace=basenames",
        "page_size=200&include=role_summary&dedupe=registration&order=desc",
        "page_size=200&include=role_summary&sort=expires_at",
    ] {
        let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?{query}");
        let pages = walk_all_pages(&database, &uri).await?;
        let rows = pages
            .iter()
            .flat_map(|page| page["data"].as_array().unwrap())
            .collect::<Vec<_>>();
        if query.contains("namespace=basenames") {
            assert!(rows.is_empty(), "{uri}: {pages:?}");
            continue;
        }
        assert_eq!(rows.len(), 260, "{uri}");
        let ids = rows
            .iter()
            .map(|row| {
                assert_bulk_role_summary(row);
                row["permission_resource_id"].as_str().unwrap().to_owned()
            })
            .collect::<BTreeSet<_>>();
        let expected = (0..260)
            .map(|index| Uuid::from_u128(0x7000_0000 + index * 4).to_string())
            .collect::<BTreeSet<_>>();
        assert_eq!(ids.len(), rows.len(), "{uri}: no duplicate resources");
        assert_eq!(ids, expected, "{uri}");
    }
    database.cleanup().await
}

// The budget counts grants over every returned row: 1,000 across two registrations are served
// and 1,001 are refused, by both reads alike.
#[tokio::test]
async fn address_names_role_summary_refuses_in_the_same_cases() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    let (first, second) = (Uuid::from_u128(0xd100), Uuid::from_u128(0xd200));
    seed_address_name_budget_grants(&database, first, 600).await?;
    let uri = format!("/v1/addresses/{V2_ADDRESS}/names?q=shared&include=role_summary");
    for (count, status) in [
        (400, StatusCode::OK),
        (401, StatusCode::UNPROCESSABLE_ENTITY),
    ] {
        seed_address_name_budget_grants(&database, second, count).await?;
        let response = v2_address_names_response_for_database(&database, &uri).await?;
        assert_eq!(response.status(), status, "{count} grants on the second");
        let payload: Value = read_json(response).await?;
        if status == StatusCode::OK {
            assert_budget_page(&payload)?;
        } else {
            assert_eq!(payload["error"]["code"], json!("unsupported"));
            assert!(payload.get("data").is_none(), "{payload}");
        }
    }
    seed_address_name_budget_grants(&database, second, 400).await?;
    let raw: Vec<(Uuid, String, String, Value, bool)> = sqlx::query_as(
        "SELECT resource_id, subject, scope, effective_powers, revoked
         FROM bigname_phase.project_grant
         WHERE chain_id = 'ethereum-mainnet' AND resource_id = ANY($1::uuid[])",
    )
    .bind(vec![first, second])
    .fetch_all(&database.pool)
    .await?;
    let first_rows = raw.iter().filter(|row| row.0 == first).collect::<Vec<_>>();
    assert_eq!(first_rows.len(), 600);
    assert!(first_rows.iter().all(|row| !row.4));
    let second_rows = raw.iter().filter(|row| row.0 == second).collect::<Vec<_>>();
    assert_eq!(second_rows.len(), 401);
    assert_eq!(second_rows.iter().filter(|row| !row.4).count(), 400);
    let revoked = second_rows.iter().filter(|row| row.4).collect::<Vec<_>>();
    assert_eq!(revoked.len(), 1);
    assert_eq!(revoked[0].1, address_budget_subject(401));
    assert_eq!(
        revoked[0].2,
        format!("resolver:ethereum-mainnet:{ADDRESS_BUDGET_RESOLVER}")
    );
    assert_eq!(revoked[0].3, json!([]));
    // 1,001 stored keys fill the first batch; composition keeps 1,000 active grants.
    // A second candidate statement establishes exhaustion after the revoked key.
    let (payload, sent) = counted_address_names(&database, &uri).await?;
    assert_budget_page(&payload)?;
    assert_eq!(sent.tagged(RESOURCE_CANDIDATES), 2, "{:?}", sent.by_tag());
    database.cleanup().await
}

/// Compare both reader paths over each stable publication of the real permission fixtures.
async fn assert_permission_fixture_reads_agree(database: &TestDatabase) -> Result<()> {
    use bigname_storage::{
        EffectivePermissionRow,
        families::{
            control::permissions::page::load_family_bounded_permissions_per_resource,
            name::FamilyPublicationUnavailable,
        },
    };
    let ids: Vec<Uuid> = sqlx::query_scalar("SELECT resource_id FROM bigname_phase.resources")
        .fetch_all(&database.pool)
        .await?;
    let answer = |read: Result<Vec<EffectivePermissionRow>>|
        -> Result<std::result::Result<Vec<EffectivePermissionRow>, String>> {
        match read {
            Ok(rows) => Ok(Ok(rows)),
            Err(error) => match error.downcast_ref::<FamilyPublicationUnavailable>() {
                Some(unavailable) => Ok(Err(unavailable.chain_id.clone())),
                None => Err(error),
            },
        }
    };
    for namespace in [None, Some("ens"), Some("basenames")] {
        for budget in [1, 2, 10, 1001] {
            let served = answer(
                bigname_storage::load_bounded_effective_permissions_by_resource_ids(
                    &database.lookup_pool,
                    &ids,
                    namespace,
                    budget,
                )
                .await,
            )?;
            let looped = answer(
                load_family_bounded_permissions_per_resource(
                    &database.lookup_pool,
                    &ids,
                    namespace,
                    budget,
                )
                .await,
            )?;
            assert_eq!(served, looped, "namespace {namespace:?}, budget {budget}");
        }
    }
    assert_name_attributions_agree(database).await?;
    Ok(())
}
