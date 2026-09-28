// The names group under the publication switch (TYR-36 step 7b slice 2): every route whose name
// rows now come from the composed name reader (`bigname_storage::families::name`) answers the
// same body with the switch off and on, `meta.as_of` excepted, over a fixture that Project and
// the owned key families both build from the same normalized events.

const SWITCH_ALICE: &str = "0x00000000000000000000000000000000000a11ce";
const SWITCH_RESOLVER: &str = "0x0000000000000000000000000000000000000abc";

/// alpha.eth granted at 201, pointed at a resolver at 202 and renewed at 203; beta.eth granted
/// at 204 and pointed at the same resolver at 205; both published at 240.
async fn seed_switch_names_fixture(database: &TestDatabase) -> Result<()> {
    seed_bounded_membership_blocks(database, 240).await?;
    let (alpha, alpha_resource) = seed_switch_name(database, "alpha.eth", 0x5a1_0000, "ens_v1").await?;
    let (beta, beta_resource) = seed_switch_name(database, "beta.eth", 0x5b1_0000, "ens_v1").await?;
    let alpha_node = alpha.strip_prefix("ens:").expect("ens id").to_owned();
    let grant = |expiry: i64| {
        json!({"authority_kind": "registrar", "registrant": SWITCH_ALICE, "expiry": expiry})
    };
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            switch_event(
                "switch-alpha-grant",
                Some(&alpha),
                Some(alpha_resource),
                "RegistrationGranted",
                "ens_v1_registrar_l1",
                201,
                0,
                grant(1_900_000_000),
            ),
            switch_event(
                "switch-alpha-resolver",
                Some(&alpha),
                Some(alpha_resource),
                "ResolverChanged",
                "ens_v1_registry_l1",
                202,
                0,
                json!({"node": alpha_node, "resolver": SWITCH_RESOLVER}),
            ),
            switch_event(
                "switch-alpha-renewal",
                Some(&alpha),
                Some(alpha_resource),
                "RegistrationRenewed",
                "ens_v1_registrar_l1",
                203,
                0,
                json!({"expiry": 1_950_000_000i64}),
            ),
            switch_event(
                "switch-beta-resolver",
                Some(&beta),
                Some(beta_resource),
                "ResolverChanged",
                "ens_v1_registry_l1",
                205,
                0,
                json!({"node": beta.strip_prefix("ens:").expect("ens id"), "resolver": SWITCH_RESOLVER}),
            ),
            switch_event(
                "switch-beta-grant",
                Some(&beta),
                Some(beta_resource),
                "RegistrationGranted",
                "ens_v1_registrar_l1",
                204,
                0,
                grant(1_800_000_000),
            ),
        ],
    )
    .await?;
    publish_project_and_families(database, 240).await
}

#[tokio::test]
async fn v2_name_detail_is_the_same_with_the_switch_off_and_on() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_names_fixture(&database).await?;
    let (status, alpha) = assert_switch_differential(&database, "/v1/names/alpha.eth").await?;
    assert_eq!(status, StatusCode::OK, "{alpha:#}");
    assert_eq!(alpha["data"]["registrant"], json!(SWITCH_ALICE), "{alpha:#}");
    assert_eq!(
        alpha["data"]["resolver"]["address"],
        json!(SWITCH_RESOLVER),
        "{alpha:#}"
    );
    for uri in [
        "/v1/names/beta.eth",
        "/v1/names/alpha.eth?include=counts",
        "/v1/names/missing.eth",
        // The diagnostics name and authority reads take the same row (ruling J11).
        "/v1/diagnostics/names/alpha.eth/coverage",
        "/v1/diagnostics/names/alpha.eth/binding",
        "/v1/diagnostics/names/alpha.eth/authority",
    ] {
        assert_switch_differential(&database, uri).await?;
    }
    for uri in [
        "/v1/names/alpha.eth",
        "/v1/diagnostics/names/alpha.eth/authority",
    ] {
        assert_switch_on_ignores_served_tables(&database, uri, &["name_current"]).await?;
    }
    database.cleanup().await
}

// Ruling J5: a composed row describes the family marker's publication only, so an `at` below
// it answers 409 with the switch on (storage's `family_name_for_snapshot`), with the wording a
// served row that cannot prove the position gets today. Project restamps every served row with
// the publication it writes, so the switch-off side refuses the same `at` for the same reason
// (a row newer than the selected position) and the bodies are equal.
#[tokio::test]
async fn v2_name_detail_refuses_an_at_below_the_publication_both_ways() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_names_fixture(&database).await?;
    for block in [230, 239] {
        let at = crate::v2::format_timestamp(OffsetDateTime::from_unix_timestamp(
            1_700_000_000 + block,
        )?);
        let (status, body) =
            assert_switch_differential(&database, &format!("/v1/names/alpha.eth?at={at}")).await?;
        assert_eq!(status, StatusCode::CONFLICT, "{body:#}");
        assert_eq!(
            body["error"],
            json!({"code": "stale", "details": {},
                   "message": "requested snapshot is not available for name"}),
            "{body:#}"
        );
    }
    let at = crate::v2::format_timestamp(OffsetDateTime::from_unix_timestamp(1_700_000_240)?);
    let (status, _) =
        assert_switch_differential(&database, &format!("/v1/names/alpha.eth?at={at}")).await?;
    assert_eq!(status, StatusCode::OK);
    database.cleanup().await
}

fn switch_timestamp(seconds: i64) -> Result<String> {
    Ok(crate::v2::format_timestamp(
        OffsetDateTime::from_unix_timestamp(seconds)?,
    ))
}

#[tokio::test]
async fn v2_expiring_names_are_the_same_with_the_switch_off_and_on() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_names_fixture(&database).await?;
    let after = switch_timestamp(1_700_000_000)?;
    let before = switch_timestamp(1_960_000_000)?;
    for order in ["asc", "desc"] {
        let pages = assert_switch_differential_pages(
            &database,
            &format!(
                "/v1/names?namespace=ens&expires_after={after}&expires_before={before}\
                 &order={order}&page_size=1"
            ),
        )
        .await?;
        let names: Vec<&Value> = pages
            .iter()
            .flat_map(|page| page["data"].as_array().into_iter().flatten())
            .map(|row| &row["name"])
            .collect();
        let expected = if order == "asc" {
            vec![json!("beta.eth"), json!("alpha.eth")]
        } else {
            vec![json!("alpha.eth"), json!("beta.eth")]
        };
        assert_eq!(names, expected.iter().collect::<Vec<_>>(), "{pages:#?}");
    }
    // A window that holds only the renewed expiry, and one that holds only the replaced one.
    for (after, before, count) in [
        (1_940_000_000, 1_960_000_000, 1),
        (1_890_000_000, 1_910_000_000, 0),
    ] {
        let pages = assert_switch_differential_pages(
            &database,
            &format!(
                "/v1/names?namespace=ens&expires_after={}&expires_before={}&page_size=5",
                switch_timestamp(after)?,
                switch_timestamp(before)?
            ),
        )
        .await?;
        assert_eq!(pages[0]["data"].as_array().map(Vec::len), Some(count));
    }
    assert_switch_on_ignores_served_tables(
        &database,
        &format!("/v1/names?namespace=ens&expires_after={after}&page_size=5"),
        &["name_current"],
    )
    .await?;
    database.cleanup().await
}

#[tokio::test]
async fn v2_search_is_the_same_with_the_switch_off_and_on() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_names_fixture(&database).await?;
    for query in [
        "q=a&match=prefix&page_size=1",
        "q=eth&match=contains&page_size=1",
        "q=eth&match=contains&namespace=ens&page_size=5",
        "q=zzz&match=prefix&page_size=5",
    ] {
        assert_switch_differential_pages(&database, &format!("/v1/search?{query}")).await?;
    }
    let pages =
        assert_switch_differential_pages(&database, "/v1/search?q=eth&match=contains&page_size=1")
            .await?;
    assert_eq!(pages.len(), 2, "{pages:#?}");
    assert_switch_on_ignores_served_tables(
        &database,
        "/v1/search?q=eth&match=contains&page_size=5",
        &["name_current"],
    )
    .await?;
    database.cleanup().await
}


#[tokio::test]
async fn v2_resolver_bound_names_are_the_same_with_the_switch_off_and_on() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_names_fixture(&database).await?;
    // The route's resolver overview still reads the served resolver row (its move is packet E5),
    // which Project writes only for a declared resolver: seed one at the publication.
    sqlx::query(
        "INSERT INTO bigname_phase.resolver_current (chain_id, resolver_address,
             declared_summary, support_status, chain_positions, canonicality_summary,
             manifest_version)
         SELECT lineage.chain_id, $2, '{}'::jsonb, 'supported',
                jsonb_build_object('target_block_number', lineage.block_number,
                                   'target_block_hash', lineage.block_hash),
                jsonb_build_object('state', 'canonical_lineage'), 1
         FROM bigname_phase.chain_lineage lineage
         WHERE lineage.chain_id = $1 AND lineage.block_number = 240",
    )
    .bind(SWITCH_CHAIN)
    .bind(SWITCH_RESOLVER)
    .execute(&database.pool)
    .await?;
    let uri = format!("/v1/resolvers/1/{SWITCH_RESOLVER}?page_size=1");
    let pages = assert_switch_differential_pages_in(&database, &uri, "/data/bound_names").await?;
    let names: Vec<&Value> = pages
        .iter()
        .flat_map(|page| page["data"].as_array().into_iter().flatten())
        .map(|name| &name["name"])
        .collect();
    assert_eq!(names, [&json!("alpha.eth"), &json!("beta.eth")], "{pages:#?}");
    for uri in [
        format!("/v1/resolvers/1/{SWITCH_RESOLVER}"),
        format!("/v1/resolvers/1/{SWITCH_RESOLVER}?page_size=5"),
    ] {
        assert_switch_differential_pages_in(&database, &uri, "/data/bound_names").await?;
    }
    let (status, _) = assert_switch_differential(
        &database,
        "/v1/resolvers/1/0x0000000000000000000000000000000000000def",
    )
    .await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_switch_on_ignores_served_tables(
        &database,
        &format!("/v1/resolvers/1/{SWITCH_RESOLVER}"),
        &["name_current"],
    )
    .await?;
    database.cleanup().await
}

// The composed listings walk candidates in batches of at least 200, which a fixture of two names
// never fills; the test-only seam shrinks the batch so every page below straddles one (the
// storage-level comparison with cursors is apps/phase-runner/tests/families_shadow_name_batches.rs).
#[tokio::test]
async fn v2_name_listings_are_the_same_across_candidate_batches() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_names_fixture(&database).await?;
    sqlx::query(
        "INSERT INTO bigname_phase.resolver_current (chain_id, resolver_address,
             declared_summary, support_status, chain_positions, canonicality_summary,
             manifest_version)
         SELECT lineage.chain_id, $2, '{}'::jsonb, 'supported',
                jsonb_build_object('target_block_number', lineage.block_number,
                                   'target_block_hash', lineage.block_hash),
                jsonb_build_object('state', 'canonical_lineage'), 1
         FROM bigname_phase.chain_lineage lineage
         WHERE lineage.chain_id = $1 AND lineage.block_number = 240",
    )
    .bind(SWITCH_CHAIN)
    .bind(SWITCH_RESOLVER)
    .execute(&database.pool)
    .await?;
    let after = switch_timestamp(1_700_000_000)?;
    let before = switch_timestamp(1_960_000_000)?;
    for batch in [1, 2] {
        for uri in [
            "/v1/search?q=eth&match=contains&page_size=1".to_owned(),
            "/v1/search?q=a&match=prefix&page_size=1".to_owned(),
            format!(
                "/v1/names?namespace=ens&expires_after={after}&expires_before={before}\
                 &order=asc&page_size=1"
            ),
            format!(
                "/v1/names?namespace=ens&expires_after={after}&expires_before={before}\
                 &order=desc&page_size=1"
            ),
        ] {
            let pages = bigname_storage::families::name::seams::with_batch_size(
                batch,
                assert_switch_differential_pages(&database, &uri),
            )
            .await?;
            assert!(
                pages.iter().any(|page| page["data"].as_array().is_some_and(|rows| !rows.is_empty())),
                "{uri}: {pages:#?}"
            );
        }
        let uri = format!("/v1/resolvers/1/{SWITCH_RESOLVER}?page_size=1");
        let pages = bigname_storage::families::name::seams::with_batch_size(
            batch,
            assert_switch_differential_pages_in(&database, &uri, "/data/bound_names"),
        )
        .await?;
        assert_eq!(pages.len(), 2, "{pages:#?}");
    }
    database.cleanup().await
}

// A family rebuild in flight (the marker `bootstrap_pending`) leaves the families half built, so
// no composed row is servable: every route whose name rows are composed answers a stale 409 with
// the switch on, with its fence's wording when the fence refuses first (the collection routes
// say the collection publication is not available) and with the name wording when the composed
// read refuses.
#[tokio::test]
async fn v2_composed_name_reads_answer_409_while_the_families_rebuild() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_names_fixture(&database).await?;
    sqlx::query("UPDATE bigname_phase.project_family_marker SET state = 'bootstrap_pending'")
        .execute(&database.pool)
        .await?;
    let mut messages = Vec::new();
    for uri in [
        "/v1/names/alpha.eth",
        "/v1/names/alpha.eth/history",
        "/v1/names/alpha.eth/subnames",
        "/v1/permissions?name=alpha.eth",
    ] {
        let response = bigname_storage::publication_source::with_serve_from_families(
            true,
            v2_get_response(&database, uri),
        )
        .await?;
        let status = response.status();
        let body: Value = read_json(response).await?;
        assert_eq!(
            (status, &body["error"]["code"]),
            (StatusCode::CONFLICT, &json!("stale")),
            "{uri}: {body:#}"
        );
        messages.push((uri, body["error"]["message"].clone()));
    }
    assert_eq!(
        messages[0].1,
        json!("requested snapshot is not available for name"),
        "{messages:#?}"
    );
    database.cleanup().await
}
