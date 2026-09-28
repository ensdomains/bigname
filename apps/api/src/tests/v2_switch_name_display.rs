#[tokio::test]
async fn v2_search_cursor_keeps_its_independent_normalized_name_boundary() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_names_fixture(&database).await?;
    let uri = "/v1/search?q=eth&match=contains&namespace=ens&page_size=1";
    let (_, next) = list_cursor_page(&database, uri, "").await?;
    let cursor = list_cursor_at(
        &next.context("alpha has a continuation")?,
        &[("normalized_name", "aardvark.eth")],
    );
    let continuation = list_cursor_continue(uri, &cursor);
    for batch in [1, 200] {
        let (status, body) = bigname_storage::families::name::seams::with_batch_size(
            batch,
            read_family_response(&database, &continuation),
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "{body:#}");
        assert_eq!(body["data"][0]["name"], "alpha.eth", "{body:#}");
        assert_eq!(body["page"]["has_more"], true, "{body:#}");
    }
    database.cleanup().await
}

/// Interpret stores the normalized labels as the surface. Display is recovered with the same
/// shared normalizer when serving it; it is not a separately forced fixture value.
async fn seed_switch_emoji_names(database: &TestDatabase) -> Result<()> {
    seed_bounded_membership_blocks(database, 240).await?;
    for (index, input) in ["🅰️🅱.eth", "🅰️🅲.eth"].into_iter().enumerate() {
        let normalized = bigname_domain::normalization::normalize_name(input)?;
        assert_ne!(
            normalized.normalized_name,
            normalized.canonical_display_name
        );
        let (name, resource) = seed_switch_name(
            database,
            &normalized.normalized_name,
            0x968_0000 + index as u128 * 16,
            "ens_v1",
        )
        .await?;
        let stored: String =
            sqlx::query_scalar("SELECT raw_name FROM name_surfaces WHERE logical_name_id = $1")
                .bind(&name)
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(stored, normalized.normalized_name);
        bigname_storage::insert_normalized_event_fixtures(
            &database.pool,
            &[
                switch_event(
                    &format!("emoji-grant-{index}"),
                    Some(&name),
                    Some(resource),
                    "RegistrationGranted",
                    "ens_v1_registrar_l1",
                    201,
                    index as i64,
                    json!({"authority_kind": "registrar", "registrant": SWITCH_ALICE,
                       "expiry": 1_900_000_000i64}),
                ),
                switch_event(
                    &format!("emoji-resolver-{index}"),
                    Some(&name),
                    Some(resource),
                    "ResolverChanged",
                    "ens_v1_registry_l1",
                    202,
                    index as i64,
                    json!({"node": name.trim_start_matches("ens:"), "resolver": SWITCH_RESOLVER}),
                ),
            ],
        )
        .await?;
    }
    seed_switch_resolver_declaration(database).await?;
    publish_test_families(database, 240).await
}

#[tokio::test]
async fn v2_bound_name_display_and_cursors_use_shared_emoji_normalization() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_emoji_names(&database).await?;
    let uri = format!("/v1/resolvers/1/{SWITCH_RESOLVER}?page_size=1");
    let pages = read_family_pages_in(&database, &uri, "/data/bound_names").await?;
    assert_eq!(pages.len(), 2);
    for page in &pages {
        let row = &page["data"][0];
        let name = row["name"].as_str().context("bound name")?;
        let normalized = bigname_domain::normalization::normalize_name(name)?;
        assert_eq!(row["display_name"], normalized.canonical_display_name);
        assert_eq!(row["name"], normalized.normalized_name);
    }
    {
        let response = v2_get_response(&database, &uri).await?;
        let body: Value = read_json(response).await?;
        let bound = &body["data"]["bound_names"];
        let cursor = bound["page"]["next_cursor"]
            .as_str()
            .context("first emoji name has a continuation")?;
        let decoded = crate::v2::decode(cursor).expect("bound cursor decodes");
        let normalized = bigname_domain::normalization::normalize_name(
            bound["data"][0]["name"].as_str().context("bound name")?,
        )?;
        assert_eq!(
            decoded.last_item["sort_value"],
            normalized.canonical_display_name
        );
        assert_eq!(
            decoded.last_item["normalized_name"],
            normalized.normalized_name
        );
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_address_name_display_uses_shared_emoji_normalization() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_emoji_names(&database).await?;
    let uri = format!("/v1/addresses/{SWITCH_ALICE}/names?namespace=ens&page_size=1");
    let pages = read_family_pages(&database, &uri).await?;
    assert_eq!(pages.len(), 2);
    for page in &pages {
        let row = &page["data"][0];
        let normalized = bigname_domain::normalization::normalize_name(
            row["name"].as_str().context("address name")?,
        )?;
        assert_eq!(row["name"], normalized.normalized_name);
        assert_eq!(row["display_name"], normalized.canonical_display_name);
        assert_ne!(row["name"], row["display_name"]);
    }
    database.cleanup().await
}
