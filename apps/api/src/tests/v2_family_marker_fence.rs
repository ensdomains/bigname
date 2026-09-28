// Collections and lookup heads capture the family publication and revalidate it after reading.
// Expiry filters use its block timestamp on both first pages and continuations.

/// Advance the generation during a paused read, modeling the marker change of a family commit.
async fn commit_family_block(pool: &PgPool) -> Result<()> {
    sqlx::query("UPDATE bigname_phase.project_family_marker SET sequence = sequence + 1")
        .execute(pool)
        .await?;
    Ok(())
}

async fn seed_marker_collection_fixture(database: &TestDatabase) -> Result<()> {
    seed_family_names_fixture(database).await
}

fn ens_head_scope() -> bigname_storage::SnapshotSelectionScope {
    bigname_storage::SnapshotSelectionScope::new(
        vec![bigname_storage::SnapshotPositionRequirement::new(
            "ethereum",
            "ethereum-mainnet",
        )],
        Some("ethereum".to_owned()),
    )
    .expect("single-chain scope is valid")
}

#[tokio::test]
async fn v2_collection_refuses_a_family_block_committed_during_the_read() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_marker_collection_fixture(&database).await?;
    let state = database.app_state();
    let snapshot = crate::v2::collection_snapshot::CollectionSnapshot::capture_for_namespace(
        &state,
        None,
        Some("ens"),
    )
    .await
    .expect("a live family publication is servable");
    commit_family_block(&database.pool).await?;
    let error = snapshot
        .finish(&state)
        .await
        .expect_err("the generation changed");
    assert_eq!(error.code(), crate::v2::ErrorCode::Stale);
    assert_eq!(
        error.envelope().error.message,
        "collection publication changed during the read; retry the request"
    );

    let first = crate::v2::collection_snapshot::CollectionSnapshot::capture_for_namespace(
        &state,
        None,
        Some("ens"),
    )
    .await
    .expect("capture the new generation");
    let cursor = crate::v2::encode(&first.bind_cursor(crate::v2::CursorPayload::new(
        "test",
        Default::default(),
        Default::default(),
        None,
    )));
    let continued = crate::v2::collection_snapshot::CollectionSnapshot::capture_for_namespace(
        &state,
        Some(&cursor),
        Some("ens"),
    )
    .await
    .expect("the cursor belongs to the current generation");
    commit_family_block(&database.pool).await?;
    let error = continued
        .finish(&state)
        .await
        .expect_err("the generation changed");
    assert_eq!(
        error.envelope().error.message,
        "collection publication is no longer available; restart pagination without a cursor"
    );
    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_head_refuses_a_family_block_committed_during_the_read() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_marker_collection_fixture(&database).await?;
    let served = crate::v2::lookup::head::load_served_head(&database.pool, &ens_head_scope())
        .await
        .expect("the served head loads")
        .context("the chain has a head")?;
    commit_family_block(&database.pool).await?;
    let error = crate::v2::lookup::head::revalidate_served_head(&database.pool, &served)
        .await
        .expect_err("the served generation moved");
    assert_eq!(
        error.envelope().error.message,
        "served data changed while the lookup was being read"
    );
    database.cleanup().await
}

#[tokio::test]
async fn v2_collection_expiry_clock_is_the_published_block_time() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_marker_collection_fixture(&database).await?;
    let state = database.app_state();
    let block_time = OffsetDateTime::from_unix_timestamp(1_700_000_240)?;
    let carried = OffsetDateTime::from_unix_timestamp(1_700_000_200)?;
    let first = crate::v2::collection_snapshot::CollectionSnapshot::capture_for_namespace(
        &state,
        None,
        Some("ens"),
    )
    .await
    .expect("the publication is servable");
    assert_eq!(first.evaluated_at(), block_time);
    let mut payload = first.bind_cursor(crate::v2::CursorPayload::new(
        "test",
        Default::default(),
        Default::default(),
        None,
    ));
    payload.evaluated_at = Some(crate::v2::format_timestamp(carried));
    let cursor = crate::v2::encode(&payload);
    let continued = crate::v2::collection_snapshot::CollectionSnapshot::capture_for_namespace(
        &state,
        Some(&cursor),
        Some("ens"),
    )
    .await
    .expect("the continuation is servable");
    assert_eq!(continued.evaluated_at(), block_time);
    database.cleanup().await
}

// Two expires after the publication but before wall time; one expired before the publication.
const TWO_EXPIRY: i64 = 1_750_000_000;
const ONE_EXPIRY: i64 = 1_600_000_000;

#[tokio::test]
async fn v2_get_subnames_include_expired_false_is_evaluated_at_the_published_block_time()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_family_children_fixture_expiring(&database, ONE_EXPIRY, TWO_EXPIRY).await?;
    let published: Vec<i64> = sqlx::query_scalar(
        "SELECT extract(epoch FROM block_timestamp)::bigint FROM project_family_marker",
    )
    .fetch_all(&database.pool)
    .await?;
    let [published] = published[..] else {
        anyhow::bail!("expected one family publication")
    };
    assert!(
        ONE_EXPIRY < published
            && published < TWO_EXPIRY
            && TWO_EXPIRY < OffsetDateTime::now_utc().unix_timestamp()
    );
    let filtered = "/v1/names/alpha.eth/subnames?include_expired=false&page_size=1";
    let mut names = Vec::new();
    let mut uri = filtered.to_owned();
    loop {
        let payload = v2_subnames_payload_for_database(&database, &uri).await?;
        assert_eq!(payload["page"]["total_count"], json!(3), "{uri}");
        let page = v2_subname_names(&payload);
        assert!(!page.iter().any(|name| name == "one.alpha.eth"));
        names.extend(page);
        let Some(cursor) = payload["page"]["next_cursor"].as_str() else {
            break;
        };
        uri = format!("{filtered}&cursor={cursor}");
    }
    assert_eq!(
        names,
        vec!["carol.alpha.eth", "dave.alpha.eth", "two.alpha.eth"]
    );
    let unfiltered = v2_subnames_payload_for_database(
        &database,
        "/v1/names/alpha.eth/subnames?include_expired=true",
    )
    .await?;
    assert!(
        v2_subname_names(&unfiltered)
            .iter()
            .any(|name| name == "one.alpha.eth")
    );
    database.cleanup().await
}

#[tokio::test]
async fn api_preflight_requires_the_family_marker() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    sqlx::query("DROP TABLE bigname_phase.project_family_marker")
        .execute(&database.lookup_pool)
        .await?;
    let missing = bigname_storage::load_missing_api_lookup_ddl(&database.lookup_pool).await?;
    assert!(
        missing
            .iter()
            .any(|object| object.identity == "bigname_phase.project_family_marker")
    );
    database.cleanup().await
}

fn collection_next_cursor(payload: &Value) -> Option<String> {
    payload["page"]["next_cursor"].as_str().map(str::to_owned)
}

/// An issued publication-bound continuation keeps refusing after the generation changes;
/// restarting from the first page issues a usable continuation.
async fn assert_family_publication_restarts(
    database: &TestDatabase,
    first_page: &str,
) -> Result<()> {
    let page = v2_resolver_payload_for_database(database, first_page).await?;
    let cursor = collection_next_cursor(&page).context("a continuation")?;
    commit_family_block(&database.pool).await?;
    for attempt in ["first", "second"] {
        let response =
            v2_resolver_response_for_database(database, &format!("{first_page}&cursor={cursor}"))
                .await?;
        assert_eq!(response.status(), StatusCode::CONFLICT, "{attempt}");
        let body: Value = read_json(response).await?;
        assert_eq!(body["error"]["code"], json!("stale"));
        assert_eq!(body["error"]["message"], json!(RESTART_WITHOUT_CURSOR));
    }
    let restarted = v2_resolver_payload_for_database(database, first_page).await?;
    let cursor = collection_next_cursor(&restarted).context("a fresh continuation")?;
    v2_resolver_payload_for_database(database, &format!("{first_page}&cursor={cursor}")).await?;
    Ok(())
}

#[tokio::test]
async fn v2_a_subnames_cursor_restarts_after_family_publication_changes() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_family_children_fixture(&database).await?;
    assert_family_publication_restarts(&database, "/v1/names/alpha.eth/subnames?page_size=1")
        .await?;
    database.cleanup().await
}

#[tokio::test]
async fn v2_a_resolver_overview_cursor_continues_after_family_publication_changes() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_family_routes_fixture(&database).await?;
    let base = format!("/v1/resolvers/1/{FAMILY_RESOLVER}?page_size=1");
    let page = v2_resolver_payload_for_database(&database, &base).await?;
    assert_eq!(
        page["data"]["bound_names"]["data"][0]["name"],
        json!("alpha.eth")
    );
    let cursor = page["data"]["bound_names"]["page"]["next_cursor"]
        .as_str()
        .context("a continuation")?;
    commit_family_block(&database.pool).await?;
    let next =
        v2_resolver_payload_for_database(&database, &format!("{base}&cursor={cursor}")).await?;
    assert_eq!(
        next["data"]["bound_names"]["data"][0]["name"],
        json!("beta.eth")
    );
    assert_eq!(
        next["data"]["bound_names"]["page"]["next_cursor"],
        Value::Null
    );
    database.cleanup().await
}

#[tokio::test]
async fn v2_a_resolver_collection_cursor_binds_its_family_generation() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_family_permissions_fixture(&database).await?;
    let base = format!("/v1/resolvers/1/{FAMILY_V2_RESOLVER}/roles?page_size=1");
    assert_family_publication_restarts(&database, &base).await?;
    let page = v2_resolver_payload_for_database(&database, &base).await?;
    let mut spliced = crate::v2::decode(&collection_next_cursor(&page).context("a continuation")?)
        .expect("issued cursor decodes");
    assert!(spliced.last_item.contains_key("generation"));
    commit_family_block(&database.pool).await?;
    let fresh = v2_resolver_payload_for_database(&database, &base).await?;
    let fresh = collection_next_cursor(&fresh).context("a continuation")?;
    let publication = crate::v2::decode(&fresh)
        .expect("fresh cursor decodes")
        .last_item["publication"]
        .clone();
    spliced
        .last_item
        .insert("publication".to_owned(), publication);
    let response = v2_resolver_response_for_database(
        &database,
        &format!("{base}&cursor={}", crate::v2::encode(&spliced)),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body: Value = read_json(response).await?;
    assert_eq!(
        body["error"]["message"],
        json!("resolver collection changed; restart pagination")
    );
    v2_resolver_payload_for_database(&database, &format!("{base}&cursor={fresh}")).await?;
    database.cleanup().await
}

#[tokio::test]
async fn api_preflight_requires_a_readable_family_marker() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let role = format!("marker_preflight_{}", std::process::id());
    for statement in [
        format!("CREATE ROLE {role} NOLOGIN"),
        format!("GRANT USAGE ON SCHEMA bigname_phase TO {role}"),
        format!("GRANT SELECT ON ALL TABLES IN SCHEMA bigname_phase TO {role}"),
        format!("REVOKE SELECT ON bigname_phase.project_family_marker FROM {role}"),
    ] {
        sqlx::query(&statement)
            .execute(&database.lookup_pool)
            .await?;
    }
    let config = database.database_config(1)?;
    let options = PgConnectOptions::from_str(config.database_url.as_deref().context("test URL")?)?;
    let set_role = format!("SET ROLE {role}");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .after_connect(move |connection, _| {
            let set_role = set_role.clone();
            Box::pin(async move { sqlx::query(&set_role).execute(connection).await.map(|_| ()) })
        })
        .connect_with(options)
        .await?;
    let missing = bigname_storage::load_missing_api_lookup_ddl(&pool).await?;
    assert!(
        missing
            .iter()
            .any(|object| object.identity == "bigname_phase.project_family_marker")
    );
    let error = crate::startup_preflight::ensure_verified_lookup_ddl_available(&pool)
        .await
        .expect_err("startup must reject an unreadable family marker");
    assert!(format!("{error:#}").contains("bigname_phase.project_family_marker"));
    pool.close().await;
    sqlx::query(&format!("DROP OWNED BY {role}"))
        .execute(&database.lookup_pool)
        .await?;
    sqlx::query(&format!("DROP ROLE {role}"))
        .execute(&database.lookup_pool)
        .await?;
    database.cleanup().await
}
