// Collections read the captured family publication on one snapshot; lookup heads revalidate it
// after reading. Expiry filters use its block timestamp on both first pages and continuations.

/// Advance the generation during a paused read, modeling the marker change of a family commit.
/// An intentional direct write: the fixtures publish through the family publisher, but a
/// commit landing inside a paused read can only be placed deterministically by bumping the
/// marker here.
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
async fn v2_collection_serves_the_captured_publication_after_a_block_lands_mid_read() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_marker_collection_fixture(&database).await?;
    let state = database.app_state();
    let mut snapshot = crate::v2::collection_snapshot::CollectionSnapshot::capture_for_namespace(
        &state,
        None,
        Some("ens"),
    )
    .await
    .expect("a live family publication is servable");
    let captured = snapshot.meta().expect("the captured publication has a meta");
    let marker_sequence = "SELECT sequence::TEXT FROM bigname_phase.project_family_marker";
    let read_before: String = sqlx::query_scalar(marker_sequence)
        .fetch_one(snapshot.conn().await.expect("the snapshot serves the capture"))
        .await?;
    commit_family_block(&database.pool).await?;
    let read_after: String = sqlx::query_scalar(marker_sequence)
        .fetch_one(snapshot.conn().await.expect("the snapshot stays open"))
        .await?;
    assert_eq!(read_after, read_before, "a later read sees the captured block");
    let meta = snapshot
        .finish(&state)
        .await
        .expect("a block after the snapshot began does not refuse the page");
    assert_eq!(meta.as_of, captured.as_of);
    database.cleanup().await
}

#[tokio::test]
async fn v2_collection_retries_a_block_committed_before_the_snapshot_begins() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_marker_collection_fixture(&database).await?;
    let state = database.app_state();
    let cursor = crate::v2::encode(&crate::v2::CursorPayload::new(
        "test",
        Default::default(),
        Default::default(),
        None,
    ));
    for cursor in [None, Some(cursor.as_str())] {
        let mut snapshot =
            crate::v2::collection_snapshot::CollectionSnapshot::capture_for_namespace(
                &state,
                cursor,
                Some("ens"),
            )
            .await
            .expect("a live family publication is servable");
        commit_family_block(&database.pool).await?;
        let error = snapshot
            .conn()
            .await
            .expect_err("the snapshot no longer serves the capture");
        assert_eq!(error.code(), crate::v2::ErrorCode::Stale);
        assert_eq!(
            error.envelope().error.message,
            "collection publication changed during the read; retry the request"
        );

        let mut retried =
            crate::v2::collection_snapshot::CollectionSnapshot::capture_for_namespace(
                &state,
                cursor,
                Some("ens"),
            )
            .await
            .expect("the retry captures the new publication");
        retried.conn().await.expect("the retry is served");
        retried.finish(&state).await.expect("the retry finishes");
    }
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
    let mut payload = crate::v2::CursorPayload::new(
        "test",
        Default::default(),
        Default::default(),
        None,
    );
    payload.evaluated_at = Some(bigname_storage::UnixSeconds::from(carried).internal_string());
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
        assert_eq!(payload["page"]["total_count"], json!(2), "{uri}");
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
        // dave's node is owned by the zero address (seed_family_children_fixture).
        vec!["carol.alpha.eth", "two.alpha.eth"]
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

/// A continuation remains usable after the family publication sequence advances.
async fn assert_family_publication_continues(database: &TestDatabase, first_page: &str) -> Result<()> {
    let page = v2_resolver_payload_for_database(database, first_page).await?;
    let cursor = collection_next_cursor(&page).context("a continuation")?;
    let payload = crate::v2::decode(&cursor).expect("issued position");
    assert!(payload.snapshot.is_none());
    assert!(payload.evaluated_at.is_none());
    let before = v2_resolver_payload_for_database(database, &format!("{first_page}&cursor={cursor}")).await?;
    commit_family_block(&database.pool).await?;
    let after = v2_resolver_payload_for_database(database, &format!("{first_page}&cursor={cursor}")).await?;
    assert_eq!(before["data"], after["data"]);
    Ok(())
}

#[tokio::test]
async fn v2_a_subnames_cursor_continues_after_family_publication_changes() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_family_children_fixture(&database).await?;
    assert_family_publication_continues(&database, "/v1/names/alpha.eth/subnames?page_size=1")
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
async fn v2_a_resolver_collection_cursor_continues_and_rejects_old_generation_shape() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_family_permissions_fixture(&database).await?;
    let base = format!("/v1/resolvers/1/{FAMILY_V2_RESOLVER}/roles?page_size=1");
    assert_family_publication_continues(&database, &base).await?;
    let page = v2_resolver_payload_for_database(&database, &base).await?;
    let mut spliced = crate::v2::decode(&collection_next_cursor(&page).context("a continuation")?)
        .expect("issued cursor decodes");
    assert!(!spliced.last_item.contains_key("generation"));
    spliced.last_item.insert("generation".to_owned(), "old-generation".to_owned());
    let response = v2_resolver_response_for_database(
        &database,
        &format!("{base}&cursor={}", crate::v2::encode(&spliced)),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
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

/// The documented `bigname_api` grants cover the family reads and snapshot guard;
/// preflight identifies any relation the family readers join that the login cannot read.
#[tokio::test]
async fn api_preflight_and_documented_grant_cover_the_family_reads() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let role = format!("family_grant_preflight_{}", std::process::id());
    let deployment = include_str!("../../../../docs/deployment.md")
        .split_once("## Surviving services")
        .context("deployment docs must contain the API service grants")?
        .1;
    let grants = deployment
        .split_once("GRANT SELECT ON TABLE\n")
        .context("deployment docs must contain the API SELECT grant")?
        .1
        .split_once("TO bigname_api;")
        .context("deployment docs must terminate the API SELECT grant")?
        .0;
    let guard = deployment
        .split_once("GRANT EXECUTE ON FUNCTION ")
        .context("deployment docs must contain the API guard grant")?
        .1
        .split_once("TO bigname_api;")
        .context("deployment docs must terminate the API guard grant")?
        .0;
    for statement in [
        format!("CREATE ROLE {role} NOLOGIN"),
        format!("GRANT USAGE ON SCHEMA bigname_phase TO {role}"),
        format!("GRANT SELECT ON TABLE {grants} TO {role}"),
        format!("GRANT EXECUTE ON FUNCTION {guard} TO {role}"),
    ] {
        sqlx::query(&statement)
            .execute(&database.lookup_pool)
            .await?;
    }
    let config = database.database_config(1)?;
    let options =
        PgConnectOptions::from_str(config.database_url.as_deref().context("test URL")?)?;
    let set_role = format!("SET ROLE {role}");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .after_connect(move |connection, _| {
            let set_role = set_role.clone();
            Box::pin(async move { sqlx::query(&set_role).execute(connection).await.map(|_| ()) })
        })
        .connect_with(options)
        .await?;
    let missing = || {
        let pool = pool.clone();
        async move {
            let missing = bigname_storage::load_missing_api_lookup_ddl(&pool).await?;
            anyhow::Ok(
                missing
                    .into_iter()
                    .map(|object| object.identity)
                    .collect::<Vec<_>>(),
            )
        }
    };
    assert_eq!(missing().await?, Vec::<String>::new());
    let mut relation_diagnostics = Vec::new();
    for relation in [
        "name_search_documents",
        "name_search_postings",
        "project_address_history_anchor",
        "project_history_source",
        "project_history_source_edge",
        "project_history_catalogue_marker",
    ] {
        sqlx::query(&format!(
            "REVOKE SELECT ON bigname_phase.{relation} FROM {role}"
        ))
        .execute(&database.lookup_pool)
        .await?;
        let unavailable = missing().await?;
        let startup_error = crate::startup_preflight::ensure_verified_lookup_ddl_available(&pool)
            .await
            .err()
            .map(|error| format!("{error:#}"));
        relation_diagnostics.push((format!("bigname_phase.{relation}"), unavailable, startup_error));
        sqlx::query(&format!(
            "GRANT SELECT ON bigname_phase.{relation} TO {role}"
        ))
        .execute(&database.lookup_pool)
        .await?;
        assert_eq!(missing().await?, Vec::<String>::new());
    }
    for relation in ["project_name_state", "label_preimages"] {
        sqlx::query(&format!(
            "REVOKE SELECT ON bigname_phase.{relation} FROM {role}"
        ))
        .execute(&database.lookup_pool)
        .await?;
    }
    assert_eq!(
        missing().await?,
        vec![
            "bigname_phase.label_preimages".to_owned(),
            "bigname_phase.project_name_state".to_owned(),
        ],
        "the family readers need both"
    );
    pool.close().await;
    sqlx::query(&format!("DROP OWNED BY {role}"))
        .execute(&database.lookup_pool)
        .await?;
    sqlx::query(&format!("DROP ROLE {role}"))
        .execute(&database.lookup_pool)
        .await?;
    assert!(
        relation_diagnostics.iter().all(|(relation, unavailable, error)| {
            unavailable == &vec![relation.clone()]
                && error.as_ref().is_some_and(|error| error.contains(relation))
        }),
        "each unreadable serving relation must be named by startup preflight: {relation_diagnostics:#?}"
    );
    database.cleanup().await
}
