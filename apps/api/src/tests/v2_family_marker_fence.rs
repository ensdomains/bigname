// The serving fence and the expiry clock under the publication switch (TYR-36 step 7b).
//
// With the switch on, a collection read is admitted against the family marker: its `sequence`
// is the generation the finish recheck compares, so a family block committed during a read
// refuses it (retry on a first page, restart on a continuation), and the expiry clock is the
// published block's timestamp, the same on the first page and every continuation. With the
// switch off, the marker is ignored and the Project row governs as before.

/// A live family marker on the Project row's current publication, at `sequence`.
async fn seed_live_family_marker(pool: &PgPool, sequence: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO bigname_phase.project_family_marker
             (chain_id, current_block_number, current_block_hash, block_timestamp,
              input_content_hash, sequence, state)
         SELECT project.chain_id, project.current_block_number, project.current_block_hash,
                lineage.block_timestamp, project.input_content_hash, $1, 'live'
         FROM bigname_phase.chain_phase_state project
         JOIN bigname_phase.chain_lineage lineage
           ON lineage.chain_id = project.chain_id
          AND lineage.block_number = project.current_block_number
          AND lineage.block_hash = project.current_block_hash
         WHERE project.phase_name = 'project'
         ON CONFLICT (chain_id) DO UPDATE SET
             current_block_number = EXCLUDED.current_block_number,
             current_block_hash = EXCLUDED.current_block_hash,
             block_timestamp = EXCLUDED.block_timestamp,
             input_content_hash = EXCLUDED.input_content_hash,
             sequence = EXCLUDED.sequence,
             state = EXCLUDED.state",
    )
    .bind(sequence)
    .execute(pool)
    .await?;
    Ok(())
}

/// What a family block commit does to the served generation: the marker's sequence grows.
async fn commit_family_block(pool: &PgPool) -> Result<()> {
    sqlx::query("UPDATE bigname_phase.project_family_marker SET sequence = sequence + 1")
        .execute(pool)
        .await?;
    Ok(())
}

async fn seed_marker_collection_fixture(database: &TestDatabase) -> Result<()> {
    seed_v2_names_fixture(database).await?;
    seed_schema_v2_ens_lookup_head(
        &database.pool,
        100,
        "0xcollection-head",
        "2026-06-10T00:00:00Z",
    )
    .await?;
    seed_live_family_marker(&database.pool, 1).await
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
async fn v2_collection_refuses_a_family_block_committed_during_the_read_with_the_switch_on()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_marker_collection_fixture(&database).await?;
    let state = database.app_state();
    bigname_storage::publication_source::with_serve_from_families(true, async {
        let snapshot = crate::v2::collection_snapshot::CollectionSnapshot::capture_for_namespace(
            &state,
            None,
            Some("ens"),
        )
        .await
        .expect("a live marker at the head is servable");
        commit_family_block(&database.pool).await?;
        let error = snapshot
            .finish(&state)
            .await
            .expect_err("a family block during the read changes the generation");
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
        let continued =
            crate::v2::collection_snapshot::CollectionSnapshot::capture_for_namespace(
                &state,
                Some(&cursor),
                Some("ens"),
            )
            .await
            .expect("the cursor is bound to the current generation");
        commit_family_block(&database.pool).await?;
        let error = continued
            .finish(&state)
            .await
            .expect_err("a family block during a continued read changes the generation");
        assert_eq!(
            error.envelope().error.message,
            "collection publication is no longer available; restart pagination without a cursor"
        );
        anyhow::Ok(())
    })
    .await?;
    database.cleanup().await
}

#[tokio::test]
async fn v2_collection_ignores_the_family_marker_with_the_switch_off() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_marker_collection_fixture(&database).await?;
    let state = database.app_state();
    bigname_storage::publication_source::with_serve_from_families(false, async {
        let snapshot = crate::v2::collection_snapshot::CollectionSnapshot::capture_for_namespace(
            &state,
            None,
            Some("ens"),
        )
        .await
        .expect("the Project row is servable");
        commit_family_block(&database.pool).await?;
        sqlx::query("UPDATE bigname_phase.project_family_marker SET state = 'bootstrap_pending'")
            .execute(&database.pool)
            .await?;
        snapshot
            .finish(&state)
            .await
            .expect("the marker does not fence reads while the switch is off");
        anyhow::Ok(())
    })
    .await?;
    database.cleanup().await
}

#[tokio::test]
async fn v2_lookup_head_refuses_a_family_block_committed_during_the_read_with_the_switch_on()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_marker_collection_fixture(&database).await?;
    for on in [true, false] {
        bigname_storage::publication_source::with_serve_from_families(on, async {
            let served = crate::v2::lookup::head::load_served_head(&database.pool, &ens_head_scope())
                .await
                .expect("the served head loads")
                .expect("the chain has a head");
            commit_family_block(&database.pool).await?;
            let revalidated =
                crate::v2::lookup::head::revalidate_served_head(&database.pool, &served).await;
            if on {
                let error = revalidated.expect_err("the served generation moved");
                assert_eq!(
                    error.envelope().error.message,
                    "served data changed while the lookup was being read"
                );
            } else {
                revalidated.expect("the marker is not the served generation with the switch off");
            }
            anyhow::Ok(())
        })
        .await?;
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_collection_expiry_clock_is_the_published_block_time_with_the_switch_on() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    seed_marker_collection_fixture(&database).await?;
    let state = database.app_state();
    let block_time = parse_rfc3339_utc_timestamp("2026-06-10T00:00:00Z")
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    let carried = parse_rfc3339_utc_timestamp("2026-06-01T00:00:00Z")
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    let capture = |cursor: Option<String>| {
        let state = state.clone();
        async move {
            crate::v2::collection_snapshot::CollectionSnapshot::capture_for_namespace(
                &state,
                cursor.as_deref(),
                Some("ens"),
            )
            .await
            .expect("the publication is servable")
        }
    };
    // A continuation carrying an older evaluation time, as a cursor issued earlier would.
    let cursor_at = |snapshot: &crate::v2::collection_snapshot::CollectionSnapshot| {
        let mut payload = snapshot.bind_cursor(crate::v2::CursorPayload::new(
            "test",
            Default::default(),
            Default::default(),
            None,
        ));
        payload.evaluated_at = Some(crate::v2::format_timestamp(carried));
        crate::v2::encode(&payload)
    };

    bigname_storage::publication_source::with_serve_from_families(true, async {
        let first = capture(None).await;
        assert_eq!(first.evaluated_at(), block_time, "first page: the block time");
        let continued = capture(Some(cursor_at(&first))).await;
        assert_eq!(
            continued.evaluated_at(),
            block_time,
            "continuation: the block time, not the cursor's"
        );
    })
    .await;

    bigname_storage::publication_source::with_serve_from_families(false, async {
        let first = capture(None).await;
        assert!(
            first.evaluated_at() > block_time,
            "switch off: a first page is evaluated at the request time"
        );
        let continued = capture(Some(cursor_at(&first))).await;
        assert_eq!(
            continued.evaluated_at(),
            carried,
            "switch off: a continuation keeps the cursor's evaluation time"
        );
    })
    .await;
    database.cleanup().await
}

/// Seeds a declared child of `parent.eth` whose registration expires at `expiry`.
async fn seed_subname_expiring_at(
    database: &TestDatabase,
    label: &str,
    block_number: i64,
    id: u128,
    expiry: &str,
) -> Result<()> {
    let name = format!("{label}.parent.eth");
    let logical_name_id = format!("ens:{name}");
    let namehash = format!("node:{name}");
    seed_v2_subnames_bound_child(
        database,
        &logical_name_id,
        &name,
        &namehash,
        block_number,
        Uuid::from_u128(0x4000 + id),
        Uuid::from_u128(0x5000 + id),
        Uuid::from_u128(0x6000 + id),
        json!({
            "registration": {
                "status": "active",
                "authority_kind": "registrar",
                "registrant": "0x00000000000000000000000000000000000000fB",
                "registered_at": "2020-01-02T03:04:05Z",
                "expiry": expiry
            },
            "control": {
                "registry_owner": "0x00000000000000000000000000000000000000fA"
            }
        }),
    )
    .await?;
    upsert_phase_children_current_rows(
        &database.pool,
        &[v2_subnames_declared_child_row(
            "ens:parent.eth",
            &logical_name_id,
            &name,
            &namehash,
            900 + block_number,
            block_number,
        )],
    )
    .await?;
    Ok(())
}

/// Zeta expires after the published block's time but before the request time: live at the
/// publication, expired by the wall clock. Epsilon expired before the published block: expired
/// by either clock.
const ZETA_EXPIRY: &str = "2026-06-01T00:00:00Z";
const EPSILON_EXPIRY: &str = "2025-06-01T00:00:00Z";

#[tokio::test]
async fn v2_get_subnames_include_expired_false_is_evaluated_at_the_published_block_time()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_fixture(&database).await?;
    seed_subname_expiring_at(&database, "zeta", 86, 0x40, ZETA_EXPIRY).await?;
    seed_subname_expiring_at(&database, "epsilon", 87, 0x41, EPSILON_EXPIRY).await?;
    seed_live_family_marker(&database.pool, 1).await?;

    // Pin the fixture's clocks, so the assertions below mean what the names say.
    let published: Vec<sqlx::types::time::OffsetDateTime> =
        sqlx::query_scalar("SELECT block_timestamp FROM bigname_phase.project_family_marker")
            .fetch_all(&database.pool)
            .await?;
    let [published] = published[..] else {
        anyhow::bail!("expected one family marker, found {}", published.len());
    };
    let expiry = |value: &str| {
        parse_rfc3339_utc_timestamp(value).map_err(|error| anyhow::anyhow!("{error}"))
    };
    assert!(
        expiry(EPSILON_EXPIRY)? < published
            && published < expiry(ZETA_EXPIRY)?
            && expiry(ZETA_EXPIRY)? < sqlx::types::time::OffsetDateTime::now_utc(),
        "fixture: epsilon expiry < publication ({published}) < zeta expiry < now"
    );

    let filtered = "/v1/names/Parent.eth/subnames?include_expired=false&page_size=1";
    bigname_storage::publication_source::with_serve_from_families(true, async {
        let mut names = Vec::new();
        let mut uri = filtered.to_owned();
        loop {
            let payload = v2_subnames_payload_for_database(&database, &uri).await?;
            assert_eq!(payload["page"]["total_count"], json!(3), "{uri}");
            let page = v2_subname_names(&payload);
            assert!(
                !page.iter().any(|name| name == "epsilon.parent.eth"),
                "{uri}: epsilon expired before the published block"
            );
            names.extend(page);
            let Some(cursor) = payload["page"]["next_cursor"].as_str() else {
                break;
            };
            uri = format!("{filtered}&cursor={cursor}");
        }
        assert_eq!(
            names,
            vec!["alpha.parent.eth", "gamma.parent.eth", "zeta.parent.eth"],
            "zeta expires after the published block, so every page keeps it"
        );
        let unfiltered = v2_subnames_payload_for_database(
            &database,
            "/v1/names/Parent.eth/subnames?include_expired=true",
        )
        .await?;
        assert!(
            v2_subname_names(&unfiltered)
                .iter()
                .any(|name| name == "epsilon.parent.eth"),
            "epsilon is a child; only the expiry filter drops it"
        );
        anyhow::Ok(())
    })
    .await?;

    bigname_storage::publication_source::with_serve_from_families(false, async {
        let payload = v2_subnames_payload_for_database(
            &database,
            "/v1/names/Parent.eth/subnames?include_expired=false",
        )
        .await?;
        assert_eq!(
            v2_subname_names(&payload),
            vec!["alpha.parent.eth", "gamma.parent.eth"],
            "switch off: zeta has expired by the request time, epsilon long before"
        );
        anyhow::Ok(())
    })
    .await?;
    database.cleanup().await
}

#[tokio::test]
async fn api_preflight_requires_the_family_marker_only_with_the_switch_on() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    sqlx::query("DROP TABLE bigname_phase.project_family_marker")
        .execute(&database.lookup_pool)
        .await?;
    let missing = |on| {
        bigname_storage::publication_source::with_serve_from_families(
            on,
            bigname_storage::load_missing_api_lookup_ddl(&database.lookup_pool),
        )
    };
    let marker = |objects: &[bigname_storage::ApiLookupDdlObject]| {
        objects
            .iter()
            .any(|object| object.identity == "bigname_phase.project_family_marker")
    };
    assert!(marker(&missing(true).await?), "switch on: the marker is a serving read");
    assert!(missing(false).await?.is_empty(), "switch off: nothing reads it");
    database.cleanup().await
}

/// Sets each family marker's `sequence` to its chain's Project row `xmin`, with both publications
/// at the same position, so the two generations are the same string and only the publication
/// source tells them apart. Asserts that precondition.
async fn collide_marker_sequence_with_project_xmin(pool: &PgPool) -> Result<()> {
    sqlx::query(
        "UPDATE bigname_phase.project_family_marker marker
         SET sequence = project.xmin::text::bigint
         FROM bigname_phase.chain_phase_state project
         WHERE project.chain_id = marker.chain_id
           AND project.phase_name = 'project'
           AND project.current_block_number = marker.current_block_number
           AND project.current_block_hash = marker.current_block_hash",
    )
    .execute(pool)
    .await?;
    let markers: Vec<(String, i64, String)> = sqlx::query_as(
        "SELECT chain_id, current_block_number, current_block_hash
         FROM bigname_phase.project_family_marker",
    )
    .fetch_all(pool)
    .await?;
    anyhow::ensure!(!markers.is_empty(), "the fixture has a family marker");
    for (chain_id, number, hash) in markers {
        let generation = |on| {
            bigname_storage::publication_source::with_serve_from_families(
                on,
                bigname_storage::load_served_project_generation(
                    pool, &chain_id, number, &hash, true, true,
                ),
            )
        };
        let (families, project) = (generation(true).await?, generation(false).await?);
        assert!(families.is_some(), "{chain_id}: the marker is servable");
        assert_eq!(families, project, "{chain_id}: the generations collide");
    }
    Ok(())
}

/// A continuation issued with the switch at `issued` and continued with it at `continued` is
/// refused with a restart, and stays refused on a second attempt; a fresh first page then
/// paginates normally. `next` reads a page's continuation.
async fn assert_flip_restarts(
    database: &TestDatabase,
    first_page: &str,
    next: impl Fn(&Value) -> Option<String>,
    (issued, continued): (bool, bool),
) -> Result<()> {
    let label = format!("{first_page}: issued with the switch {issued}, continued with it {continued}");
    let page = bigname_storage::publication_source::with_serve_from_families(
        issued,
        v2_resolver_payload_for_database(database, first_page),
    )
    .await?;
    let cursor = next(&page).context("the first page has a continuation")?;
    bigname_storage::publication_source::with_serve_from_families(continued, async {
        for attempt in ["first", "second"] {
            let response = v2_resolver_response_for_database(
                database,
                &format!("{first_page}&cursor={cursor}"),
            )
            .await?;
            assert_eq!(response.status(), StatusCode::CONFLICT, "{label}, {attempt} attempt");
            let body: Value = read_json(response).await?;
            assert_eq!(body["error"]["code"], json!("stale"), "{label}");
            assert_eq!(body["error"]["message"], json!(RESTART_WITHOUT_CURSOR), "{label}");
        }
        let restarted = v2_resolver_payload_for_database(database, first_page).await?;
        let cursor = next(&restarted).context("the restarted first page has a continuation")?;
        v2_resolver_payload_for_database(database, &format!("{first_page}&cursor={cursor}"))
            .await?;
        anyhow::Ok(())
    })
    .await
}

const SWITCH_FLIPS: [(bool, bool); 2] = [(false, true), (true, false)];

fn collection_next_cursor(payload: &Value) -> Option<String> {
    payload["page"]["next_cursor"].as_str().map(str::to_owned)
}

fn overview_next_cursor(payload: &Value) -> Option<String> {
    payload["data"]["bound_names"]["page"]["next_cursor"]
        .as_str()
        .map(str::to_owned)
}

/// Flipping the switch changes the publication source a publication-bound cursor is tagged with,
/// so an outstanding continuation is refused with a restart in either direction even when the
/// Project row's `xmin` and the marker's `sequence` are the same number at the same position.
#[tokio::test]
async fn v2_a_subnames_cursor_restarts_when_the_switch_flips_either_way() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_fixture(&database).await?;
    seed_live_family_marker(&database.pool, 1).await?;
    collide_marker_sequence_with_project_xmin(&database.pool).await?;
    for flip in SWITCH_FLIPS {
        assert_flip_restarts(
            &database,
            "/v1/names/Parent.eth/subnames?page_size=1",
            collection_next_cursor,
            flip,
        )
        .await?;
    }
    database.cleanup().await
}

/// The resolver overview's bound-names cursor carries the resolver generation beside the
/// publication token; each is tagged with the publication source on its own. The second half
/// splices the continuing mode's publication token into the flipped cursor, so only the
/// resolver generation can refuse it.
#[tokio::test]
async fn v2_a_resolver_overview_cursor_restarts_when_the_switch_flips_either_way() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_bound_names_fixture(&database).await?;
    upsert_test_resolver_current_rows(
        &database,
        &[resolver_current_row("ethereum-mainnet", V2_RESOLVER_ADDRESS)],
    )
    .await?;
    seed_live_family_marker(&database.pool, 1).await?;
    collide_marker_sequence_with_project_xmin(&database.pool).await?;
    let base = format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}?page_size=1");
    for flip in SWITCH_FLIPS {
        assert_flip_restarts(&database, &base, overview_next_cursor, flip).await?;
        assert_generation_alone_restarts(
            &database,
            &base,
            overview_next_cursor,
            "resolver_generation",
            "resolver publication changed; restart pagination",
            flip,
        )
        .await?;
    }
    database.cleanup().await
}

/// The resolver `/roles`, `/links` and `/aliases` cursors carry their own generation map beside
/// the publication token, tagged the same way.
#[tokio::test]
async fn v2_a_resolver_collection_cursor_restarts_when_the_switch_flips_either_way() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    seed_v2_resolver_roles_pages(&database).await?;
    seed_live_family_marker(&database.pool, 1).await?;
    collide_marker_sequence_with_project_xmin(&database.pool).await?;
    let base = format!("/v1/resolvers/1/{V2_RESOLVER_ADDRESS}/roles?page_size=1");
    for flip in SWITCH_FLIPS {
        assert_flip_restarts(&database, &base, collection_next_cursor, flip).await?;
        assert_generation_alone_restarts(
            &database,
            &base,
            collection_next_cursor,
            "generation",
            "resolver collection changed; restart pagination",
            flip,
        )
        .await?;
    }
    database.cleanup().await
}

/// Issues a continuation with the switch at `issued`, replaces its publication token with the
/// one a first page issues with the switch at `continued`, and continues it there: the
/// generation named `field` is then the only binding left to refuse it.
async fn assert_generation_alone_restarts(
    database: &TestDatabase,
    first_page: &str,
    next: impl Fn(&Value) -> Option<String>,
    field: &str,
    message: &str,
    (issued, continued): (bool, bool),
) -> Result<()> {
    let label = format!("{first_page} {field}: issued with the switch {issued}, continued with it {continued}");
    let page = bigname_storage::publication_source::with_serve_from_families(
        issued,
        v2_resolver_payload_for_database(database, first_page),
    )
    .await?;
    let flipped = crate::v2::decode(&next(&page).context("a continuation")?)
        .expect("the issued cursor decodes");
    assert!(flipped.last_item.contains_key(field), "{label}: the cursor binds {field}");
    bigname_storage::publication_source::with_serve_from_families(continued, async {
        let fresh = v2_resolver_payload_for_database(database, first_page).await?;
        let fresh = next(&fresh).context("a continuation")?;
        let publication = crate::v2::decode(&fresh).expect("the fresh cursor decodes").last_item
            ["publication"]
            .clone();
        let mut spliced = flipped.clone();
        spliced.last_item.insert("publication".to_owned(), publication);
        let response = v2_resolver_response_for_database(
            database,
            &format!("{first_page}&cursor={}", crate::v2::encode(&spliced)),
        )
        .await?;
        assert_eq!(response.status(), StatusCode::CONFLICT, "{label}");
        let body: Value = read_json(response).await?;
        assert_eq!(body["error"]["message"], json!(message), "{label}");
        v2_resolver_payload_for_database(database, &format!("{first_page}&cursor={fresh}"))
            .await?;
        anyhow::Ok(())
    })
    .await
}

#[tokio::test]
async fn api_preflight_requires_a_readable_family_marker_only_with_the_switch_on() -> Result<()> {
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
    let marker_missing = |on| {
        let pool = pool.clone();
        bigname_storage::publication_source::with_serve_from_families(on, async move {
            let missing = bigname_storage::load_missing_api_lookup_ddl(&pool).await?;
            anyhow::Ok(
                missing
                    .iter()
                    .any(|object| object.identity == "bigname_phase.project_family_marker"),
            )
        })
    };
    assert!(
        marker_missing(true).await?,
        "switch on: an unreadable marker is listed"
    );
    assert!(
        !marker_missing(false).await?,
        "switch off: nothing reads the marker"
    );
    let error = bigname_storage::publication_source::with_serve_from_families(
        true,
        crate::startup_preflight::ensure_verified_lookup_ddl_available(&pool),
    )
    .await
    .expect_err("startup must reject an unreadable marker with the switch on");
    assert!(
        format!("{error:#}").contains("bigname_phase.project_family_marker"),
        "unexpected error: {error:#}"
    );
    bigname_storage::publication_source::with_serve_from_families(
        false,
        crate::startup_preflight::ensure_verified_lookup_ddl_available(&pool),
    )
    .await
    .expect("switch off: startup does not need the marker");
    pool.close().await;
    sqlx::query(&format!("DROP OWNED BY {role}"))
        .execute(&database.lookup_pool)
        .await?;
    sqlx::query(&format!("DROP ROLE {role}"))
        .execute(&database.lookup_pool)
        .await?;
    database.cleanup().await
}
