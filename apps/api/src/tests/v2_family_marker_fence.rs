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

/// A child whose expiry lies after the published block's time but before the request time: live
/// at the publication, expired by the wall clock.
async fn seed_subname_expiring_after_the_publication(database: &TestDatabase) -> Result<()> {
    seed_v2_subnames_bound_child(
        database,
        "ens:zeta.parent.eth",
        "zeta.parent.eth",
        "node:zeta.parent.eth",
        86,
        Uuid::from_u128(0x4040),
        Uuid::from_u128(0x5040),
        Uuid::from_u128(0x6040),
        json!({
            "registration": {
                "status": "active",
                "authority_kind": "registrar",
                "registrant": "0x00000000000000000000000000000000000000fB",
                "registered_at": "2025-01-02T03:04:05Z",
                "expiry": "2026-06-01T00:00:00Z"
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
            "ens:zeta.parent.eth",
            "zeta.parent.eth",
            "node:zeta.parent.eth",
            906,
            86,
        )],
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn v2_get_subnames_include_expired_false_is_evaluated_at_the_published_block_time()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_subnames_fixture(&database).await?;
    seed_subname_expiring_after_the_publication(&database).await?;
    seed_live_family_marker(&database.pool, 1).await?;
    let filtered = "/v1/names/Parent.eth/subnames?include_expired=false&page_size=1";

    bigname_storage::publication_source::with_serve_from_families(true, async {
        let mut names = Vec::new();
        let mut uri = filtered.to_owned();
        loop {
            let payload = v2_subnames_payload_for_database(&database, &uri).await?;
            assert_eq!(payload["page"]["total_count"], json!(3), "{uri}");
            names.extend(v2_subname_names(&payload));
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
            "switch off: zeta has expired by the request time"
        );
        anyhow::Ok(())
    })
    .await?;
    database.cleanup().await
}
