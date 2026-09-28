// The flag finalizer used by PhaseRunner changes mutable Interpret identity. Its completion
// transaction also stamps the required Interpret and Project redo. The runner's operating
// test `active_to_shadow_recompute_stamps_redo_then_replay_retracts_the_binding` proves that
// transaction; these route tests exercise the finalizer against event-built family data and
// represent the resulting phase handoff explicitly, including the pending Project interval.
async fn finalize_switch_name_flags(database: &TestDatabase) -> Result<()> {
    let mut transaction = database.pool.begin().await?;
    let summary =
        bigname_interpret::finalize_recompute_flags(&mut transaction, SWITCH_CHAIN, 200, 240)
            .await?;
    assert_eq!(summary.shadow_to_active_names, 1);
    assert_eq!(summary.earliest_transition_block(), Some(200));
    sqlx::query(
        "UPDATE chain_phase_state SET redo_in_progress = true, redo_mode = 'redo',
             redo_from_block_number = 200, redo_to_block_number = 240,
             redo_previous_phase_status = phase_status,
             redo_previous_started_at = started_at, redo_previous_finished_at = finished_at,
             phase_status = 'running', started_at = now(), finished_at = NULL,
             redo_attempt_generation = redo_attempt_generation + 1
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')",
    )
    .bind(SWITCH_CHAIN)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(())
}

async fn finish_switch_phase_redo(database: &TestDatabase, phase: &str) -> Result<()> {
    sqlx::query(
        "UPDATE chain_phase_state SET phase_status = redo_previous_phase_status,
             started_at = redo_previous_started_at, finished_at = redo_previous_finished_at,
             redo_in_progress = false, redo_mode = NULL, redo_previous_phase_status = NULL,
             redo_previous_started_at = NULL, redo_previous_finished_at = NULL,
             redo_from_block_number = NULL, redo_to_block_number = NULL
         WHERE chain_id = $1 AND phase_name = $2 AND redo_in_progress",
    )
    .bind(SWITCH_CHAIN)
    .bind(phase)
    .execute(&database.pool)
    .await?;
    Ok(())
}

async fn seed_switch_old_normalizer(database: &TestDatabase) -> Result<()> {
    seed_switch_names_events(database).await?;
    // An older normalizer rejected this now-valid label. The raw name and labels remain the
    // authentic input; the real finalizer decides the new visibility, not this test.
    sqlx::query(
        "UPDATE name_surfaces SET visibility_state = 'shadow',
             normalizer_version = 'old-normalizer',
             normalization_errors = '[{\"error\": \"old normalizer rejected this label\"}]',
             deactivation_reason = 'normalization_gate', deactivated_at = to_timestamp(1700000200)
         WHERE raw_name = 'alpha.eth'",
    )
    .execute(&database.pool)
    .await?;
    publish_test_families(database, 240).await
}

#[tokio::test]
async fn v2_recomputed_name_flags_refuse_the_old_publication_until_project_replays() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_old_normalizer(&database).await?;
    let (status, before) = read_family_response(&database, "/v1/names/alpha.eth").await?;
    assert_eq!(status, StatusCode::NOT_FOUND, "{before:#}");
    finalize_switch_name_flags(&database).await?;
    let flags: String = sqlx::query_scalar(
        "SELECT visibility_state FROM name_surfaces WHERE raw_name = 'alpha.eth'",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(flags, "active");
    for interpret_redo in [true, false] {
        // The second iteration is the real lifecycle window after Interpret completes but
        // before the required Project replay consumes the changed identity.
        if !interpret_redo {
            finish_switch_phase_redo(&database, "interpret").await?;
        }
        for uri in ["/v1/names/alpha.eth", SWITCH_SEARCH] {
            let (status, body) = read_family_response(&database, uri).await?;
            assert_eq!(
                status,
                StatusCode::CONFLICT,
                "interpret_redo={interpret_redo} {uri}: {body:#}"
            );
            if uri != SWITCH_SEARCH {
                assert_eq!(body["error"]["code"], "stale", "{body:#}");
            }
        }
        let (status, events) =
            read_family_response(&database, "/v1/diagnostics/events?name=alpha.eth").await?;
        assert_eq!(status, StatusCode::OK, "{events:#}");
        assert!(
            events["data"]
                .as_array()
                .is_some_and(|rows| !rows.is_empty()),
            "{events:#}"
        );
        assert!(
            events["data"]
                .as_array()
                .unwrap()
                .iter()
                .all(|row| row.get("name").is_none_or(Value::is_null)),
            "{events:#}"
        );
    }
    // Actual Project/family replacement consumes the now-active identity and restores reads.
    reset_switch_families(&database).await?;

    let token = bigname_project::families::input_token(&database.pool, SWITCH_CHAIN).await?;
    let outcome = bigname_project::families::apply(
        &database.pool,
        SWITCH_CHAIN,
        &bigname_project::Marker {
            number: 240,
            hash: "0xhistory240".to_owned(),
        },
        bigname_project::families::FamilyMode::Normal,
        &token,
        &bigname_project::families::FamilyOptions::new(
            bigname_content_hash::INTERPRETER_CONTENT_HASH,
        ),
    )
    .await?;
    anyhow::ensure!(
        outcome.marker.as_ref().map(|m| m.number) == Some(240),
        "Project replay replaced the families: {outcome:?}"
    );
    finish_switch_phase_redo(&database, "project").await?;
    let (status, after) = read_family_response(&database, "/v1/names/alpha.eth").await?;
    assert_eq!(status, StatusCode::OK, "{after:#}");
    database.cleanup().await
}

#[tokio::test]
async fn v2_recompute_after_collection_capture_refuses_changed_names() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_old_normalizer(&database).await?;
    let (status, before) = read_family_response(&database, SWITCH_SEARCH).await?;
    assert_eq!(status, StatusCode::OK, "{before:#}");
    assert_eq!(before["data"].as_array().map(Vec::len), Some(1));
    let (status, body) = get_with_family_change_after_fence(&database, SWITCH_SEARCH, || async {
        finalize_switch_name_flags(&database).await?;
        finish_switch_phase_redo(&database, "interpret").await?;
        Ok(())
    })
    .await?;
    assert_eq!(status, StatusCode::CONFLICT, "{body:#}");
    database.cleanup().await
}
