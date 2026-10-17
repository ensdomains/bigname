use super::*;

#[tokio::test]
async fn covering_restart_retains_the_interrupted_undo_replay_target() -> Result<()> {
    covering_restart(false).await
}

#[tokio::test]
async fn covering_restart_retains_the_interrupted_rebuild_replay_target() -> Result<()> {
    covering_restart(true).await
}

async fn covering_restart(rebuild: bool) -> Result<()> {
    let chain = if rebuild {
        "project-covering-rebuild"
    } else {
        "project-covering-undo"
    };
    let (scratch, fixture) = ready(chain).await?;
    let before = pointers(&scratch, chain).await?;
    if rebuild {
        sqlx::query("DELETE FROM project_family_undo WHERE chain_id=$1 AND block_number<=20")
            .bind(chain)
            .execute(scratch.pool())
            .await?;
    }
    stop_at(
        &scratch,
        &fixture,
        chain,
        BlockRange::new(10, 11)?,
        if rebuild { 3 } else { 9 },
        if rebuild { 3 } else { 1 },
    )
    .await?;
    let pending = bounds(&scratch, chain).await?;
    assert_eq!(pending.1, HEAD);
    // A request that fails to cover the interrupted demand cannot discard it.
    let refused = runner(&scratch, chain, 2)?
        .redo(
            &live_chain(chain, &fixture.endpoint)?,
            RedoPhase::Phase(PhaseName::Project),
            BlockRange::new(11, 15)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("noncovering restart must retain pending work");
    assert!(
        refused.to_string().contains("interrupted redo"),
        "{refused}"
    );
    assert_eq!(bounds(&scratch, chain).await?, pending);
    runner(&scratch, chain, 2)?
        .redo(
            &live_chain(chain, &fixture.endpoint)?,
            RedoPhase::Phase(PhaseName::Project),
            BlockRange::new(8, 15)?,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(
        family_marker(&scratch, chain).await?,
        HEAD,
        "a covering restart must restore the outstanding publication before clearing redo"
    );
    if !rebuild {
        // A plain Project redo owns no required-redo stamp, so a covering restart widens: its
        // undo reaches below its own start 8, not the interrupted request's 10.
        let base: Option<i64> = sqlx::query_scalar(
            "SELECT trusted_base_number FROM project_repair_record WHERE chain_id = $1",
        )
        .bind(chain)
        .fetch_one(scratch.pool())
        .await?;
        assert!(
            base.is_some_and(|base| base < 8),
            "the covering restart did not widen: trusted base {base:?}"
        );
    }
    assert_eq!(pointers(&scratch, chain).await?, before);
    let active: bool = sqlx::query_scalar(
        "SELECT redo_in_progress FROM chain_phase_state WHERE chain_id=$1 AND phase_name='project'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert!(!active);
    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn partial_request_replays_above_its_end_after_restart() -> Result<()> {
    let chain = "project-short-request";
    let (scratch, fixture) = ready(chain).await?;
    let before = pointers(&scratch, chain).await?;
    let range = BlockRange::new(10, 11)?;
    stop_at(&scratch, &fixture, chain, range, 9, 1).await?;
    assert_eq!(
        bounds(&scratch, chain).await?,
        (9, HEAD, Some(10), Some(11), Some(9))
    );
    runner(&scratch, chain, 2)?
        .redo(
            &live_chain(chain, &fixture.endpoint)?,
            RedoPhase::Phase(PhaseName::Project),
            range,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(pointers(&scratch, chain).await?, before);
    assert_eq!(family_marker(&scratch, chain).await?, HEAD);
    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn a_real_rebuild_has_truthful_bounded_progress_and_resumes_without_another_reset()
-> Result<()> {
    let chain = "project-deep-request";
    let (scratch, fixture) = ready(chain).await?;
    let before = pointers(&scratch, chain).await?;
    // Model journal retention: the requested predecessor is no longer reachable.
    sqlx::query("DELETE FROM project_family_undo WHERE chain_id = $1 AND block_number <= 20")
        .bind(chain)
        .execute(scratch.pool())
        .await?;
    let range = BlockRange::new(10, 11)?;
    stop_at(&scratch, &fixture, chain, range, 3, 3).await?;
    assert_eq!(
        bounds(&scratch, chain).await?,
        (0, HEAD, Some(10), Some(11), Some(3))
    );
    let reset = reset_sequence(&scratch, chain)
        .await?
        .expect("a deep redo rebuilds");
    runner(&scratch, chain, 3)?
        .redo(
            &live_chain(chain, &fixture.endpoint)?,
            RedoPhase::Phase(PhaseName::Project),
            range,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(reset_sequence(&scratch, chain).await?, Some(reset));
    assert_eq!(pointers(&scratch, chain).await?, before);
    assert_eq!(family_marker(&scratch, chain).await?, HEAD);
    fixture.server.abort();
    scratch.cleanup().await
}

/// A redo that rebuilds in ranges, one block of budget a run: most runs of the composition pass
/// commit a chunk or the pass end and apply no block, and the runner continues through them
/// without recording a Project failure.
#[tokio::test]
async fn a_ranged_rebuild_continues_through_runs_that_only_compose() -> Result<()> {
    let chain = "project-compose-runs";
    let (scratch, fixture) = ready(chain).await?;
    let before = pointers(&scratch, chain).await?;
    sqlx::query("DELETE FROM project_family_undo WHERE chain_id = $1 AND block_number <= 20")
        .bind(chain)
        .execute(scratch.pool())
        .await?;
    sqlx::raw_sql(
        "CREATE TABLE project_failures_seen (last_error text);
         CREATE FUNCTION project_failure_seen() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             INSERT INTO project_failures_seen VALUES (NEW.last_error);
             RETURN NEW;
         END $$;
         CREATE TRIGGER project_failure_seen AFTER UPDATE ON chain_phase_state
         FOR EACH ROW WHEN (NEW.phase_name = 'project' AND NEW.last_error IS NOT NULL
                            AND NEW.last_error IS DISTINCT FROM OLD.last_error)
         EXECUTE FUNCTION project_failure_seen();",
    )
    .execute(scratch.pool())
    .await?;
    runner_in(&scratch, chain, 1, RebuildRanges::Through(HEAD))?
        .redo(
            &live_chain(chain, &fixture.endpoint)?,
            RedoPhase::Phase(PhaseName::Project),
            BlockRange::new(10, 11)?,
            CancellationToken::new(),
        )
        .await?;
    let composed: Option<i64> =
        sqlx::query_scalar("SELECT composed_block FROM project_repair_record WHERE chain_id = $1")
            .bind(chain)
            .fetch_one(scratch.pool())
            .await?;
    assert!(
        composed.is_some_and(|block| block > 0),
        "the rebuild composed in a pass: {composed:?}"
    );
    let failures: Vec<Option<String>> =
        sqlx::query_scalar("SELECT last_error FROM project_failures_seen")
            .fetch_all(scratch.pool())
            .await?;
    assert!(failures.is_empty(), "no Project failure: {failures:?}");
    assert_eq!(pointers(&scratch, chain).await?, before);
    assert_eq!(family_marker(&scratch, chain).await?, HEAD);
    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn undo_of_a_range_transaction_records_its_actual_predecessor() -> Result<()> {
    let chain = "project-ranged-undo";
    let (scratch, fixture) = ready(chain).await?;
    let target = bigname_project::Marker {
        number: HEAD,
        hash: block_hash(1, HEAD),
    };
    // A rebuild range before the composition pass journals nothing, so only a range after it can
    // be undone. Rebuild the blocks through 20 in ranges and end the pass there, one block of
    // budget a run. The next run then rebuilds the rest in ranges after the pass, which compose
    // and journal.
    let mut composed: Option<i64> = None;
    for attempt in 0..100 {
        let token = bigname_project::families::input_token(scratch.pool(), chain).await?;
        let (_, error) = bigname_project::families::run(
            scratch.pool(),
            chain,
            &target,
            if attempt == 0 {
                FamilyMode::Rebuild
            } else {
                FamilyMode::Normal
            },
            &token,
            &FamilyOptions::new(PROJECTION_CONTENT_HASH)
                .with_rebuild_ranges(RebuildRanges::Through(20))
                .with_max_blocks_per_run(1),
        )
        .await;
        if let Some(error) = error {
            return Err(error.into());
        }
        composed = sqlx::query_scalar(
            "SELECT composed_block FROM project_repair_record WHERE chain_id = $1",
        )
        .bind(chain)
        .fetch_one(scratch.pool())
        .await?;
        if composed.is_some() {
            break;
        }
    }
    assert!(
        composed.is_some_and(|block| block <= 20),
        "the pass ends at or below block 20: {composed:?}"
    );
    let token = bigname_project::families::input_token(scratch.pool(), chain).await?;
    let (_, error) = bigname_project::families::run(
        scratch.pool(),
        chain,
        &target,
        FamilyMode::Normal,
        &token,
        &FamilyOptions::new(PROJECTION_CONTENT_HASH)
            .with_rebuild_ranges(RebuildRanges::Through(HEAD)),
    )
    .await;
    if let Some(error) = error {
        return Err(error.into());
    }
    let base: i64 = sqlx::query_scalar(
        "SELECT (before_image->>'current_block_number')::bigint FROM project_family_undo
         WHERE chain_id = $1 AND family = 'marker' AND block_number = 29",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert!(
        base < 27,
        "fixture must undo a range below the requested predecessor"
    );
    let before = pointers(&scratch, chain).await?;
    let range = BlockRange::new(28, 28)?;
    let before_sequence: i64 =
        sqlx::query_scalar("SELECT sequence FROM project_family_marker WHERE chain_id=$1")
            .bind(chain)
            .fetch_one(scratch.pool())
            .await?;
    stop_at(&scratch, &fixture, chain, range, base, 1).await?;
    assert_eq!(
        bounds(&scratch, chain).await?,
        (base, HEAD, Some(28), Some(28), Some(base))
    );
    let after_sequence: i64 =
        sqlx::query_scalar("SELECT sequence FROM project_family_marker WHERE chain_id=$1")
            .bind(chain)
            .fetch_one(scratch.pool())
            .await?;
    assert!(
        after_sequence > before_sequence,
        "range undo invalidates captured publication"
    );
    assert!(
        bigname_storage::load_served_project_generation(
            scratch.pool(),
            chain,
            HEAD,
            &block_hash(1, HEAD),
            true,
            false,
            bigname_storage::PROJECT_PUBLICATION_LAG_TOLERANCE_BLOCKS,
        )
        .await?
        .is_none(),
        "undo below the request cannot serve the old publication"
    );
    runner(&scratch, chain, 3)?
        .redo(
            &live_chain(chain, &fixture.endpoint)?,
            RedoPhase::Phase(PhaseName::Project),
            range,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(reset_sequence(&scratch, chain).await?, None);
    assert_eq!(pointers(&scratch, chain).await?, before);
    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn legacy_interrupted_rebuild_recovers_without_editing_progress_or_dropping_constraint()
-> Result<()> {
    let chain = "project-legacy-rebuild";
    let (scratch, fixture, reset) = legacy_state(chain).await?;
    runner(&scratch, chain, 3)?
        .redo(
            &live_chain(chain, &fixture.endpoint)?,
            RedoPhase::Phase(PhaseName::Project),
            BlockRange::new(HEAD, HEAD)?,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(
        reset_sequence(&scratch, chain).await?,
        reset,
        "recovery adopts the existing rebuild"
    );
    assert_eq!(family_marker(&scratch, chain).await?, HEAD);
    let rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM project_registry_pointer WHERE chain_id = $1")
            .bind(chain)
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(rows, HEAD - 1);
    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn old_hash_interrupted_rebuild_recovers_through_full_interpret_project_adoption()
-> Result<()> {
    let chain = "project-legacy-hash-adoption";
    let (scratch, fixture, old_reset) = legacy_state(chain).await?;
    sqlx::query(
        "UPDATE chain_phase_state SET input_content_hash = 'keccak256:prior-release'
        WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE project_family_marker SET input_content_hash = 'keccak256:prior-release'
        WHERE chain_id = $1",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    sqlx::query("UPDATE project_repair_record SET prefix_interpret_input_content_hash = 'keccak256:prior-release'
        WHERE chain_id = $1").bind(chain).execute(scratch.pool()).await?;
    let phase = runner(&scratch, chain, 3)?;
    let refused = phase
        .redo(
            &live_chain(chain, &fixture.endpoint)?,
            RedoPhase::Phase(PhaseName::Project),
            BlockRange::new(HEAD, HEAD)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("old hash cannot adopt a narrow repair");
    assert_eq!(refused.kind(), ErrorKind::ContentHashMismatch);
    phase
        .redo(
            &live_chain(chain, &fixture.endpoint)?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(0, HEAD)?,
            CancellationToken::new(),
        )
        .await?;
    assert_ne!(
        reset_sequence(&scratch, chain).await?,
        old_reset,
        "the old hash prefix must be rebuilt"
    );
    let state: (String, String, String, bool) = sqlx::query_as(
        "SELECT marker.state, marker.input_content_hash, phase.input_content_hash, phase.redo_in_progress
         FROM project_family_marker marker JOIN chain_phase_state phase USING (chain_id)
         WHERE marker.chain_id = $1 AND phase.phase_name = 'project'")
        .bind(chain).fetch_one(scratch.pool()).await?;
    assert_eq!(
        state,
        (
            "live".into(),
            PROJECTION_CONTENT_HASH.into(),
            PROJECTION_CONTENT_HASH.into(),
            false
        )
    );
    assert_eq!(family_marker(&scratch, chain).await?, HEAD);
    fixture.server.abort();
    scratch.cleanup().await
}

// A projection-only rotation: Interpret records this binary's interpreter content hash, while the
// Project row and the family marker record another projection content hash. One full-range Project
// redo rebuilds the families under this binary's projection hash and leaves Interpret alone.
#[tokio::test]
async fn projection_only_rotation_rebuilds_the_families_with_one_project_redo() -> Result<()> {
    let chain = "project-projection-only-rotation";
    let (scratch, fixture) = ready(chain).await?;
    let before = pointers(&scratch, chain).await?;
    let reset = reset_sequence(&scratch, chain).await?;
    let interpret_row = "SELECT input_content_hash, redo_attempt_generation, finished_at::text
        FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'interpret'";
    let interpret_before: (Option<String>, i64, Option<String>) = sqlx::query_as(interpret_row)
        .bind(chain)
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(
        interpret_before.0.as_deref(),
        Some(INTERPRETER_CONTENT_HASH)
    );
    sqlx::query(
        "UPDATE chain_phase_state SET input_content_hash = 'keccak256:prior-projection'
        WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE project_family_marker SET input_content_hash = 'keccak256:prior-projection'
        WHERE chain_id = $1",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    let phase = runner(&scratch, chain, 3)?;
    let refused = phase
        .redo(
            &live_chain(chain, &fixture.endpoint)?,
            RedoPhase::Phase(PhaseName::Project),
            BlockRange::new(HEAD, HEAD)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("a projection-only rotation cannot adopt a narrow repair");
    assert_eq!(refused.kind(), ErrorKind::ContentHashMismatch);
    phase
        .redo(
            &live_chain(chain, &fixture.endpoint)?,
            RedoPhase::Phase(PhaseName::Project),
            BlockRange::new(0, HEAD)?,
            CancellationToken::new(),
        )
        .await?;
    assert_ne!(
        reset_sequence(&scratch, chain).await?,
        reset,
        "families from another projection content hash must be rebuilt"
    );
    let state: (String, String, String, bool) = sqlx::query_as(
        "SELECT marker.state, marker.input_content_hash, phase.input_content_hash, phase.redo_in_progress
         FROM project_family_marker marker JOIN chain_phase_state phase USING (chain_id)
         WHERE marker.chain_id = $1 AND phase.phase_name = 'project'")
        .bind(chain).fetch_one(scratch.pool()).await?;
    assert_eq!(
        state,
        (
            "live".into(),
            PROJECTION_CONTENT_HASH.into(),
            PROJECTION_CONTENT_HASH.into(),
            false
        )
    );
    let interpret_after: (Option<String>, i64, Option<String>) = sqlx::query_as(interpret_row)
        .bind(chain)
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(interpret_after, interpret_before, "Interpret is not redone");
    let attestations: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM manifest_authority_attestations WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(attestations, 0, "a Project redo is never attested");
    assert_eq!(family_marker(&scratch, chain).await?, HEAD);
    assert_eq!(pointers(&scratch, chain).await?, before);
    fixture.server.abort();
    scratch.cleanup().await
}

async fn legacy_state(chain: &str) -> Result<(ScratchDatabase, RpcFixture, Option<i64>)> {
    let (scratch, fixture) = ready(chain).await?;
    install_reorg(&scratch, &fixture, chain).await?;
    // A bounded wrapper reproduces the old Project decision using the production family
    // rebuild, before reporting its actual marker through normal runner persistence.
    let old = PhaseRunner::new(
        scratch.runner(),
        PhaseSet::with_ingest_interpret_and_project(
            Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
            Arc::new(InterpretPhase::new(scratch.pool().clone())),
            Arc::new(LegacyRebuild {
                pool: scratch.pool().clone(),
            }),
        )?,
        CapacityGuard::system(CapacityConfig::default()),
        chain,
        fast_timing(),
    )?;
    let error = old
        .redo(
            &live_chain(chain, &fixture.endpoint)?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(HEAD, HEAD)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("legacy narrow bounds reject rebuild progress");
    assert!(
        error.to_string().contains("chain_phase_state_check4"),
        "{error}"
    );
    assert_eq!(family_marker(&scratch, chain).await?, 3);
    let reset = reset_sequence(&scratch, chain).await?;
    // Exact legacy request shape: the predecessor schema had no requested-bound columns.
    // This is fixture setup only; recovery below runs the supported redo command unchanged.
    sqlx::query(
        "UPDATE chain_phase_state SET redo_from_block_number = $2,
        redo_requested_from_block_number = NULL, redo_requested_to_block_number = NULL
        WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(chain)
    .bind(HEAD)
    .execute(scratch.pool())
    .await?;
    Ok((scratch, fixture, reset))
}

struct LegacyRebuild {
    pool: PgPool,
}
impl Phase for LegacyRebuild {
    fn name(&self) -> PhaseName {
        PhaseName::Project
    }
    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            let target = context.available_heads.as_ref().unwrap().latest.clone();
            let token = bigname_project::families::input_token(&self.pool, &context.chain_id)
                .await
                .unwrap();
            let (outcome, error) = bigname_project::families::run(
                &self.pool,
                &context.chain_id,
                &bigname_project::Marker {
                    number: target.number,
                    hash: target.hash.clone(),
                },
                FamilyMode::Rebuild,
                &token,
                &FamilyOptions::new(PROJECTION_CONTENT_HASH)
                    .with_max_blocks_per_run(3)
                    .with_rebuild_ranges(RebuildRanges::Off),
            )
            .await;
            assert!(error.is_none(), "{error:?}");
            Ok(PhaseBatchOutcome::Continue(PhaseProgress {
                current: outcome.marker.map(|marker| BlockMarker {
                    number: marker.number,
                    hash: marker.hash,
                }),
                target: Some(target),
                ..PhaseProgress::default()
            }))
        })
    }
}

async fn stop_at(
    scratch: &ScratchDatabase,
    fixture: &RpcFixture,
    chain: &'static str,
    range: BlockRange,
    stop_number: i64,
    budget: u64,
) -> Result<()> {
    let stop = CancellationToken::new();
    let after = stop.clone();
    let pool = scratch.pool().clone();
    let runner = runner(scratch, chain, budget)?.with_before_redo_progress_write(move || {
        let pool = pool.clone();
        let after = after.clone();
        async move {
            let number: Option<i64> = sqlx::query_scalar(
                "SELECT current_block_number FROM project_family_marker WHERE chain_id = $1",
            )
            .bind(chain)
            .fetch_one(&pool)
            .await
            .unwrap();
            if number == Some(stop_number) {
                after.cancel();
            }
        }
    });
    let result = tokio::time::timeout(
        Duration::from_secs(15),
        runner.redo(
            &live_chain(chain, &fixture.endpoint)?,
            RedoPhase::Phase(PhaseName::Project),
            range,
            stop,
        ),
    )
    .await?;
    let error = result.expect_err("the stop leaves redo active");
    assert!(!error.to_string().contains("check4"), "{error}");
    assert_eq!(family_marker(scratch, chain).await?, stop_number);
    Ok(())
}

async fn bounds(
    scratch: &ScratchDatabase,
    chain: &str,
) -> Result<(i64, i64, Option<i64>, Option<i64>, Option<i64>)> {
    Ok(sqlx::query_as(
        "SELECT redo_from_block_number, redo_to_block_number,
        redo_requested_from_block_number, redo_requested_to_block_number, redo_current_block_number
        FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?)
}
async fn family_marker(scratch: &ScratchDatabase, chain: &str) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT current_block_number FROM project_family_marker WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?)
}
async fn reset_sequence(scratch: &ScratchDatabase, chain: &str) -> Result<Option<i64>> {
    Ok(
        sqlx::query_scalar("SELECT reset_sequence FROM project_repair_record WHERE chain_id = $1")
            .bind(chain)
            .fetch_one(scratch.pool())
            .await?,
    )
}
async fn pointers(scratch: &ScratchDatabase, chain: &str) -> Result<Value> {
    Ok(sqlx::query_scalar("SELECT jsonb_agg(to_jsonb(pointer) ORDER BY node) FROM project_registry_pointer pointer WHERE chain_id = $1")
        .bind(chain).fetch_one(scratch.pool()).await?)
}
