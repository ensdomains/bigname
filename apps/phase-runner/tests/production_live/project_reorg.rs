//! The actual Live publication -> required redo -> Project -> restart boundary.
use super::*;
use bigname_project::families::{FamilyMode, FamilyOptions, RebuildRanges};

const HEAD: i64 = 30;

#[path = "project_redo_recovery.rs"]
mod recovery;

#[tokio::test]
async fn shallow_live_reorg_undoes_without_reset_and_resumes_below_the_requested_range()
-> Result<()> {
    let chain = "project-shallow-reorg";
    let (scratch, fixture) = ready(chain).await?;
    let initial_reset: i64 =
        sqlx::query_scalar("SELECT reset_sequence FROM project_repair_record WHERE chain_id = $1")
            .bind(chain)
            .fetch_one(scratch.pool())
            .await?;
    install_reorg(&scratch, &fixture, chain).await?;
    let stop = CancellationToken::new();
    let stop_after_batch = stop.clone();
    let pool = scratch.pool().clone();
    let first_runner = runner(&scratch, chain, 1)?.with_before_redo_progress_write(move || {
        let stop = stop_after_batch.clone();
        let pool = pool.clone();
        async move {
            let marker: Option<i64> = sqlx::query_scalar(
                "SELECT current_block_number FROM project_family_marker WHERE chain_id = $1",
            )
            .bind(chain)
            .fetch_one(&pool)
            .await
            .unwrap();
            if marker != Some(HEAD) {
                stop.cancel();
            }
        }
    });
    let result = first_runner
        .redo(
            &live_chain(chain, &fixture.endpoint)?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(HEAD, HEAD)?,
            stop,
        )
        .await;
    let row: (String, Option<i64>, Option<i64>, Option<i64>) = sqlx::query_as(
        "SELECT repair.state, repair.reset_sequence, marker.current_block_number,
                phase.redo_current_block_number
         FROM project_repair_record repair
         JOIN project_family_marker marker USING (chain_id)
         JOIN chain_phase_state phase USING (chain_id)
         WHERE repair.chain_id = $1 AND phase.phase_name = 'project'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        row.1, None,
        "a shallow reorg must undo, not reset (original reset {initial_reset}); result={result:?}; row={row:?}"
    );
    assert_eq!(row.2, Some(HEAD - 1));
    assert_eq!(
        row.3, row.2,
        "the below-request marker is recorded truthfully; result={result:?}"
    );
    assert!(result.is_err(), "the stop leaves durable redo work");

    runner(&scratch, chain, 1)?
        .redo(
            &live_chain(chain, &fixture.endpoint)?,
            RedoPhase::Phase(PhaseName::Project),
            BlockRange::new(HEAD, HEAD)?,
            CancellationToken::new(),
        )
        .await?;
    let completed: (String, String, bool) = sqlx::query_as(
        "SELECT marker.current_block_hash, repair.state, phase.redo_in_progress
         FROM project_family_marker marker
         JOIN project_repair_record repair USING (chain_id)
         JOIN chain_phase_state phase USING (chain_id)
         WHERE marker.chain_id = $1 AND phase.phase_name = 'project'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(completed, (block_hash(2, HEAD), "complete".into(), false));
    let pointers: i64 =
        sqlx::query_scalar("SELECT count(*) FROM project_registry_pointer WHERE chain_id = $1")
            .bind(chain)
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(
        pointers,
        HEAD - 1,
        "the losing block's pointer is removed, retained pointers survive"
    );
    fixture.server.abort();
    scratch.cleanup().await
}

async fn ready(chain: &str) -> Result<(ScratchDatabase, RpcFixture)> {
    let scratch = ScratchDatabase::create(chain).await?;
    seed_branch(scratch.pool(), chain, 1, HEAD, None).await?;
    publish(scratch.pool(), chain, 1, HEAD, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, HEAD, &block_hash(1, HEAD)).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;
    for number in 1..=HEAD {
        sqlx::query(
            "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
                 manifest_version, chain_id, block_number, block_hash, derivation_kind,
                 canonicality_state, after_state, raw_fact_ref)
             VALUES ($1, 'ens', 'ResolverChanged', 'ens_v1_registry_l1', 1, $2, $3, $4,
                     'ens_v2_registry_resource_surface', 'canonical', $5, $6)",
        )
        .bind(format!("budget:{number}"))
        .bind(chain)
        .bind(number)
        .bind(block_hash(1, number))
        .bind(json!({"node": format!("0x{number:064x}"), "resolver": ADDRESS}))
        .bind(json!({"emitting_address": WATCH_ADDRESS_B}))
        .execute(scratch.pool())
        .await?;
    }
    let target = bigname_project::Marker {
        number: HEAD,
        hash: block_hash(1, HEAD),
    };
    let token = bigname_project::families::input_token(scratch.pool(), chain).await?;
    let (_, error) = bigname_project::families::run(
        scratch.pool(),
        chain,
        &target,
        FamilyMode::Normal,
        &token,
        &FamilyOptions::new(INTERPRETER_CONTENT_HASH).with_rebuild_ranges(RebuildRanges::Off),
    )
    .await;
    if let Some(error) = error {
        return Err(error.into());
    }
    Ok((scratch, RpcFixture::spawn(1, HEAD).await?))
}

async fn install_reorg(scratch: &ScratchDatabase, fixture: &RpcFixture, chain: &str) -> Result<()> {
    fixture.reorg(2, HEAD - 1, HEAD).await;
    let outcome = LivePhase::new(scratch.pool().clone())
        .run_batch(live_context(
            chain,
            &fixture.endpoint,
            HEAD,
            block_hash(1, HEAD),
        )?)
        .await?;
    publish_heads(
        scratch.pool(),
        chain,
        outcome.progress().heads.as_ref().unwrap(),
    )
    .await?;
    Ok(())
}

fn runner(scratch: &ScratchDatabase, chain: &str, budget: u64) -> Result<PhaseRunner> {
    Ok(PhaseRunner::new(
        scratch.runner(),
        PhaseSet::with_ingest_interpret_and_project(
            Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
            Arc::new(InterpretPhase::new(scratch.pool().clone())),
            Arc::new(
                ProjectPhase::new(scratch.pool().clone()).with_family_settings(FamilySettings {
                    max_blocks_per_run: budget,
                    retry_family_failures: false,
                    rebuild_ranges: RebuildRanges::Off,
                    ..FamilySettings::default()
                }),
            ),
        )?,
        CapacityGuard::system(CapacityConfig::default()),
        format!("repair-{chain}"),
        fast_timing(),
    )?)
}
