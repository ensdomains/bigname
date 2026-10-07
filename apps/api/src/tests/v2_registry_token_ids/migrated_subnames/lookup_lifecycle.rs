//! Populated lookup state crosses actual Project publication and repair transitions.
use super::*;
use bigname_project::families::{self, FamilyMode, FamilyOptions, RebuildRanges};
use lookup_publication::{assert_name_prepared_parity, components};

fn options() -> FamilyOptions {
    FamilyOptions::new(bigname_content_hash::INTERPRETER_CONTENT_HASH)
        .with_rebuild_ranges(RebuildRanges::Off)
}
async fn apply(
    database: &TestDatabase,
    block: i64,
    mode: FamilyMode,
    options: &FamilyOptions,
) -> Result<families::FamilyOutcome> {
    let token = families::input_token(&database.pool, PATH_CHAIN).await?;
    Ok(families::apply(
        &database.pool,
        PATH_CHAIN,
        &bigname_project::Marker {
            number: BASE + block,
            hash: format!("0xhistory{}", BASE + block),
        },
        mode,
        &token,
        options,
    )
    .await?)
}
async fn response(database: &TestDatabase) -> Result<(StatusCode, Value)> {
    let response = app_router(AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    ))
    .oneshot(
        Request::builder()
            .method("POST")
            .uri("/v1/lookup")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(
                &json!({"profile":"detail","inputs":[{"name":CHILD}]}),
            )?))?,
    )
    .await?;
    let status = response.status();
    Ok((status, read_json(response).await?))
}
fn paired_text(resolver: Address, block: i64, value: &str) -> Result<Vec<RawLogInput>> {
    let mut logs = lookup_publication::text(resolver, block, "atomic-a", value)?;
    logs.extend(lookup_publication::text(
        resolver, block, "atomic-b", value,
    )?);
    for (i, log) in logs.iter_mut().enumerate() {
        log.log_index = i as i64;
    }
    Ok(logs)
}
#[tokio::test]
async fn populated_lookup_publication_rebuild_resume_and_pruned_redo() -> Result<()> {
    let (database, logs, resolver) = setup().await?;
    let initial: Vec<_> = logs
        .into_iter()
        .filter(|log| log.block_number <= BASE + 121)
        .collect();
    seed_and_run(&database, &initial, 120, 121).await?;
    seed_and_run_with(
        &database,
        &paired_text(resolver, 122, "0")?,
        122,
        122,
        &[(122, 0, GRANTEE)],
        None,
    )
    .await?;
    let before = assert_name_prepared_parity(&database, CHILD).await?;
    assert_eq!(before["records"]["texts"]["atomic-a"], "0");
    assert_eq!(before["records"]["texts"]["atomic-b"], "0");
    let stored = components(&database).await?;
    assert!(stored["project_lookup_record"].as_array().unwrap().len() >= 2);

    // The height and payload remain identical, but the complete publication generation moves.
    let sequence: i64 =
        sqlx::query_scalar("SELECT sequence FROM project_family_marker WHERE chain_id=$1")
            .bind(PATH_CHAIN)
            .fetch_one(&database.pool)
            .await?;
    let (guard, control) =
        crate::v2::lookup_served_head_revalidation_test_hooks::install(&database.lookup_pool)
            .await?;
    let state = AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    );
    let request = tokio::spawn(async move {
        app_router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/lookup")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({"profile":"detail","inputs":[{"name":CHILD}]}))
                            .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
    });
    control.wait_until_reached().await;
    let rebuilt = apply(&database, 122, FamilyMode::Rebuild, &options()).await?;
    assert!(rebuilt.reset);
    assert_eq!(components(&database).await?, stored);
    let next: i64 =
        sqlx::query_scalar("SELECT sequence FROM project_family_marker WHERE chain_id=$1")
            .bind(PATH_CHAIN)
            .fetch_one(&database.pool)
            .await?;
    assert!(next > sequence);
    control.resume().await;
    let raced = request.await??;
    assert_eq!(raced.status(), StatusCode::CONFLICT);
    assert_eq!(read_json::<Value>(raced).await?["error"]["code"], "stale");
    drop(guard);
    assert_eq!(assert_name_prepared_parity(&database, CHILD).await?, before);

    // Real raw logs update both keys in the same publication while HTTP readers run.
    let (stage_tx, mut stage_rx) = tokio::sync::watch::channel(122_i64);
    let (ack_tx, mut ack_rx) = tokio::sync::watch::channel(122_i64);
    let writer = async {
        let mut expected = vec![before.clone()];
        for block in 123..=125 {
            seed_and_run_with(
                &database,
                &paired_text(resolver, block, &(block - 122).to_string())?,
                block,
                block,
                &[(block, 0, GRANTEE)],
                None,
            )
            .await?;
            expected.push(assert_name_prepared_parity(&database, CHILD).await?);
            stage_tx.send_replace(block);
            while *ack_rx.borrow() < block {
                ack_rx.changed().await?;
            }
        }
        Ok::<_, anyhow::Error>(expected)
    };
    let reader = async {
        let mut observed = vec![response(&database).await?];
        for stage in 123..=125 {
            for _ in 0..10 {
                observed.push(response(&database).await?);
                if *stage_rx.borrow() >= stage {
                    break;
                }
                tokio::task::yield_now().await;
            }
            while *stage_rx.borrow() < stage {
                stage_rx.changed().await?;
            }
            let committed = response(&database).await?;
            assert_eq!(committed.0, StatusCode::OK);
            assert_eq!(
                committed.1["data"][0]["record"]["records"]["texts"]["atomic-a"],
                (stage - 122).to_string()
            );
            observed.push(committed);
            ack_tx.send_replace(stage);
        }
        Ok::<_, anyhow::Error>(observed)
    };
    let (expected, observed) = tokio::join!(writer, reader);
    let expected = expected?;
    let mut served = 0;
    let mut observations = std::collections::BTreeMap::<String, usize>::new();
    for (status, payload) in observed? {
        match status {
            StatusCode::OK => {
                let record = &payload["data"][0]["record"];
                assert!(
                    expected.contains(record),
                    "partial or mixed published record: {payload:#}"
                );
                assert_eq!(
                    record["records"]["texts"]["atomic-a"],
                    record["records"]["texts"]["atomic-b"]
                );
                served += 1;
                *observations
                    .entry(format!(
                        "200:value:{}",
                        record["records"]["texts"]["atomic-a"]
                    ))
                    .or_default() += 1;
            }
            StatusCode::CONFLICT => {
                assert_eq!(payload["error"]["code"], "stale", "{payload:#}");
                *observations.entry("409:stale".into()).or_default() += 1;
            }
            _ => panic!("unexpected lookup status {status}: {payload:#}"),
        }
    }
    assert!(
        served > 0,
        "bounded readers must include a complete live response"
    );
    eprintln!(
        "populated writer/read observations: {}",
        json!(observations)
    );
    let stored = components(&database).await?;
    let current = assert_name_prepared_parity(&database, CHILD).await?;

    // Rebuild may publish partial progress internally, but bootstrap never serves false misses.
    let budget = options().with_max_blocks_per_run(2);
    let mut outcome = apply(&database, 125, FamilyMode::Rebuild, &budget).await?;
    assert!(outcome.budget_exhausted);
    let (status, payload) = response(&database).await?;
    assert_eq!(status, StatusCode::CONFLICT, "{payload:#}");
    assert_eq!(payload["error"]["code"], "stale");
    let mut resumed = 0;
    while outcome.marker.as_ref().map(|m| m.number) != Some(BASE + 125) {
        outcome = apply(&database, 125, FamilyMode::Normal, &budget).await?;
        assert!(!outcome.reset, "resume must retain completed lookup work");
        resumed += 1;
        assert!(resumed < 10);
    }
    assert!(resumed > 0);
    assert_eq!(components(&database).await?, stored);
    assert_eq!(
        assert_name_prepared_parity(&database, CHILD).await?,
        current
    );

    // Retain only a short undo tail, then ask the real family redo path for a pruned block.
    seed_and_run(&database, &[], 126, 140).await?;
    let shallow = options().with_retained_undo_depth(2);
    apply(&database, 140, FamilyMode::Rebuild, &shallow).await?;
    let retained: Option<i64> =
        sqlx::query_scalar("SELECT min(block_number) FROM project_family_undo WHERE chain_id=$1")
            .bind(PATH_CHAIN)
            .fetch_one(&database.pool)
            .await?;
    assert!(
        retained.is_some_and(|n| n > BASE + 122),
        "old undo must actually be pruned: {retained:?}"
    );
    let stored = components(&database).await?;
    let current = assert_name_prepared_parity(&database, CHILD).await?;
    use phase_runner::{
        capacity::CapacityGuard,
        config::{CapacityConfig, ChainConfig, SeedBasis, SourceConfig, TimingConfig},
        database::RunnerDatabase,
        interpret_phase::InterpretPhase,
        phase::{BlockRange, LoopbackPhase, PhaseName, PhaseSet},
        project_phase::ProjectPhase,
        runner::{PhaseRunner, RedoPhase},
    };
    use std::sync::Arc;
    let runner_db =
        RunnerDatabase::connect_with_options(database.pool.connect_options().as_ref().clone(), 4)
            .await?;
    let phases = PhaseSet::with_ingest_interpret_and_project(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(InterpretPhase::new(runner_db.pool().clone())),
        Arc::new(ProjectPhase::new(runner_db.pool().clone())),
    )?;
    let runner = PhaseRunner::new(
        runner_db,
        phases,
        CapacityGuard::system(CapacityConfig {
            writable_path: std::env::temp_dir(),
            ..Default::default()
        }),
        "populated-lookup-pruned-redo",
        TimingConfig::default(),
    )?;
    let chain = ChainConfig::new(
        PATH_CHAIN,
        vec![SourceConfig::new(
            PATH_CHAIN,
            "populated-lookup",
            "test",
            SeedBasis::EthereumHead,
            BASE + 120,
            "http://unused.invalid",
        )?],
        false,
    )?;
    // This fixture has actually produced the complete raw range; attest that intake coverage
    // for the same source key before asking the real runner to replay only Project.
    sqlx::query("INSERT INTO chain_phase_state(chain_id,phase_name,phase_status,current_block_number,current_block_hash,target_block_number,target_block_hash,live_handoff_block_number,live_handoff_block_hash,started_at,finished_at) VALUES($1,'ingest','completed',$2,$3,$2,$3,$2,$3,now(),now()) ON CONFLICT(chain_id,phase_name) DO UPDATE SET phase_status='completed',current_block_number=EXCLUDED.current_block_number,current_block_hash=EXCLUDED.current_block_hash,target_block_number=EXCLUDED.target_block_number,target_block_hash=EXCLUDED.target_block_hash,live_handoff_block_number=EXCLUDED.live_handoff_block_number,live_handoff_block_hash=EXCLUDED.live_handoff_block_hash")
        .bind(PATH_CHAIN).bind(BASE+140).bind(format!("0xhistory{}",BASE+140)).execute(&database.pool).await?;
    sqlx::query("INSERT INTO ingest_cursors(chain_id,source_key,source_kind,seed_basis,start_block_number,next_block_number,target_block_number,last_processed_block_number,last_processed_block_hash) VALUES($1,'populated-lookup','test','ethereum_head',$2,$3,$4,$4,$5)")
        .bind(PATH_CHAIN).bind(BASE+120).bind(BASE+141).bind(BASE+140).bind(format!("0xhistory{}",BASE+140)).execute(&database.pool).await?;
    tokio::time::timeout(
        std::time::Duration::from_secs(60),
        runner.redo(
            &chain,
            RedoPhase::Phase(PhaseName::Project),
            BlockRange::new(BASE + 122, BASE + 140)?,
            tokio_util::sync::CancellationToken::new(),
        ),
    )
    .await
    .context("populated pruned redo exceeded60seconds")??;
    let repair: Value =
        sqlx::query_scalar("SELECT to_jsonb(r) FROM project_repair_record r WHERE chain_id=$1")
            .bind(PATH_CHAIN)
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(repair["state"], "complete");
    assert_eq!(repair["reason"], "operator_redo");
    assert!(
        repair["trusted_base_number"].is_null(),
        "pruned journal must force rebuild: {repair:#}"
    );
    eprintln!("populated pruned redo: {repair}");
    assert_eq!(components(&database).await?, stored);
    assert_eq!(
        assert_name_prepared_parity(&database, CHILD).await?,
        current
    );
    database.cleanup().await
}
