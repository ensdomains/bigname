//! Publication invalidation through the actual runner redo transition.
use super::*;
use phase_runner::{
    capacity::CapacityGuard,
    config::{CapacityConfig, ChainConfig, SeedBasis, SourceConfig, TimingConfig},
    database::RunnerDatabase,
    phase::{BlockRange, LoopbackPhase, Phase, PhaseContext, PhaseFuture, PhaseName, PhaseSet},
    runner::{PhaseRunner, RedoPhase},
    state::PhaseStore,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;

struct PausedPhase {
    phase: PhaseName,
    reached: Arc<Notify>,
    resume: Arc<Notify>,
}

impl Phase for PausedPhase {
    fn name(&self) -> PhaseName {
        self.phase
    }
    fn run_batch(&self, _context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async {
            self.reached.notify_one();
            self.resume.notified().await;
            Err(phase_runner::error::RunnerError::data_integrity(
                "fixture stops after committed redo start",
            ))
        })
    }
}

struct RedoFixture {
    runner: PhaseRunner,
    chain: ChainConfig,
    reached: Arc<Notify>,
    resume: Arc<Notify>,
}

impl RedoFixture {
    async fn new(database: &TestDatabase, phase: PhaseName) -> Result<Self> {
        let runner_db = RunnerDatabase::connect_with_options(
            database
                .pool
                .connect_options()
                .as_ref()
                .clone()
                .application_name("lookup-redo-regression"),
            5,
        )
        .await?;
        PhaseStore::new(runner_db.pool().clone())
            .initialize_chain(FAMILY_CHAIN)
            .await?;
        // The family publication/head stay at 240. An already processed block 241 lets the
        // non-overlap control redo only work above that publication, through the real runner.
        sqlx::query("INSERT INTO chain_lineage (chain_id,block_number,block_hash,parent_hash,block_timestamp,canonicality_state) VALUES ($1,241,'0xhistory241','0xhistory240',to_timestamp(241),'canonical') ON CONFLICT DO NOTHING")
            .bind(FAMILY_CHAIN).execute(&database.pool).await?;
        sqlx::query("UPDATE chain_phase_state SET phase_status='completed',current_block_number=241,current_block_hash='0xhistory241',target_block_number=241,target_block_hash='0xhistory241',input_content_hash=$2,started_at=now(),finished_at=now() WHERE chain_id=$1")
            .bind(FAMILY_CHAIN).bind(bigname_content_hash::INTERPRETER_CONTENT_HASH).execute(&database.pool).await?;
        sqlx::query("INSERT INTO ingest_cursors (chain_id,source_key,source_kind,seed_basis,start_block_number,next_block_number,target_block_number,last_processed_block_number,last_processed_block_hash) VALUES ($1,'lookup-redo','jsonrpc','new_signature_range',200,242,241,241,'0xhistory241')")
            .bind(FAMILY_CHAIN).execute(&database.pool).await?;
        let chain = ChainConfig::new(
            FAMILY_CHAIN,
            vec![SourceConfig::new(
                FAMILY_CHAIN,
                "lookup-redo",
                "jsonrpc",
                SeedBasis::NewSignatureRange,
                200,
                "http://127.0.0.1:1",
            )?],
            false,
        )?;
        let reached = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        let phases = PhaseSet::new(PhaseName::ALL.map(|name| {
            if name == phase {
                Arc::new(PausedPhase {
                    phase,
                    reached: reached.clone(),
                    resume: resume.clone(),
                }) as Arc<dyn Phase>
            } else {
                Arc::new(LoopbackPhase::new(name)) as Arc<dyn Phase>
            }
        }))?;
        let runner = PhaseRunner::new(
            runner_db,
            phases,
            CapacityGuard::system(CapacityConfig::default()),
            "lookup-redo-regression",
            TimingConfig::default(),
        )?;
        Ok(Self {
            runner,
            chain,
            reached,
            resume,
        })
    }

    fn start(
        &self,
        phase: PhaseName,
        from: i64,
    ) -> tokio::task::JoinHandle<phase_runner::error::RunnerResult<()>> {
        let runner = self.runner.clone();
        let chain = self.chain.clone();
        tokio::spawn(async move {
            runner
                .redo(
                    &chain,
                    RedoPhase::Phase(phase),
                    BlockRange::new(from, 241)?,
                    tokio_util::sync::CancellationToken::new(),
                )
                .await
        })
    }
}

async fn lookup_answer(
    database: &TestDatabase,
    id: &str,
    address: &str,
) -> Result<bigname_lookup::LookupResponse> {
    let (url, handle) =
        spawn_primary_name_mock_rpc(vec![resolution_universal_resolver_addr60_response(address)])
            .await?;
    let engine = bigname_lookup::LookupEngine::new(
        database.pool.clone(),
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("{FAMILY_CHAIN}={url}")])?,
    );
    let response = engine
        .lookup(bigname_lookup::LookupRequest::new(id, ["addr:60"])?)
        .await?;
    join_primary_name_mock_rpc_requests(handle).await?;
    Ok(response)
}

#[tokio::test]
async fn family_lookup_refuses_insert_and_clear_after_actual_overlapping_redo() -> Result<()> {
    for phase in [PhaseName::Interpret, PhaseName::Project] {
        for (from, clearing) in [(240, false), (240, true), (241, false), (241, true)] {
            let database = TestDatabase::new_migrated().await?;
            let id = seed_family_lookup_fixture(&database).await?;
            let redo = RedoFixture::new(&database, phase).await?;
            if clearing {
                assert_eq!(
                    lookup_answer(&database, &id, FAMILY_BOB).await?.records[0].ledger_action,
                    bigname_lookup::LedgerAction::Written
                );
            }
            let before: Value = sqlx::query_scalar("SELECT jsonb_build_object('head',(SELECT to_jsonb(h) FROM chain_heads h WHERE chain_id=$1),'marker',(SELECT to_jsonb(m) FROM project_family_marker m WHERE chain_id=$1),'manifests',(SELECT jsonb_agg(to_jsonb(m) ORDER BY manifest_id) FROM manifest_versions m WHERE chain_id=$1))")
                .bind(FAMILY_CHAIN).fetch_one(&database.pool).await?;
            let (url, reached, release, handle) =
                spawn_primary_name_mock_rpc_with_last_response_gate(vec![
                    resolution_universal_resolver_addr60_response(if clearing {
                        FAMILY_ALICE
                    } else {
                        FAMILY_BOB
                    }),
                ])
                .await?;
            let engine = bigname_lookup::LookupEngine::new(
                database.pool.clone(),
                bigname_lookup::ChainRpcUrls::from_entries(&[format!("{FAMILY_CHAIN}={url}")])?,
            );
            let request = bigname_lookup::LookupRequest::new(&id, ["addr:60"])?;
            let lookup = tokio::spawn(async move { engine.lookup(request).await });
            tokio::time::timeout(Duration::from_secs(10), reached)
                .await
                .context("lookup RPC not reached")??;
            let mut task = redo.start(phase, from);
            tokio::select! {
                _ = redo.reached.notified() => {},
                result = &mut task => anyhow::bail!("redo exited before its paused batch: {result:?}"),
                _ = tokio::time::sleep(Duration::from_secs(10)) => anyhow::bail!("redo did not reach its batch"),
            }
            let after: Value = sqlx::query_scalar("SELECT jsonb_build_object('head',(SELECT to_jsonb(h) FROM chain_heads h WHERE chain_id=$1),'marker',(SELECT to_jsonb(m) FROM project_family_marker m WHERE chain_id=$1),'manifests',(SELECT jsonb_agg(to_jsonb(m) ORDER BY manifest_id) FROM manifest_versions m WHERE chain_id=$1))")
                .bind(FAMILY_CHAIN).fetch_one(&database.pool).await?;
            assert_eq!(
                before, after,
                "redo start must leave captured lookup inputs fixed"
            );
            release
                .send(())
                .map_err(|_| anyhow::anyhow!("RPC response gate closed"))?;
            let result = lookup.await?;
            // Always release the runner before assertions, including the initial red proof.
            redo.resume.notify_one();
            assert!(task.await?.is_err());
            if from <= 240 {
                assert_eq!(
                    result
                        .expect_err("overlapping redo invalidates the publication")
                        .kind(),
                    bigname_lookup::ErrorKind::ConcurrentState
                );
            } else {
                assert_eq!(
                    result?.records[0].ledger_action,
                    if clearing {
                        bigname_lookup::LedgerAction::Cleared
                    } else {
                        bigname_lookup::LedgerAction::Written
                    }
                );
            }
            let active: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM resolution_divergences WHERE cleared_at IS NULL",
            )
            .fetch_one(&database.pool)
            .await?;
            assert_eq!(
                active,
                if from <= 240 {
                    i64::from(clearing)
                } else {
                    i64::from(!clearing)
                }
            );
            join_primary_name_mock_rpc_requests(handle).await?;
            drop(redo);
            database.cleanup().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn forward_lookup_maps_publication_loss_to_stale_for_both_profiles() -> Result<()> {
    for profile in ["feed", "detail"] {
        let database = TestDatabase::new_migrated().await?;
        seed_family_records_fixture(&database).await?;
        let redo = RedoFixture::new(&database, PhaseName::Interpret).await?;
        let reached = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        let state = database.app_state();
        let reached_task = reached.clone();
        let resume_task = resume.clone();
        let request = tokio::spawn(
            bigname_storage::families::name::seams::with_pause_before_snapshot(
                reached_task,
                resume_task,
                async move {
                    app_router(state)
                        .oneshot(
                            Request::builder()
                                .method("POST")
                                .uri("/v1/lookup")
                                .header("content-type", "application/json")
                                .body(Body::from(
                                    serde_json::to_vec(
                                        &json!({"profile":profile,"inputs":[{"name":"alpha.eth"}]}),
                                    )
                                    .unwrap(),
                                ))
                                .unwrap(),
                        )
                        .await
                },
            ),
        );
        tokio::time::timeout(Duration::from_secs(10), reached.notified())
            .await
            .context("forward read did not reach family snapshot")?;
        let mut task = redo.start(PhaseName::Interpret, 240);
        tokio::select! {
            _ = redo.reached.notified() => {},
            result = &mut task => anyhow::bail!("redo exited before snapshot invalidation: {result:?}"),
            _ = tokio::time::sleep(Duration::from_secs(10)) => anyhow::bail!("redo did not reach its batch"),
        }
        resume.notify_one();
        let response = request.await??;
        redo.resume.notify_one();
        assert!(task.await?.is_err());
        drop(redo);
        assert_eq!(response.status(), StatusCode::CONFLICT, "{profile}");
        let body: Value = read_json(response).await?;
        assert_eq!(body["error"]["code"], "stale", "{body:#}");
        assert!(body.get("data").is_none());
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn family_lookup_holds_redo_admission_until_the_ledger_commits() -> Result<()> {
    for phase in [PhaseName::Interpret, PhaseName::Project] {
        let database = TestDatabase::new_migrated().await?;
        let id = seed_family_lookup_fixture(&database).await?;
        let redo = RedoFixture::new(&database, phase).await?;
        // Stop inside the ledger INSERT, after both guard invocations have succeeded.
        // This database-local trigger is only a deterministic transaction observation seam.
        sqlx::raw_sql("CREATE FUNCTION pause_lookup_ledger() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(97320260928); RETURN NEW; END $$; CREATE TRIGGER pause_lookup_ledger BEFORE INSERT ON resolution_divergences FOR EACH ROW EXECUTE FUNCTION pause_lookup_ledger()")
            .execute(&database.pool).await?;
        let mut gate = database.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(97320260928)")
            .execute(&mut *gate)
            .await?;
        let (url, handle) =
            spawn_primary_name_mock_rpc(vec![resolution_universal_resolver_addr60_response(
                FAMILY_BOB,
            )])
            .await?;
        let engine = bigname_lookup::LookupEngine::new(
            database.pool.clone(),
            bigname_lookup::ChainRpcUrls::from_entries(&[format!("{FAMILY_CHAIN}={url}")])?,
        );
        let request = bigname_lookup::LookupRequest::new(&id, ["addr:60"])?;
        let lookup = tokio::spawn(async move { engine.lookup(request).await });
        let writer: i32 = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let pid: Option<i32> = sqlx::query_scalar("SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory' AND query LIKE '%SELECT write_resolution_divergence(%'")
                    .fetch_optional(&database.pool).await?;
                if let Some(pid) = pid { break Ok::<_, anyhow::Error>(pid); }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.context("ledger write did not reach transaction gate")??;
        let mut task = redo.start(phase, 240);
        let blocked = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let blocked: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND application_name='lookup-redo-regression' AND $1=ANY(pg_blocking_pids(pid)))")
                    .bind(writer).fetch_one(&database.pool).await?;
                if blocked { break Ok::<_, anyhow::Error>(()); }
                tokio::select! {
                    _ = redo.reached.notified() => anyhow::bail!("redo committed while the ledger transaction was still open"),
                    result = &mut task => anyhow::bail!("redo exited before waiting on the ledger: {result:?}"),
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {},
                }
            }
        }).await.context("redo did not wait on the ledger transaction")?;
        gate.commit().await?;
        blocked?;
        let answer = lookup.await??;
        assert_eq!(
            answer.records[0].ledger_action,
            bigname_lookup::LedgerAction::Written
        );
        tokio::time::timeout(Duration::from_secs(10), redo.reached.notified())
            .await
            .context("redo did not resume after ledger commit")?;
        redo.resume.notify_one();
        assert!(task.await?.is_err());
        drop(redo);
        join_primary_name_mock_rpc_requests(handle).await?;
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn forward_lookup_keeps_unrelated_database_failures_internal() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_family_records_fixture(&database).await?;
    let reached = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    let state = database.app_state();
    let request = tokio::spawn(
        bigname_storage::families::name::seams::with_pause_before_snapshot(
            reached.clone(),
            resume.clone(),
            async move {
                app_router(state)
                    .oneshot(
                        Request::builder()
                            .method("POST")
                            .uri("/v1/lookup")
                            .header("content-type", "application/json")
                            .body(Body::from(
                                r#"{"profile":"detail","inputs":[{"name":"alpha.eth"}]}"#,
                            ))
                            .unwrap(),
                    )
                    .await
            },
        ),
    );
    tokio::time::timeout(Duration::from_secs(10), reached.notified())
        .await
        .context("forward read did not reach family snapshot")?;
    // A genuine query failure is not a stale publication. The marker and lineage stay valid.
    sqlx::query("ALTER TABLE project_name_history RENAME TO fixture_unavailable_name_history")
        .execute(&database.pool)
        .await?;
    resume.notify_one();
    let response = request.await??;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body: Value = read_json(response).await?;
    assert_eq!(body["error"]["code"], "internal_error", "{body:#}");
    database.cleanup().await
}
