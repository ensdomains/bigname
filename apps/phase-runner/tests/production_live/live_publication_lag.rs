//! Live publishes the readable head N blocks below the newest block it loaded. The rows run the
//! production Live, Interpret and Project phases against the fixture node, reorg it, and read
//! what was published, orphaned and stamped.
use super::*;

const LAG: u32 = 2;

fn lagged_runner(
    scratch: &ScratchDatabase,
    engine: Arc<Engine>,
    chain: &str,
    lag: u32,
) -> Result<PhaseRunner> {
    let phases = PhaseSet::with_ingest_interpret_project_and_live(
        Arc::new(IngestPhase::with_engine(Arc::clone(&engine))),
        Arc::new(InterpretPhase::new(scratch.pool().clone())),
        Arc::new(ProjectPhase::new(scratch.pool().clone())),
        Arc::new(LoopbackPhase::new(PhaseName::Verify)),
        Arc::new(LivePhase::with_engine(engine).with_publication_lag_blocks(lag)),
    )?;
    Ok(PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        format!("lagged-{chain}"),
        fast_timing(),
    )?)
}

type RunnerTask = tokio::task::JoinHandle<phase_runner::error::RunnerResult<()>>;

struct Lagged {
    scratch: ScratchDatabase,
    fixture: RpcFixture,
    chain: String,
    stop: CancellationToken,
    task: RunnerTask,
}

/// A chain published and derived through `published` on branch 1, a node at `node`, and the
/// supervised runner following it with `lag`.
async fn start(label: &str, published: i64, node: i64, lag: u32) -> Result<Lagged> {
    let (scratch, fixture, chain) = seed(label, published, node).await?;
    let (stop, task) = spawn(&scratch, &fixture, &chain, lag, None)?;
    Ok(Lagged {
        scratch,
        fixture,
        chain,
        stop,
        task,
    })
}

async fn seed(
    label: &str,
    published: i64,
    node: i64,
) -> Result<(ScratchDatabase, RpcFixture, String)> {
    let scratch = ScratchDatabase::create(label).await?;
    let chain = label.replace('_', "-");
    seed_branch(scratch.pool(), &chain, 1, published, None).await?;
    publish(scratch.pool(), &chain, 1, published, 0, 0).await?;
    seed_completed_spine(scratch.pool(), &chain, published, &block_hash(1, published)).await?;
    seed_empty_watch_manifest(scratch.pool(), &chain).await?;
    let fixture = RpcFixture::spawn(1, node).await?;
    Ok((scratch, fixture, chain))
}

type PhaseHook = Box<
    dyn Fn(PhaseName) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        + Send
        + Sync,
>;

fn spawn(
    scratch: &ScratchDatabase,
    fixture: &RpcFixture,
    chain: &str,
    lag: u32,
    hook: Option<PhaseHook>,
) -> Result<(CancellationToken, RunnerTask)> {
    let engine = Arc::new(Engine::new(scratch.pool().clone()));
    let mut runner = lagged_runner(scratch, engine, chain, lag)?;
    if let Some(hook) = hook {
        runner = runner.with_before_phase_context(hook);
    }
    let configured = live_chain(chain, &fixture.endpoint)?;
    let stop = CancellationToken::new();
    let run_stop = stop.clone();
    let task = tokio::spawn(async move { runner.run_chain(&configured, run_stop).await });
    Ok((stop, task))
}

#[derive(Debug)]
struct Settled {
    head: (i64, String),
    safe: Option<(i64, String)>,
    interpret: (i64, String),
    /// None until Project has published once; the seeded spine has no family marker.
    project_marker: Option<(i64, String)>,
}

/// Wait until Live has loaded through `newest` and Interpret and Project stand completed, out of
/// redo, on the published head.
async fn settled(
    pool: &PgPool,
    chain: &str,
    newest: (i64, String),
    task: &mut RunnerTask,
) -> Result<Settled> {
    let wait = async {
        tokio::time::timeout(HEAD_ADVANCE_DEADLINE, async {
            loop {
                if let Some(settled) = settled_now(pool, chain, &newest).await? {
                    return Result::<Settled>::Ok(settled);
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| {
            anyhow::anyhow!("the runner did not settle on a head after loading {newest:?}")
        })?
    };
    tokio::pin!(wait);
    tokio::select! {
        result = task => match result {
            Ok(Ok(())) => anyhow::bail!("the lagged runner stopped before it settled"),
            Ok(Err(error)) => Err(error.into()),
            Err(error) => Err(error.into()),
        },
        result = &mut wait => result,
    }
}

/// A derived phase: name, status, whether it holds a redo, and its position.
type PhaseRow = (String, String, bool, Option<i64>, Option<String>);

async fn settled_now(
    pool: &PgPool,
    chain: &str,
    newest: &(i64, String),
) -> Result<Option<Settled>> {
    let live: Option<(Option<i64>, Option<String>)> = sqlx::query_as(
        "SELECT current_block_number, current_block_hash FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'live'",
    )
    .bind(chain)
    .fetch_optional(pool)
    .await?;
    if live != Some((Some(newest.0), Some(newest.1.clone()))) {
        return Ok(None);
    }
    let head: (i64, String, Option<i64>, Option<String>) = sqlx::query_as(
        "SELECT latest_block_number, latest_block_hash, safe_block_number, safe_block_hash
         FROM chain_heads WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(pool)
    .await?;
    let phases: Vec<PhaseRow> = sqlx::query_as(
        "SELECT phase_name, phase_status, redo_in_progress, current_block_number, current_block_hash
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')
         ORDER BY phase_name",
    )
    .bind(chain)
    .fetch_all(pool)
    .await?;
    let at_head = phases.iter().all(|(_, status, redo, number, hash)| {
        status == "completed"
            && !redo
            && *number == Some(head.0)
            && hash.as_deref() == Some(head.1.as_str())
    });
    if !at_head {
        return Ok(None);
    }
    let project_marker: Option<(i64, String)> = sqlx::query_as(
        "SELECT current_block_number, current_block_hash FROM project_family_marker WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_optional(pool)
    .await?;
    Ok(Some(Settled {
        head: (head.0, head.1),
        safe: head.2.zip(head.3),
        interpret: (
            phases[0].3.unwrap_or_default(),
            phases[0].4.clone().unwrap_or_default(),
        ),
        project_marker,
    }))
}

async fn lineage(pool: &PgPool, chain: &str, from: i64) -> Result<Vec<(i64, String, String)>> {
    Ok(sqlx::query_as(
        "SELECT block_number, block_hash, canonicality_state::text
         FROM chain_lineage WHERE chain_id = $1 AND block_number >= $2
         ORDER BY block_number, block_hash",
    )
    .bind(chain)
    .bind(from)
    .fetch_all(pool)
    .await?)
}

/// Each derived phase's redo generation and whether it holds a redo. Every stamp increments the
/// generation and completion leaves it, so an unchanged generation means no redo ran.
async fn redo_generations(pool: &PgPool, chain: &str) -> Result<Vec<(String, i64, bool)>> {
    Ok(sqlx::query_as(
        "SELECT phase_name, redo_attempt_generation, redo_in_progress
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project', 'verify')
         ORDER BY phase_name",
    )
    .bind(chain)
    .fetch_all(pool)
    .await?)
}

fn at(branch: u64, number: i64) -> (i64, String) {
    (number, block_hash(branch, number))
}

fn row(branch: u64, number: i64, state: &str) -> (i64, String, String) {
    (number, block_hash(branch, number), state.to_owned())
}

impl Lagged {
    async fn settled(&mut self, newest: (i64, String)) -> Result<Settled> {
        settled(self.scratch.pool(), &self.chain, newest, &mut self.task).await
    }

    /// Stop the runner, extend the node's branch 1 through `extend_to` when given, and start the
    /// runner again with `lag`.
    async fn restart(&mut self, lag: u32, extend_to: Option<i64>) -> Result<()> {
        self.stop.cancel();
        let previous = std::mem::replace(&mut self.task, tokio::spawn(async { Ok(()) }));
        previous.await??;
        if let Some(through) = extend_to {
            self.fixture.reorg(1, through - 1, through).await;
        }
        let (stop, task) = spawn(&self.scratch, &self.fixture, &self.chain, lag, None)?;
        self.stop = stop;
        self.task = task;
        Ok(())
    }

    async fn finish(self) -> Result<()> {
        self.stop.cancel();
        self.task.await??;
        self.fixture.server.abort();
        self.scratch.cleanup().await
    }
}

// Row 2, with the fence of row 10: the head advances from 3 to 6.
#[tokio::test]
async fn lagged_live_publishes_two_blocks_below_the_loaded_head_and_serves_at_tolerance_one()
-> Result<()> {
    let mut lagged = start("live_lag_advance", 3, 6, LAG).await?;
    let settled = lagged.settled(at(1, 6)).await?;
    assert_eq!(settled.head, at(1, 4), "{settled:?}");
    assert_eq!(settled.interpret, at(1, 4));
    assert_eq!(settled.project_marker, Some(at(1, 4)));
    let pool = lagged.scratch.pool();
    assert_eq!(
        lineage(pool, &lagged.chain, 4).await?,
        vec![
            row(1, 4, "canonical"),
            row(1, 5, "observed"),
            row(1, 6, "observed")
        ]
    );
    // The API's own fence query: the publication is one the head is not ahead of.
    let (number, hash) = settled
        .project_marker
        .as_ref()
        .context("a family publication")?;
    for tolerance in [0, 1] {
        let served = bigname_storage::load_served_project_generation(
            pool,
            &lagged.chain,
            *number,
            hash,
            true,
            true,
            tolerance,
        )
        .await?;
        assert!(
            served.is_some(),
            "tolerance {tolerance} must serve the lagged publication"
        );
    }
    lagged.finish().await
}

// Row 3: a depth-1 reorg of the newest block, then the head advances on the new branch.
#[tokio::test]
async fn depth_one_reorg_inside_the_lag_orphans_without_a_stamp_and_follows_the_new_branch()
-> Result<()> {
    let mut lagged = start("live_lag_depth_one", 3, 6, LAG).await?;
    lagged.settled(at(1, 6)).await?;
    let pool = lagged.scratch.pool().clone();
    let before = redo_generations(&pool, &lagged.chain).await?;
    lagged.fixture.reorg(2, 5, 6).await;
    let settled = lagged.settled(at(2, 6)).await?;
    assert_eq!(settled.head, at(1, 4));
    assert_eq!(
        lineage(&pool, &lagged.chain, 6).await?,
        vec![row(1, 6, "orphaned"), row(2, 6, "observed")]
    );
    assert_eq!(
        redo_generations(&pool, &lagged.chain).await?,
        before,
        "no phase was stamped"
    );
    lagged.fixture.reorg(2, 6, 8).await;
    let settled = lagged.settled(at(2, 8)).await?;
    assert_eq!(settled.head, at(2, 6));
    assert_eq!(
        settled.interpret,
        at(2, 6),
        "Interpret follows the new branch"
    );
    assert_eq!(settled.project_marker, Some(at(2, 6)));
    assert_eq!(redo_generations(&pool, &lagged.chain).await?, before);
    lagged.finish().await
}

// Row 4: a depth-2 reorg replaces both observed blocks.
#[tokio::test]
async fn depth_two_reorg_inside_the_lag_orphans_without_a_stamp() -> Result<()> {
    let mut lagged = start("live_lag_depth_two", 3, 6, LAG).await?;
    lagged.settled(at(1, 6)).await?;
    let pool = lagged.scratch.pool().clone();
    let before = redo_generations(&pool, &lagged.chain).await?;
    lagged.fixture.reorg(2, 4, 6).await;
    let settled = lagged.settled(at(2, 6)).await?;
    assert_eq!(settled.head, at(1, 4), "the readable head stays");
    assert_eq!(
        lineage(&pool, &lagged.chain, 4).await?,
        vec![
            row(1, 4, "canonical"),
            row(1, 5, "orphaned"),
            row(2, 5, "observed"),
            row(1, 6, "orphaned"),
            row(2, 6, "observed"),
        ]
    );
    assert_eq!(
        redo_generations(&pool, &lagged.chain).await?,
        before,
        "no phase was stamped"
    );
    lagged.finish().await
}

// Row 5: a depth-3 reorg reaches the readable head, so the displaced readable block is stamped.
#[tokio::test]
async fn depth_three_reorg_stamps_only_the_displaced_readable_block() -> Result<()> {
    let (scratch, fixture, chain) = seed("live_lag_depth_three", 3, 6).await?;
    let stamps = Arc::new(Mutex::new(Vec::<(String, i64, i64)>::new()));
    let hook: PhaseHook = {
        let pool = scratch.pool().clone();
        let chain = chain.clone();
        let stamps = Arc::clone(&stamps);
        Box::new(move |_phase| {
            let pool = pool.clone();
            let chain = chain.clone();
            let stamps = Arc::clone(&stamps);
            Box::pin(async move {
                let active: Vec<(String, i64, i64)> = sqlx::query_as(
                    "SELECT phase_name, redo_from_block_number, redo_to_block_number
                     FROM chain_phase_state
                     WHERE chain_id = $1 AND redo_in_progress
                       AND phase_name IN ('interpret', 'project')
                     ORDER BY phase_name",
                )
                .bind(&chain)
                .fetch_all(&pool)
                .await
                .unwrap();
                let mut seen = stamps.lock().unwrap();
                for stamp in active {
                    if !seen.iter().any(|(phase, _, _)| *phase == stamp.0) {
                        seen.push(stamp);
                    }
                }
            })
        })
    };
    let (stop, task) = spawn(&scratch, &fixture, &chain, LAG, Some(hook))?;
    let mut lagged = Lagged {
        scratch,
        fixture,
        chain,
        stop,
        task,
    };
    lagged.settled(at(1, 6)).await?;
    assert!(stamps.lock().unwrap().is_empty());
    lagged.fixture.reorg(2, 3, 6).await;
    let settled = lagged.settled(at(2, 6)).await?;
    assert_eq!(
        *stamps.lock().unwrap(),
        vec![("interpret".to_owned(), 4, 4), ("project".to_owned(), 4, 4)],
        "the stamp covers the one readable block the reorg displaced"
    );
    assert_eq!(
        settled.head,
        at(2, 4),
        "the readable head is the lag below the new branch"
    );
    assert_eq!(settled.interpret, at(2, 4));
    let pool = lagged.scratch.pool();
    assert_eq!(
        lineage(pool, &lagged.chain, 4).await?,
        vec![
            row(1, 4, "orphaned"),
            row(2, 4, "canonical"),
            row(1, 5, "orphaned"),
            row(2, 5, "observed"),
            row(1, 6, "orphaned"),
            row(2, 6, "observed"),
        ]
    );
    lagged.finish().await
}

// Row 6, and row 15: the node stops at 6, then reports a block below the readable head.
#[tokio::test]
async fn a_stalled_or_lagging_node_keeps_the_readable_head_without_restarting_derivation()
-> Result<()> {
    let mut lagged = start("live_lag_stalled", 3, 6, LAG).await?;
    let settled = lagged.settled(at(1, 6)).await?;
    assert_eq!(settled.head, at(1, 4));
    let pool = lagged.scratch.pool().clone();
    let started = |pool: PgPool, chain: String| async move {
        sqlx::query_as::<_, (String, String)>(
            "SELECT phase_name, started_at::text FROM chain_phase_state
             WHERE chain_id = $1 AND phase_name IN ('interpret', 'project') ORDER BY phase_name",
        )
        .bind(chain)
        .fetch_all(&pool)
        .await
    };
    let derivation = started(pool.clone(), lagged.chain.clone()).await?;
    for step in 0..2 {
        if step == 1 {
            // The node reports block 3 of the same branch, below the readable head.
            apply_fixture_reorg(&mut *lagged.fixture.state.write().await, 1, 3, 3);
        }
        let polled = lagged.fixture.requests().await + 20;
        tokio::time::timeout(HEAD_ADVANCE_DEADLINE, async {
            while lagged.fixture.requests().await < polled {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert!(
            !lagged.task.is_finished(),
            "Live keeps following without an error"
        );
        let head: (i64, String) = sqlx::query_as(
            "SELECT latest_block_number, latest_block_hash FROM chain_heads WHERE chain_id = $1",
        )
        .bind(&lagged.chain)
        .fetch_one(&pool)
        .await?;
        assert_eq!(head, at(1, 4), "step {step}");
        assert_eq!(
            started(pool.clone(), lagged.chain.clone()).await?,
            derivation,
            "a caught-up Interpret and Project are not restarted"
        );
    }
    lagged.finish().await
}

// Row 7: fewer than N blocks above the published head.
#[tokio::test]
async fn fewer_loaded_blocks_than_the_lag_keep_the_published_head() -> Result<()> {
    let mut lagged = start("live_lag_short", 3, 4, LAG).await?;
    let settled = lagged.settled(at(1, 4)).await?;
    assert_eq!(settled.head, at(1, 3));
    assert_eq!(settled.interpret, at(1, 3));
    assert_eq!(
        lineage(lagged.scratch.pool(), &lagged.chain, 3).await?,
        vec![row(1, 3, "canonical"), row(1, 4, "observed")]
    );
    lagged.finish().await
}

// Row 8: a restart finds 4..6 loaded but never published.
#[tokio::test]
async fn restart_with_an_unpublished_window_publishes_the_lag_below_it() -> Result<()> {
    let (scratch, fixture, chain) = seed("live_lag_restart_unpublished", 3, 6).await?;
    let staged = Engine::new(scratch.pool().clone())
        .run_live_batch(live_request(&chain, &fixture.endpoint, 3, block_hash(1, 3)))
        .await?;
    assert_eq!(staged.current.number, 6);
    let (stop, task) = spawn(&scratch, &fixture, &chain, LAG, None)?;
    let mut lagged = Lagged {
        scratch,
        fixture,
        chain,
        stop,
        task,
    };
    let settled = lagged.settled(at(1, 6)).await?;
    assert_eq!(settled.head, at(1, 4));
    assert_eq!(
        lineage(lagged.scratch.pool(), &lagged.chain, 4).await?,
        vec![
            row(1, 4, "canonical"),
            row(1, 5, "observed"),
            row(1, 6, "observed")
        ]
    );
    lagged.fixture.reorg(1, 6, 7).await;
    let settled = lagged.settled(at(1, 7)).await?;
    assert_eq!(settled.head, at(1, 5));
    assert_eq!(settled.interpret, at(1, 5));
    lagged.finish().await
}

// Row 8b: a restart after a lagged publication reloads the observed blocks and adds no rows.
#[tokio::test]
async fn restart_after_a_lagged_publication_resumes_from_the_readable_head() -> Result<()> {
    let mut lagged = start("live_lag_restart_published", 3, 6, LAG).await?;
    lagged.settled(at(1, 6)).await?;
    lagged.stop.cancel();
    let first = std::mem::replace(&mut lagged.task, tokio::spawn(async { Ok(()) }));
    first.await??;
    let pool = lagged.scratch.pool().clone();
    let rows_before: i64 =
        sqlx::query_scalar("SELECT count(*) FROM chain_lineage WHERE chain_id = $1")
            .bind(&lagged.chain)
            .fetch_one(&pool)
            .await?;
    lagged.fixture.reorg(1, 6, 7).await;
    let (stop, task) = spawn(&lagged.scratch, &lagged.fixture, &lagged.chain, LAG, None)?;
    lagged.stop = stop;
    lagged.task = task;
    let settled = lagged.settled(at(1, 7)).await?;
    assert_eq!(settled.head, at(1, 5));
    let rows_after: i64 =
        sqlx::query_scalar("SELECT count(*) FROM chain_lineage WHERE chain_id = $1")
            .bind(&lagged.chain)
            .fetch_one(&pool)
            .await?;
    assert_eq!(rows_after, rows_before + 1, "only block 7 is new");
    assert_eq!(
        lineage(&pool, &lagged.chain, 5).await?,
        vec![
            row(1, 5, "canonical"),
            row(1, 6, "observed"),
            row(1, 7, "observed")
        ]
    );
    lagged.finish().await
}

// Row 12: the node's safe block is its head, inside the lag window.
#[tokio::test]
async fn a_safe_block_inside_the_window_is_published_at_the_readable_head() -> Result<()> {
    let (scratch, fixture, chain) = seed("live_lag_safe", 3, 6).await?;
    fixture.state.write().await.safe_at_head = true;
    let (stop, task) = spawn(&scratch, &fixture, &chain, LAG, None)?;
    let mut lagged = Lagged {
        scratch,
        fixture,
        chain,
        stop,
        task,
    };
    let settled = lagged.settled(at(1, 6)).await?;
    assert_eq!(settled.head, at(1, 4));
    assert_eq!(settled.safe, Some(at(1, 4)));
    lagged.fixture.reorg(1, 6, 7).await;
    let settled = lagged.settled(at(1, 7)).await?;
    assert_eq!(settled.head, at(1, 5));
    assert_eq!(
        settled.safe,
        Some(at(1, 5)),
        "safe advances with the head, never past it"
    );
    assert_eq!(
        lineage(lagged.scratch.pool(), &lagged.chain, 4).await?,
        vec![
            row(1, 4, "safe"),
            row(1, 5, "safe"),
            row(1, 6, "observed"),
            row(1, 7, "observed"),
        ]
    );
    lagged.finish().await
}

// A reorg and an extension in one poll: the node forks at the readable head and runs four blocks
// past it. The two replaced blocks were only observed, so nothing is stamped, and the new branch's
// blocks up to the lag below its top become readable straight from observed.
#[tokio::test]
async fn a_reorg_that_also_extends_promotes_the_new_branch_without_a_stamp() -> Result<()> {
    let mut lagged = start("live_lag_reorg_extend", 3, 6, LAG).await?;
    lagged.settled(at(1, 6)).await?;
    let pool = lagged.scratch.pool().clone();
    let before = redo_generations(&pool, &lagged.chain).await?;
    lagged.fixture.reorg(2, 4, 8).await;
    let settled = lagged.settled(at(2, 8)).await?;
    assert_eq!(settled.head, at(2, 6));
    assert_eq!(settled.interpret, at(2, 6));
    assert_eq!(settled.project_marker, Some(at(2, 6)));
    assert_eq!(
        lineage(&pool, &lagged.chain, 4).await?,
        vec![
            row(1, 4, "canonical"),
            row(1, 5, "orphaned"),
            row(2, 5, "canonical"),
            row(1, 6, "orphaned"),
            row(2, 6, "canonical"),
            row(2, 7, "observed"),
            row(2, 8, "observed"),
        ]
    );
    assert_eq!(
        redo_generations(&pool, &lagged.chain).await?,
        before,
        "no phase was stamped"
    );
    lagged.finish().await
}

// A fork that shortens the chain: the node drops to 5 on a branch forked at the readable head.
// The old block 6 sits above the orphan ceiling and stays observed, unread, until the new branch
// passes it.
#[tokio::test]
async fn a_shortening_fork_leaves_the_block_above_its_top_observed_until_passed() -> Result<()> {
    let mut lagged = start("live_lag_shortening", 3, 6, LAG).await?;
    lagged.settled(at(1, 6)).await?;
    let pool = lagged.scratch.pool().clone();
    let before = redo_generations(&pool, &lagged.chain).await?;
    lagged.fixture.reorg(2, 4, 5).await;
    let settled = lagged.settled(at(2, 5)).await?;
    assert_eq!(settled.head, at(1, 4), "the readable head stays");
    assert_eq!(
        lineage(&pool, &lagged.chain, 5).await?,
        vec![
            row(1, 5, "orphaned"),
            row(2, 5, "observed"),
            row(1, 6, "observed"),
        ]
    );
    lagged.fixture.reorg(2, 5, 8).await;
    let settled = lagged.settled(at(2, 8)).await?;
    assert_eq!(settled.head, at(2, 6));
    assert_eq!(settled.interpret, at(2, 6));
    assert_eq!(
        lineage(&pool, &lagged.chain, 6).await?,
        vec![
            row(1, 6, "orphaned"),
            row(2, 6, "canonical"),
            row(2, 7, "observed"),
            row(2, 8, "observed"),
        ]
    );
    assert_eq!(
        redo_generations(&pool, &lagged.chain).await?,
        before,
        "no phase was stamped"
    );
    lagged.finish().await
}

fn readable(heads: Option<bigname_ingest::HeadMarkers>) -> Result<HeadMarkers> {
    let heads = heads.context("live batch did not request publication")?;
    let marker = |marker: bigname_ingest::Marker| BlockMarker::new(marker.number, marker.hash);
    Ok(HeadMarkers {
        latest: marker(heads.latest)?,
        safe: heads.safe.map(marker).transpose()?,
        finalized: heads.finalized.map(marker).transpose()?,
    })
}

// Catch-up under the lag: each bounded window publishes the lag below its top, and the next
// window resumes from that readable head, so the readable lineage stays contiguous.
#[tokio::test]
async fn catch_up_windows_each_publish_the_lag_below_their_top() -> Result<()> {
    let scratch = ScratchDatabase::create("live_lag_catch_up").await?;
    let chain = "live-lag-catch-up";
    seed_branch(scratch.pool(), chain, 1, 0, None).await?;
    publish(scratch.pool(), chain, 1, 0, 0, 0).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;
    let fixture = RpcFixture::spawn(1, 600).await?;
    let engine = Engine::new(scratch.pool().clone());
    let request = || live_request(chain, &fixture.endpoint, 0, block_hash(1, 0));
    let head = || async {
        sqlx::query_as::<_, (i64, String)>(
            "SELECT latest_block_number, latest_block_hash FROM chain_heads WHERE chain_id = $1",
        )
        .bind(chain)
        .fetch_one(scratch.pool())
        .await
    };
    let mut windows = Vec::new();
    loop {
        let batch = engine.run_live_batch(request()).await?;
        publish_lagged_heads(scratch.pool(), chain, &readable(batch.heads)?, LAG).await?;
        windows.push((batch.current.number, head().await?));
        if batch.caught_up {
            break;
        }
    }
    assert_eq!(
        windows,
        vec![(256, at(1, 254)), (510, at(1, 508)), (600, at(1, 598))],
        "each window resumes from the readable head the previous one published"
    );
    let states: Vec<(String, i64, i64, i64)> = sqlx::query_as(
        "SELECT canonicality_state::text, count(*), min(block_number), max(block_number)
         FROM chain_lineage WHERE chain_id = $1
         GROUP BY canonicality_state ORDER BY canonicality_state::text",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    let readable_rows = states
        .iter()
        .filter(|(state, ..)| ["canonical", "safe", "finalized"].contains(&state.as_str()))
        .map(|(_, count, ..)| *count)
        .sum::<i64>();
    assert_eq!(
        readable_rows, 599,
        "blocks 0 through 598, with no gap: {states:?}"
    );
    assert!(
        states.contains(&("observed".to_owned(), 2, 599, 600)),
        "{states:?}"
    );
    assert!(!states.iter().any(|(state, ..)| state == "orphaned"));
    fixture.server.abort();
    scratch.cleanup().await
}

// The lag changed across a restart. Raised from 2 to 3 with no new block, the lag would put the
// head below the block already readable, which holds instead. Raised with new blocks, the head
// stalls until the lag catches up. Lowered to 0, every loaded block becomes readable, as on a
// runner without the setting.
#[tokio::test]
async fn a_lag_changed_across_a_restart_never_moves_the_head_backward() -> Result<()> {
    let mut lagged = start("live_lag_changed", 3, 6, LAG).await?;
    lagged.settled(at(1, 6)).await?;
    let pool = lagged.scratch.pool().clone();
    let before = redo_generations(&pool, &lagged.chain).await?;
    // The node still stands at 6, so 6 - 3 = 3 is below the readable head 4.
    lagged.restart(3, None).await?;
    // Live keeps polling a node with nothing new. Twenty more requests take the restarted runner
    // past its first batch and the publication that follows it.
    let polled = lagged.fixture.requests().await + 20;
    tokio::time::timeout(HEAD_ADVANCE_DEADLINE, async {
        while lagged.fixture.requests().await < polled && !lagged.task.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("the restarted runner stopped polling the node"))?;
    let published: (i64, String) = sqlx::query_as(
        "SELECT latest_block_number, latest_block_hash FROM chain_heads WHERE chain_id = $1",
    )
    .bind(&lagged.chain)
    .fetch_one(&pool)
    .await?;
    assert_eq!(published, at(1, 4), "lag 3 with no new block");
    let settled = lagged.settled(at(1, 6)).await?;
    assert_eq!(settled.head, at(1, 4));
    assert_eq!(settled.interpret, at(1, 4));
    assert_eq!(
        lineage(&pool, &lagged.chain, 4).await?,
        vec![
            row(1, 4, "canonical"),
            row(1, 5, "observed"),
            row(1, 6, "observed")
        ]
    );
    assert_eq!(count_in_state(&pool, &lagged.chain, "orphaned").await?, 0);
    assert_eq!(
        redo_generations(&pool, &lagged.chain).await?,
        before,
        "no phase was stamped"
    );
    for (lag, through, head) in [(3, 7, 4), (3, 8, 5), (0, 9, 9)] {
        lagged.restart(lag, Some(through)).await?;
        let settled = lagged.settled(at(1, through)).await?;
        assert_eq!(
            settled.head,
            at(1, head),
            "lag {lag} with block {through} loaded"
        );
        assert_eq!(settled.interpret, at(1, head));
    }
    assert_eq!(
        count_in_state(&pool, &lagged.chain, "observed").await?,
        0,
        "at lag 0 nothing stays observed"
    );
    lagged.finish().await
}

async fn count_in_state(pool: &PgPool, chain: &str, state: &str) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM chain_lineage
         WHERE chain_id = $1 AND canonicality_state::text = $2",
    )
    .bind(chain)
    .bind(state)
    .fetch_one(pool)
    .await?)
}

// Metrics under the lag. The chain-head gauge is the published, lagged head. Live's target is the
// node's head and its position the newest block it loaded, which match once it has caught up, so
// its head lag reads 0, as do Interpret and Project, which stand on the published head. The
// publication gap reads the configured lag, and so does the served lag, which measures from the
// node's head.
#[tokio::test]
async fn metrics_report_the_publication_gap_and_the_configured_lag() -> Result<()> {
    for (label, lag, head) in [("live_lag_metrics", LAG, 4), ("live_lag_metrics_off", 0, 6)] {
        let mut lagged = start(label, 3, 6, lag).await?;
        lagged.settled(at(1, 6)).await?;
        let body = scrape_metrics(lagged.scratch.pool(), lag).await?;
        let sample = |name: &str, labels: &[&str]| -> Result<f64> {
            let line = body
                .lines()
                .find(|line| {
                    line.starts_with(&format!("{name}{{"))
                        && line.contains(&format!("chain=\"{}\"", lagged.chain))
                        && labels.iter().all(|label| line.contains(label))
                })
                .with_context(|| format!("missing metric {name} {labels:?}"))?;
            Ok(line.rsplit_once(' ').context("no value")?.1.parse()?)
        };
        let n = f64::from(lag);
        assert_eq!(
            sample("phase_runner_chain_head_block", &[])?,
            f64::from(head)
        );
        assert_eq!(
            sample("phase_runner_publication_gap_blocks", &[])?,
            n,
            "lag {lag}"
        );
        assert_eq!(
            sample("phase_runner_publication_lag_blocks", &[])?,
            n,
            "lag {lag}"
        );
        assert_eq!(
            sample("phase_runner_served_lag_blocks", &[])?,
            n,
            "lag {lag}"
        );
        for phase in ["live", "interpret", "project"] {
            assert_eq!(
                sample(
                    "phase_runner_head_lag_blocks",
                    &[&format!("phase=\"{phase}\"")]
                )?,
                0.0,
                "{phase} at lag {lag}"
            );
        }
        assert_eq!(
            sample("phase_runner_phase_current_block", &["phase=\"live\""])?,
            6.0,
            "Live's position is the newest loaded block"
        );
        lagged.finish().await?;
    }
    Ok(())
}

async fn scrape_metrics(pool: &PgPool, lag: u32) -> Result<String> {
    let stop = CancellationToken::new();
    let feed = phase_runner::metrics::RunnerMetricsFeed::default();
    feed.set_live_publication_lag_blocks(lag);
    let address = phase_runner::metrics::start(
        "127.0.0.1:0".parse()?,
        pool.clone(),
        stop.clone(),
        900,
        phase_runner::metrics::RunnerLoopHeartbeat::default(),
        phase_runner::RunnerPhaseProgress::default(),
        feed,
    )
    .await?;
    let body = tokio::task::spawn_blocking(move || -> Result<String> {
        use std::io::Read as _;
        let mut stream = std::net::TcpStream::connect(address)?;
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")?;
        let mut response = String::new();
        stream.read_to_string(&mut response)?;
        Ok(response)
    })
    .await??;
    stop.cancel();
    Ok(body)
}
