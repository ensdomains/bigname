//! A supervised `run` after a chain stops on a terminal error. The process keeps serving its
//! metrics until a stop is requested, and the failure does not fail that stop's exit.
//!
//! | Row | Input | Expected | Test |
//! | --- | --- | --- | --- |
//! | S1 | One chain, terminal stop | `run` waits for the stop, the failure is not a stop-time failure | `one_chain_stays_up_after_a_terminal_stop` |
//! | S2 | One chain stopped, another live | Live chain keeps running, same report | `a_stopped_chain_leaves_the_live_chain_running_until_the_stop` |
//! | S3 | Every chain stopped | `run` waits for the stop, all reported | `every_chain_stopped_stays_up_until_the_stop` |
//! | S4 | Second `run` over a `failed` row | Same stop on the same block, stays up | `a_restart_over_a_failed_row_stops_again_and_stays_up` |
//! | S5 | Chain task panics | `run` waits for the stop, row stays `running` | `a_panicked_chain_stays_up_until_the_stop` |
//! | S6 | One chain failed, then a stop overruns on another | Only the overrun is a stop-time failure | `a_stop_overrun_after_a_chain_failure_is_the_stop_time_failure` |
//! | S9 | Scrape while staying up | `failed` status and the chain's loop series are served | `metrics_stay_served_after_a_terminal_stop` |
//!
//! Row S7 (`redo`), S8 (clean stop) and the exit mapping are in `src/main/tests.rs`. A stop during
//! a required redo is row 9 of `redo_overtaken_by_reorg.rs`.

use std::{
    io::{Read, Write},
    net::SocketAddr,
};

use phase_runner::runner::SupervisorReport;
use tokio::task::JoinHandle;
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::fmt::MakeWriter;

use super::*;

const CHAIN_STOPPED: &str = "chain supervisor stopped after a terminal error";
/// A `run` that returned as soon as its chains ended has returned by then.
const STAY_UP_WINDOW: Duration = Duration::from_millis(300);
const STOP_DEADLINE: Duration = Duration::from_secs(10);

#[tokio::test]
async fn one_chain_stays_up_after_a_terminal_stop() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_stop_one_chain").await?;
    let logs = CapturedLogs::default();
    let _logging = logs.capture();
    let run = SupervisedRun::start(
        runner(
            scratch.runner(),
            failing_ingest()?,
            available_capacity(),
            "stop-one-chain",
        )?,
        vec![chain("bad-chain")?],
    )?;

    logs.wait_for_stopped_chains(1).await?;
    wait_for_phase_status(scratch.pool(), "bad-chain", PhaseName::Ingest, "failed").await?;
    run.assert_stays_up().await;
    let report = run.stop().await?;

    assert_reported_before_the_stop(&report, &["bad-chain"]);
    assert_eq!(report.stopped_chains[0].1.kind(), ErrorKind::DataIntegrity);
    scratch.cleanup().await
}

#[tokio::test]
async fn a_stopped_chain_leaves_the_live_chain_running_until_the_stop() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_stop_live_sibling").await?;
    seed_identified_lineage(scratch.pool(), "good-chain", 0).await?;
    let logs = CapturedLogs::default();
    let _logging = logs.capture();
    let good_live = Arc::new(Notify::new());
    let run = SupervisedRun::start(
        runner(
            scratch.runner(),
            routing_phase_set(Arc::clone(&good_live))?,
            available_capacity(),
            "stop-live-sibling",
        )?,
        vec![chain("bad-chain")?, chain("good-chain")?],
    )?;

    good_live.notified().await;
    logs.wait_for_stopped_chains(1).await?;
    wait_for_phase_status(scratch.pool(), "good-chain", PhaseName::Live, "running").await?;
    run.assert_stays_up().await;
    let report = run.stop().await?;

    assert_reported_before_the_stop(&report, &["bad-chain"]);
    scratch.cleanup().await
}

#[tokio::test]
async fn every_chain_stopped_stays_up_until_the_stop() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_stop_every_chain").await?;
    let logs = CapturedLogs::default();
    let _logging = logs.capture();
    let run = SupervisedRun::start(
        runner(
            scratch.runner(),
            failing_ingest()?,
            available_capacity(),
            "stop-every-chain",
        )?,
        vec![chain("bad-chain")?, chain("worse-chain")?],
    )?;

    logs.wait_for_stopped_chains(2).await?;
    run.assert_stays_up().await;
    let report = run.stop().await?;

    let mut stopped = report
        .stopped_chains
        .iter()
        .map(|(chain_id, _)| chain_id.as_str())
        .collect::<Vec<_>>();
    stopped.sort_unstable();
    assert_eq!(stopped, ["bad-chain", "worse-chain"]);
    assert!(report.stop_time_failures().is_empty(), "{report:?}");
    scratch.cleanup().await
}

#[tokio::test]
async fn a_restart_over_a_failed_row_stops_again_and_stays_up() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_stop_restart").await?;
    let logs = CapturedLogs::default();
    let _logging = logs.capture();
    let first = SupervisedRun::start(
        runner(
            scratch.runner(),
            failing_ingest()?,
            available_capacity(),
            "stop-restart-first",
        )?,
        vec![chain("bad-chain")?],
    )?;
    logs.wait_for_stopped_chains(1).await?;
    wait_for_phase_status(scratch.pool(), "bad-chain", PhaseName::Ingest, "failed").await?;
    assert_reported_before_the_stop(&first.stop().await?, &["bad-chain"]);

    let second = SupervisedRun::start(
        runner(
            scratch.runner(),
            failing_ingest()?,
            available_capacity(),
            "stop-restart-second",
        )?,
        vec![chain("bad-chain")?],
    )?;
    logs.wait_for_stopped_chains(2).await?;
    wait_for_phase_status(scratch.pool(), "bad-chain", PhaseName::Ingest, "failed").await?;
    second.assert_stays_up().await;

    assert_reported_before_the_stop(&second.stop().await?, &["bad-chain"]);
    scratch.cleanup().await
}

#[tokio::test]
async fn a_panicked_chain_stays_up_until_the_stop() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_stop_panic").await?;
    let logs = CapturedLogs::default();
    let _logging = logs.capture();
    let panic_trigger = Arc::new(Notify::new());
    panic_trigger.notify_one();
    let run = SupervisedRun::start(
        runner(
            scratch.runner(),
            panic_routing_phase_set(Arc::new(Notify::new()), panic_trigger)?,
            available_capacity(),
            "stop-panic",
        )?,
        vec![chain("bad-chain")?],
    )?;

    logs.wait_for_stopped_chains(1).await?;
    run.assert_stays_up().await;
    // A panic records nothing, so this chain has no `failed` row for the loop alert's gate.
    wait_for_phase_status(scratch.pool(), "bad-chain", PhaseName::Ingest, "running").await?;
    let report = run.stop().await?;

    assert_reported_before_the_stop(&report, &["bad-chain"]);
    assert!(report.stopped_chains[0].1.to_string().contains("panicked"));
    scratch.cleanup().await
}

#[tokio::test]
async fn a_stop_overrun_after_a_chain_failure_is_the_stop_time_failure() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_stop_overrun").await?;
    let blocked = "stop-overrun-chain";
    seed_identified_lineage(scratch.pool(), blocked, 3).await?;
    PhaseStore::new(scratch.runner().pool().clone())
        .initialize_chain(blocked)
        .await?;
    let logs = CapturedLogs::default();
    let _logging = logs.capture();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let ingest = Arc::new(FailsOneChain {
        failing: "bad-chain",
        others: BlockingIngestPhase {
            heads: HeadMarkers {
                latest: BlockMarker::new(3, format!("{blocked}-block-3"))?,
                safe: Some(BlockMarker::new(2, format!("{blocked}-block-2"))?),
                finalized: Some(BlockMarker::new(1, format!("{blocked}-block-1"))?),
            },
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        },
    });
    let run = SupervisedRun::start(
        runner(
            scratch.runner(),
            phase_set_replacing(PhaseName::Ingest, ingest)?,
            available_capacity(),
            "stop-overrun",
        )?
        .with_stop_deadline(Duration::from_millis(200)),
        vec![chain("bad-chain")?, chain(blocked)?],
    )?;

    entered.notified().await;
    logs.wait_for_stopped_chains(1).await?;
    let mut holder = scratch.pool().begin().await?;
    sqlx::query(
        "SELECT 1 FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'ingest' FOR UPDATE",
    )
    .bind(blocked)
    .execute(&mut *holder)
    .await?;
    release.notify_one();
    wait_for_a_blocked_row_write(scratch.pool()).await?;
    let report = run.stop().await?;

    assert_eq!(report.stopped_chains.len(), 2);
    assert_eq!(report.failed_before_stop, 1);
    assert_eq!(report.stopped_chains[0].0, "bad-chain");
    let failures = report.stop_time_failures();
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].0, blocked);
    let message = failures[0].1.to_string();
    assert!(message.contains("post-batch settlement"), "{message}");
    holder.rollback().await?;
    scratch.cleanup().await
}

#[tokio::test]
async fn metrics_stay_served_after_a_terminal_stop() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_stop_metrics").await?;
    let loop_heartbeat = RunnerLoopHeartbeat::default();
    let cancellation = CancellationToken::new();
    let address = phase_runner::metrics::start(
        "127.0.0.1:0".parse()?,
        scratch.pool().clone(),
        cancellation.clone(),
        900,
        loop_heartbeat.clone(),
        RunnerPhaseProgress::default(),
        RunnerMetricsFeed::default(),
    )
    .await?;
    let run = SupervisedRun::start_with(
        runner(
            scratch.runner(),
            failing_ingest()?,
            available_capacity(),
            "stop-metrics",
        )?
        .with_loop_heartbeat(loop_heartbeat),
        vec![chain("bad-chain")?],
        cancellation,
    )?;

    wait_for_phase_status(scratch.pool(), "bad-chain", PhaseName::Ingest, "failed").await?;
    let body = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let body = scrape(address).await?;
            if body.lines().any(|line| {
                line.starts_with("phase_runner_phase_status{")
                    && line.contains("chain=\"bad-chain\"")
                    && line.contains("phase=\"ingest\"")
                    && line.contains("status=\"failed\"")
                    && line.ends_with(" 1")
            }) {
                return Ok::<_, anyhow::Error>(body);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .context("the failed status never reached the metrics listener")??;
    assert!(
        body.lines().any(|line| {
            line.starts_with("phase_runner_loop_heartbeat_age_seconds{")
                && line.contains("chain=\"bad-chain\"")
        }),
        "the stopped chain's loop series must stay exported:\n{body}"
    );
    run.assert_stays_up().await;
    assert_eq!(run.stop().await?.stopped_chains.len(), 1);
    scratch.cleanup().await
}

struct SupervisedRun {
    task: JoinHandle<RunnerResult<SupervisorReport>>,
    cancellation: CancellationToken,
}

impl SupervisedRun {
    fn start(runner: PhaseRunner, chains: Vec<ChainConfig>) -> Result<Self> {
        Self::start_with(runner, chains, CancellationToken::new())
    }

    fn start_with(
        runner: PhaseRunner,
        chains: Vec<ChainConfig>,
        cancellation: CancellationToken,
    ) -> Result<Self> {
        let runtime = RuntimeConfig::new(
            "terminal-stop-runner",
            chains,
            CapacityConfig::default(),
            test_timing(),
        )?;
        let run_cancellation = cancellation.clone();
        let task =
            tokio::spawn(async move { Arc::new(runner).run(&runtime, run_cancellation).await });
        Ok(Self { task, cancellation })
    }

    async fn assert_stays_up(&self) {
        tokio::time::sleep(STAY_UP_WINDOW).await;
        assert!(
            !self.task.is_finished(),
            "run returned before a stop was requested"
        );
    }

    async fn stop(self) -> Result<SupervisorReport> {
        self.cancellation.cancel();
        Ok(tokio::time::timeout(STOP_DEADLINE, self.task).await???)
    }
}

fn assert_reported_before_the_stop(report: &SupervisorReport, chains: &[&str]) {
    let stopped = report
        .stopped_chains
        .iter()
        .map(|(chain_id, _)| chain_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(stopped, chains);
    assert!(report.stop_time_failures().is_empty(), "{report:?}");
}

fn failing_ingest() -> RunnerResult<PhaseSet> {
    phase_set_replacing(
        PhaseName::Ingest,
        Arc::new(FunctionPhase {
            name: PhaseName::Ingest,
            handler: Arc::new(|_| Err(RunnerError::data_integrity("bad source count"))),
        }),
    )
}

async fn wait_for_a_blocked_row_write(pool: &sqlx::PgPool) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let waiting: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity
                 WHERE datname = current_database() AND wait_event_type = 'Lock'
                   AND query LIKE '%chain_phase_state%'",
            )
            .fetch_one(pool)
            .await?;
            if waiting >= 1 {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .context("no row write blocked on the held row")?
}

async fn scrape(address: SocketAddr) -> Result<String> {
    tokio::task::spawn_blocking(move || {
        let mut stream = std::net::TcpStream::connect(address)?;
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")?;
        let mut response = String::new();
        stream.read_to_string(&mut response)?;
        Ok::<_, anyhow::Error>(response)
    })
    .await?
}

struct FailsOneChain {
    failing: &'static str,
    others: BlockingIngestPhase,
}

impl Phase for FailsOneChain {
    fn name(&self) -> PhaseName {
        PhaseName::Ingest
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        if context.chain_id == self.failing {
            return Box::pin(async {
                Err::<PhaseBatchOutcome, _>(RunnerError::data_integrity("bad source count"))
            });
        }
        self.others.run_batch(context)
    }
}

/// The supervisor's log, which is the only signal that it has recorded a chain's stop. A stop
/// raised before that line would count the chain as a stop-time failure.
#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

struct CapturedLogWriter(Arc<Mutex<Vec<u8>>>);

impl CapturedLogs {
    /// A `#[tokio::test]` runs every task on this thread, so a thread default sees them all.
    fn capture(&self) -> DefaultGuard {
        tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_writer(self.clone())
                .with_ansi(false)
                .with_max_level(tracing::Level::ERROR)
                .finish(),
        )
    }

    fn stopped_chains(&self) -> usize {
        String::from_utf8_lossy(&self.0.lock().expect("captured log lock"))
            .matches(CHAIN_STOPPED)
            .count()
    }

    async fn wait_for_stopped_chains(&self, count: usize) -> Result<()> {
        tokio::time::timeout(STOP_DEADLINE, async {
            while self.stopped_chains() < count {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .with_context(|| format!("the supervisor never recorded {count} stopped chains"))
    }
}

impl<'a> MakeWriter<'a> for CapturedLogs {
    type Writer = CapturedLogWriter;

    fn make_writer(&'a self) -> Self::Writer {
        CapturedLogWriter(Arc::clone(&self.0))
    }
}

impl Write for CapturedLogWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .expect("captured log lock")
            .extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
