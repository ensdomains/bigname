//! Before Project runs, the runner rebuilds the `normalized_events` indexes that
//! `ops/walk-index-set/drop.sql` drops for a from-zero walk or a full-history Interpret redo.
//!
//! | Row | Input | Expected | Test |
//! | --- | --- | --- | --- |
//! | 1 | Every index in place | No DDL, every index keeps its OID | `restore_is_a_no_op_on_a_complete_set` |
//! | 2 | One missing | Built, valid and ready, named in the log | `restore_builds_a_missing_index` |
//! | 3 | One not valid, or not ready | Dropped and rebuilt under a new OID | `restore_drops_and_rebuilds_an_invalid_leftover` |
//! | 4 | An index the list does not name | Left alone | `restore_leaves_an_unlisted_index_alone` |
//! | 5 | A table, a view or another table's index under a listed name | Refused, nothing dropped, Project not started | `restore_refuses_a_foreign_relation_under_a_dropped_name` |
//! | 7 | A writer while the build waits | The writer commits, the build finishes | `restore_builds_concurrently_while_a_writer_commits` |
//! | 9 | A second Project entry, then a fresh runner | No DDL | `restore_is_idempotent_across_boots` |
//! | 10 | A valid index with another definition | Kept, warned with both definitions once per process | `restore_keeps_a_valid_index_with_another_definition_and_warns` |
//! | 11 | A supervised walk | Interpret runs without the index, Project's first batch has it | `restore_runs_before_projects_first_batch_and_not_during_interpret` |
//! | 12 | A one-shot Interpret redo | Absent through Interpret, present for the stamped Project redo | `interpret_redo_walks_without_the_index_and_project_redo_gets_it_back` |
//! | 13 | Another chain still in an Interpret redo | Built, warned naming that chain | `restore_warns_about_a_chain_still_in_an_interpret_redo` |
//! | 26 | Another chain on its first Interpret walk, and a chain whose Interpret completed | Built, warned naming only the walking chain and its state | `restore_warns_about_a_chain_still_on_its_first_interpret_walk` |
//! | 14 | A build that fails | Error names the index and the cause, not retried, Project not started (the supervisor's non-zero exit on a not-retried error is the runner's existing behaviour) | `a_failed_build_stops_project_with_the_error_line` |
//! | 15 | A stop during a build | Returns within the stop budget, the server build is cancelled, the leftover is rebuilt by the next entry, in the same process too, whatever cancelled the token | `a_stop_during_the_restore_cancels_the_build_and_leaves_a_rebuildable_leftover` |
//! | 20 | The transfer node schema-migration | Adopts the runner's build, and the runner keeps the migration's | `the_transfer_node_migration_and_the_runner_build_each_others_index_once` |
//! | 22 | The fresh baseline | Every listed definition is the baseline's, and the two lists are every index | `module_definitions_are_the_baselines` |
//! | 23 | Another login's build, `VACUUM` or `ANALYZE` on the table | Waits naming the backend, keeps the loop heartbeat fresh, then builds | `restore_waits_for_another_build_on_the_table` |
//! | 25 | A build that failed, then another chain's Project entry in the same process, then a restart | The other chain stops with the same error and no build, the restart builds again | `a_failed_build_stops_every_chain_in_the_process_and_a_restart_builds_again` |
//! | D5 | Rebuild settings set, and unset | The build session runs with them, or with the server's | `restore_applies_the_build_settings` |

use std::collections::{BTreeMap, BTreeSet};

use bigname_storage::walk_index_set::{DROP_LIST, IndexState, KEPT, load_drop_list_state};
use phase_runner::{database::RunnerDatabase, walk_index_restore::IndexBuildSettings};
use sqlx::{Connection, PgConnection};
use tracing::instrument::WithSubscriber;

use super::*;

/// One of the indexes `drop.sql` drops.
const DROPPED: &str = "normalized_events_registry_announcement_idx";
const TRANSFER_NODE: &str = "normalized_events_v1_transfer_node_idx";
const TRANSFER_NODE_MIGRATION: &str = include_str!(
    "../../../../migrations/20261102130000_normalized_events_v1_transfer_node_index.sql"
);
const BUILD_WAIT: Duration = Duration::from_secs(30);

/// A chain whose Ingest, Interpret and Project completed under this build's hashes, with
/// readable blocks through 3, so a Project redo of 2..=3 runs.
async fn completed_chain(scratch: &ScratchDatabase, chain_id: &str) -> Result<()> {
    PhaseStore::new(scratch.pool().clone())
        .initialize_chain(chain_id)
        .await?;
    seed_interpret_redo_presence(scratch.pool(), chain_id, 3).await?;
    mark_completed(scratch.pool(), chain_id, PhaseName::Ingest, None).await?;
    mark_completed(
        scratch.pool(),
        chain_id,
        PhaseName::Interpret,
        Some(phase_runner::INTERPRETER_CONTENT_HASH),
    )
    .await?;
    mark_completed(
        scratch.pool(),
        chain_id,
        PhaseName::Project,
        Some(phase_runner::PROJECTION_CONTENT_HASH),
    )
    .await?;
    Ok(())
}

fn loopback_runner(scratch: &ScratchDatabase, instance: &str) -> RunnerResult<PhaseRunner> {
    runner(
        scratch.runner(),
        PhaseSet::loopback(),
        available_capacity(),
        instance,
    )
}

async fn project_redo(
    runner: &PhaseRunner,
    chain_id: &str,
    cancellation: CancellationToken,
) -> RunnerResult<()> {
    runner
        .redo(
            &chain(chain_id)?,
            RedoPhase::Phase(PhaseName::Project),
            BlockRange::new(2, 3)?,
            cancellation,
        )
        .await
}

/// Runs `work` with its log lines captured.
async fn logged<F: std::future::Future>(work: F) -> (F::Output, String) {
    let logs = super::support::CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_writer(logs.clone())
        .finish();
    let output = work.with_subscriber(subscriber).await;
    (output, logs.text())
}

/// Every index on `normalized_events`, by name, with its OID.
async fn index_oids(pool: &sqlx::PgPool) -> Result<BTreeMap<String, i64>> {
    Ok(sqlx::query_as::<_, (String, i64)>(
        "SELECT indexrelid::regclass::text, indexrelid::oid::bigint FROM pg_index
         WHERE indrelid = 'normalized_events'::regclass",
    )
    .fetch_all(pool)
    .await?
    .into_iter()
    .collect())
}

async fn oid(pool: &sqlx::PgPool, name: &str) -> Result<Option<i64>> {
    Ok(sqlx::query_scalar("SELECT to_regclass($1)::oid::bigint")
        .bind(name)
        .fetch_one(pool)
        .await?)
}

/// Whether `name` is a valid, ready index on `normalized_events`.
async fn usable(pool: &sqlx::PgPool, name: &str) -> Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_index WHERE indexrelid = to_regclass($1)
             AND indrelid = 'normalized_events'::regclass AND indisvalid AND indisready)",
    )
    .bind(name)
    .fetch_one(pool)
    .await?)
}

async fn drop_index(pool: &sqlx::PgPool, name: &str) -> Result<()> {
    sqlx::raw_sql(&format!("DROP INDEX {name}"))
        .execute(pool)
        .await?;
    Ok(())
}

async fn project_row(pool: &sqlx::PgPool, chain_id: &str) -> Result<(String, bool)> {
    Ok(sqlx::query_as(
        "SELECT phase_status, redo_in_progress FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(chain_id)
    .fetch_one(pool)
    .await?)
}

/// One stored event, so the table is populated, as `scripts/schema-check` seeds it.
async fn seed_event(pool: &sqlx::PgPool, identity: &str) -> Result<()> {
    sqlx::query(
        "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
             manifest_version, chain_id, derivation_kind)
         VALUES ($1, 'schema-v2-check', 'ResolverRecordLinked', 'ens_v2_resolver_l1', 1,
             'schema-v2-check', 'ens_v2_resolver')",
    )
    .bind(identity)
    .execute(pool)
    .await?;
    Ok(())
}

/// A connection holding the lock a writer holds on `normalized_events`, inside an open
/// transaction. A concurrent index build waits for it before it builds, with its invalid
/// catalog entry already committed.
async fn open_writer(scratch: &ScratchDatabase) -> Result<PgConnection> {
    let mut writer = PgConnection::connect_with(&scratch.writer_connect_options()).await?;
    sqlx::raw_sql("BEGIN; LOCK TABLE normalized_events IN ROW EXCLUSIVE MODE")
        .execute(&mut writer)
        .await?;
    Ok(writer)
}

/// Waits until a concurrent build on `normalized_events` waits for the writers, by which point
/// its invalid catalog entry is committed.
async fn await_build(pool: &sqlx::PgPool) -> Result<()> {
    tokio::time::timeout(BUILD_WAIT, async {
        loop {
            let building: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_progress_create_index
                 WHERE relid = 'normalized_events'::regclass
                   AND phase = 'waiting for writers before build'",
            )
            .fetch_one(pool)
            .await?;
            if building > 0 {
                return anyhow::Ok(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("no index build on normalized_events started")?
}

#[tokio::test]
async fn restore_is_a_no_op_on_a_complete_set() -> Result<()> {
    let scratch = ScratchDatabase::create("walk_index_restore_complete").await?;
    let chain_id = "walk-index-complete";
    completed_chain(&scratch, chain_id).await?;
    let before = index_oids(scratch.pool()).await?;

    project_redo(
        &loopback_runner(&scratch, "walk-index-complete-runner")?,
        chain_id,
        CancellationToken::new(),
    )
    .await?;

    assert_eq!(index_oids(scratch.pool()).await?, before);
    scratch.cleanup().await
}

#[tokio::test]
async fn restore_builds_a_missing_index() -> Result<()> {
    let scratch = ScratchDatabase::create("walk_index_restore_missing").await?;
    let chain_id = "walk-index-missing";
    completed_chain(&scratch, chain_id).await?;
    drop_index(scratch.pool(), DROPPED).await?;
    let others = index_oids(scratch.pool()).await?;

    let (result, logs) = logged(project_redo(
        &loopback_runner(&scratch, "walk-index-missing-runner")?,
        chain_id,
        CancellationToken::new(),
    ))
    .await;
    result?;

    assert!(usable(scratch.pool(), DROPPED).await?, "{logs}");
    assert!(logs.contains(DROPPED), "{logs}");
    let mut after = index_oids(scratch.pool()).await?;
    after.remove(DROPPED);
    assert_eq!(after, others);
    scratch.cleanup().await
}

#[tokio::test]
async fn restore_drops_and_rebuilds_an_invalid_leftover() -> Result<()> {
    for flag in ["indisvalid", "indisready"] {
        let scratch = ScratchDatabase::create("walk_index_restore_invalid").await?;
        let chain_id = "walk-index-invalid";
        completed_chain(&scratch, chain_id).await?;
        sqlx::raw_sql(&format!(
            "UPDATE pg_index SET {flag} = false WHERE indexrelid = '{DROPPED}'::regclass"
        ))
        .execute(scratch.pool())
        .await?;
        let leftover = oid(scratch.pool(), DROPPED).await?;

        project_redo(
            &loopback_runner(&scratch, "walk-index-invalid-runner")?,
            chain_id,
            CancellationToken::new(),
        )
        .await?;

        assert!(usable(scratch.pool(), DROPPED).await?, "{flag}");
        assert_ne!(oid(scratch.pool(), DROPPED).await?, leftover, "{flag}");
        scratch.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn restore_leaves_an_unlisted_index_alone() -> Result<()> {
    let scratch = ScratchDatabase::create("walk_index_restore_unlisted").await?;
    let chain_id = "walk-index-unlisted";
    completed_chain(&scratch, chain_id).await?;
    let unlisted = "walk_index_restore_unlisted_idx";
    sqlx::raw_sql(&format!(
        "CREATE INDEX {unlisted} ON normalized_events (chain_id, block_number)"
    ))
    .execute(scratch.pool())
    .await?;
    drop_index(scratch.pool(), DROPPED).await?;
    let unlisted_oid = oid(scratch.pool(), unlisted).await?;

    let (result, logs) = logged(project_redo(
        &loopback_runner(&scratch, "walk-index-unlisted-runner")?,
        chain_id,
        CancellationToken::new(),
    ))
    .await;
    result?;

    assert!(usable(scratch.pool(), DROPPED).await?);
    assert_eq!(oid(scratch.pool(), unlisted).await?, unlisted_oid);
    assert!(usable(scratch.pool(), unlisted).await?);
    assert!(!logs.contains(unlisted), "{logs}");
    scratch.cleanup().await
}

#[tokio::test]
async fn restore_refuses_a_foreign_relation_under_a_dropped_name() -> Result<()> {
    for (occupant, ddl, named) in [
        ("table", format!("CREATE TABLE {DROPPED} ()"), "is a table"),
        (
            "view",
            format!("CREATE VIEW {DROPPED} AS SELECT 1 AS occupied"),
            "is a view",
        ),
        (
            "other table's index",
            format!("CREATE INDEX {DROPPED} ON chain_lineage (chain_id)"),
            "is an index on bigname_phase.chain_lineage",
        ),
    ] {
        let scratch = ScratchDatabase::create("walk_index_restore_foreign").await?;
        let chain_id = "walk-index-foreign";
        completed_chain(&scratch, chain_id).await?;
        drop_index(scratch.pool(), DROPPED).await?;
        sqlx::raw_sql(&ddl).execute(scratch.pool()).await?;
        let held = oid(scratch.pool(), DROPPED).await?;

        let error = project_redo(
            &loopback_runner(&scratch, "walk-index-foreign-runner")?,
            chain_id,
            CancellationToken::new(),
        )
        .await
        .expect_err("a listed name held by another relation must stop Project");

        let message = error.to_string();
        assert!(message.contains(DROPPED), "{occupant}: {message}");
        assert!(message.contains(named), "{occupant}: {message}");
        assert!(!error.is_retryable(), "{occupant}");
        assert_eq!(oid(scratch.pool(), DROPPED).await?, held, "{occupant}");
        assert_eq!(
            project_row(scratch.pool(), chain_id).await?,
            ("completed".to_owned(), false),
            "{occupant}"
        );
        scratch.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn restore_builds_concurrently_while_a_writer_commits() -> Result<()> {
    let scratch = ScratchDatabase::create("walk_index_restore_concurrent").await?;
    let chain_id = "walk-index-concurrent";
    completed_chain(&scratch, chain_id).await?;
    seed_event(scratch.pool(), "walk-index-concurrent-event").await?;
    drop_index(scratch.pool(), DROPPED).await?;
    let mut holder = open_writer(&scratch).await?;
    let runner = loopback_runner(&scratch, "walk-index-concurrent-runner")?;
    let task =
        tokio::spawn(
            async move { project_redo(&runner, chain_id, CancellationToken::new()).await },
        );
    await_build(scratch.pool()).await?;

    // A plain CREATE INDEX would queue for a SHARE lock behind the holder, and this writer
    // would queue behind it until its lock timeout.
    let mut writer = PgConnection::connect_with(&scratch.writer_connect_options()).await?;
    sqlx::raw_sql(
        "BEGIN; SET LOCAL lock_timeout = '2s';
         DELETE FROM normalized_events WHERE event_identity = 'walk-index-concurrent-event';
         COMMIT",
    )
    .execute(&mut writer)
    .await
    .context("a writer must commit while the build runs")?;
    writer.close().await?;
    sqlx::raw_sql("ROLLBACK").execute(&mut holder).await?;
    holder.close().await?;

    tokio::time::timeout(BUILD_WAIT, task).await???;
    assert!(usable(scratch.pool(), DROPPED).await?);
    scratch.cleanup().await
}

#[tokio::test]
async fn restore_waits_for_another_build_on_the_table() -> Result<()> {
    let scratch = ScratchDatabase::create("walk_index_restore_other_build").await?;
    let chain_id = "walk-index-other-build";
    completed_chain(&scratch, chain_id).await?;
    drop_index(scratch.pool(), DROPPED).await?;
    // The lock a concurrent build, a VACUUM or an ANALYZE holds, taken by another login, whose
    // builds pg_stat_progress_create_index would not show.
    let mut holder = PgConnection::connect_with(&scratch.writer_connect_options()).await?;
    sqlx::raw_sql("BEGIN; LOCK TABLE normalized_events IN SHARE UPDATE EXCLUSIVE MODE")
        .execute(&mut holder)
        .await?;
    let holder_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut holder)
        .await?;
    let heartbeat = RunnerLoopHeartbeat::default();
    let runner = loopback_runner(&scratch, "walk-index-other-build-runner")?
        .with_loop_heartbeat(heartbeat.clone());
    let task = tokio::spawn(logged(async move {
        project_redo(&runner, chain_id, CancellationToken::new()).await
    }));

    tokio::time::sleep(Duration::from_secs(7)).await;
    assert!(!task.is_finished(), "the restore must wait for the holder");
    assert_eq!(oid(scratch.pool(), DROPPED).await?, None);
    let age = heartbeat.age_seconds(chain_id);
    assert!(
        age.is_some_and(|age| age <= 4),
        "the wait must keep recording loop progress, last {age:?} s ago"
    );
    sqlx::raw_sql("ROLLBACK").execute(&mut holder).await?;
    holder.close().await?;

    let (result, logs) = tokio::time::timeout(BUILD_WAIT, task).await??;
    result?;
    assert!(usable(scratch.pool(), DROPPED).await?);
    let waited = logs
        .lines()
        .find(|line| line.contains("waiting for another index build or maintenance"))
        .unwrap_or_else(|| panic!("no wait line: {logs}"));
    assert!(waited.contains(&holder_pid.to_string()), "{waited}");
    scratch.cleanup().await
}

#[tokio::test]
async fn restore_is_idempotent_across_boots() -> Result<()> {
    let scratch = ScratchDatabase::create("walk_index_restore_idempotent").await?;
    let chain_id = "walk-index-idempotent";
    completed_chain(&scratch, chain_id).await?;
    drop_index(scratch.pool(), DROPPED).await?;
    let first = loopback_runner(&scratch, "walk-index-idempotent-runner")?;
    project_redo(&first, chain_id, CancellationToken::new()).await?;
    assert!(usable(scratch.pool(), DROPPED).await?);
    let built = index_oids(scratch.pool()).await?;

    project_redo(&first, chain_id, CancellationToken::new()).await?;
    assert_eq!(index_oids(scratch.pool()).await?, built);
    project_redo(
        &loopback_runner(&scratch, "walk-index-idempotent-second-runner")?,
        chain_id,
        CancellationToken::new(),
    )
    .await?;
    assert_eq!(index_oids(scratch.pool()).await?, built);
    scratch.cleanup().await
}

#[tokio::test]
async fn restore_keeps_a_valid_index_with_another_definition_and_warns() -> Result<()> {
    let scratch = ScratchDatabase::create("walk_index_restore_definition").await?;
    let chain_id = "walk-index-definition";
    completed_chain(&scratch, chain_id).await?;
    drop_index(scratch.pool(), DROPPED).await?;
    sqlx::raw_sql(&format!(
        "CREATE INDEX {DROPPED} ON normalized_events (chain_id)"
    ))
    .execute(scratch.pool())
    .await?;
    let held = oid(scratch.pool(), DROPPED).await?;

    let (result, logs) = logged(project_redo(
        &loopback_runner(&scratch, "walk-index-definition-runner")?,
        chain_id,
        CancellationToken::new(),
    ))
    .await;
    result?;

    assert_eq!(oid(scratch.pool(), DROPPED).await?, held);
    let warning = logs
        .lines()
        .find(|line| line.contains("WARN") && line.contains(DROPPED))
        .unwrap_or_else(|| panic!("no warning names {DROPPED}: {logs}"));
    assert!(warning.contains("USING btree (chain_id)"), "{warning}");
    assert!(warning.contains("RegistryCreated"), "{warning}");
    assert!(warning.contains("docs/storage.md"), "{warning}");

    let (result, logs) = logged(project_redo(
        &loopback_runner(&scratch, "walk-index-definition-second-runner")?,
        chain_id,
        CancellationToken::new(),
    ))
    .await;
    result?;
    assert!(
        !logs
            .lines()
            .any(|line| line.contains("WARN") && line.contains(DROPPED)),
        "one warning per definition per process: {logs}"
    );
    assert_eq!(oid(scratch.pool(), DROPPED).await?, held);
    scratch.cleanup().await
}

/// Records, at each Interpret and Project batch, whether the dropped index is usable.
fn recording_hook(
    runner: PhaseRunner,
    pool: sqlx::PgPool,
    seen: Arc<Mutex<Vec<(PhaseName, bool)>>>,
) -> PhaseRunner {
    runner.with_before_phase_context(move |phase| {
        let pool = pool.clone();
        let seen = Arc::clone(&seen);
        async move {
            if matches!(phase, PhaseName::Interpret | PhaseName::Project) {
                let present = usable(&pool, DROPPED).await.expect("index state");
                seen.lock().expect("seen lock").push((phase, present));
            }
        }
    })
}

/// Interpret's batches before the first Project batch run without the index, and that Project
/// batch and every later batch have it.
fn assert_rebuilt_at_project(seen: &[(PhaseName, bool)]) {
    let project = seen
        .iter()
        .position(|(phase, _)| *phase == PhaseName::Project)
        .unwrap_or_else(|| panic!("no Project batch: {seen:?}"));
    assert!(project > 0, "no Interpret batch before Project: {seen:?}");
    assert!(
        seen[..project]
            .iter()
            .all(|(phase, present)| *phase == PhaseName::Interpret && !present),
        "{seen:?}"
    );
    assert!(
        seen[project..].iter().all(|(_, present)| *present),
        "{seen:?}"
    );
}

#[tokio::test]
async fn restore_runs_before_projects_first_batch_and_not_during_interpret() -> Result<()> {
    let scratch = ScratchDatabase::create("walk_index_restore_supervised").await?;
    let chain_id = "walk-index-supervised";
    seed_identified_lineage(scratch.pool(), chain_id, 3).await?;
    let heads = HeadMarkers {
        latest: BlockMarker::new(3, format!("{chain_id}-block-3"))?,
        safe: Some(BlockMarker::new(2, format!("{chain_id}-block-2"))?),
        finalized: Some(BlockMarker::new(1, format!("{chain_id}-block-1"))?),
    };
    drop_index(scratch.pool(), DROPPED).await?;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let runner = recording_hook(
        runner(
            scratch.runner(),
            complete_phase_set(Some(heads)),
            available_capacity(),
            "walk-index-supervised-runner",
        )?,
        scratch.pool().clone(),
        Arc::clone(&seen),
    );
    let mut chain = chain(chain_id)?;
    chain.verify_before_live = true;

    runner.run_chain(&chain, CancellationToken::new()).await?;

    assert_rebuilt_at_project(&seen.lock().expect("seen lock"));
    assert!(usable(scratch.pool(), DROPPED).await?);
    scratch.cleanup().await
}

#[tokio::test]
async fn interpret_redo_walks_without_the_index_and_project_redo_gets_it_back() -> Result<()> {
    let scratch = ScratchDatabase::create("walk_index_restore_interpret_redo").await?;
    let chain_id = "walk-index-interpret-redo";
    completed_chain(&scratch, chain_id).await?;
    for phase in [PhaseName::Ingest, PhaseName::Interpret, PhaseName::Project] {
        set_phase_extent(scratch.pool(), chain_id, phase, 3).await?;
    }
    drop_index(scratch.pool(), DROPPED).await?;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let runner = recording_hook(
        loopback_runner(&scratch, "walk-index-interpret-redo-runner")?,
        scratch.pool().clone(),
        Arc::clone(&seen),
    );

    runner
        .redo(
            &chain(chain_id)?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(0, 3)?,
            CancellationToken::new(),
        )
        .await?;

    assert_rebuilt_at_project(&seen.lock().expect("seen lock"));
    assert!(usable(scratch.pool(), DROPPED).await?);
    scratch.cleanup().await
}

#[tokio::test]
async fn restore_warns_about_a_chain_still_in_an_interpret_redo() -> Result<()> {
    let scratch = ScratchDatabase::create("walk_index_restore_other_chain").await?;
    let chain_id = "walk-index-served-chain";
    let walking = "walk-index-walking-chain";
    completed_chain(&scratch, chain_id).await?;
    PhaseStore::new(scratch.pool().clone())
        .initialize_chain(walking)
        .await?;
    mark_completed(
        scratch.pool(),
        walking,
        PhaseName::Interpret,
        Some(phase_runner::INTERPRETER_CONTENT_HASH),
    )
    .await?;
    let stamped = sqlx::query(phase_runner::CREATE_STAMP)
        .bind(walking)
        .bind("interpret")
        .bind(0_i64)
        .bind(3_i64)
        .bind(format!(
            "{}walk index test",
            phase_runner::REQUIRED_REDO_PREFIX
        ))
        .execute(scratch.pool())
        .await?;
    assert_eq!(stamped.rows_affected(), 1);
    drop_index(scratch.pool(), DROPPED).await?;

    let (result, logs) = logged(project_redo(
        &loopback_runner(&scratch, "walk-index-other-chain-runner")?,
        chain_id,
        CancellationToken::new(),
    ))
    .await;
    result?;

    assert!(usable(scratch.pool(), DROPPED).await?);
    assert!(
        logs.lines()
            .any(|line| line.contains("WARN") && line.contains(walking)),
        "{logs}"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn restore_warns_about_a_chain_still_on_its_first_interpret_walk() -> Result<()> {
    let scratch = ScratchDatabase::create("walk_index_restore_first_walk").await?;
    let chain_id = "walk-index-first-walk-served";
    let walking = "walk-index-first-walk-walking";
    let done = "walk-index-first-walk-done";
    completed_chain(&scratch, chain_id).await?;
    completed_chain(&scratch, done).await?;
    PhaseStore::new(scratch.pool().clone())
        .initialize_chain(walking)
        .await?;
    sqlx::query(
        "UPDATE chain_phase_state SET phase_status = 'running'
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(walking)
    .execute(scratch.pool())
    .await?;
    drop_index(scratch.pool(), DROPPED).await?;

    let (result, logs) = logged(project_redo(
        &loopback_runner(&scratch, "walk-index-first-walk-runner")?,
        chain_id,
        CancellationToken::new(),
    ))
    .await;
    result?;

    assert!(usable(scratch.pool(), DROPPED).await?);
    let warning = logs
        .lines()
        .find(|line| line.contains("WARN") && line.contains("has not completed"))
        .unwrap_or_else(|| panic!("no warning names the walking chain: {logs}"));
    assert!(
        warning.contains(&format!("{walking} (running)")),
        "{warning}"
    );
    assert!(!warning.contains(done), "{warning}");
    assert!(
        !warning.contains(&format!("{chain_id} (")),
        "the restoring chain is not named: {warning}"
    );
    scratch.cleanup().await
}

const NON_OWNER: &str = "bigname_walk_index_non_owner_test";
const NON_OWNER_PASSWORD: &str = "bigname-walk-index-non-owner-test";

/// A login with every row privilege on the phase schema that owns none of its tables, so it
/// can run the phase runner's writes but cannot build an index on `normalized_events`.
async fn non_owner_database(scratch: &ScratchDatabase) -> Result<RunnerDatabase> {
    let options = scratch.writer_connect_options();
    let database = options
        .get_database()
        .context("scratch database name")?
        .replace('"', "\"\"");
    // Roles are shared by every scratch database: a test that loses the race to create it
    // takes the duplicate error and carries on.
    let mut transaction = scratch.pool().begin().await?;
    sqlx::raw_sql(&format!(
        "DO $role$
         BEGIN
             CREATE ROLE {NON_OWNER} LOGIN PASSWORD '{NON_OWNER_PASSWORD}'
                 NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
         EXCEPTION
             WHEN duplicate_object OR unique_violation THEN NULL;
         END
         $role$;
         GRANT CONNECT ON DATABASE \"{database}\" TO {NON_OWNER};
         GRANT USAGE ON SCHEMA bigname_phase TO {NON_OWNER};
         GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA bigname_phase
             TO {NON_OWNER};
         GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA bigname_phase TO {NON_OWNER};"
    ))
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(RunnerDatabase::connect_with_options(
        options.username(NON_OWNER).password(NON_OWNER_PASSWORD),
        4,
    )
    .await?)
}

#[tokio::test]
async fn a_failed_build_stops_project_with_the_error_line() -> Result<()> {
    let scratch = ScratchDatabase::create("walk_index_restore_failed").await?;
    let chain_id = "walk-index-failed";
    completed_chain(&scratch, chain_id).await?;
    drop_index(scratch.pool(), DROPPED).await?;
    let database = non_owner_database(&scratch).await?;

    let error = project_redo(
        &runner(
            database.clone(),
            PhaseSet::loopback(),
            available_capacity(),
            "walk-index-failed-runner",
        )?,
        chain_id,
        CancellationToken::new(),
    )
    .await
    .expect_err("a build the runner's login may not run must stop Project");
    database.pool().close().await;

    let message = error.to_string();
    assert!(message.contains(DROPPED), "{message}");
    assert!(message.contains("must be owner"), "{message}");
    assert!(message.contains("restart the runner"), "{message}");
    assert!(!error.is_retryable());
    assert_eq!(
        project_row(scratch.pool(), chain_id).await?,
        ("completed".to_owned(), false)
    );
    assert_eq!(oid(scratch.pool(), DROPPED).await?, None);
    scratch.cleanup().await
}

#[tokio::test]
async fn a_failed_build_stops_every_chain_in_the_process_and_a_restart_builds_again() -> Result<()>
{
    let scratch = ScratchDatabase::create("walk_index_restore_failed_twice").await?;
    let (first, second) = ("walk-index-failed-first", "walk-index-failed-second");
    completed_chain(&scratch, first).await?;
    completed_chain(&scratch, second).await?;
    drop_index(scratch.pool(), DROPPED).await?;
    let database = non_owner_database(&scratch).await?;
    let process = runner(
        database.clone(),
        PhaseSet::loopback(),
        available_capacity(),
        "walk-index-failed-twice-runner",
    )?;

    let failed = project_redo(&process, first, CancellationToken::new())
        .await
        .expect_err("the first build fails")
        .to_string();
    assert!(!failed.contains("already failed"), "{failed}");
    let error = project_redo(&process, second, CancellationToken::new())
        .await
        .expect_err("another chain in the process must not build again");
    let message = error.to_string();
    assert!(
        message.contains("already failed in this process"),
        "{message}"
    );
    assert!(message.contains(DROPPED), "{message}");
    assert!(message.contains("must be owner"), "{message}");
    assert!(!error.is_retryable());
    assert_eq!(
        project_row(scratch.pool(), second).await?,
        ("completed".to_owned(), false)
    );
    database.pool().close().await;

    // A restart is a new process, which builds again.
    let restarted = non_owner_database(&scratch).await?;
    let again = project_redo(
        &runner(
            restarted.clone(),
            PhaseSet::loopback(),
            available_capacity(),
            "walk-index-failed-twice-restarted",
        )?,
        second,
        CancellationToken::new(),
    )
    .await
    .expect_err("the cause persists")
    .to_string();
    restarted.pool().close().await;
    assert!(!again.contains("already failed"), "{again}");
    assert!(again.contains("must be owner"), "{again}");
    scratch.cleanup().await
}

#[tokio::test]
async fn a_stop_during_the_restore_cancels_the_build_and_leaves_a_rebuildable_leftover()
-> Result<()> {
    let scratch = ScratchDatabase::create("walk_index_restore_stopped").await?;
    let chain_id = "walk-index-stopped";
    completed_chain(&scratch, chain_id).await?;
    drop_index(scratch.pool(), DROPPED).await?;
    let mut holder = open_writer(&scratch).await?;
    let runner = loopback_runner(&scratch, "walk-index-stopped-runner")?;
    let cancellation = CancellationToken::new();
    let stop = cancellation.clone();
    let task = tokio::spawn(async move { project_redo(&runner, chain_id, stop).await });
    await_build(scratch.pool()).await?;

    cancellation.cancel();
    let stopped = tokio::time::timeout(Duration::from_secs(15), task)
        .await
        .context("the restore must return within the stop budget")??;
    assert!(
        stopped.is_err(),
        "a stopped Project redo reports itself incomplete"
    );
    // The backend reports its error before it clears its progress row.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let building: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_progress_create_index
                 WHERE relid = 'normalized_events'::regclass",
            )
            .fetch_one(scratch.pool())
            .await?;
            if building == 0 {
                return anyhow::Ok(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("the stop must cancel the build on the server")??;
    let leftover = oid(scratch.pool(), DROPPED).await?;
    assert!(
        leftover.is_some(),
        "the cancelled build leaves its invalid entry"
    );
    assert!(!usable(scratch.pool(), DROPPED).await?);
    sqlx::raw_sql("ROLLBACK").execute(&mut holder).await?;
    holder.close().await?;
    let mut others = index_oids(scratch.pool()).await?;
    others.remove(DROPPED);

    // The second runner shares the first one's database, as a chain re-entering Project in the
    // same process does after a Verify failure cancelled its token: the cancel latched nothing.
    project_redo(
        &loopback_runner(&scratch, "walk-index-stopped-second-runner")?,
        chain_id,
        CancellationToken::new(),
    )
    .await?;

    assert!(usable(scratch.pool(), DROPPED).await?);
    assert_ne!(oid(scratch.pool(), DROPPED).await?, leftover);
    let mut after = index_oids(scratch.pool()).await?;
    after.remove(DROPPED);
    assert_eq!(after, others);
    scratch.cleanup().await
}

#[tokio::test]
async fn the_transfer_node_migration_and_the_runner_build_each_others_index_once() -> Result<()> {
    let scratch = ScratchDatabase::create("walk_index_restore_migration").await?;
    let chain_id = "walk-index-migration";
    completed_chain(&scratch, chain_id).await?;
    let runner = loopback_runner(&scratch, "walk-index-migration-runner")?;

    // A populated table without the index: the runner builds it, and the schema-migration
    // adopts that build without building again.
    seed_event(scratch.pool(), "walk-index-migration-event").await?;
    drop_index(scratch.pool(), TRANSFER_NODE).await?;
    project_redo(&runner, chain_id, CancellationToken::new()).await?;
    let built = oid(scratch.pool(), TRANSFER_NODE).await?;
    assert!(usable(scratch.pool(), TRANSFER_NODE).await?);
    sqlx::raw_sql(TRANSFER_NODE_MIGRATION)
        .execute(scratch.pool())
        .await?;
    assert_eq!(oid(scratch.pool(), TRANSFER_NODE).await?, built);

    // An empty table without the index: the schema-migration builds it, and the runner keeps
    // that build.
    sqlx::query("DELETE FROM normalized_events")
        .execute(scratch.pool())
        .await?;
    drop_index(scratch.pool(), TRANSFER_NODE).await?;
    sqlx::raw_sql(TRANSFER_NODE_MIGRATION)
        .execute(scratch.pool())
        .await?;
    let migrated = oid(scratch.pool(), TRANSFER_NODE).await?;
    assert!(migrated.is_some());
    project_redo(&runner, chain_id, CancellationToken::new()).await?;
    assert_eq!(oid(scratch.pool(), TRANSFER_NODE).await?, migrated);
    scratch.cleanup().await
}

#[tokio::test]
async fn module_definitions_are_the_baselines() -> Result<()> {
    let scratch = ScratchDatabase::create("walk_index_restore_baseline").await?;
    let mut connection = scratch.pool().acquire().await?;
    let states = load_drop_list_state(&mut connection)
        .await?
        .context("the baseline has normalized_events")?;
    drop(connection);
    for status in &states {
        assert_eq!(
            status.state,
            IndexState::Present {
                definition: status.index.definition.to_owned()
            },
            "{}",
            status.index.name
        );
    }
    let listed: BTreeSet<String> = KEPT
        .iter()
        .copied()
        .chain(DROP_LIST.iter().map(|index| index.name))
        .map(str::to_owned)
        .collect();
    assert_eq!(listed.len(), KEPT.len() + DROP_LIST.len());
    let baseline: BTreeSet<String> = index_oids(scratch.pool()).await?.into_keys().collect();
    assert_eq!(baseline, listed);
    scratch.cleanup().await
}

#[tokio::test]
async fn restore_applies_the_build_settings() -> Result<()> {
    for settings in [
        IndexBuildSettings::default(),
        IndexBuildSettings {
            maintenance_work_mem: Some("96MB".to_owned()),
            max_parallel_maintenance_workers: Some(0),
        },
    ] {
        let scratch = ScratchDatabase::create("walk_index_restore_settings").await?;
        let chain_id = "walk-index-settings";
        completed_chain(&scratch, chain_id).await?;
        drop_index(scratch.pool(), DROPPED).await?;
        let (server_memory, server_workers): (String, String) = sqlx::query_as(
            "SELECT current_setting('maintenance_work_mem'),
                    current_setting('max_parallel_maintenance_workers')",
        )
        .fetch_one(scratch.pool())
        .await?;
        let memory = settings
            .maintenance_work_mem
            .clone()
            .unwrap_or(server_memory);
        let workers = settings
            .max_parallel_maintenance_workers
            .map_or(server_workers, |workers| workers.to_string());
        let runner = runner(
            scratch.runner().with_index_build_settings(settings.clone()),
            PhaseSet::loopback(),
            available_capacity(),
            "walk-index-settings-runner",
        )?;

        let (result, logs) =
            logged(project_redo(&runner, chain_id, CancellationToken::new())).await;
        result?;

        let started = logs
            .lines()
            .find(|line| line.contains("building the normalized_events indexes"))
            .unwrap_or_else(|| panic!("no build line: {logs}"));
        assert!(
            started.contains(&format!("maintenance_work_mem={memory}")),
            "{settings:?}: {started}"
        );
        assert!(
            started.contains(&format!("max_parallel_maintenance_workers={workers}")),
            "{settings:?}: {started}"
        );
        assert!(usable(scratch.pool(), DROPPED).await?);
        scratch.cleanup().await?;
    }
    Ok(())
}
