//! The walk index set (`ops/walk-index-set`): the `normalized_events` indexes Interpret keeps
//! during a from-zero walk or a full-history Interpret redo. The others are dropped for the
//! length of the walk and rebuilt before Project runs, so Interpret must still have an index
//! for every read and store the same rows without them.
use std::{collections::BTreeSet, num::NonZeroU32, time::Duration};

use bigname_manifests::{load_repository, sync_schema_v2_repository};
use bigname_test_support::TestDatabase;
use sqlx::{PgPool, postgres::PgPoolOptions};

use super::equivalence_tests::{
    self as ensv1, FIRST_BLOCK, History, database, database_with_manifests, stored_events,
};
use super::{basenames_equivalence_tests as basenames, ensv2_equivalence_tests as ensv2};
use crate::{BatchRequest, Engine, Marker, RunMode};

type TestResult<T = ()> = anyhow::Result<T>;

const DROP_SQL: &str = include_str!("../../../../ops/walk-index-set/drop.sql");
const INSTALL_SQL: &str = include_str!("../../../../ops/walk-index-set/install.sql");
const RUNNER: &str = "walk-index-set-runner";

/// The kept indexes, as `docs/storage.md` § Walk index set lists them.
const KEEP: [&str; 16] = [
    "normalized_events_pkey",
    "normalized_events_event_identity_key",
    "normalized_events_interpreter_state_history_idx",
    "normalized_events_resource_history_idx",
    "normalized_events_name_history_idx",
    "normalized_events_chain_block_number_idx",
    "normalized_events_chain_block_number_desc_idx",
    "normalized_events_projection_idx",
    "normalized_events_v1_direct_node_probe_idx",
    "normalized_events_v1_due_probe_idx",
    "normalized_events_basenames_direct_node_probe_idx",
    "normalized_events_basenames_due_probe_idx",
    "normalized_events_v2_direct_node_probe_idx",
    "normalized_events_v2_key_probe_idx",
    "normalized_events_v2_due_probe_idx",
    "normalized_events_v2_lookahead_probe_idx",
];

fn listed(script: &str, statement: &str) -> BTreeSet<String> {
    script
        .lines()
        .filter_map(|line| line.trim().strip_prefix(statement))
        .map(|rest| {
            rest.trim_start_matches("bigname_phase.")
                .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .next()
                .expect("index name")
                .to_owned()
        })
        .collect()
}

fn dropped() -> BTreeSet<String> {
    listed(DROP_SQL, "DROP INDEX CONCURRENTLY IF EXISTS ")
}

#[derive(Clone, Copy, Debug)]
enum Fixture {
    Ensv1,
    Basenames,
    Ensv2,
}

impl Fixture {
    fn chain(self) -> &'static str {
        match self {
            Self::Ensv1 => ensv1::CHAIN,
            Self::Basenames => basenames::CHAIN,
            Self::Ensv2 => ensv2::CHAIN,
        }
    }

    fn network(self) -> &'static str {
        match self {
            Self::Ensv1 | Self::Basenames => "mainnet",
            Self::Ensv2 => "sepolia",
        }
    }

    fn last_block(self) -> i64 {
        let blocks = match self {
            Self::Ensv1 => History::Lifecycle.offsets().len(),
            Self::Basenames => basenames::OFFSETS.len(),
            Self::Ensv2 => ensv2::OFFSETS.len(),
        };
        FIRST_BLOCK + i64::try_from(blocks).expect("block count") - 1
    }

    async fn seeded(self) -> TestResult<TestDatabase> {
        let prefix = "interpret_walk_index_set";
        let database = match self {
            Self::Ensv2 => database_with_manifests(prefix, "sepolia").await?,
            Self::Ensv1 | Self::Basenames => database(prefix).await?,
        };
        match self {
            Self::Ensv1 => ensv1::seed_history(database.pool(), History::Lifecycle).await?,
            Self::Basenames => {
                basenames::seed_history(database.pool(), &basenames::OFFSETS).await?
            }
            Self::Ensv2 => {
                ensv2::seed_history(database.pool(), &ensv2::OFFSETS).await?;
                ensv2::stamp_interpreter_hash(database.pool()).await?;
            }
        }
        Ok(database)
    }
}

/// A from-zero walk, a full-history redo and a flag recompute over the fixture, then the
/// manifest sync a runner restart performs, all on `pool`.
async fn walk_and_redo(fixture: Fixture, pool: &PgPool, force_full_state: bool) -> TestResult {
    let engine = Engine::new(pool.clone())
        .with_blocks_per_batch(NonZeroU32::new(3).expect("positive batch"))
        .with_full_state_loader_forced(force_full_state);
    for mode in [RunMode::Normal, RunMode::Redo, RunMode::RecomputeFlags] {
        let mut current: Option<Marker> = None;
        loop {
            let outcome = engine
                .run_batch(BatchRequest {
                    chain_id: fixture.chain().to_owned(),
                    from_block: FIRST_BLOCK,
                    to_block: fixture.last_block(),
                    resume_current: current,
                    mode,
                })
                .await?;
            if outcome.complete {
                break;
            }
            current = Some(outcome.current);
        }
    }
    let manifests = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../manifests")
        .join(fixture.network());
    sync_schema_v2_repository(pool, &load_repository(manifests)?).await?;
    Ok(())
}

/// A pool on which PostgreSQL scans `normalized_events` sequentially only for a statement no
/// remaining index can serve.
async fn runner_pool(database: &TestDatabase) -> TestResult<PgPool> {
    let options = database
        .pool()
        .connect_options()
        .as_ref()
        .clone()
        .application_name(RUNNER)
        .options([
            ("enable_seqscan", "off"),
            (
                "bigname.interpreter_content_hash",
                bigname_content_hash::INTERPRETER_CONTENT_HASH,
            ),
        ]);
    Ok(PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await?)
}

/// Waits until no backend of this database but the caller's is left. A backend reports its
/// scan counters when it exits.
async fn await_backends_exited(pool: &PgPool) -> TestResult {
    for _ in 0..200 {
        let open: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity
             WHERE datname = current_database() AND pid <> pg_backend_pid()",
        )
        .fetch_one(pool)
        .await?;
        if open == 0 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    anyhow::bail!("test database backends did not exit")
}

/// The drop list and the keep list together are every index the baseline puts on
/// `normalized_events`, and the install script rebuilds exactly the drop list. A new index must
/// be classified before it lands.
#[tokio::test]
async fn walk_index_set_lists_cover_every_baseline_index() -> TestResult {
    let database = database("interpret_walk_index_set_lists").await?;
    let baseline: BTreeSet<String> = sqlx::query_scalar(
        "SELECT indexrelid::regclass::text FROM pg_index
         WHERE indrelid = 'normalized_events'::regclass",
    )
    .fetch_all(database.pool())
    .await?
    .into_iter()
    .collect();
    database.cleanup().await?;
    let keep: BTreeSet<String> = KEEP.iter().map(|name| (*name).to_owned()).collect();
    let dropped = dropped();
    assert!(
        keep.is_disjoint(&dropped),
        "both kept and dropped: {:?}",
        keep.intersection(&dropped).collect::<Vec<_>>()
    );
    assert_eq!(
        &keep | &dropped,
        baseline,
        "unclassified or unknown indexes"
    );
    assert_eq!(
        listed(INSTALL_SQL, "CREATE INDEX CONCURRENTLY IF NOT EXISTS "),
        dropped,
        "install.sql must rebuild exactly what drop.sql drops"
    );
    Ok(())
}

/// With the drop list dropped and sequential scans disabled, a walk, a full-history redo, a
/// flag recompute and a restart's manifest sync never scan `normalized_events` sequentially,
/// over ENSv1, Basenames and ENSv2 histories (the last with an ENSv1→ENSv2 migration), through
/// either loader, and store the rows a run with every index stores.
#[tokio::test]
async fn interpret_runs_on_the_walk_index_set() -> TestResult {
    let dropped = dropped();
    for fixture in [Fixture::Ensv1, Fixture::Basenames, Fixture::Ensv2] {
        for force_full_state in [false, true] {
            let case = format!("{fixture:?} full-state={force_full_state}");

            let database = fixture.seeded().await?;
            walk_and_redo(fixture, database.pool(), force_full_state).await?;
            let expected = stored_events(database.pool(), fixture.chain()).await?;
            database.cleanup().await?;
            assert!(!expected.is_empty(), "{case}");

            let database = fixture.seeded().await?;
            for name in &dropped {
                sqlx::raw_sql(&format!("DROP INDEX {name}"))
                    .execute(database.pool())
                    .await?;
            }
            let reader = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(database.pool().connect_options().as_ref().clone())
                .await?;
            // Seeding's counters reach the statistics only when its backends exit.
            database.pool().close().await;
            await_backends_exited(&reader).await?;
            sqlx::query("SELECT pg_stat_reset()")
                .execute(&reader)
                .await?;
            let runner = runner_pool(&database).await?;
            walk_and_redo(fixture, &runner, force_full_state).await?;
            runner.close().await;
            await_backends_exited(&reader).await?;
            sqlx::query("SELECT pg_stat_clear_snapshot()")
                .execute(&reader)
                .await?;
            let sequential: i64 = sqlx::query_scalar(
                "SELECT seq_scan FROM pg_stat_user_tables
                 WHERE relid = 'normalized_events'::regclass",
            )
            .fetch_one(&reader)
            .await?;
            assert_eq!(
                sequential, 0,
                "{case}: sequential scans of normalized_events"
            );
            assert_eq!(
                stored_events(&reader, fixture.chain()).await?,
                expected,
                "{case}"
            );
            reader.close().await;
            database.cleanup().await?;
        }
    }
    Ok(())
}
