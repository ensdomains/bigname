use std::sync::{Arc, Mutex};

use anyhow::Result;
use bigname_project::{BatchRequest, Engine, Marker, PROJECT_STEPS, RunMode, StepObserver};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::{PgPool, raw_sql};

const CHAIN: &str = "ethereum-sepolia";
const HASH: &str = "0x0000000000000000000000000000000000000000000000000000000000000035";
const NEXT_HASH: &str = "0x0000000000000000000000000000000000000000000000000000000000000036";

#[derive(Default)]
struct Recorded(Mutex<Vec<Option<&'static str>>>);

impl StepObserver for Recorded {
    fn project_step(&self, chain_id: &str, step: Option<&'static str>) {
        assert_eq!(chain_id, CHAIN);
        self.0.lock().expect("recorded steps").push(step);
    }
}

impl Recorded {
    fn take(&self) -> Vec<Option<&'static str>> {
        std::mem::take(&mut *self.0.lock().expect("recorded steps"))
    }
}

#[tokio::test]
async fn long_runs_report_every_step_in_order_and_normal_batches_report_none() -> Result<()> {
    let (database, pool) = database("project_steps").await?;
    let recorded = Arc::new(Recorded::default());
    let engine = Engine::new(pool.clone()).with_step_observer(recorded.clone());
    let every_step_then_idle: Vec<_> = PROJECT_STEPS
        .iter()
        .copied()
        .map(Some)
        .chain([None])
        .collect();

    engine.run_batch(request(RunMode::Normal, None, 10)).await?;
    assert_eq!(recorded.take(), every_step_then_idle, "full rebuild");

    engine
        .run_batch(request(RunMode::Normal, Some(marker()), 10))
        .await?;
    assert_eq!(recorded.take(), Vec::new(), "normal incremental batch");

    engine
        .run_batch(request(RunMode::Redo, Some(marker()), 10))
        .await?;
    assert_eq!(recorded.take(), every_step_then_idle, "redo");

    engine
        .run_batch(request(RunMode::Normal, Some(marker()), 11))
        .await?;
    assert_eq!(recorded.take(), Vec::new(), "advancing normal batch");

    drop(pool);
    database.cleanup().await
}

#[tokio::test]
async fn a_long_run_that_fails_in_derive_reports_idle_once() -> Result<()> {
    let (database, pool) = database("project_steps_fail").await?;
    // The children builder reads label preimages, so derivation fails partway through
    // the builders, well after `prepare`.
    raw_sql("DROP TABLE label_preimages CASCADE")
        .execute(&pool)
        .await?;
    let recorded = Arc::new(Recorded::default());
    let engine = Engine::new(pool.clone()).with_step_observer(recorded.clone());

    let result = engine.run_batch(request(RunMode::Normal, None, 10)).await;
    assert!(result.is_err(), "derive must fail without label preimages");
    let steps = recorded.take();
    let (last, reached) = steps.split_last().expect("the run reported steps");
    assert_eq!(*last, None, "the run ends idle");
    assert!(
        reached.iter().all(Option::is_some),
        "exactly one trailing idle: {steps:?}"
    );
    let reached: Vec<_> = reached.iter().flatten().copied().collect();
    assert_eq!(
        reached.last(),
        Some(&"children"),
        "the failure lands in the children builder"
    );
    assert_eq!(
        reached,
        PROJECT_STEPS[..reached.len()],
        "a prefix, in order"
    );

    drop(pool);
    database.cleanup().await
}

fn marker() -> Marker {
    Marker {
        number: 10,
        hash: HASH.to_owned(),
    }
}

fn request(mode: RunMode, resume_current: Option<Marker>, target_block: i64) -> BatchRequest {
    BatchRequest {
        chain_id: CHAIN.into(),
        target_block,
        affected_from_block: target_block,
        affected_to_block: target_block,
        resume_current,
        mode,
    }
}

async fn database(prefix: &str) -> Result<(TestDatabase, PgPool)> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new(prefix).pool_max_connections(1)).await?;
    let pool = database.pool().clone();
    let mut tx = pool.begin().await?;
    raw_sql("CREATE SCHEMA bigname_phase; SET LOCAL search_path TO bigname_phase, public")
        .execute(&mut *tx)
        .await?;
    for script in [
        include_str!("../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../schema-v2/baseline/05_normalized_events.sql"),
        include_str!("../../../schema-v2/baseline/06_projections.sql"),
        include_str!("../../../schema-v2/baseline/07_labels.sql"),
        include_str!("../../../schema-v2/baseline/08_heartbeats.sql"),
        include_str!("../../../schema-v2/baseline/09_divergence.sql"),
        include_str!("../../../schema-v2/baseline/10_phase_state.sql"),
    ] {
        raw_sql(script).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    pool.set_connect_options(
        pool.connect_options()
            .as_ref()
            .clone()
            .options([("search_path", "bigname_phase,public")]),
    );
    let mut connection = pool.acquire().await?;
    sqlx::query("SET search_path TO bigname_phase, public")
        .execute(&mut *connection)
        .await?;
    drop(connection);
    sqlx::query(
        "INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         VALUES ($1, $2, 10, '2026-09-25T00:00:00Z', 'canonical'),
                ($1, $3, 11, '2026-09-25T00:00:12Z', 'canonical')",
    )
    .bind(CHAIN)
    .bind(HASH)
    .bind(NEXT_HASH)
    .execute(&pool)
    .await?;
    Ok((database, pool))
}
