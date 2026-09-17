//! Full-rebuild benchmark, run by hand:
//!
//! ```text
//! REBUILD_BENCHMARK_NAMES=30000 REBUILD_BENCHMARK_LOG=/tmp/plans.log \
//!     cargo test -p bigname-project --test rebuild_performance -- --ignored --nocapture
//! ```
//!
//! It seeds `rebuild_performance/seed.sql`, projects it from zero, and writes what `auto_explain`
//! reports for every statement slower than `REBUILD_BENCHMARK_MIN_MS` (default 20) to the log
//! file. The plans reach the client as `LOG` notices, so no access to the server log is needed.
//! `auto_explain` does not report utility statements (`CREATE INDEX`, `ANALYZE`, `ALTER TABLE`),
//! so the file also carries sqlx's own line per statement with its elapsed time.
use std::{fs::File, sync::Mutex, time::Instant};

use anyhow::{Context, Result};
use bigname_project::{BatchRequest, Engine, RunMode};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::{PgPool, postgres::PgPoolOptions, raw_sql};
use tracing_subscriber::{EnvFilter, fmt};

const CHAIN: &str = "ethereum-sepolia";
const SEED: &str = include_str!("rebuild_performance/seed.sql");
const PROJECTIONS: &[&str] = &[
    "name_current",
    "children_current",
    "permissions_current",
    "account_permission_state_current",
    "permissions_current_resource_summary",
    "record_inventory_current",
    "resolver_current",
    "address_names_current",
    "address_records_current",
    "primary_names_current",
];

#[tokio::test]
#[ignore = "benchmark; run by hand with REBUILD_BENCHMARK_NAMES set"]
async fn full_rebuild_statement_timings() -> Result<()> {
    let names: i64 = std::env::var("REBUILD_BENCHMARK_NAMES")
        .context("REBUILD_BENCHMARK_NAMES")?
        .parse()?;
    let log = std::env::var("REBUILD_BENCHMARK_LOG").context("REBUILD_BENCHMARK_LOG")?;
    let min_ms = std::env::var("REBUILD_BENCHMARK_MIN_MS").unwrap_or_else(|_| "20".to_owned());
    fmt()
        .with_env_filter(EnvFilter::new(
            "off,sqlx::postgres::notice=trace,sqlx::query=debug",
        ))
        .with_ansi(false)
        .with_writer(Mutex::new(File::create(&log)?))
        .init();

    let database = TestDatabase::create(TestDatabaseConfig::new("rebuild_benchmark")).await?;
    let setup = database.pool().clone();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&setup)
        .await?;
    let quoted = format!(r#""{}""#, database_name.replace('"', r#""""#));
    let mut transaction = setup.begin().await?;
    raw_sql("CREATE SCHEMA bigname_phase; SET LOCAL search_path TO bigname_phase, public")
        .execute(&mut *transaction)
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
        raw_sql(script).execute(&mut *transaction).await?;
    }
    // `REBUILD_BENCHMARK_SEED` swaps in another seed file, to repeat a run recorded earlier.
    let seed = match std::env::var("REBUILD_BENCHMARK_SEED") {
        Ok(file) => std::fs::read_to_string(file)?,
        Err(_) => SEED.to_owned(),
    };
    raw_sql(
        &seed
            .replace("__NAMES__", &names.to_string())
            .replace("__CHAIN__", CHAIN),
    )
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    for setting in [
        "search_path TO bigname_phase, public".to_owned(),
        "session_preload_libraries TO 'auto_explain'".to_owned(),
        format!("auto_explain.log_min_duration TO {min_ms}"),
        "auto_explain.log_nested_statements TO on".to_owned(),
        "auto_explain.log_analyze TO on".to_owned(),
        format!(
            "auto_explain.log_timing TO {}",
            std::env::var("REBUILD_BENCHMARK_NODE_TIMING").unwrap_or_else(|_| "off".to_owned())
        ),
        "client_min_messages TO log".to_owned(),
    ] {
        raw_sql(&format!("ALTER DATABASE {quoted} SET {setting}"))
            .execute(&setup)
            .await?;
    }
    let pool: PgPool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(setup.connect_options().as_ref().clone())
        .await?;
    let events: i64 = sqlx::query_scalar("SELECT count(*) FROM normalized_events")
        .fetch_one(&pool)
        .await?;
    println!("REBUILD_BENCHMARK names={names} events={events}");

    let started = Instant::now();
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: 300,
            affected_from_block: 1,
            affected_to_block: 300,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;
    println!(
        "REBUILD_BENCHMARK project_seconds={:.1}",
        started.elapsed().as_secs_f64()
    );
    for table in PROJECTIONS {
        // The digest lets two runs of the same seed be compared row for row.
        let (rows, digest): (i64, Option<String>) = sqlx::query_as(&format!(
            "SELECT count(*), md5(string_agg(md5((to_jsonb(row) - 'last_recomputed_at'
                 - 'inserted_at')::text), '' ORDER BY md5((to_jsonb(row) - 'last_recomputed_at'
                 - 'inserted_at')::text)))
             FROM {table} row"
        ))
        .fetch_one(&pool)
        .await?;
        println!("REBUILD_BENCHMARK {table} rows={rows} digest={digest:?}");
    }
    pool.close().await;
    database.cleanup().await?;
    Ok(())
}
