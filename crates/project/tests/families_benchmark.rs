//! Owned key family benchmark, run by hand:
//!
//! ```text
//! FAMILY_BENCHMARK_NAMES=30000 FAMILY_BENCHMARK_MIN_MS=0 FAMILY_BENCHMARK_LOG=/tmp/plans.log \
//!     cargo test -p bigname-project --test families_benchmark -- --ignored --nocapture
//! ```
//!
//! It seeds `rebuild_performance/seed.sql`, rebuilds the families to `FAMILY_BENCHMARK_BASE`
//! (default 200), follows block by block to 300, undoes back to the base and replays. It prints the
//! elapsed time of each phase and the per-block distribution. With `FAMILY_BENCHMARK_MIN_MS`
//! set, it writes what `auto_explain` reports for every statement of the follow, the undo and
//! the replay slower than that to `FAMILY_BENCHMARK_LOG`; the plans reach the client as `LOG`
//! notices. Unset, `auto_explain` stays off and the timings carry no analyze overhead.
use std::{fs::File, sync::Mutex, time::Instant};

use anyhow::{Context, Result, ensure};
use bigname_project::{
    Marker,
    families::{self, FamilyMode, FamilyOptions, FamilyOutcome},
};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::{PgPool, postgres::PgPoolOptions, raw_sql};
use tracing_subscriber::{EnvFilter, fmt};

const CHAIN: &str = "ethereum-sepolia";
const SEED: &str = include_str!("rebuild_performance/seed.sql");
const CONTENT_HASH: &str = "families-benchmark";

async fn marker(pool: &PgPool, number: i64) -> Result<Marker> {
    let hash: String = sqlx::query_scalar(
        "SELECT block_hash FROM chain_lineage WHERE chain_id = $1 AND block_number = $2",
    )
    .bind(CHAIN)
    .bind(number)
    .fetch_one(pool)
    .await?;
    Ok(Marker { number, hash })
}

async fn run(pool: &PgPool, target: i64, mode: FamilyMode) -> Result<FamilyOutcome> {
    let token = families::input_token(pool, CHAIN).await?;
    let outcome = families::apply(
        pool,
        CHAIN,
        &marker(pool, target).await?,
        mode,
        &token,
        &FamilyOptions::new(CONTENT_HASH).with_max_blocks_per_run(u64::MAX),
    )
    .await;
    ensure!(outcome.skipped.is_none(), "{:?}", outcome.skipped);
    Ok(outcome)
}

fn distribution(label: &str, mut samples: Vec<u64>) {
    samples.sort_unstable();
    let at = |q: f64| {
        let index = ((samples.len() - 1) as f64 * q).round() as usize;
        samples[index]
    };
    let total: u64 = samples.iter().sum();
    println!(
        "FAMILY_BENCHMARK {label} blocks={} total_ms={total} median_ms={} p95_ms={} max_ms={}",
        samples.len(),
        at(0.5),
        at(0.95),
        at(1.0)
    );
}

#[tokio::test]
#[ignore = "benchmark; run by hand with FAMILY_BENCHMARK_NAMES set"]
async fn family_block_timings() -> Result<()> {
    let names: i64 = std::env::var("FAMILY_BENCHMARK_NAMES")
        .context("FAMILY_BENCHMARK_NAMES")?
        .parse()?;
    let base: i64 = std::env::var("FAMILY_BENCHMARK_BASE")
        .unwrap_or_else(|_| "200".to_owned())
        .parse()?;
    let min_ms = std::env::var("FAMILY_BENCHMARK_MIN_MS").ok();
    if let Ok(log) = std::env::var("FAMILY_BENCHMARK_LOG") {
        fmt()
            .with_env_filter(EnvFilter::new("off,sqlx::postgres::notice=trace"))
            .with_ansi(false)
            .with_writer(Mutex::new(File::create(&log)?))
            .init();
    }

    let database = TestDatabase::create(TestDatabaseConfig::new("family_benchmark")).await?;
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
    raw_sql(
        &SEED
            .replace("__NAMES__", &names.to_string())
            .replace("__CHAIN__", CHAIN),
    )
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    raw_sql("ANALYZE").execute(&setup).await?;
    raw_sql(&format!(
        "ALTER DATABASE {quoted} SET search_path TO bigname_phase, public"
    ))
    .execute(&setup)
    .await?;
    let connect = || {
        PgPoolOptions::new()
            .max_connections(2)
            .connect_with(setup.connect_options().as_ref().clone())
    };
    let pool: PgPool = connect().await?;
    let events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events WHERE block_number > $1 AND block_number <= 300",
    )
    .bind(base)
    .fetch_one(&pool)
    .await?;
    println!("FAMILY_BENCHMARK names={names} base={base} followed_events={events}");

    let started = Instant::now();
    run(&pool, base, FamilyMode::Rebuild).await?;
    println!(
        "FAMILY_BENCHMARK rebuild_to={base} elapsed_ms={}",
        started.elapsed().as_millis()
    );
    // The seed writes every binding at its opening SurfaceBound's log, as the adapter does. Each
    // binding the rebuild reached (through the base) must have one candidate at that
    // SurfaceBound's identity and position, on the binding's resource, carrying the wrapper
    // lease the SurfaceBound recorded when it is a NameWrapper one.
    let (bindings, without_bound, mismatched, candidates, leases): (i64, i64, i64, i64, i64) =
        sqlx::query_as(
            "WITH expected AS (
                 SELECT binding.surface_binding_id, binding.resource_id, bound.event_identity,
                        bound.block_number, bound.transaction_index, bound.log_index,
                        CASE WHEN bound.source_family = 'ens_v1_wrapper_l1'
                             THEN (bound.after_state ->> 'wrapped_registrar_resource_id')::uuid
                        END AS lease
                 FROM surface_bindings binding
                 LEFT JOIN normalized_events bound
                   ON bound.chain_id = binding.chain_id AND bound.event_kind = 'SurfaceBound'
                  AND bound.logical_name_id = binding.logical_name_id
                  AND bound.resource_id = binding.resource_id
                  AND bound.block_number = binding.block_number
                  AND bound.transaction_index =
                      (binding.provenance ->> 'transaction_index')::bigint
                  AND bound.log_index = (binding.provenance ->> 'log_index')::bigint
                 WHERE binding.chain_id = $1 AND binding.block_number <= $2
             )
             SELECT count(*),
                    count(*) FILTER (WHERE expected.event_identity IS NULL),
                    count(*) FILTER (WHERE candidate.surface_binding_id IS NULL
                        OR (candidate.event_identity, candidate.block_number,
                            candidate.transaction_index, candidate.log_index,
                            candidate.resource_id)
                           IS DISTINCT FROM (expected.event_identity, expected.block_number,
                            expected.transaction_index, expected.log_index,
                            expected.resource_id)
                        OR candidate.wrapped_registrar_resource_id
                           IS DISTINCT FROM expected.lease),
                    (SELECT count(*) FROM project_binding_candidate WHERE chain_id = $1),
                    count(expected.lease)
             FROM expected
             LEFT JOIN project_binding_candidate candidate USING (surface_binding_id)",
        )
        .bind(CHAIN)
        .bind(base)
        .fetch_one(&pool)
        .await?;
    println!(
        "FAMILY_BENCHMARK bindings={bindings} without_surface_bound={without_bound} \
         mismatched_candidates={mismatched} candidates={candidates} wrapper_leases={leases}"
    );
    ensure!(
        bindings > 0,
        "FAMILY_BENCHMARK_BASE {base} is below the first seed binding, so no binding is checked"
    );
    ensure!(
        without_bound == 0 && mismatched == 0 && candidates == bindings,
        "seed bindings do not pair with their SurfaceBound"
    );
    // The plans cover the follow, the undo and the replay; the rebuild ran without them.
    if let Some(min_ms) = &min_ms {
        for setting in [
            "session_preload_libraries TO 'auto_explain'".to_owned(),
            format!("auto_explain.log_min_duration TO {min_ms}"),
            "auto_explain.log_nested_statements TO on".to_owned(),
            "auto_explain.log_analyze TO on".to_owned(),
            "auto_explain.log_buffers TO on".to_owned(),
            "auto_explain.log_timing TO off".to_owned(),
            "client_min_messages TO log".to_owned(),
        ] {
            raw_sql(&format!("ALTER DATABASE {quoted} SET {setting}"))
                .execute(&setup)
                .await?;
        }
        pool.close().await;
    }
    let pool = if min_ms.is_some() {
        connect().await?
    } else {
        pool
    };
    let log = std::env::var("FAMILY_BENCHMARK_LOG").ok();
    let log_offset = log
        .as_ref()
        .and_then(|log| std::fs::metadata(log).ok())
        .map_or(0, |metadata| metadata.len());
    let mut follow = Vec::new();
    for number in base + 1..=300 {
        follow.extend(run(&pool, number, FamilyMode::Normal).await?.block_ms);
    }
    let followed = follow.len();
    distribution("follow", follow);
    // With every statement explained, count the follow's statements that read the manifest
    // updates from normalized_events.
    if let (Some(log), Some("0")) = (&log, min_ms.as_deref()) {
        let text = std::fs::read(log)?;
        let start = usize::try_from(log_offset)?.min(text.len());
        let follow_text = String::from_utf8_lossy(&text[start..]);
        // (whether it is the per-run manifest read, whether it reads the manifest updates); the
        // counts are totals over the whole follow, printed, not asserted.
        let statements: Vec<(bool, bool)> = follow_text
            .split("Query Text:")
            .skip(1)
            .map(|statement| {
                let text: Vec<&str> = statement
                    .lines()
                    .take_while(|line| !line.starts_with("Query Parameters"))
                    .collect();
                (
                    text.iter()
                        .any(|line| line.contains("project:families.manifests.read")),
                    text.iter()
                        .any(|line| line.contains("'SourceManifestUpdated'")),
                )
            })
            .collect();
        let run_reads = statements
            .iter()
            .filter(|(run, reads)| *run && *reads)
            .count();
        let block_reads = statements
            .iter()
            .filter(|(run, reads)| !*run && *reads)
            .count();
        println!(
            "FAMILY_BENCHMARK follow blocks={followed} runs={} manifest_read_statements={run_reads} \
             manifest_reads_in_block_statements={block_reads}",
            300 - base
        );
    }

    let started = Instant::now();
    let undone = families::undo_to(&pool, CHAIN, base).await?;
    let undo_ms = started.elapsed().as_millis();
    println!(
        "FAMILY_BENCHMARK undo blocks={undone} total_ms={undo_ms} per_block_ms={:.1}",
        undo_ms as f64 / undone.max(1) as f64
    );
    let replay = run(&pool, 300, FamilyMode::Normal).await?;
    distribution("replay", replay.block_ms);

    let rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM project_family_undo WHERE chain_id = $1")
            .bind(CHAIN)
            .fetch_one(&pool)
            .await?;
    println!("FAMILY_BENCHMARK undo_rows_retained={rows}");
    pool.close().await;
    database.cleanup().await?;
    Ok(())
}
