//! Opt-in retained Sepolia benchmark. Runs the production derivation/publication path inside
//! one repeatable-read transaction and unconditionally rolls it back. No hydration or phase
//! cursor/hash updates are invoked. The caller must arrange a paused, stable dataset.
use std::{str::FromStr, time::Instant};

use anyhow::{Context, Result, ensure};
use sqlx::{Postgres, Transaction, postgres::PgConnectOptions, postgres::PgPoolOptions};

use super::{BatchRequest, RunMode};

#[cfg(test)]
#[path = "engine_reference.rs"]
mod reference_output;

#[cfg(test)]
#[path = "engine_benchmark_safety.rs"]
mod safety;
#[cfg(test)]
#[path = "engine_same_head.rs"]
mod same_head;

const TABLES: &[&str] = &[
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
#[ignore = "requires explicitly configured, paused retained Sepolia database"]
async fn retained_sepolia_passes_rollback() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("bigname_project=debug")
        .with_target(false)
        .try_init();
    let options = PgConnectOptions::from_str(&std::env::var("BIGNAME_BENCHMARK_DATABASE_URL")?)?
        .application_name("bigname-project-rollback-benchmark")
        .options([("search_path", "bigname_phase,public")]);
    let previous: i64 = std::env::var("BIGNAME_BENCHMARK_PREVIOUS")?.parse()?;
    let targets: Vec<i64> = std::env::var("BIGNAME_BENCHMARK_TARGETS")?
        .split(',')
        .map(str::parse)
        .collect::<std::result::Result<_, _>>()?;
    ensure!(
        std::env::var("BIGNAME_BENCHMARK_COMPARE_FULL").as_deref() != Ok("1"),
        "raw incremental/full equality is invalid for documented untouched target provenance; use COMPARE_REFERENCE=1"
    );
    let compare = std::env::var("BIGNAME_BENCHMARK_COMPARE_REFERENCE").as_deref() == Ok("1");
    let contract = std::env::var("BIGNAME_BENCHMARK_COMPARE_CONTRACT").as_deref() == Ok("1");
    ensure!(
        !(compare && contract),
        "choose exact reference or separately named contract comparison"
    );
    let profile = std::env::var("BIGNAME_BENCHMARK_PROFILE").as_deref() == Ok("1");
    let evidence_dir = if compare || contract || profile {
        Some(std::path::PathBuf::from(std::env::var(
            "BIGNAME_BENCHMARK_EVIDENCE_DIR",
        )?))
    } else {
        None
    };
    run_benchmark(
        options,
        Benchmark {
            previous,
            targets,
            compare,
            evidence_dir,
            profile,
            rebuild_baseline: std::env::var("BIGNAME_BENCHMARK_REBUILD_BASELINE").as_deref()
                == Ok("1"),
            contract,
        },
    )
    .await
}

/// What one benchmark run does, read from the `BIGNAME_BENCHMARK_*` environment.
struct Benchmark {
    previous: i64,
    targets: Vec<i64>,
    compare: bool,
    evidence_dir: Option<std::path::PathBuf>,
    profile: bool,
    rebuild_baseline: bool,
    contract: bool,
}

async fn run_benchmark(options: PgConnectOptions, benchmark: Benchmark) -> Result<()> {
    let Benchmark {
        previous,
        targets,
        compare,
        evidence_dir,
        profile,
        rebuild_baseline,
        contract,
    } = benchmark;
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options)
        .await?;
    let chain = "ethereum-sepolia";
    ensure!(!targets.is_empty(), "at least one target is required");
    let interpreted: (Option<i64>, bool) = sqlx::query_as(
        "SELECT current_block_number, redo_in_progress FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .fetch_one(&pool)
    .await?;
    ensure!(
        !interpreted.1 && interpreted.0.is_some(),
        "Interpret must have a completed retained frontier without redo"
    );
    let mut last = previous;
    for number in &targets {
        ensure!(
            *number > last && Some(*number) <= interpreted.0,
            "targets must increase and remain within the retained Interpret frontier"
        );
        super::load_marker(&pool, chain, *number).await?;
        last = *number;
    }
    let profile_session = if profile {
        Some(crate::profile::Session::create(
            evidence_dir
                .as_deref()
                .context("profile evidence directory")?,
        )?)
    } else {
        None
    };
    if profile {
        eprintln!(
            "SEPOLIA_ENGINE_PROFILE mode=PROFILED acceptance=forbidden plans=private candidate_only=true"
        );
    }
    let mut resume = super::load_marker(&pool, chain, previous).await?;
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx)
        .await?;
    sqlx::query("SET LOCAL bigname.benchmark_reference = 'off'")
        .execute(&mut *tx)
        .await?;
    sqlx::query("SET LOCAL statement_timeout = '20min'")
        .execute(&mut *tx)
        .await?;
    safety::projection_schema(&mut tx).await?;
    if rebuild_baseline {
        let started = Instant::now();
        let baseline = BatchRequest {
            chain_id: chain.into(),
            target_block: previous,
            affected_from_block: 0,
            affected_to_block: previous,
            resume_current: None,
            mode: RunMode::Normal,
        };
        super::validate_request(&baseline)?;
        super::revalidate_target(&mut tx, chain, &resume).await?;
        super::derive(&mut tx, &baseline, &resume).await?;
        eprintln!(
            "SEPOLIA_ENGINE_BASELINE target={} elapsed_ms={} mode=full_rebuild transaction=rollback",
            previous,
            started.elapsed().as_millis()
        );
        drop_work_tables(&mut tx).await?;
    } else {
        let published: (Option<i64>, Option<String>, bool) = sqlx::query_as(
            "SELECT current_block_number, current_block_hash, redo_in_progress
             FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'project'",
        )
        .bind(chain)
        .fetch_one(&mut *tx)
        .await?;
        ensure!(
            published == (Some(previous), Some(resume.hash.clone()), false),
            "persistent projections are not at the requested previous marker; rebuild a historical baseline first"
        );
    }
    let mut targets = targets.into_iter().peekable();
    while let Some(number) = targets.next() {
        ensure!(number > resume.number, "targets must increase");
        let started = Instant::now();
        let target = super::load_marker(&pool, chain, number).await?;
        let request = BatchRequest {
            chain_id: chain.into(),
            target_block: number,
            affected_from_block: resume.number + 1,
            affected_to_block: number,
            resume_current: Some(resume.clone()),
            mode: RunMode::Normal,
        };
        super::validate_request(&request)?;
        super::validate_resume(&pool, &request, &target).await?;
        super::revalidate_target(&mut tx, chain, &target).await?;
        let guards_elapsed = started.elapsed();
        let mut contract_baseline = if contract {
            Some(
                super::contract_compare::Snapshot::capture(
                    &mut tx,
                    evidence_dir
                        .as_deref()
                        .context("contract evidence directory")?,
                )
                .await?,
            )
        } else {
            None
        };
        if compare || contract {
            sqlx::query("SAVEPOINT benchmark_comparison")
                .execute(&mut *tx)
                .await?;
        }
        let candidate_started = Instant::now();
        let rows = if let Some(session) = &profile_session {
            session
                .scope(super::derive(&mut tx, &request, &target))
                .await?
        } else {
            super::derive(&mut tx, &request, &target).await?
        };
        let elapsed = if compare || contract {
            guards_elapsed + candidate_started.elapsed()
        } else {
            started.elapsed()
        };
        eprintln!(
            "SEPOLIA_ENGINE_BENCHMARK from={} to={} blocks={} rows={} elapsed_ms={} commit=excluded transaction=rollback measurement={} acceptance={}",
            request.affected_from_block,
            number,
            number - resume.number,
            rows,
            elapsed.as_millis(),
            if profile { "PROFILED" } else { "unprofiled" },
            if profile {
                "forbidden"
            } else {
                "requires_commit_and_phase_overhead"
            }
        );
        let counts: (i64, i64, i64) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM project_scope_names),
                    (SELECT count(*) FROM project_scope_resources),
                    (SELECT count(*) FROM project_events)",
        )
        .fetch_one(&mut *tx)
        .await?;
        eprintln!(
            "SEPOLIA_ENGINE_CARDINALITY names={} resources={} events={}",
            counts.0, counts.1, counts.2
        );
        if contract {
            let root = evidence_dir
                .as_deref()
                .context("contract evidence directory")?;
            let mut candidate = super::contract_compare::Snapshot::capture(&mut tx, root).await?;
            sqlx::query("ROLLBACK TO SAVEPOINT benchmark_comparison")
                .execute(&mut *tx)
                .await?;
            let mandatory =
                super::contract_compare::audit::capture(&mut tx, &request, &target).await?;
            sqlx::query("SET LOCAL bigname.benchmark_reference='on'")
                .execute(&mut *tx)
                .await?;
            super::derive(&mut tx, &request, &target).await?;
            let legacy_scope = super::contract_compare::Scopes::capture(&mut tx).await?;
            let mut reference = super::contract_compare::Snapshot::capture(&mut tx, root).await?;
            let target_metadata = super::contract_compare::Target::load(&mut tx, &target).await?;
            super::contract_compare::compare(
                &mut tx,
                super::contract_compare::Snapshots {
                    baseline: contract_baseline.as_mut().unwrap(),
                    candidate: &mut candidate,
                    reference: &mut reference,
                },
                super::contract_compare::Expectations {
                    mandatory: &mandatory,
                    old_scope: &legacy_scope,
                    target: &target_metadata,
                    previous: resume.number,
                },
            )
            .await?;
            sqlx::query("ROLLBACK TO SAVEPOINT benchmark_comparison")
                .execute(&mut *tx)
                .await?;
            sqlx::query("RELEASE SAVEPOINT benchmark_comparison")
                .execute(&mut *tx)
                .await?;
            ensure!(
                !crate::reference::enabled(&mut tx).await?
                    && !super::contract_compare::audit::enabled(&mut tx).await?,
                "comparison mode leaked"
            );
            if targets.peek().is_some() {
                super::derive(&mut tx, &request, &target).await?;
            }
        }
        if compare {
            let expected = reference_output::Snapshot::capture(
                &mut tx,
                TABLES,
                evidence_dir
                    .as_deref()
                    .context("reference evidence directory")?,
            )
            .await?;
            // Candidate output is kept outside the transaction. Both derivations start
            // from the identical complete baseline, including redo-consumption state.
            sqlx::query("ROLLBACK TO SAVEPOINT benchmark_comparison")
                .execute(&mut *tx)
                .await?;
            sqlx::query("SET LOCAL bigname.benchmark_reference = 'on'")
                .execute(&mut *tx)
                .await?;
            let reference_started = Instant::now();
            super::derive(&mut tx, &request, &target).await?;
            eprintln!(
                "SEPOLIA_ENGINE_REFERENCE from={} to={} elapsed_ms={} mirror_strategy=deployed_2abf622",
                request.affected_from_block,
                number,
                reference_started.elapsed().as_millis()
            );
            expected.assert_equal(&mut tx, TABLES).await?;
            expected.cleanup()?;
            eprintln!(
                "SEPOLIA_ENGINE_REFERENCE_COMPARISON target={number} tables={} result=exact_equal order=candidate_first",
                TABLES.len()
            );
            sqlx::query("ROLLBACK TO SAVEPOINT benchmark_comparison")
                .execute(&mut *tx)
                .await?;
            sqlx::query("RELEASE SAVEPOINT benchmark_comparison")
                .execute(&mut *tx)
                .await?;
            // Do not advance from reference rows even though meaningful outputs match.
            // Recreate candidate state only when the next window needs it, preserving
            // operational columns and all transactional side effects as well.
            if targets.peek().is_some() {
                ensure!(
                    !crate::reference::enabled(&mut tx).await?,
                    "reference mode leaked across rollback"
                );
                let advance_started = Instant::now();
                super::derive(&mut tx, &request, &target).await?;
                eprintln!(
                    "SEPOLIA_ENGINE_CANDIDATE_ADVANCE target={number} elapsed_ms={} timing=excluded",
                    advance_started.elapsed().as_millis()
                );
            }
        }
        drop_work_tables(&mut tx).await?;
        resume = target;
    }
    tx.rollback()
        .await
        .context("explicit benchmark rollback failed")?;
    eprintln!(
        "SEPOLIA_ENGINE_BENCHMARK_ROLLBACK complete=true hydration=not_invoked phase_metadata=not_invoked"
    );
    pool.close().await;
    Ok(())
}

async fn drop_work_tables(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT relname FROM pg_class WHERE relnamespace = pg_my_temp_schema()
         AND relkind = 'r' AND starts_with(relname, 'project_')",
    )
    .fetch_all(&mut **tx)
    .await?;
    for table in tables {
        sqlx::query(&format!(
            "DROP TABLE IF EXISTS \"{}\" CASCADE",
            table.replace('"', "\"\"")
        ))
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

async fn lifecycle_database() -> Result<bigname_test_support::TestDatabase> {
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};
    let database = TestDatabase::create(TestDatabaseConfig::new("benchmark_lifecycle")).await?;
    let mut setup = database.pool().begin().await?;
    sqlx::raw_sql("CREATE SCHEMA bigname_phase; SET LOCAL search_path TO bigname_phase,public")
        .execute(&mut *setup)
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
        sqlx::raw_sql(script).execute(&mut *setup).await?;
    }
    sqlx::raw_sql("INSERT INTO chain_lineage(chain_id,block_number,block_hash,block_timestamp,canonicality_state)
        SELECT 'ethereum-sepolia',i,'block-'||i,to_timestamp(1800000000+i),'canonical' FROM generate_series(10,12)i;
        INSERT INTO chain_phase_state(chain_id,phase_name,current_block_number,current_block_hash)
        VALUES('ethereum-sepolia','interpret',12,'block-12')")
        .execute(&mut *setup).await?;
    setup.commit().await?;
    Ok(database)
}

#[tokio::test]
async fn historical_baseline_reference_and_two_candidates_clean_work_tables() -> Result<()> {
    let database = lifecycle_database().await?;
    let evidence = std::env::temp_dir().join(format!("bigname-lifecycle-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&evidence)?;
    let options = database
        .pool()
        .connect_options()
        .as_ref()
        .clone()
        .options([("search_path", "bigname_phase,public")]);
    // This empty-history fixture exercises the actual guarded benchmark lifecycle,
    // including baseline cleanup, savepoint rollback and the next candidate head.
    // Output semantics use the separate nonempty differential/integration fixtures.
    for profile in [false, true] {
        run_benchmark(
            options.clone(),
            Benchmark {
                previous: 10,
                targets: vec![11, 12],
                compare: true,
                evidence_dir: Some(evidence.clone()),
                profile,
                rebuild_baseline: true,
                contract: false,
            },
        )
        .await?;
        let entries = std::fs::read_dir(&evidence)?.collect::<std::result::Result<Vec<_>, _>>()?;
        if profile {
            ensure!(
                entries.len() == 1,
                "expected one private plan directory only"
            );
            let plans = std::fs::read_dir(entries[0].path())?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            // Plans come only from the explicitly profiled candidate futures.
            ensure!(!plans.is_empty(), "profiled lifecycle produced no plans");
            for plan in plans {
                let value: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(plan.path())?)?;
                ensure!(value[0]["Plan"].is_object(), "missing actual query plan");
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    ensure!(
                        plan.metadata()?.permissions().mode() & 0o777 == 0o600,
                        "plan is not private"
                    );
                }
            }
            std::fs::remove_dir_all(entries[0].path())?;
        } else {
            ensure!(
                entries.is_empty(),
                "private reference evidence was not cleaned"
            );
        }
    }
    std::fs::remove_dir(evidence)?;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn contract_baseline_audit_reference_and_two_candidates_rollback() -> Result<()> {
    let database = lifecycle_database().await?;
    let evidence =
        std::env::temp_dir().join(format!("contract-lifecycle-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&evidence)?;
    let options = database
        .pool()
        .connect_options()
        .as_ref()
        .clone()
        .options([("search_path", "bigname_phase,public")]);
    run_benchmark(
        options,
        Benchmark {
            previous: 10,
            targets: vec![11, 12],
            compare: false,
            evidence_dir: Some(evidence.clone()),
            profile: false,
            rebuild_baseline: true,
            contract: true,
        },
    )
    .await?;
    let phase:Option<i64>=sqlx::query_scalar("SELECT current_block_number FROM bigname_phase.chain_phase_state WHERE chain_id='ethereum-sepolia' AND phase_name='interpret'").fetch_one(database.pool()).await?;
    ensure!(phase == Some(12), "phase metadata mutated");
    std::fs::remove_dir_all(evidence)?;
    database.cleanup().await?;
    Ok(())
}
