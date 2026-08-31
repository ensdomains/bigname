use std::{path::Path, process::Stdio, str::FromStr, time::Duration};

use anyhow::{Context, Result, ensure};
use bigname_interpret::{
    BatchRequest as InterpretRequest, Engine as InterpretEngine, RunMode as InterpretMode,
};
use bigname_project::{
    BatchRequest as ProjectRequest, Engine as ProjectEngine, RunMode as ProjectMode,
};
use bigname_test_support::{TestDatabase, TestDatabaseConfig, database_url_from_env};
use serde::Serialize;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tokio::process::{Child, Command};
use url::Url;

use crate::{
    api_load::{self, ApiReport},
    budgets::GateBudgets,
    database,
    indexing::{self, IndexingInput, IndexingReport},
};

mod fixture;
use fixture::{CHAIN, HEAD};

#[derive(Debug, Serialize)]
pub struct SmokeReport {
    pub indexing: IndexingReport,
    pub api: ApiReport,
    pub green: bool,
}

pub async fn run(api_binary: &Path, budgets: &GateBudgets) -> Result<SmokeReport> {
    ensure!(
        api_binary.is_file(),
        "API binary {} does not exist",
        api_binary.display()
    );
    let scratch = TestDatabase::create(
        TestDatabaseConfig::new("benchmark_gate_smoke").pool_max_connections(20),
    )
    .await?;
    let scratch_url = scratch_database_url(scratch.database_name())?;

    let result = run_in_scratch(&scratch_url, scratch.pool(), api_binary, budgets).await;
    scratch
        .cleanup()
        .await
        .context("failed to clean benchmark smoke database")?;
    result
}

pub fn configured_database_host() -> Result<String> {
    let url = Url::parse(&database_url_from_env()).context("failed to parse test database URL")?;
    url.host_str()
        .map(str::to_owned)
        .context("test database URL has no host")
}

async fn run_in_scratch(
    scratch_url: &str,
    bootstrap_pool: &PgPool,
    api_binary: &Path,
    budgets: &GateBudgets,
) -> Result<SmokeReport> {
    initialize_schema_v2(bootstrap_pool).await?;
    let writer = smoke_writer_pool(scratch_url).await?;
    fixture::seed(&writer).await?;
    prepare_existing_projection(&writer).await?;
    fixture::seed_publication_state(&writer).await?;

    let indexing = indexing::run(
        &writer,
        &IndexingInput {
            chain_id: CHAIN.to_owned(),
            head_block: HEAD,
            walk_from_block: 1,
            walk_to_block: HEAD,
            hydration_rpc_urls: None,
        },
        budgets,
    )
    .await?;
    fixture::normalize_serving_timestamps(&writer).await?;

    let (api_addr, metrics_addr) = reserve_addresses().await?;
    let mut api = spawn_api(api_binary, scratch_url, &api_addr, &metrics_addr)?;
    wait_for_api(&api_addr, &mut api).await?;
    let reader = database::connect_read_only(scratch_url, 8).await?;
    let api_report = api_load::run(&reader, &format!("http://{api_addr}"), None, budgets).await;
    reader.close().await;
    stop_child(&mut api).await;
    writer.close().await;
    let api = api_report?;
    Ok(SmokeReport {
        green: indexing.green && api.green,
        indexing,
        api,
    })
}

async fn prepare_existing_projection(pool: &PgPool) -> Result<()> {
    let interpret = InterpretEngine::with_state_cache_capacity(pool.clone(), 65_536);
    let mut resume_current = None;
    loop {
        let outcome = interpret
            .run_batch(InterpretRequest {
                chain_id: CHAIN.to_owned(),
                from_block: 1,
                to_block: HEAD,
                resume_current,
                mode: InterpretMode::Redo,
            })
            .await
            .context("failed to prepare smoke interpreted rows")?;
        if outcome.complete {
            break;
        }
        resume_current = Some(outcome.current);
    }
    let project = ProjectEngine::new(pool.clone())
        .run_batch(ProjectRequest {
            chain_id: CHAIN.to_owned(),
            target_block: HEAD,
            affected_from_block: 0,
            affected_to_block: HEAD,
            resume_current: None,
            mode: ProjectMode::Normal,
        })
        .await
        .context("failed to prepare smoke projection rows")?;
    ensure!(
        project.complete,
        "smoke projection preparation did not complete"
    );
    Ok(())
}

async fn smoke_writer_pool(database_url: &str) -> Result<PgPool> {
    let options = PgConnectOptions::from_str(database_url)?
        .application_name("bigname-benchmark-gate-smoke")
        .options([("search_path", "bigname_phase")]);
    PgPoolOptions::new()
        .max_connections(12)
        .connect_with(options)
        .await
        .context("failed to connect to smoke database phase schema")
}

async fn initialize_schema_v2(pool: &PgPool) -> Result<()> {
    const BASELINE: &[&str] = &[
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
        include_str!("../../../schema-v2/baseline/11_manifest_authority_attestations.sql"),
        include_str!("../../../schema-v2/baseline/12_project_generation_failures.sql"),
        include_str!("../../../schema-v2/baseline/13_interpret_decode_skips.sql"),
        include_str!("../../../schema-v2/baseline/14_discovery_watch_admissions.sql"),
    ];
    let mut transaction = pool.begin().await?;
    sqlx::query("CREATE SCHEMA bigname_phase")
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SET LOCAL search_path TO bigname_phase, public")
        .execute(&mut *transaction)
        .await?;
    for source in BASELINE {
        sqlx::raw_sql(source).execute(&mut *transaction).await?;
    }
    transaction.commit().await?;
    Ok(())
}

fn scratch_database_url(database_name: &str) -> Result<String> {
    let mut url =
        Url::parse(&database_url_from_env()).context("failed to parse test database URL")?;
    url.set_path(&format!("/{database_name}"));
    Ok(url.into())
}

async fn reserve_addresses() -> Result<(String, String)> {
    let api = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let metrics = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let api_addr = api.local_addr()?.to_string();
    let metrics_addr = metrics.local_addr()?.to_string();
    drop(api);
    drop(metrics);
    Ok((api_addr, metrics_addr))
}

fn spawn_api(
    api_binary: &Path,
    database_url: &str,
    bind_addr: &str,
    metrics_addr: &str,
) -> Result<Child> {
    Command::new(api_binary)
        .arg("serve")
        .arg("--bind-addr")
        .arg(bind_addr)
        .arg("--metrics-bind-addr")
        .arg(metrics_addr)
        .arg("--database-url")
        .arg(database_url)
        .arg("--max-connections")
        .arg("20")
        .env("BIGNAME_API_MAX_IN_FLIGHT", "256")
        .env("BIGNAME_API_HEALTH_MAX_IN_FLIGHT", "16")
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .context("failed to start API for benchmark smoke run")
}

async fn wait_for_api(bind_addr: &str, child: &mut Child) -> Result<()> {
    let client = reqwest::Client::new();
    let health = format!("http://{bind_addr}/healthz");
    for _ in 0..100 {
        if let Some(status) = child.try_wait()? {
            anyhow::bail!("smoke API exited before it was ready: {status}");
        }
        if client
            .get(&health)
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    anyhow::bail!("smoke API did not become healthy at {health}")
}

async fn stop_child(child: &mut Child) {
    let _ = child.start_kill();
    let _ = child.wait().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::budgets::{BudgetProfile, BudgetsFile};

    #[tokio::test]
    async fn fixture_projects_admitted_resolver_and_bound_names() {
        let scratch = TestDatabase::create(TestDatabaseConfig::new("benchmark_resolver_fixture"))
            .await
            .unwrap();
        let scratch_url = scratch_database_url(scratch.database_name()).unwrap();
        initialize_schema_v2(scratch.pool()).await.unwrap();
        let writer = smoke_writer_pool(&scratch_url).await.unwrap();
        fixture::seed(&writer).await.unwrap();
        prepare_existing_projection(&writer).await.unwrap();
        fixture::seed_publication_state(&writer).await.unwrap();
        let budgets = BudgetsFile::load(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../benchmarks/release-gate.toml"),
        )
        .unwrap();
        indexing::run(
            &writer,
            &IndexingInput {
                chain_id: CHAIN.to_owned(),
                head_block: HEAD,
                walk_from_block: 1,
                walk_to_block: HEAD,
                hydration_rpc_urls: None,
            },
            budgets.profile(BudgetProfile::Smoke),
        )
        .await
        .unwrap();

        let resolver_rows: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM resolver_current
             WHERE chain_id = $1 AND resolver_address = lower($2)",
        )
        .bind(CHAIN)
        .bind(fixture::RESOLVER)
        .fetch_one(&writer)
        .await
        .unwrap();
        let bound_names: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM name_current
             WHERE lower(declared_summary #>> '{resolver,address}') = lower($1)",
        )
        .bind(fixture::RESOLVER)
        .fetch_one(&writer)
        .await
        .unwrap();
        let resolver_bindings: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM normalized_events event
             JOIN surface_bindings binding
               ON binding.logical_name_id = event.logical_name_id
              AND binding.resource_id = event.resource_id
             WHERE event.event_kind = 'ResolverChanged'",
        )
        .fetch_one(&writer)
        .await
        .unwrap();
        let projected_children: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM children_current child
             JOIN name_current parent
               ON parent.logical_name_id = child.parent_logical_name_id
             WHERE parent.namespace = 'ens' AND parent.raw_name <> ''",
        )
        .fetch_one(&writer)
        .await
        .unwrap();
        let manifest_events: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM normalized_events
             WHERE event_kind = 'SourceManifestUpdated'",
        )
        .fetch_one(&writer)
        .await
        .unwrap();
        let corpus_resolver_rows: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM (
                 SELECT declared_summary FROM name_current
                 ORDER BY logical_name_id LIMIT 8
             ) corpus
             WHERE corpus.declared_summary #>> '{resolver,address}' IS NOT NULL",
        )
        .fetch_one(&writer)
        .await
        .unwrap();
        assert_eq!(manifest_events, 3, "all fixture sources must be admitted");
        assert!(
            resolver_bindings >= HEAD,
            "Interpret must bind every resolver event"
        );
        assert_eq!(
            resolver_rows, 1,
            "Project must publish the admitted resolver"
        );
        assert!(bound_names > 1, "Project must publish pageable bound names");
        assert!(
            projected_children >= HEAD,
            "Project must publish every admitted registry child"
        );
        assert_eq!(
            corpus_resolver_rows, 0,
            "exact-name smoke corpus must retain its registration-only coverage"
        );

        writer.close().await;
        scratch.cleanup().await.unwrap();
    }
}
