use std::{path::Path, process::Stdio, str::FromStr, time::Duration};

use anyhow::{Context, Result, ensure};
use bigname_interpret::{
    BatchRequest as InterpretRequest, Engine as InterpretEngine, RunMode as InterpretMode,
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
    fixture::seed_publication_state(pool).await?;
    let target = bigname_project::Marker {
        number: HEAD,
        hash: fixture::block_hash(HEAD),
    };
    let options = bigname_project::families::FamilyOptions::new(
        bigname_content_hash::INTERPRETER_CONTENT_HASH,
    );
    let mut mode = bigname_project::families::FamilyMode::Rebuild;
    loop {
        let token = bigname_project::families::input_token(pool, CHAIN).await?;
        let outcome =
            bigname_project::families::apply(pool, CHAIN, &target, mode, &token, &options).await?;
        if outcome.marker.as_ref() == Some(&target) {
            break;
        }
        ensure!(
            outcome.budget_exhausted && outcome.blocks > 0,
            "smoke family rebuild made no progress"
        );
        mode = bigname_project::families::FamilyMode::Normal;
    }
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
    async fn published_head_reapply_requires_exact_current_family_publication() -> Result<()> {
        let scratch =
            TestDatabase::create(TestDatabaseConfig::new("benchmark_head_publication")).await?;
        let url = scratch_database_url(scratch.database_name())?;
        initialize_schema_v2(scratch.pool()).await?;
        let writer = smoke_writer_pool(&url).await?;
        fixture::seed(&writer).await?;
        prepare_existing_projection(&writer).await?;
        indexing::publication::require_published_head(&writer, CHAIN, HEAD).await?;

        // Live follow can store a new head before Project has published it. The
        // benchmark requires the exact head, even though serving admits one-block lag.
        sqlx::query("INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number, block_timestamp, canonicality_state) SELECT chain_id, 'next-head', block_hash, block_number + 1, block_timestamp + interval '1 second', canonicality_state FROM chain_lineage WHERE chain_id=$1 AND block_number=$2")
            .bind(CHAIN).bind(HEAD).execute(&writer).await?;
        sqlx::query("UPDATE chain_heads SET latest_block_number=$2, latest_block_hash='next-head' WHERE chain_id=$1")
            .bind(CHAIN).bind(HEAD + 1).execute(&writer).await?;
        let error = indexing::publication::require_published_head(&writer, CHAIN, HEAD + 1)
            .await
            .expect_err("one-block serving tolerance must not admit benchmark replay")
            .to_string();
        assert!(
            error.contains("already be a completed Project publication"),
            "{error}"
        );
        sqlx::query("UPDATE chain_heads SET latest_block_number=$2, latest_block_hash=(SELECT current_block_hash FROM project_family_marker WHERE chain_id=$1) WHERE chain_id=$1")
            .bind(CHAIN).bind(HEAD).execute(&writer).await?;
        indexing::publication::require_published_head(&writer, CHAIN, HEAD).await?;
        sqlx::query("UPDATE project_family_marker SET input_content_hash='keccak256:prior' WHERE chain_id=$1")
            .bind(CHAIN).execute(&writer).await?;
        let error = indexing::publication::require_published_head(&writer, CHAIN, HEAD)
            .await
            .expect_err("a prior interpreter generation must be refused")
            .to_string();
        assert!(
            error.contains("current interpreter content hash"),
            "{error}"
        );
        writer.close().await;
        scratch.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn family_project_replay_rebuild_and_scale_use_current_publications() -> Result<()> {
        let scratch =
            TestDatabase::create(TestDatabaseConfig::new("benchmark_family_project")).await?;
        let url = scratch_database_url(scratch.database_name())?;
        initialize_schema_v2(scratch.pool()).await?;
        let writer = smoke_writer_pool(&url).await?;
        fixture::seed(&writer).await?;
        prepare_existing_projection(&writer).await?;
        let input = IndexingInput {
            chain_id: CHAIN.into(),
            head_block: HEAD,
            walk_from_block: 1,
            walk_to_block: HEAD,
            hydration_rpc_urls: Some(bigname_lookup::ChainRpcUrls::from_entries(&[format!(
                "{CHAIN}=http://127.0.0.1:1"
            )])?),
        };
        let count = indexing::publication::projection_name_count(&writer, CHAIN).await?;
        assert!(count > 0);
        assert_eq!(
            indexing::publication::projection_name_count(&writer, "base-mainnet").await?,
            0
        );
        let mut sequence: i64 =
            sqlx::query_scalar("SELECT sequence FROM project_family_marker WHERE chain_id=$1")
                .bind(CHAIN)
                .fetch_one(&writer)
                .await?;
        for from in [HEAD, HEAD, 0] {
            assert_eq!(indexing::project::replay(&writer, &input, from).await?, 0);
            let next: i64 =
                sqlx::query_scalar("SELECT sequence FROM project_family_marker WHERE chain_id=$1")
                    .bind(CHAIN)
                    .fetch_one(&writer)
                    .await?;
            assert!(
                next > sequence,
                "every invocation must actually publish, including repeated same-head redo"
            );
            sequence = next;
            indexing::publication::require_published_head(&writer, CHAIN, HEAD).await?;
            assert_eq!(
                indexing::publication::projection_name_count(&writer, CHAIN).await?,
                count
            );
        }
        sqlx::query("UPDATE name_surfaces SET canonicality_state='orphaned'")
            .execute(&writer)
            .await?;
        assert_eq!(
            indexing::publication::projection_name_count(&writer, CHAIN).await?,
            0,
            "unreadable identity must not satisfy the scale floor"
        );
        writer.close().await;
        scratch.cleanup().await?;
        Ok(())
    }

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
            "SELECT count(*) FROM project_resolver_classification
             WHERE chain_id = $1 AND resolver_address = lower($2)",
        )
        .bind(CHAIN)
        .bind(fixture::RESOLVER)
        .fetch_one(&writer)
        .await
        .unwrap();
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT logical_name_id FROM name_surfaces ORDER BY logical_name_id",
        )
        .fetch_all(&writer)
        .await
        .unwrap();
        let names =
            bigname_storage::families::name::load_family_names_by_logical_name_ids(&writer, &ids)
                .await
                .unwrap();
        let bound_names = names
            .values()
            .filter(|name| name.declared_summary["resolver"]["address"] == fixture::RESOLVER)
            .count();
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
        let projected_children: u64 =
            bigname_storage::families::topology::count_children_shadow(&writer, &ids)
                .await
                .unwrap()
                .into_iter()
                .map(|(_, count)| count)
                .sum();
        let manifest_events: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM normalized_events
             WHERE event_kind = 'SourceManifestUpdated'",
        )
        .fetch_one(&writer)
        .await
        .unwrap();
        let corpus_resolver_rows = names
            .values()
            .take(8)
            .filter(|name| {
                name.declared_summary["resolver"]["address"]
                    .as_str()
                    .is_some()
            })
            .count();
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
            projected_children >= HEAD as u64,
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
