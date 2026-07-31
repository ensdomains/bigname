use std::str::FromStr;

use anyhow::{Context, Result};
use bigname_test_support::{TestDatabase, TestDatabaseConfig, database_url_from_env};
use phase_runner::database::RunnerDatabase;
use sqlx::{Connection, PgConnection, postgres::PgConnectOptions};

pub struct ScratchDatabase {
    database: TestDatabase,
    runner: RunnerDatabase,
}

impl ScratchDatabase {
    pub async fn create(prefix: &str) -> Result<Self> {
        let database = TestDatabase::create(
            TestDatabaseConfig::new(prefix)
                .pool_max_connections(10)
                .parse_context("failed to parse database URL for phase-runner tests")
                .admin_connect_context("failed to connect phase-runner test admin pool")
                .pool_connect_context("failed to connect phase-runner test pool"),
        )
        .await?;
        apply_schema(database.pool()).await?;
        let options = PgConnectOptions::from_str(&database_url_from_env())?
            .database(database.database_name());
        let runner = RunnerDatabase::connect_with_options(options, 10).await?;
        Ok(Self { database, runner })
    }

    pub fn runner(&self) -> RunnerDatabase {
        self.runner.clone()
    }

    pub fn pool(&self) -> &sqlx::PgPool {
        self.database.pool()
    }

    pub async fn cleanup(self) -> Result<()> {
        self.runner.pool().close().await;
        self.database.cleanup().await
    }
}

async fn apply_schema(pool: &sqlx::PgPool) -> Result<()> {
    for (name, sql) in [
        (
            "chain",
            include_str!("../../../../schema-v2/baseline/01_chain.sql"),
        ),
        (
            "raw facts",
            include_str!("../../../../schema-v2/baseline/02_raw_facts.sql"),
        ),
        (
            "identity",
            include_str!("../../../../schema-v2/baseline/03_identity.sql"),
        ),
        (
            "manifests",
            include_str!("../../../../schema-v2/baseline/04_manifests.sql"),
        ),
        (
            "normalized events",
            include_str!("../../../../schema-v2/baseline/05_normalized_events.sql"),
        ),
        (
            "projections",
            include_str!("../../../../schema-v2/baseline/06_projections.sql"),
        ),
        (
            "labels",
            include_str!("../../../../schema-v2/baseline/07_labels.sql"),
        ),
        (
            "heartbeats",
            include_str!("../../../../schema-v2/baseline/08_heartbeats.sql"),
        ),
        (
            "resolution differences",
            include_str!("../../../../schema-v2/baseline/09_divergence.sql"),
        ),
        (
            "phase state",
            include_str!("../../../../schema-v2/baseline/10_phase_state.sql"),
        ),
    ] {
        sqlx::raw_sql(sql)
            .execute(pool)
            .await
            .with_context(|| format!("failed to apply schema-v2 {name} baseline"))?;
    }
    Ok(())
}

pub async fn assert_connection_hash_stamp(database: &RunnerDatabase) -> Result<()> {
    let stamp: String = sqlx::query_scalar("SELECT current_setting($1, true)")
        .bind(phase_runner::database::INTERPRETER_CONTENT_HASH_SETTING)
        .fetch_one(database.pool())
        .await?;
    assert_eq!(stamp, phase_runner::INTERPRETER_CONTENT_HASH);

    let mut dedicated = PgConnection::connect_with(&database_options(database)).await?;
    let dedicated_stamp: String = sqlx::query_scalar("SELECT current_setting($1, true)")
        .bind(phase_runner::database::INTERPRETER_CONTENT_HASH_SETTING)
        .fetch_one(&mut dedicated)
        .await?;
    assert_eq!(dedicated_stamp, phase_runner::INTERPRETER_CONTENT_HASH);
    dedicated.close().await?;
    Ok(())
}

fn database_options(database: &RunnerDatabase) -> PgConnectOptions {
    database.pool().connect_options().as_ref().clone()
}

pub async fn seed_lineage(pool: &sqlx::PgPool, chain_id: &str, through: i64) -> Result<()> {
    for number in 0..=through {
        let hash = format!("{chain_id}-block-{number}");
        let parent = (number > 0).then(|| format!("{chain_id}-block-{}", number - 1));
        sqlx::query(
            "
            INSERT INTO chain_lineage (
                chain_id,
                block_hash,
                parent_hash,
                block_number,
                block_timestamp,
                canonicality_state
            )
            VALUES ($1, $2, $3, $4, to_timestamp($4), 'observed')
            ",
        )
        .bind(chain_id)
        .bind(hash)
        .bind(parent)
        .bind(number)
        .execute(pool)
        .await?;
    }
    Ok(())
}
