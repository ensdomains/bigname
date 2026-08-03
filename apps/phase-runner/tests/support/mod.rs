use std::str::FromStr;

use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig, database_url_from_env};
use phase_runner::database::{RunnerDatabase, VerificationDatabase};
use sqlx::{Connection, PgConnection, postgres::PgConnectOptions};

const VERIFICATION_ROLE: &str = "bigname_phase_verification_reader_test";
const VERIFICATION_PASSWORD: &str = "bigname-phase-verification-reader-test";
const VERIFICATION_ROLE_LOCK: i64 = 7_312_026_073_000_004;

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
        self.runner.pool()
    }

    pub fn legacy_pool(&self) -> &sqlx::PgPool {
        self.database.pool()
    }

    pub fn writer_connect_options(&self) -> PgConnectOptions {
        self.runner.pool().connect_options().as_ref().clone()
    }

    pub async fn verification_database(
        &self,
        maximum_connections: u32,
    ) -> Result<VerificationDatabase> {
        Ok(VerificationDatabase::connect_with_options(
            self.verification_connect_options().await?,
            &self.runner,
            maximum_connections,
        )
        .await?)
    }

    pub async fn verification_connect_options(&self) -> Result<PgConnectOptions> {
        let mut transaction = self.pool().begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(VERIFICATION_ROLE_LOCK)
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            "DO $test_role$
             BEGIN
                 CREATE ROLE bigname_phase_verification_reader_test
                     LOGIN PASSWORD 'bigname-phase-verification-reader-test'
                     NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT
                     NOREPLICATION NOBYPASSRLS;
             EXCEPTION
                 WHEN duplicate_object THEN NULL;
             END
             $test_role$",
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
            .execute(&mut *transaction)
            .await?;
        let database_identifier = self.database.database_name().replace('"', "\"\"");
        let database_privileges = format!(
            "REVOKE CREATE ON DATABASE \"{database_identifier}\" FROM PUBLIC;
             GRANT CONNECT ON DATABASE \"{database_identifier}\"
                 TO bigname_phase_verification_reader_test"
        );
        sqlx::raw_sql(&database_privileges)
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            "GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system()
                 TO bigname_phase_verification_reader_test",
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "REVOKE ALL PRIVILEGES ON SCHEMA bigname_phase
                 FROM bigname_phase_verification_reader_test",
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "GRANT USAGE ON SCHEMA bigname_phase
                 TO bigname_phase_verification_reader_test",
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA bigname_phase
                 FROM bigname_phase_verification_reader_test",
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "GRANT SELECT ON ALL TABLES IN SCHEMA bigname_phase
                 TO bigname_phase_verification_reader_test",
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bigname_phase
                 FROM bigname_phase_verification_reader_test",
        )
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;

        Ok(PgConnectOptions::from_str(&database_url_from_env())?
            .database(self.database.database_name())
            .username(VERIFICATION_ROLE)
            .password(VERIFICATION_PASSWORD))
    }

    pub async fn writer_assuming_verification_role_options(&self) -> Result<PgConnectOptions> {
        self.verification_connect_options().await?;
        Ok(self
            .writer_connect_options()
            .options([("role", VERIFICATION_ROLE)]))
    }

    pub async fn cleanup(self) -> Result<()> {
        self.runner.pool().close().await;
        self.database.cleanup().await
    }
}

async fn apply_schema(pool: &sqlx::PgPool) -> Result<()> {
    phase_runner::schema::initialize_schema_v2(pool).await?;
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
