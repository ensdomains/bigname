use std::{str::FromStr, time::Duration};

use anyhow::{Context, Result, ensure};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

const DATABASE_INSTANCE_IDENTITY_SQL: &str = "SELECT database.oid::text,
            extract(epoch FROM pg_postmaster_start_time())::text,
            COALESCE(inet_server_addr()::text, 'local-socket'),
            COALESCE(inet_server_port()::text, 'local-socket')
     FROM pg_database database
     WHERE database.datname = current_database()";

pub async fn connect_read_only(database_url: &str, maximum_connections: u32) -> Result<PgPool> {
    let options = common_options(database_url, "bigname-benchmark-gate-read-only")?.options([
        ("search_path", "bigname_phase"),
        ("default_transaction_read_only", "on"),
        ("statement_timeout", "120000"),
    ]);
    let pool = PgPoolOptions::new()
        .max_connections(maximum_connections)
        .acquire_timeout(Duration::from_secs(30))
        .connect_with(options)
        .await
        .context("failed to connect to the benchmark target in read-only mode")?;
    let read_only: String = sqlx::query_scalar("SHOW transaction_read_only")
        .fetch_one(&pool)
        .await
        .context("failed to verify the benchmark read-only session")?;
    ensure!(
        read_only == "on",
        "benchmark read connection is not read-only"
    );
    Ok(pool)
}

pub async fn connect_disposable_copy(
    database_url: &str,
    maximum_connections: u32,
    statement_timeout: Duration,
) -> Result<PgPool> {
    let timeout_ms = statement_timeout.as_millis().max(1).to_string();
    let options =
        common_options(database_url, "bigname-benchmark-gate-disposable-copy")?.options([
            ("search_path", "bigname_phase".to_owned()),
            ("statement_timeout", timeout_ms),
            (
                "bigname.interpreter_content_hash",
                bigname_content_hash::INTERPRETER_CONTENT_HASH.to_owned(),
            ),
        ]);
    let pool = PgPoolOptions::new()
        .max_connections(maximum_connections)
        .acquire_timeout(Duration::from_secs(30))
        .connect_with(options)
        .await
        .context("failed to connect to the disposable benchmark copy")?;
    let read_only: String = sqlx::query_scalar("SHOW transaction_read_only")
        .fetch_one(&pool)
        .await
        .context("failed to verify the disposable-copy session")?;
    ensure!(
        read_only == "off",
        "disposable-copy benchmark target is read-only; indexing measurements require writes"
    );
    Ok(pool)
}

pub async fn database_identity(pool: &PgPool) -> Result<String> {
    sqlx::query_scalar("SELECT current_database()")
        .fetch_one(pool)
        .await
        .context("failed to identify benchmark database")
}

pub async fn database_host(pool: &PgPool) -> Result<String> {
    sqlx::query_scalar("SELECT COALESCE(inet_server_addr()::text, 'local-socket')")
        .fetch_one(pool)
        .await
        .context("failed to identify benchmark database host")
}

pub async fn database_instance_identity(pool: &PgPool) -> Result<String> {
    let (database_oid, postmaster_started_at, server_address, server_port): (
        String,
        String,
        String,
        String,
    ) = sqlx::query_as(DATABASE_INSTANCE_IDENTITY_SQL)
        .fetch_one(pool)
        .await
        .context("failed to identify benchmark database instance")?;
    Ok(format!(
        "keccak256:{:#x}",
        alloy_primitives::keccak256(format!(
            "{database_oid}:{postmaster_started_at}:{server_address}:{server_port}"
        ))
    )
    .replace("keccak256:0x", "keccak256:"))
}

pub fn require_database_identity(actual: &str, expected: &str) -> Result<()> {
    ensure!(
        !expected.trim().is_empty() && actual == expected,
        "connected database {actual:?} does not match --expected-database-name {expected:?}; refusing disposable-copy writes"
    );
    Ok(())
}

pub async fn require_disposable_marker(pool: &PgPool, expected: Uuid) -> Result<()> {
    let marker_table_exists: bool = sqlx::query_scalar(
        "SELECT to_regclass('bigname_benchmark.disposable_copy_marker') IS NOT NULL",
    )
    .fetch_one(pool)
    .await
    .context("failed to inspect the disposable-copy marker")?;
    ensure!(
        marker_table_exists,
        "database has no disposable-copy marker table; refusing indexing writes"
    );
    let marker_matches: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
             FROM bigname_benchmark.disposable_copy_marker
             WHERE marker = $1 AND database_name = current_database()
         )",
    )
    .bind(expected)
    .fetch_one(pool)
    .await
    .context("failed to verify the disposable-copy marker")?;
    ensure!(
        marker_matches,
        "disposable-copy marker does not match this database; refusing indexing writes"
    );
    Ok(())
}

fn common_options(database_url: &str, application_name: &str) -> Result<PgConnectOptions> {
    Ok(PgConnectOptions::from_str(database_url)
        .context("failed to parse benchmark PostgreSQL URL")?
        .application_name(application_name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigname_test_support::{TestDatabase, TestDatabaseConfig, database_url_from_env};
    use uuid::Uuid;

    #[test]
    fn disposable_database_identity_must_match_exactly() {
        assert!(require_database_identity("bigname-copy", "bigname-copy").is_ok());
        assert!(require_database_identity("bigname", "bigname-copy").is_err());
        assert!(require_database_identity("bigname", "").is_err());
    }

    #[tokio::test]
    async fn disposable_copy_requires_matching_preparation_marker() {
        let database = TestDatabase::create(TestDatabaseConfig::new("benchmark_marker"))
            .await
            .unwrap();
        sqlx::query("CREATE SCHEMA bigname_benchmark")
            .execute(database.pool())
            .await
            .unwrap();
        let marker = Uuid::new_v4();

        let absent = require_disposable_marker(database.pool(), marker).await;
        assert!(absent.is_err(), "an unprepared database must be refused");

        sqlx::query(
            "CREATE TABLE bigname_benchmark.disposable_copy_marker (
                 marker uuid PRIMARY KEY,
                 database_name text NOT NULL,
                 prepared_at timestamptz NOT NULL DEFAULT now()
             )",
        )
        .execute(database.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO bigname_benchmark.disposable_copy_marker (marker, database_name)
             VALUES ($1, current_database())",
        )
        .bind(marker)
        .execute(database.pool())
        .await
        .unwrap();

        assert!(
            require_disposable_marker(database.pool(), Uuid::new_v4())
                .await
                .is_err(),
            "a marker value not prepared on this copy must be refused"
        );
        sqlx::query(
            "UPDATE bigname_benchmark.disposable_copy_marker
             SET database_name = 'production-name'",
        )
        .execute(database.pool())
        .await
        .unwrap();
        assert!(
            require_disposable_marker(database.pool(), marker)
                .await
                .is_err(),
            "a marker prepared for a different database name must be refused"
        );
        sqlx::query(
            "UPDATE bigname_benchmark.disposable_copy_marker
             SET database_name = current_database()",
        )
        .execute(database.pool())
        .await
        .unwrap();
        require_disposable_marker(database.pool(), marker)
            .await
            .unwrap();
        database.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn instance_identity_needs_only_read_role_privileges() {
        let database = TestDatabase::create(TestDatabaseConfig::new("benchmark_identity"))
            .await
            .unwrap();
        let role = format!("benchmark_identity_reader_{}", Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE ROLE {role} NOLOGIN"))
            .execute(database.pool())
            .await
            .unwrap();

        let set_role = format!("SET ROLE {role}");
        let options = PgConnectOptions::from_str(&database_url_from_env())
            .unwrap()
            .database(database.database_name());
        let restricted_pool = PgPoolOptions::new()
            .max_connections(1)
            .after_connect(move |connection, _metadata| {
                let set_role = set_role.clone();
                Box::pin(async move {
                    sqlx::query(&set_role).execute(&mut *connection).await?;
                    Ok(())
                })
            })
            .connect_with(options)
            .await
            .unwrap();

        let identity = database_instance_identity(&restricted_pool).await;
        restricted_pool.close().await;
        sqlx::query(&format!("DROP ROLE {role}"))
            .execute(database.pool())
            .await
            .unwrap();
        assert!(
            identity
                .as_deref()
                .is_ok_and(|value| value.starts_with("keccak256:")),
            "identity query must work for a role with no elevated function privileges: {identity:?}"
        );
        database.cleanup().await.unwrap();
    }
}
