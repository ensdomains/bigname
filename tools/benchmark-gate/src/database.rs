use std::{str::FromStr, time::Duration};

use anyhow::{Context, Result, ensure};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};

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

pub fn require_database_identity(actual: &str, expected: &str) -> Result<()> {
    ensure!(
        !expected.trim().is_empty() && actual == expected,
        "connected database {actual:?} does not match --expected-database-name {expected:?}; refusing disposable-copy writes"
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

    #[test]
    fn disposable_database_identity_must_match_exactly() {
        assert!(require_database_identity("bigname-copy", "bigname-copy").is_ok());
        assert!(require_database_identity("bigname", "bigname-copy").is_err());
        assert!(require_database_identity("bigname", "").is_err());
    }
}
