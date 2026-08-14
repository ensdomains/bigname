use std::{str::FromStr, time::Duration};

use anyhow::{Context, Result, ensure};
use sqlx::{
    Connection, PgConnection, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

const DISPOSABLE_MARKER_MAX_AGE: &str = "12 hours";
const DISPOSABLE_MARKER_MAX_FUTURE_SKEW: &str = "5 minutes";

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
    let expected_database_instance_identity =
        preflight_read_only(&options, Duration::from_secs(30)).await?;
    let pool = PgPoolOptions::new()
        .max_connections(maximum_connections)
        .acquire_timeout(Duration::from_secs(30))
        .after_connect(move |connection, _metadata| {
            let expected_database_instance_identity = expected_database_instance_identity.clone();
            Box::pin(async move {
                validate_read_only_runtime_connection(
                    connection,
                    &expected_database_instance_identity,
                )
                .await
                .map_err(|error| {
                    eprintln!("benchmark read-only connection refused: {error:#}");
                    sqlx::Error::Protocol(error.to_string())
                })
            })
        })
        .connect_with(options)
        .await
        .context("failed to connect to the benchmark target in read-only mode")?;
    Ok(pool)
}

async fn preflight_read_only(options: &PgConnectOptions, timeout: Duration) -> Result<String> {
    let timeout_ms = timeout.as_millis();
    tokio::time::timeout(timeout, async {
        let mut preflight = PgConnection::connect_with(options)
            .await
            .context("failed to open the benchmark read-only preflight connection")?;
        require_read_only_connection(&mut preflight).await?;
        let database_instance_identity =
            connection_database_instance_identity(&mut preflight).await?;
        preflight
            .close()
            .await
            .context("failed to close the benchmark read-only preflight connection")?;
        Ok::<String, anyhow::Error>(database_instance_identity)
    })
    .await
    .with_context(|| {
        format!("benchmark read-only preflight did not complete within {timeout_ms}ms")
    })?
}

async fn validate_read_only_runtime_connection(
    connection: &mut PgConnection,
    expected_database_instance_identity: &str,
) -> Result<()> {
    require_read_only_connection(connection).await?;
    let actual_database_instance_identity =
        connection_database_instance_identity(connection).await?;
    ensure!(
        actual_database_instance_identity == expected_database_instance_identity,
        "benchmark read-only database instance identity changed from {expected_database_instance_identity:?} to {actual_database_instance_identity:?}; refusing corpus reads"
    );
    Ok(())
}

async fn require_read_only_connection(connection: &mut PgConnection) -> Result<()> {
    let read_only: String = sqlx::query_scalar("SHOW transaction_read_only")
        .fetch_one(&mut *connection)
        .await
        .context("failed to inspect a benchmark read-only connection")?;
    ensure!(
        read_only == "on",
        "benchmark read connection is not read-only"
    );
    Ok(())
}

pub async fn connect_disposable_copy(
    database_url: &str,
    maximum_connections: u32,
    statement_timeout: Duration,
    expected_database_name: &str,
    expected_marker: Uuid,
) -> Result<PgPool> {
    connect_disposable_copy_with_acquire_timeout(
        database_url,
        maximum_connections,
        statement_timeout,
        expected_database_name,
        expected_marker,
        Duration::from_secs(30),
    )
    .await
}

async fn connect_disposable_copy_with_acquire_timeout(
    database_url: &str,
    maximum_connections: u32,
    statement_timeout: Duration,
    expected_database_name: &str,
    expected_marker: Uuid,
    acquire_timeout: Duration,
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
    let expected_database_instance_identity = preflight_disposable_copy(
        &options,
        expected_database_name,
        expected_marker,
        Duration::from_secs(30),
    )
    .await?;
    let expected_database_name = expected_database_name.to_owned();
    let pool = PgPoolOptions::new()
        .max_connections(maximum_connections)
        .acquire_timeout(acquire_timeout)
        .after_connect(move |connection, _metadata| {
            let expected_database_name = expected_database_name.clone();
            let expected_database_instance_identity = expected_database_instance_identity.clone();
            Box::pin(async move {
                validate_disposable_runtime_connection(
                    connection,
                    &expected_database_name,
                    expected_marker,
                    &expected_database_instance_identity,
                )
                .await
                .map_err(|error| {
                    eprintln!("disposable-copy connection refused: {error:#}");
                    sqlx::Error::Protocol(error.to_string())
                })
            })
        })
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

async fn preflight_disposable_copy(
    options: &PgConnectOptions,
    expected_database_name: &str,
    expected_marker: Uuid,
    timeout: Duration,
) -> Result<String> {
    let timeout_ms = timeout.as_millis();
    let database_instance_identity = tokio::time::timeout(timeout, async {
        let mut preflight = PgConnection::connect_with(options)
            .await
            .context("failed to open the disposable-copy preflight connection")?;
        validate_disposable_startup_connection(
            &mut preflight,
            expected_database_name,
            expected_marker,
        )
        .await?;
        let database_instance_identity =
            connection_database_instance_identity(&mut preflight).await?;
        preflight
            .close()
            .await
            .context("failed to close the disposable-copy preflight connection")?;
        Ok::<String, anyhow::Error>(database_instance_identity)
    })
    .await
    .with_context(|| {
        format!("disposable-copy preflight did not complete within {timeout_ms}ms")
    })??;
    Ok(database_instance_identity)
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
    let identity = sqlx::query_as(DATABASE_INSTANCE_IDENTITY_SQL)
        .fetch_one(pool)
        .await
        .context("failed to identify benchmark database instance")?;
    Ok(format_database_instance_identity(identity))
}

async fn connection_database_instance_identity(connection: &mut PgConnection) -> Result<String> {
    let identity = sqlx::query_as(DATABASE_INSTANCE_IDENTITY_SQL)
        .fetch_one(&mut *connection)
        .await
        .context("failed to identify benchmark database connection instance")?;
    Ok(format_database_instance_identity(identity))
}

fn format_database_instance_identity(
    (database_oid, postmaster_started_at, server_address, server_port): (
        String,
        String,
        String,
        String,
    ),
) -> String {
    // Unix-socket connections consistently use the local-socket placeholders;
    // the postmaster start epoch still distinguishes concurrent server instances.
    format!(
        "keccak256:{:#x}",
        alloy_primitives::keccak256(format!(
            "{database_oid}:{postmaster_started_at}:{server_address}:{server_port}"
        ))
    )
    .replace("keccak256:0x", "keccak256:")
}

pub fn require_database_identity(actual: &str, expected: &str) -> Result<()> {
    ensure!(
        !expected.trim().is_empty() && actual == expected,
        "connected database {actual:?} does not match --expected-database-name {expected:?}; refusing disposable-copy writes"
    );
    Ok(())
}

async fn validate_disposable_startup_connection(
    connection: &mut PgConnection,
    expected_database_name: &str,
    expected_marker: Uuid,
) -> Result<()> {
    validate_disposable_connection_authorization(
        connection,
        expected_database_name,
        expected_marker,
    )
    .await?;
    let marker_is_fresh: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
             FROM bigname_benchmark.disposable_copy_marker
             WHERE marker = $1
               AND database_name = current_database()
               AND prepared_at >= now() - $2::interval
               AND prepared_at <= now() + $3::interval
         )",
    )
    .bind(expected_marker)
    .bind(DISPOSABLE_MARKER_MAX_AGE)
    .bind(DISPOSABLE_MARKER_MAX_FUTURE_SKEW)
    .fetch_one(&mut *connection)
    .await
    .context("failed to verify disposable-copy marker freshness")?;
    ensure!(
        marker_is_fresh,
        "disposable-copy marker is older than {DISPOSABLE_MARKER_MAX_AGE} or more than {DISPOSABLE_MARKER_MAX_FUTURE_SKEW} in the future; refusing indexing writes"
    );
    Ok(())
}

async fn validate_disposable_runtime_connection(
    connection: &mut PgConnection,
    expected_database_name: &str,
    expected_marker: Uuid,
    expected_database_instance_identity: &str,
) -> Result<()> {
    // Marker freshness authorizes the gate start. Replacement connections keep
    // checking authorization and must match the preflighted database instance,
    // but they do not expire a legitimate long run. A mid-run restart or
    // retarget is therefore refused before a replacement connection can query.
    validate_disposable_connection_authorization(
        connection,
        expected_database_name,
        expected_marker,
    )
    .await?;
    let actual_database_instance_identity =
        connection_database_instance_identity(connection).await?;
    ensure!(
        actual_database_instance_identity == expected_database_instance_identity,
        "disposable-copy database instance identity changed from {expected_database_instance_identity:?} to {actual_database_instance_identity:?}; refusing indexing writes"
    );
    Ok(())
}

async fn validate_disposable_connection_authorization(
    connection: &mut PgConnection,
    expected_database_name: &str,
    expected_marker: Uuid,
) -> Result<()> {
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&mut *connection)
        .await
        .context("failed to identify a disposable-copy connection")?;
    require_database_identity(&database_name, expected_database_name)?;
    let read_only: String = sqlx::query_scalar("SHOW transaction_read_only")
        .fetch_one(&mut *connection)
        .await
        .context("failed to inspect a disposable-copy connection")?;
    ensure!(
        read_only == "off",
        "disposable-copy connection is read-only; refusing indexing writes"
    );
    let marker_table_exists: bool = sqlx::query_scalar(
        "SELECT to_regclass('bigname_benchmark.disposable_copy_marker') IS NOT NULL",
    )
    .fetch_one(&mut *connection)
    .await
    .context("failed to inspect the disposable-copy connection marker")?;
    ensure!(
        marker_table_exists,
        "database has no disposable-copy marker table; refusing indexing writes"
    );
    let marker_matches: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
             FROM bigname_benchmark.disposable_copy_marker
             WHERE marker = $1
               AND database_name = current_database()
         )",
    )
    .bind(expected_marker)
    .fetch_one(&mut *connection)
    .await
    .context("failed to validate the disposable-copy connection marker")?;
    ensure!(
        marker_matches,
        "disposable-copy marker UUID or database name does not match; refusing indexing writes"
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

    fn scratch_database_url(database_name: &str) -> String {
        let mut url = url::Url::parse(&database_url_from_env()).unwrap();
        url.set_path(&format!("/{database_name}"));
        url.into()
    }

    async fn validate_startup(pool: &PgPool, database_name: &str, marker: Uuid) -> Result<()> {
        let mut connection = pool.acquire().await.unwrap();
        validate_disposable_startup_connection(&mut connection, database_name, marker).await
    }

    async fn validate_runtime(
        pool: &PgPool,
        database_name: &str,
        marker: Uuid,
        expected_database_instance_identity: &str,
    ) -> Result<()> {
        let mut connection = pool.acquire().await.unwrap();
        validate_disposable_runtime_connection(
            &mut connection,
            database_name,
            marker,
            expected_database_instance_identity,
        )
        .await
    }

    async fn install_disposable_marker(pool: &PgPool, marker: Uuid) {
        sqlx::query("CREATE SCHEMA bigname_benchmark")
            .execute(pool)
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE bigname_benchmark.disposable_copy_marker (
                 marker uuid PRIMARY KEY,
                 database_name text NOT NULL,
                 prepared_at timestamptz NOT NULL DEFAULT now()
             )",
        )
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO bigname_benchmark.disposable_copy_marker (marker, database_name)
             VALUES ($1, current_database())",
        )
        .bind(marker)
        .execute(pool)
        .await
        .unwrap();
    }

    #[test]
    fn disposable_database_identity_must_match_exactly() {
        assert!(require_database_identity("bigname-copy", "bigname-copy").is_ok());
        assert!(require_database_identity("bigname", "bigname-copy").is_err());
        assert!(require_database_identity("bigname", "").is_err());
    }

    #[test]
    fn local_socket_identity_changes_with_postmaster_epoch() {
        // This pins the load-bearing restart input for local sockets. The pooled
        // connection tests exercise preflight-to-pool equality on real TCP sessions.
        let first = format_database_instance_identity((
            "42".to_owned(),
            "1234.5".to_owned(),
            "local-socket".to_owned(),
            "local-socket".to_owned(),
        ));
        let restarted = format_database_instance_identity((
            "42".to_owned(),
            "1235.5".to_owned(),
            "local-socket".to_owned(),
            "local-socket".to_owned(),
        ));

        assert_ne!(first, restarted);
    }

    #[tokio::test]
    async fn disposable_copy_startup_reports_the_missing_marker() {
        let database =
            TestDatabase::create(TestDatabaseConfig::new("benchmark_named_marker_startup"))
                .await
                .unwrap();
        let database_url = scratch_database_url(database.database_name());
        let started = std::time::Instant::now();

        let error = connect_disposable_copy(
            &database_url,
            1,
            Duration::from_secs(30),
            database.database_name(),
            Uuid::new_v4(),
        )
        .await
        .expect_err("an unprepared database must be refused");

        let elapsed = started.elapsed();
        database.cleanup().await.unwrap();
        assert!(
            error
                .to_string()
                .contains("no disposable-copy marker table"),
            "startup refusal lost its named reason after {elapsed:?}: {error:#}"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "startup refusal was not fast"
        );
    }

    #[tokio::test]
    async fn disposable_copy_preflight_bounds_a_blocked_marker_query() {
        let database =
            TestDatabase::create(TestDatabaseConfig::new("benchmark_blocked_marker_startup"))
                .await
                .unwrap();
        let marker = Uuid::new_v4();
        sqlx::query("CREATE SCHEMA bigname_benchmark")
            .execute(database.pool())
            .await
            .unwrap();
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
        let mut lock = database.pool().acquire().await.unwrap();
        sqlx::query("BEGIN").execute(&mut *lock).await.unwrap();
        sqlx::query("LOCK TABLE bigname_benchmark.disposable_copy_marker IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *lock)
            .await
            .unwrap();
        let database_url = scratch_database_url(database.database_name());
        let options = common_options(&database_url, "benchmark-blocked-preflight").unwrap();

        let result = tokio::time::timeout(
            Duration::from_secs(2),
            preflight_disposable_copy(
                &options,
                database.database_name(),
                marker,
                Duration::from_millis(100),
            ),
        )
        .await;

        sqlx::query("ROLLBACK").execute(&mut *lock).await.unwrap();
        drop(lock);
        database.cleanup().await.unwrap();
        let error = result
            .expect("preflight ignored its own short timeout")
            .expect_err("a blocked marker query must be refused")
            .to_string();
        assert!(error.contains("did not complete within 100ms"));
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

        let absent = validate_startup(database.pool(), database.database_name(), marker)
            .await
            .unwrap_err()
            .to_string();
        assert!(absent.contains("no disposable-copy marker table"));

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

        let wrong_marker =
            validate_startup(database.pool(), database.database_name(), Uuid::new_v4())
                .await
                .unwrap_err()
                .to_string();
        assert!(wrong_marker.contains("marker UUID or database name does not match"));
        sqlx::query(
            "UPDATE bigname_benchmark.disposable_copy_marker
             SET database_name = 'production-name'",
        )
        .execute(database.pool())
        .await
        .unwrap();
        let wrong_name = validate_startup(database.pool(), database.database_name(), marker)
            .await
            .unwrap_err()
            .to_string();
        assert!(wrong_name.contains("marker UUID or database name does not match"));
        sqlx::query(
            "UPDATE bigname_benchmark.disposable_copy_marker
             SET database_name = current_database()",
        )
        .execute(database.pool())
        .await
        .unwrap();
        validate_startup(database.pool(), database.database_name(), marker)
            .await
            .unwrap();
        database.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn stale_disposable_marker_is_refused() {
        let database = TestDatabase::create(TestDatabaseConfig::new("benchmark_stale_marker"))
            .await
            .unwrap();
        let marker = Uuid::new_v4();
        sqlx::query("CREATE SCHEMA bigname_benchmark")
            .execute(database.pool())
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE bigname_benchmark.disposable_copy_marker (
                 marker uuid PRIMARY KEY,
                 database_name text NOT NULL,
                 prepared_at timestamptz NOT NULL
             )",
        )
        .execute(database.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO bigname_benchmark.disposable_copy_marker
                 (marker, database_name, prepared_at)
             VALUES ($1, current_database(), now() - interval '13 hours')",
        )
        .bind(marker)
        .execute(database.pool())
        .await
        .unwrap();

        let result = validate_startup(database.pool(), database.database_name(), marker).await;
        database.cleanup().await.unwrap();
        let error = result.expect_err("a stale marker must not authorize writes");
        assert!(error.to_string().contains("older than 12 hours"));
    }

    #[tokio::test]
    async fn future_dated_disposable_marker_is_refused() {
        let database = TestDatabase::create(TestDatabaseConfig::new("benchmark_future_marker"))
            .await
            .unwrap();
        let marker = Uuid::new_v4();
        sqlx::query("CREATE SCHEMA bigname_benchmark")
            .execute(database.pool())
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE bigname_benchmark.disposable_copy_marker (
                 marker uuid PRIMARY KEY,
                 database_name text NOT NULL,
                 prepared_at timestamptz NOT NULL
             )",
        )
        .execute(database.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO bigname_benchmark.disposable_copy_marker
                 (marker, database_name, prepared_at)
             VALUES ($1, current_database(), now() + interval '30 days')",
        )
        .bind(marker)
        .execute(database.pool())
        .await
        .unwrap();

        let result = validate_startup(database.pool(), database.database_name(), marker).await;
        let database_instance_identity = database_instance_identity(database.pool()).await.unwrap();
        let runtime_result = validate_runtime(
            database.pool(),
            database.database_name(),
            marker,
            &database_instance_identity,
        )
        .await;
        database.cleanup().await.unwrap();
        assert!(
            result.is_err(),
            "a future-dated marker must not extend write authorization"
        );
        assert!(
            runtime_result.is_ok(),
            "mid-run validation must not reapply startup freshness"
        );
    }

    #[tokio::test]
    async fn every_new_disposable_pool_connection_revalidates_the_marker() {
        let database = TestDatabase::create(TestDatabaseConfig::new("benchmark_marker_pool"))
            .await
            .unwrap();
        let marker = Uuid::new_v4();
        sqlx::query("CREATE SCHEMA bigname_benchmark")
            .execute(database.pool())
            .await
            .unwrap();
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
        let database_url = scratch_database_url(database.database_name());
        let pool = connect_disposable_copy_with_acquire_timeout(
            &database_url,
            1,
            Duration::from_secs(30),
            database.database_name(),
            marker,
            Duration::from_millis(250),
        )
        .await
        .unwrap();
        let held = pool.acquire().await.unwrap();
        sqlx::query("DELETE FROM bigname_benchmark.disposable_copy_marker")
            .execute(database.pool())
            .await
            .unwrap();
        held.close().await.unwrap();

        let error = pool
            .acquire()
            .await
            .expect_err("a new connection accepted a removed marker");
        pool.close().await;
        database.cleanup().await.unwrap();
        assert!(
            matches!(error, sqlx::Error::PoolTimedOut),
            "marker refusal had the wrong pool error shape: {error:?}"
        );
    }

    #[tokio::test]
    async fn pooled_connection_to_a_second_database_instance_is_rejected() {
        let first = TestDatabase::create(TestDatabaseConfig::new("benchmark_instance_first"))
            .await
            .unwrap();
        let second = TestDatabase::create(TestDatabaseConfig::new("benchmark_instance_second"))
            .await
            .unwrap();
        let marker = Uuid::new_v4();
        install_disposable_marker(first.pool(), marker).await;
        install_disposable_marker(second.pool(), marker).await;
        let first_url = scratch_database_url(first.database_name());
        let first_options = common_options(&first_url, "benchmark-instance-preflight").unwrap();
        let first_identity = preflight_disposable_copy(
            &first_options,
            first.database_name(),
            marker,
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        let second_identity = database_instance_identity(second.pool()).await.unwrap();
        assert_ne!(first_identity, second_identity);

        let result = validate_runtime(
            second.pool(),
            second.database_name(),
            marker,
            &first_identity,
        )
        .await;

        first.cleanup().await.unwrap();
        second.cleanup().await.unwrap();
        let error = result.expect_err(
            "a pooled connection to a different preflighted database instance must be refused",
        );
        assert!(
            error
                .to_string()
                .contains("database instance identity changed"),
            "instance refusal lost its named reason: {error:#}"
        );
    }

    #[tokio::test]
    async fn all_eight_disposable_pool_connections_match_the_preflight_instance() {
        let database = TestDatabase::create(TestDatabaseConfig::new("benchmark_instance_pool"))
            .await
            .unwrap();
        let marker = Uuid::new_v4();
        install_disposable_marker(database.pool(), marker).await;
        let database_url = scratch_database_url(database.database_name());
        let pool = connect_disposable_copy(
            &database_url,
            8,
            Duration::from_secs(30),
            database.database_name(),
            marker,
        )
        .await
        .unwrap();

        let mut connections = Vec::with_capacity(8);
        for _ in 0..8 {
            let mut connection = pool.acquire().await.unwrap();
            let database_name: String = sqlx::query_scalar("SELECT current_database()")
                .fetch_one(&mut *connection)
                .await
                .unwrap();
            assert_eq!(database_name, database.database_name());
            connections.push(connection);
        }

        drop(connections);
        pool.close().await;
        database.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn read_only_connection_to_a_second_database_instance_is_rejected() {
        let first = TestDatabase::create(TestDatabaseConfig::new("benchmark_reader_first"))
            .await
            .unwrap();
        let second = TestDatabase::create(TestDatabaseConfig::new("benchmark_reader_second"))
            .await
            .unwrap();
        let first_identity = database_instance_identity(first.pool()).await.unwrap();
        let second_url = scratch_database_url(second.database_name());
        let options = common_options(&second_url, "benchmark-reader-second")
            .unwrap()
            .options([("default_transaction_read_only", "on")]);
        let mut second_connection = PgConnection::connect_with(&options).await.unwrap();
        let second_identity = connection_database_instance_identity(&mut second_connection)
            .await
            .unwrap();
        assert_ne!(first_identity, second_identity);

        let result =
            validate_read_only_runtime_connection(&mut second_connection, &first_identity).await;

        second_connection.close().await.unwrap();
        first.cleanup().await.unwrap();
        second.cleanup().await.unwrap();
        let error = result.expect_err(
            "a read-only connection to a different preflighted database instance must be refused",
        );
        assert!(
            error
                .to_string()
                .contains("database instance identity changed"),
            "read-only instance refusal lost its named reason: {error:#}"
        );
    }

    #[tokio::test]
    async fn read_only_identity_query_failure_uses_mode_neutral_context() {
        let database = TestDatabase::create(TestDatabaseConfig::new(
            "benchmark_reader_identity_diagnostic",
        ))
        .await
        .unwrap();
        let database_url = scratch_database_url(database.database_name());
        let options = common_options(&database_url, "benchmark-reader-diagnostic")
            .unwrap()
            .options([("default_transaction_read_only", "on")]);
        let mut connection = PgConnection::connect_with(&options).await.unwrap();
        let backend_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut connection)
            .await
            .unwrap();
        let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
            .bind(backend_pid)
            .fetch_one(database.pool())
            .await
            .unwrap();
        assert!(terminated);

        let error = connection_database_instance_identity(&mut connection)
            .await
            .expect_err("a terminated read-only connection unexpectedly returned an identity");

        database.cleanup().await.unwrap();
        let diagnostic = format!("{error:#}");
        assert!(
            diagnostic.contains("failed to identify benchmark database connection instance"),
            "read-only identity failure lost its mode-neutral context: {diagnostic}"
        );
        assert!(!diagnostic.contains("disposable-copy"));
    }

    #[tokio::test]
    async fn all_eight_read_only_pool_connections_match_the_preflight_instance() {
        let database = TestDatabase::create(TestDatabaseConfig::new("benchmark_reader_pool"))
            .await
            .unwrap();
        let database_url = scratch_database_url(database.database_name());
        let pool = connect_read_only(&database_url, 8).await.unwrap();
        let expected_identity = database_instance_identity(database.pool()).await.unwrap();

        let mut connections = Vec::with_capacity(8);
        for _ in 0..8 {
            let mut connection = pool.acquire().await.unwrap();
            let read_only: String = sqlx::query_scalar("SHOW transaction_read_only")
                .fetch_one(&mut *connection)
                .await
                .unwrap();
            let identity = connection_database_instance_identity(&mut connection)
                .await
                .unwrap();
            assert_eq!(read_only, "on");
            assert_eq!(identity, expected_identity);
            connections.push(connection);
        }

        drop(connections);
        pool.close().await;
        database.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn marker_age_does_not_expire_an_active_run() {
        let database = TestDatabase::create(TestDatabaseConfig::new("benchmark_marker_age"))
            .await
            .unwrap();
        let marker = Uuid::new_v4();
        sqlx::query("CREATE SCHEMA bigname_benchmark")
            .execute(database.pool())
            .await
            .unwrap();
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
        let database_url = scratch_database_url(database.database_name());
        let pool = connect_disposable_copy(
            &database_url,
            1,
            Duration::from_secs(30),
            database.database_name(),
            marker,
        )
        .await
        .unwrap();
        let held = pool.acquire().await.unwrap();
        sqlx::query(
            "UPDATE bigname_benchmark.disposable_copy_marker
             SET prepared_at = now() - interval '13 hours'",
        )
        .execute(database.pool())
        .await
        .unwrap();
        held.close().await.unwrap();

        let replacement = tokio::time::timeout(Duration::from_secs(2), pool.acquire()).await;
        let accepted = matches!(replacement, Ok(Ok(_)));
        drop(replacement);
        pool.close().await;
        database.cleanup().await.unwrap();
        assert!(
            accepted,
            "a marker that ages during the run must not block replacement connections"
        );
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
