//! The API's startup check of the `normalized_events` indexes `ops/walk-index-set/drop.sql`
//! drops. The phase runner rebuilds them before Project, and the API refuses to start without
//! them unless `BIGNAME_API_WALK_INDEX_SET_CHECK=warn`.
//!
//! | Row | Input | Expected | Test |
//! | --- | --- | --- | --- |
//! | 1 | Every index in place | Serves, INFO line, gauge 0 | `the_api_starts_with_every_dropped_index_in_place` |
//! | 2 | One missing | Refuses to start naming it, ERROR line, gauge 1 | `the_api_refuses_to_start_without_a_dropped_index_and_names_it` |
//! | 2b | One missing and another not valid | Refuses naming both in the list's order, gauge 2 | `the_api_names_every_index_not_in_place_in_the_lists_order` |
//! | 3 | One not valid, or not ready | Refuses the same way, naming it | `the_api_refuses_an_index_that_is_not_valid_or_not_ready` |
//! | 4 | A table or another table's index under a listed name | Refuses, naming what holds it | `the_api_refuses_a_name_another_relation_holds` |
//! | 5 | Override `warn` with every index in place, then with one missing | Serves with a WARN that the setting is set, gauge 0, then with a WARN naming the index and the setting, gauge 1 | `warn_serves_without_a_missing_index_and_says_so` |
//! | 6 | An index the list does not name, and a valid index with another definition | Serves, the other definition warned with both texts, the unlisted index not named | `the_api_ignores_an_unlisted_index_and_warns_on_another_definition` |
//! | 7 | A read-only session under a role with schema `USAGE` and no table privilege, as on a standby | Reads the catalog only: passes on the full set, refuses without one | `the_check_reads_only_the_catalog_on_a_read_only_session` |
//! | 8 | The catalog read fails: a closed pool, or a role without `USAGE` on the schema | Refuses in both modes | `a_catalog_read_that_fails_refuses_in_both_modes` |
//! | 9 | No phase schema | Skipped, the API starts | `api_serve_tolerates_an_absent_phase_schema` (in `api_storage_quick_wins.rs`) |
//! | 10 | A value other than `refuse` or `warn` | Argument parsing fails | `the_walk_index_set_check_refuses_by_default_and_takes_only_its_two_values` (in `cli.rs`) |
//! | 11 | Every advisory index missing, under `refuse` | Serves, a WARN naming each one, gauge 0; with them in place, no such WARN | `the_api_serves_without_an_advisory_index_and_names_it` |
use std::{
    io::{Read, Write},
    sync::{Arc, Mutex},
};

use bigname_storage::walk_index_set::{ADVISORY, DROP_LIST};
use tracing::instrument::WithSubscriber;

use super::*;
use crate::cli::WalkIndexSetCheck;

/// One of the indexes `drop.sql` drops.
const DROPPED: &str = "normalized_events_registry_announcement_idx";
const REFUSED: &str = "API walk index set check refused to start";

/// The gauge is process-wide. nextest runs each test in its own process, and under
/// `cargo test` every test that runs the check takes this lock.
pub(super) static GAUGE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl CapturedLogs {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().expect("log buffer")).into_owned()
    }
}

impl Write for CapturedLogs {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("log buffer").extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

async fn logged<F: Future>(work: F) -> (F::Output, String) {
    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_writer(logs.clone())
        .finish();
    let output = work.with_subscriber(subscriber).await;
    (output, logs.text())
}

fn line_with<'a>(logs: &'a str, level: &str, text: &str) -> &'a str {
    logs.lines()
        .find(|line| line.contains(level) && line.contains(text))
        .unwrap_or_else(|| panic!("no {level} line with {text:?}: {logs}"))
}

/// Serve arguments whose listener address is already taken, so `serve` returns once its
/// startup checks have passed.
fn serve_args(
    database: &TestDatabase,
    listener: &tokio::net::TcpListener,
    check: WalkIndexSetCheck,
) -> Result<ServeArgs> {
    Ok(ServeArgs {
        bind_addr: listener.local_addr()?,
        metrics_bind_addr: "127.0.0.1:0".parse()?,
        chain_rpc_urls: Vec::new(),
        rpc_connect_timeout_ms: 2_000,
        rpc_timeout_ms: 8_000,
        rpc_chain_check: crate::cli::RpcChainCheck::Full,
        walk_index_set_check: check,
        ccip_gateway_deny_cidrs: None,
        bounds: ApiBoundsConfig::default(),
        phase_heartbeat_max_age_secs: state::DEFAULT_PHASE_HEARTBEAT_MAX_AGE_SECS,
        status_provider_timeout_ms: v2::support::status_freshness::DEFAULT_PROVIDER_TIMEOUT_MS,
        status_provider_refresh_secs: v2::support::status_freshness::DEFAULT_PROVIDER_REFRESH_SECS,
        status_provider_cache_ttl_secs:
            v2::support::status_freshness::DEFAULT_PROVIDER_CACHE_TTL_SECS,
        status_max_block_lag: v2::support::status_freshness::DEFAULT_MAX_BLOCK_LAG,
        status_max_lag_secs: v2::support::status_freshness::DEFAULT_MAX_LAG_SECS,
        publication_lag_tolerance_blocks: bigname_storage::PROJECT_PUBLICATION_LAG_TOLERANCE_BLOCKS,
        label_staging: crate::label_staging::LabelStagingConfig::default(),
        database: database.database_config(6)?,
    })
}

/// Runs `serve` against `database` and returns its error and log lines.
async fn serve_once(database: &TestDatabase, check: WalkIndexSetCheck) -> Result<(String, String)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let (result, logs) = logged(serve(serve_args(database, &listener, check)?)).await;
    let error = result.expect_err("an occupied listener stops serve");
    Ok((format!("{error:#}"), logs))
}

const STARTED: &str = "failed to bind the API listener";

/// The gauge as the metrics endpoint exports it.
async fn scraped_gauge() -> Result<i64> {
    let server = crate::metrics::bind("127.0.0.1:0".parse()?).await?;
    let address = server.local_addr()?;
    let task = tokio::spawn(server.serve());
    let response = tokio::task::spawn_blocking(move || -> Result<String> {
        let mut stream = std::net::TcpStream::connect(address)?;
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")?;
        let mut response = String::new();
        stream.read_to_string(&mut response)?;
        Ok(response)
    })
    .await??;
    task.abort();
    let value = response
        .lines()
        .find_map(|line| line.strip_prefix("walk_index_set_unusable "))
        .with_context(|| format!("the scrape lacks walk_index_set_unusable: {response}"))?;
    Ok(value.trim().parse()?)
}

async fn cleanup_role(database: &TestDatabase, pool: PgPool, role: String) -> Result<()> {
    pool.close().await;
    raw_sql(&format!("DROP OWNED BY {role}; DROP ROLE {role}"))
        .execute(&database.pool)
        .await?;
    Ok(())
}

async fn drop_index(pool: &PgPool, name: &str) -> Result<()> {
    raw_sql(&format!("DROP INDEX bigname_phase.{name}"))
        .execute(pool)
        .await?;
    Ok(())
}

#[test]
fn the_named_index_is_one_drop_sql_drops() {
    assert!(DROP_LIST.iter().any(|index| index.name == DROPPED));
}

#[tokio::test]
async fn the_api_starts_with_every_dropped_index_in_place() -> Result<()> {
    let _gauge = GAUGE.lock().await;
    let database = TestDatabase::new_migrated().await?;

    let (error, logs) = serve_once(&database, WalkIndexSetCheck::Refuse).await?;

    assert!(error.starts_with(STARTED), "{error}: {logs}");
    let passed = line_with(
        &logs,
        "INFO",
        "every normalized_events index the walk index set scripts drop is in place",
    );
    assert!(
        passed.contains(&format!("indexes={}", DROP_LIST.len())),
        "{passed}"
    );
    assert!(
        passed.contains("walk_index_set_check=\"refuse\""),
        "{passed}"
    );
    assert!(
        !logs
            .lines()
            .any(|line| line.contains("WARN") && line.contains("walk index set")),
        "{logs}"
    );
    assert_eq!(scraped_gauge().await?, 0);
    database.cleanup().await
}

#[tokio::test]
async fn the_api_refuses_to_start_without_a_dropped_index_and_names_it() -> Result<()> {
    let _gauge = GAUGE.lock().await;
    let database = TestDatabase::new_migrated().await?;
    drop_index(&database.lookup_pool, DROPPED).await?;

    let (error, logs) = serve_once(&database, WalkIndexSetCheck::Refuse).await?;

    assert!(error.starts_with(REFUSED), "{error}");
    assert!(
        error.contains(&format!(
            "1 of the {} normalized_events indexes the walk index set scripts drop are not in \
             place: {DROPPED} (missing).",
            DROP_LIST.len()
        )),
        "{error}"
    );
    assert!(
        error.contains("BIGNAME_API_WALK_INDEX_SET_CHECK=warn"),
        "{error}"
    );
    let refused = line_with(&logs, "ERROR", REFUSED);
    assert!(
        refused.contains(&format!("{DROPPED} (missing)")),
        "{refused}"
    );
    assert!(!logs.contains("is in place"), "{logs}");
    assert_eq!(scraped_gauge().await?, 1);
    database.cleanup().await
}

#[tokio::test]
async fn the_api_names_every_index_not_in_place_in_the_lists_order() -> Result<()> {
    let _gauge = GAUGE.lock().await;
    let database = TestDatabase::new_migrated().await?;
    let invalid = DROP_LIST[0].name;
    assert_ne!(invalid, DROPPED);
    drop_index(&database.lookup_pool, DROPPED).await?;
    raw_sql(&format!(
        "UPDATE pg_index SET indisvalid = false \
         WHERE indexrelid = 'bigname_phase.{invalid}'::regclass"
    ))
    .execute(&database.lookup_pool)
    .await?;

    let (error, _) = serve_once(&database, WalkIndexSetCheck::Refuse).await?;

    let listed = DROP_LIST
        .iter()
        .filter_map(|index| match index.name {
            name if name == DROPPED => Some(format!("{name} (missing)")),
            name if name == invalid => Some(format!("{name} (not valid and ready)")),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(", ");
    assert!(
        error.contains(&format!(
            "2 of the {} normalized_events indexes the walk index set scripts drop are not in \
             place: {listed}.",
            DROP_LIST.len()
        )),
        "{error}"
    );
    assert_eq!(scraped_gauge().await?, 2);
    database.cleanup().await
}

#[tokio::test]
async fn the_api_refuses_an_index_that_is_not_valid_or_not_ready() -> Result<()> {
    let _gauge = GAUGE.lock().await;
    for flag in ["indisvalid", "indisready"] {
        let database = TestDatabase::new_migrated().await?;
        raw_sql(&format!(
            "UPDATE pg_index SET {flag} = false \
             WHERE indexrelid = 'bigname_phase.{DROPPED}'::regclass"
        ))
        .execute(&database.lookup_pool)
        .await?;

        let (error, _) = serve_once(&database, WalkIndexSetCheck::Refuse).await?;

        assert!(error.starts_with(REFUSED), "{flag}: {error}");
        assert!(
            error.contains(&format!("{DROPPED} (not valid and ready)")),
            "{flag}: {error}"
        );
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn the_api_refuses_a_name_another_relation_holds() -> Result<()> {
    let _gauge = GAUGE.lock().await;
    for (ddl, held_by) in [
        (
            format!("CREATE TABLE bigname_phase.{DROPPED} ()"),
            "held by a table",
        ),
        (
            format!("CREATE INDEX {DROPPED} ON bigname_phase.chain_lineage (chain_id)"),
            "held by an index on bigname_phase.chain_lineage",
        ),
    ] {
        let database = TestDatabase::new_migrated().await?;
        drop_index(&database.lookup_pool, DROPPED).await?;
        raw_sql(&ddl).execute(&database.lookup_pool).await?;

        let error = crate::startup_preflight::ensure_walk_index_set(
            &database.lookup_pool,
            WalkIndexSetCheck::Refuse,
        )
        .await
        .expect_err("a name another relation holds refuses the start");

        let error = format!("{error:#}");
        assert!(error.contains(&format!("{DROPPED} ({held_by})")), "{error}");
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn warn_serves_without_a_missing_index_and_says_so() -> Result<()> {
    let _gauge = GAUGE.lock().await;
    let database = TestDatabase::new_migrated().await?;

    let (error, logs) = serve_once(&database, WalkIndexSetCheck::Warn).await?;
    assert!(error.starts_with(STARTED), "{error}: {logs}");
    line_with(
        &logs,
        "WARN",
        "BIGNAME_API_WALK_INDEX_SET_CHECK=warn is set",
    );
    assert_eq!(scraped_gauge().await?, 0);

    drop_index(&database.lookup_pool, DROPPED).await?;
    let (error, logs) = serve_once(&database, WalkIndexSetCheck::Warn).await?;

    assert!(error.starts_with(STARTED), "{error}: {logs}");
    let warning = line_with(
        &logs,
        "WARN",
        "the API serves without normalized_events indexes the walk index set scripts drop \
         (BIGNAME_API_WALK_INDEX_SET_CHECK=warn)",
    );
    assert!(
        warning.contains(&format!("{DROPPED} (missing)")),
        "{warning}"
    );
    assert!(!logs.contains(REFUSED), "{logs}");
    assert_eq!(scraped_gauge().await?, 1);
    let returned = crate::startup_preflight::ensure_walk_index_set(
        &database.lookup_pool,
        WalkIndexSetCheck::Warn,
    )
    .await?;
    assert_eq!(returned, 1, "the boot line reports the count");
    database.cleanup().await
}

#[tokio::test]
async fn the_api_ignores_an_unlisted_index_and_warns_on_another_definition() -> Result<()> {
    let _gauge = GAUGE.lock().await;
    let database = TestDatabase::new_migrated().await?;
    let unlisted = "walk_index_set_check_unlisted_idx";
    raw_sql(&format!(
        "CREATE INDEX {unlisted} ON bigname_phase.normalized_events (chain_id, block_number); \
         DROP INDEX bigname_phase.{DROPPED}; \
         CREATE INDEX {DROPPED} ON bigname_phase.normalized_events (chain_id)"
    ))
    .execute(&database.lookup_pool)
    .await?;

    let (error, logs) = serve_once(&database, WalkIndexSetCheck::Refuse).await?;

    assert!(error.starts_with(STARTED), "{error}: {logs}");
    let warning = line_with(&logs, "WARN", DROPPED);
    assert!(warning.contains("has another definition"), "{warning}");
    assert!(warning.contains("USING btree (chain_id)"), "{warning}");
    assert!(warning.contains("RegistryCreated"), "{warning}");
    assert!(warning.contains("docs/storage.md"), "{warning}");
    assert!(!logs.contains(unlisted), "{logs}");
    line_with(
        &logs,
        "INFO",
        "every normalized_events index the walk index set scripts drop is in place",
    );
    assert_eq!(scraped_gauge().await?, 0);
    database.cleanup().await
}

#[tokio::test]
async fn the_check_reads_only_the_catalog_on_a_read_only_session() -> Result<()> {
    let _gauge = GAUGE.lock().await;
    let database = TestDatabase::new_migrated().await?;
    let role = format!(
        "bigname_walk_check_{}_{}",
        std::process::id(),
        NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed)
    );
    raw_sql(&format!(
        "CREATE ROLE {role} NOLOGIN; GRANT USAGE ON SCHEMA bigname_phase TO {role}"
    ))
    .execute(&database.lookup_pool)
    .await?;
    let config = database.database_config(1)?;
    let options = PgConnectOptions::from_str(
        config
            .database_url
            .as_deref()
            .context("test database URL is missing")?,
    )?
    .options([
        ("search_path", "bigname_phase".to_owned()),
        ("default_transaction_read_only", "on".to_owned()),
    ]);
    let set_role = format!("SET ROLE {role}");
    let restricted = PgPoolOptions::new()
        .max_connections(1)
        .after_connect(move |connection, _metadata| {
            let set_role = set_role.clone();
            Box::pin(async move {
                sqlx::query(&set_role).execute(&mut *connection).await?;
                Ok(())
            })
        })
        .connect_with(options)
        .await?;
    let denied = sqlx::query("SELECT 1 FROM bigname_phase.normalized_events LIMIT 1")
        .execute(&restricted)
        .await
        .expect_err("the role reads no table");
    assert!(denied.to_string().contains("permission denied"), "{denied}");

    let passed =
        crate::startup_preflight::ensure_walk_index_set(&restricted, WalkIndexSetCheck::Refuse)
            .await?;
    assert_eq!(passed, 0);

    drop_index(&database.lookup_pool, DROPPED).await?;
    let error =
        crate::startup_preflight::ensure_walk_index_set(&restricted, WalkIndexSetCheck::Refuse)
            .await
            .expect_err("a missing index refuses on a read-only session");
    assert!(
        format!("{error:#}").contains(&format!("{DROPPED} (missing)")),
        "{error:#}"
    );
    cleanup_role(&database, restricted, role).await?;
    database.cleanup().await
}

#[tokio::test]
async fn a_catalog_read_that_fails_refuses_in_both_modes() -> Result<()> {
    let _gauge = GAUGE.lock().await;
    let database = TestDatabase::new_migrated().await?;
    let url = database
        .database_config(1)?
        .database_url
        .context("test database URL is missing")?;
    let closed = PgPool::connect(&url).await?;
    closed.close().await;
    let role = format!(
        "bigname_walk_check_no_usage_{}_{}",
        std::process::id(),
        NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed)
    );
    raw_sql(&format!("CREATE ROLE {role} NOLOGIN"))
        .execute(&database.lookup_pool)
        .await?;
    let set_role = format!("SET ROLE {role}");
    let without_usage = PgPoolOptions::new()
        .max_connections(1)
        .after_connect(move |connection, _metadata| {
            let set_role = set_role.clone();
            Box::pin(async move {
                sqlx::query(&set_role).execute(&mut *connection).await?;
                Ok(())
            })
        })
        .connect(&url)
        .await?;

    for (pool, cause) in [
        (&closed, "closed pool"),
        (&without_usage, "no schema usage"),
    ] {
        for check in [WalkIndexSetCheck::Refuse, WalkIndexSetCheck::Warn] {
            let error = crate::startup_preflight::ensure_walk_index_set(pool, check)
                .await
                .expect_err("a check that cannot read the catalog refuses the start");
            assert!(
                format!("{error:#}")
                    .starts_with("API walk index set check could not read the catalog"),
                "{cause}, {check:?}: {error:#}"
            );
        }
    }
    cleanup_role(&database, without_usage, role).await?;
    database.cleanup().await
}

/// An advisory index is outside the walk index set. Without one the API starts, even under
/// `refuse`, with a WARN naming each one that is missing.
#[tokio::test]
async fn the_api_serves_without_an_advisory_index_and_names_it() -> Result<()> {
    const ADVISORY_WARNING: &str = "an advisory normalized_events index is not in place";
    let _gauge = GAUGE.lock().await;
    let database = TestDatabase::new_migrated().await?;
    let (error, logs) = serve_once(&database, WalkIndexSetCheck::Refuse).await?;
    assert!(error.starts_with(STARTED), "{error}: {logs}");
    assert!(!logs.contains(ADVISORY_WARNING), "{logs}");
    assert!(!ADVISORY.is_empty());
    for index in ADVISORY {
        drop_index(&database.lookup_pool, index.name).await?;
    }

    let (error, logs) = serve_once(&database, WalkIndexSetCheck::Refuse).await?;

    assert!(error.starts_with(STARTED), "{error}: {logs}");
    for index in ADVISORY {
        let warned = line_with(&logs, "WARN", index.name);
        assert!(
            warned.contains(ADVISORY_WARNING) && warned.contains("missing"),
            "{warned}"
        );
    }
    assert_eq!(scraped_gauge().await?, 0);
    database.cleanup().await
}
