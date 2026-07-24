use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, PgConnection, PgPool};

static SEQ: AtomicU64 = AtomicU64::new(0);
const TEMPLATE_LOCK_KEY: i64 = 0x62_69_67_6e_61_6d_65;
const MIGRATION_FINGERPRINT_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const MIGRATION_FINGERPRINT_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Per-test database: cloned from a migration-fingerprinted template on the
/// server from `BIGNAME_DATABASE_URL` (see `scripts/test-db`) and dropped on
/// cleanup.
pub struct HarnessDb {
    pub url: String,
    pub pool: PgPool,
    cleanup_guard: DatabaseCleanupGuard,
}

struct DatabaseCleanupGuard {
    admin_url: String,
    name: String,
    database_url: String,
    armed: bool,
}

impl HarnessDb {
    pub async fn create() -> Result<Self> {
        let base_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| "postgres://bigname:bigname@127.0.0.1:5432/bigname".to_string());
        let name = unique_database_name("bigname_e2e")?;
        let url = replace_database(&base_url, &name)?;

        let admin_options = bigname_storage::stamp_projection_replay_version(base_url.parse()?);
        let mut admin = PgConnection::connect_with(&admin_options)
            .await
            .with_context(|| {
                format!("connect admin database at {base_url}; is scripts/test-db running?")
            })?;
        // All processes using this harness share a server-side lock while they
        // validate/create the template and clone from it. This prevents a
        // second process from connecting to the template while PostgreSQL is
        // taking a clone, which would make CREATE DATABASE ... TEMPLATE fail.
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(TEMPLATE_LOCK_KEY)
            .execute(&mut admin)
            .await
            .context("lock the e2e migration template")?;
        let template_name = ensure_migration_template(&mut admin, &base_url).await?;
        let create_sql = format!(
            "CREATE DATABASE {} TEMPLATE {}",
            quote_identifier(&name),
            quote_identifier(&template_name)
        );
        sqlx::query(&create_sql).execute(&mut admin).await?;
        // Arm cleanup as soon as CREATE DATABASE succeeds. If connection,
        // setup, or a later scenario assertion fails, dropping the guard
        // removes the per-test database unless explicit keep mode is on.
        let cleanup_guard = DatabaseCleanupGuard {
            admin_url: base_url.clone(),
            name,
            database_url: url.clone(),
            armed: true,
        };
        admin.close().await?;
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(bigname_storage::stamp_projection_replay_version(
                url.parse()?,
            ))
            .await?;
        Ok(Self {
            url,
            pool,
            cleanup_guard,
        })
    }

    pub async fn cleanup(mut self) -> Result<()> {
        if std::env::var_os("BIGNAME_E2E_KEEP_DB").is_some() {
            eprintln!("BIGNAME_E2E_KEEP_DB set; keeping {}", self.url);
            self.cleanup_guard.disarm();
            return Ok(());
        }
        self.pool.close().await;
        drop_database(&self.cleanup_guard.admin_url, &self.cleanup_guard.name).await?;
        self.cleanup_guard.disarm();
        Ok(())
    }
}

impl DatabaseCleanupGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for DatabaseCleanupGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if std::env::var_os("BIGNAME_E2E_KEEP_DB").is_some() {
            eprintln!(
                "BIGNAME_E2E_KEEP_DB set; keeping {} after early return or failure",
                self.database_url
            );
            return;
        }

        let admin_url = self.admin_url.clone();
        let name = self.name.clone();
        let thread_name = format!("drop-{name}");
        let cleanup = std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || -> Result<()> {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .context("build runtime for e2e database cleanup")?;
                runtime.block_on(drop_database(&admin_url, &name))
            });

        match cleanup {
            Ok(cleanup) => match cleanup.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    eprintln!("failed to clean up e2e database {}: {error:#}", self.name);
                }
                Err(_) => {
                    eprintln!("e2e database cleanup thread panicked for {}", self.name);
                }
            },
            Err(error) => {
                eprintln!(
                    "failed to start e2e database cleanup thread for {}: {error}",
                    self.name
                );
            }
        }
    }
}

async fn drop_database(admin_url: &str, name: &str) -> Result<()> {
    let admin_options = bigname_storage::stamp_projection_replay_version(admin_url.parse()?);
    let mut admin = PgConnection::connect_with(&admin_options).await?;
    drop_database_with_connection(&mut admin, name).await?;
    admin.close().await?;
    Ok(())
}

async fn drop_database_with_connection(admin: &mut PgConnection, name: &str) -> Result<()> {
    sqlx::query(&format!(
        "DROP DATABASE IF EXISTS {} WITH (FORCE)",
        quote_identifier(name)
    ))
    .execute(admin)
    .await?;
    Ok(())
}

fn replace_database(base_url: &str, name: &str) -> Result<String> {
    let (url_without_query, query) = match base_url.find('?') {
        Some(index) => (&base_url[..index], &base_url[index..]),
        None => (base_url, ""),
    };
    let authority_start = url_without_query
        .find("://")
        .map(|index| index + 3)
        .context("database url has no scheme separator")?;
    let database_separator = url_without_query[authority_start..]
        .rfind('/')
        .map(|index| authority_start + index)
        .context("database url has no path segment")?;
    if database_separator + 1 == url_without_query.len() {
        bail!("database url has an empty path segment");
    }
    Ok(format!(
        "{}{name}{query}",
        &url_without_query[..=database_separator]
    ))
}

fn unique_database_name(prefix: &str) -> Result<String> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.subsec_nanos();
    Ok(format!(
        "{prefix}_{}_{}_{}",
        std::process::id(),
        nanos,
        SEQ.fetch_add(1, Ordering::Relaxed)
    ))
}

fn migration_fingerprint() -> u64 {
    let mut fingerprint = MIGRATION_FINGERPRINT_OFFSET;
    for migration in bigname_storage::MIGRATOR.iter() {
        fingerprint = fingerprint_bytes(fingerprint, &migration.version.to_le_bytes());
        fingerprint = fingerprint_bytes(fingerprint, &migration.checksum);
    }
    fingerprint
}

fn fingerprint_bytes(mut fingerprint: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        fingerprint ^= u64::from(*byte);
        fingerprint = fingerprint.wrapping_mul(MIGRATION_FINGERPRINT_PRIME);
    }
    fingerprint
}

fn migration_template_identity(scope: &str) -> (String, String) {
    let migration_fingerprint = migration_fingerprint();
    let scope_fingerprint = fingerprint_bytes(MIGRATION_FINGERPRINT_OFFSET, scope.as_bytes());
    (
        format!("bigname_e2e_template_{scope_fingerprint:016x}_{migration_fingerprint:016x}"),
        format!(
            "bigname e2e migration template {scope_fingerprint:016x}:{migration_fingerprint:016x}"
        ),
    )
}

async fn ensure_migration_template(admin: &mut PgConnection, base_url: &str) -> Result<String> {
    // Advisory locks are scoped to the connected PostgreSQL database. Include
    // that database and role in the cache identity so test harnesses using
    // different admin databases or owners on one cluster cannot race on or
    // reuse each other's template.
    let (admin_database, admin_role) =
        sqlx::query_as::<_, (String, String)>("SELECT current_database(), current_user")
            .fetch_one(&mut *admin)
            .await?;
    let (template_name, expected_marker) =
        migration_template_identity(&format!("{admin_database}\0{admin_role}"));
    let template = sqlx::query_as::<_, (Option<String>, bool)>(
        "SELECT shobj_description(oid, 'pg_database'), datallowconn FROM pg_database WHERE datname = $1",
    )
    .bind(&template_name)
    .fetch_optional(&mut *admin)
    .await?;
    match template {
        Some((Some(marker), allow_connections)) if marker == expected_marker => {
            if allow_connections {
                disable_template_connections(admin, &template_name).await?;
            }
            return Ok(template_name);
        }
        Some((marker, _)) => {
            bail!(
                "refusing to reuse database {template_name}: expected harness template marker {expected_marker:?}, found {marker:?}"
            );
        }
        None => {}
    }

    // Build under a disposable unique name and rename only after every
    // migration and the ownership marker succeed. A crash can leave a build
    // database behind, but can never expose a partial database as the shared
    // ready template.
    let build_name = unique_database_name("bigname_e2e_template_build")?;
    sqlx::query(&format!(
        "CREATE DATABASE {}",
        quote_identifier(&build_name)
    ))
    .execute(&mut *admin)
    .await
    .context("create e2e migration template build database")?;

    let setup_result = setup_migration_template(
        admin,
        base_url,
        &build_name,
        &template_name,
        &expected_marker,
    )
    .await;
    if let Err(error) = setup_result {
        if let Err(cleanup_error) = drop_database_with_connection(admin, &build_name).await {
            return Err(error).context(format!(
                "also failed to drop incomplete template database {build_name}: {cleanup_error:#}"
            ));
        }
        return Err(error);
    }
    Ok(template_name)
}

async fn setup_migration_template(
    admin: &mut PgConnection,
    base_url: &str,
    build_name: &str,
    template_name: &str,
    marker: &str,
) -> Result<()> {
    let build_url = replace_database(base_url, build_name)?;
    let build_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(bigname_storage::stamp_projection_replay_version(
            build_url.parse()?,
        ))
        .await
        .context("connect e2e migration template build database")?;
    let migration_result = bigname_storage::MIGRATOR
        .run(&build_pool)
        .await
        .context("migrate e2e template database");
    build_pool.close().await;
    migration_result?;

    sqlx::query(&format!(
        "COMMENT ON DATABASE {} IS '{}'",
        quote_identifier(build_name),
        marker
    ))
    .execute(&mut *admin)
    .await
    .context("mark completed e2e migration template")?;
    disable_template_connections(admin, build_name).await?;
    sqlx::query(&format!(
        "ALTER DATABASE {} RENAME TO {}",
        quote_identifier(build_name),
        quote_identifier(template_name)
    ))
    .execute(&mut *admin)
    .await
    .context("publish completed e2e migration template")?;
    Ok(())
}

async fn disable_template_connections(admin: &mut PgConnection, database: &str) -> Result<()> {
    sqlx::query(&format!(
        "ALTER DATABASE {} WITH ALLOW_CONNECTIONS false",
        quote_identifier(database)
    ))
    .execute(admin)
    .await
    .context("disable direct connections to the e2e migration template")?;
    Ok(())
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replace_database_preserves_query_parameters() -> Result<()> {
        assert_eq!(
            replace_database(
                "postgres://user:password@localhost:5432/source?sslmode=require&application_name=e2e",
                "scenario"
            )?,
            "postgres://user:password@localhost:5432/scenario?sslmode=require&application_name=e2e"
        );
        assert_eq!(
            replace_database("postgresql:///source?host=%2Ftmp%2Fpostgres", "scenario")?,
            "postgresql:///scenario?host=%2Ftmp%2Fpostgres"
        );
        Ok(())
    }

    #[test]
    fn replace_database_rejects_missing_database_path() {
        for invalid in ["postgres://localhost", "postgres://localhost/"] {
            assert!(
                replace_database(invalid, "scenario").is_err(),
                "{invalid} must not be treated as a database URL with a path"
            );
        }
    }

    #[test]
    fn migration_template_identity_is_stable_and_identifier_sized() {
        let first = migration_template_identity("admin-db\0admin-role");
        let second = migration_template_identity("admin-db\0admin-role");
        assert_eq!(first, second);
        assert!(first.0.starts_with("bigname_e2e_template_"));
        assert!(first.0.len() <= 63, "PostgreSQL identifier limit");
        assert_ne!(
            first,
            migration_template_identity("other-db\0admin-role"),
            "different lock scopes must not share a template database"
        );
        assert!(
            first
                .1
                .ends_with(&format!("{:016x}", migration_fingerprint()))
        );
    }

    #[tokio::test]
    async fn concurrent_harness_databases_clone_the_migrated_template() -> Result<()> {
        if std::env::var_os("BIGNAME_E2E_KEEP_DB").is_some() {
            return Ok(());
        }

        let (first, second) = tokio::try_join!(HarnessDb::create(), HarnessDb::create())?;
        assert_ne!(first.url, second.url);
        for database in [&first, &second] {
            let applied_migrations: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
                    .fetch_one(&database.pool)
                    .await?;
            assert!(applied_migrations > 0, "template clone must be migrated");
        }
        tokio::try_join!(first.cleanup(), second.cleanup())?;
        Ok(())
    }

    #[tokio::test]
    async fn dropped_harness_database_is_removed_after_early_return() -> Result<()> {
        if std::env::var_os("BIGNAME_E2E_KEEP_DB").is_some() {
            return Ok(());
        }

        let database = HarnessDb::create().await?;
        let admin_url = database.cleanup_guard.admin_url.clone();
        let name = database.cleanup_guard.name.clone();
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(bigname_storage::stamp_projection_replay_version(
                admin_url.parse()?,
            ))
            .await?;
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)")
                .bind(&name)
                .fetch_one(&admin_pool)
                .await?;
        assert!(exists, "harness database must exist before guard drop");

        drop(database);

        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)")
                .bind(&name)
                .fetch_one(&admin_pool)
                .await?;
        assert!(!exists, "guard drop must remove the harness database");
        admin_pool.close().await;
        Ok(())
    }
}
