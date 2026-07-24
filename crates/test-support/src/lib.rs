use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};

mod test_hook_registry;

pub use test_hook_registry::{ScopedTestHookGuard, ScopedTestHookRegistry};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

/// Default database URL for local development.
pub const fn default_database_url() -> &'static str {
    "postgres://bigname:bigname@127.0.0.1:5432/bigname"
}

/// Resolve the PostgreSQL URL used by database-backed tests.
pub fn database_url_from_env() -> String {
    std::env::var("BIGNAME_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .unwrap_or_else(|_| default_database_url().to_owned())
}

/// Return the database name used to isolate a database-backed test hook.
pub async fn current_test_database(pool: &PgPool) -> Result<String> {
    sqlx::query_scalar("SELECT current_database()")
        .fetch_one(pool)
        .await
        .context("failed to identify the current test database")
}

pub const fn test_database_harness_hint() -> &'static str {
    "Run DB-backed tests through ./scripts/test-db -- <cargo test command>, or set BIGNAME_TEST_DATABASE_URL for an already-running PostgreSQL server."
}

#[derive(Clone, Debug)]
pub struct TestDatabaseConfig {
    name_prefix: String,
    admin_database: Option<String>,
    admin_max_connections: u32,
    pool_max_connections: u32,
    stamp_projection_replay_version: bool,
    parse_context: String,
    admin_connect_context: String,
    pool_connect_context: String,
}

impl TestDatabaseConfig {
    pub fn new(name_prefix: impl Into<String>) -> Self {
        Self {
            name_prefix: name_prefix.into(),
            admin_database: Some("postgres".to_owned()),
            admin_max_connections: 1,
            pool_max_connections: 5,
            stamp_projection_replay_version: true,
            parse_context: "failed to parse database URL for tests".to_owned(),
            admin_connect_context: "failed to connect admin pool for tests".to_owned(),
            pool_connect_context: "failed to connect test pool".to_owned(),
        }
    }

    pub fn admin_database(mut self, database: impl Into<String>) -> Self {
        self.admin_database = Some(database.into());
        self
    }

    pub fn admin_database_from_url(mut self) -> Self {
        self.admin_database = None;
        self
    }

    pub fn admin_max_connections(mut self, max_connections: u32) -> Self {
        self.admin_max_connections = max_connections;
        self
    }

    pub fn pool_max_connections(mut self, max_connections: u32) -> Self {
        self.pool_max_connections = max_connections;
        self
    }

    /// Leave the writable test pool unstamped to exercise behavior from before the
    /// [projection replay-version fence](../../../docs/glossary.md#projection-replay-version-fence).
    ///
    /// Normal test databases must keep the default stamp. This escape hatch exists only for
    /// replay-version fence tests that deliberately emulate an old process.
    pub fn without_projection_replay_version_stamp(mut self) -> Self {
        self.stamp_projection_replay_version = false;
        self
    }

    pub fn parse_context(mut self, context: impl Into<String>) -> Self {
        self.parse_context = context.into();
        self
    }

    pub fn admin_connect_context(mut self, context: impl Into<String>) -> Self {
        self.admin_connect_context = context.into();
        self
    }

    pub fn pool_connect_context(mut self, context: impl Into<String>) -> Self {
        self.pool_connect_context = context.into();
        self
    }
}

pub struct TestDatabase {
    admin_pool: PgPool,
    pool: PgPool,
    database_name: String,
}

impl TestDatabase {
    pub async fn create(config: TestDatabaseConfig) -> Result<Self> {
        let database_url = database_url_from_env();
        let base_options =
            PgConnectOptions::from_str(&database_url).context(config.parse_context.clone())?;
        let database_name = unique_database_name(&config.name_prefix)?;
        let admin_options = bigname_storage::stamp_projection_replay_version(
            match config.admin_database.as_deref() {
                Some(database) => base_options.clone().database(database),
                None => base_options.clone(),
            },
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(config.admin_max_connections)
            .connect_with(admin_options)
            .await
            .with_context(|| {
                format!(
                    "{}. {}",
                    config.admin_connect_context,
                    test_database_harness_hint()
                )
            })?;

        sqlx::query(&format!(
            "CREATE DATABASE {}",
            quote_identifier(&database_name)
        ))
        .execute(&admin_pool)
        .await
        .with_context(|| format!("failed to create test database {database_name}"))?;

        let database_options = base_options.database(&database_name);
        let database_options = if config.stamp_projection_replay_version {
            bigname_storage::stamp_projection_replay_version(database_options)
        } else {
            explicitly_unstamped_projection_replay_version_options_for_test(database_options)
        };
        let pool = PgPoolOptions::new()
            .max_connections(config.pool_max_connections)
            .connect_with(database_options)
            .await
            .context(config.pool_connect_context)?;

        Ok(Self {
            admin_pool,
            pool,
            database_name,
        })
    }

    pub async fn create_migrated(
        config: TestDatabaseConfig,
        migrator: &sqlx::migrate::Migrator,
        context: impl Into<String>,
    ) -> Result<Self> {
        let database = Self::create(config).await?;
        database.apply_migrations(migrator, context).await?;
        Ok(database)
    }

    pub async fn apply_migrations(
        &self,
        migrator: &sqlx::migrate::Migrator,
        context: impl Into<String>,
    ) -> Result<()> {
        migrator.run(&self.pool).await.context(context.into())
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    pub async fn cleanup(self) -> Result<()> {
        let Self {
            admin_pool,
            pool,
            database_name,
        } = self;

        pool.close().await;
        sqlx::query(&format!(
            "DROP DATABASE IF EXISTS {} WITH (FORCE)",
            quote_identifier(&database_name)
        ))
        .execute(&admin_pool)
        .await
        .with_context(|| format!("failed to drop test database {database_name}"))?;
        admin_pool.close().await;
        Ok(())
    }
}

/// Mark the sole test-harness escape hatch from the process connection stamp.
///
/// Keeping this marker in the options expression lets the workspace constructor audit distinguish
/// deliberate pre-fence fixtures from accidental unstamped connections.
fn explicitly_unstamped_projection_replay_version_options_for_test(
    options: PgConnectOptions,
) -> PgConnectOptions {
    options
}

fn unique_database_name(prefix: &str) -> Result<String> {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?
        .as_nanos();
    let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
    let suffix = format!("{}_{}_{unique:x}", std::process::id(), sequence);
    let max_prefix_len = 63usize.saturating_sub(suffix.len() + 1);
    let prefix = truncate_identifier_prefix(prefix, max_prefix_len);

    if prefix.is_empty() {
        Ok(suffix)
    } else {
        Ok(format!("{prefix}_{suffix}"))
    }
}

fn truncate_identifier_prefix(prefix: &str, max_bytes: usize) -> String {
    let mut end = 0;
    for (index, character) in prefix.char_indices() {
        let next = index + character.len_utf8();
        if next > max_bytes {
            break;
        }
        end = next;
    }
    prefix[..end].to_owned()
}

fn quote_identifier(identifier: &str) -> String {
    format!(r#""{}""#, identifier.replace('"', r#""""#))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn writable_test_database_connections_carry_projection_replay_version() -> Result<()> {
        let database =
            TestDatabase::create(TestDatabaseConfig::new("bigname_test_support_stamp")).await?;
        let replay_version: String = sqlx::query_scalar("SELECT current_setting($1, true)")
            .bind(bigname_storage::PROJECTION_REPLAY_VERSION_SETTING)
            .fetch_one(database.pool())
            .await?;

        assert_eq!(
            replay_version,
            bigname_storage::CURRENT_PROJECTION_REPLAY_VERSION.to_string()
        );
        database.cleanup().await
    }
}
