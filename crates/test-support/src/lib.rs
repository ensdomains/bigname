use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use sqlx::{
    PgConnection, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};

mod test_hook_registry;
#[cfg(test)]
mod tests;

pub mod interpreter_content_hash {
    pub use bigname_content_hash::{INTERPRETER_CONTENT_HASH, interpreter_content_hash};
}

pub use bigname_content_hash::{INTERPRETER_CONTENT_HASH, interpreter_content_hash};
pub use test_hook_registry::{ScopedTestHookGuard, ScopedTestHookRegistry};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

const TEMPLATE_PREFIX: &str = "bigname_tpl_";
const TEMPLATE_SCRATCH_PREFIX: &str = "bigname_tpl_scratch";

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
        Self::create_copy(config, None).await
    }

    /// Create a database copied from a template that holds what `build` installs.
    ///
    /// The template is built once per server, named `bigname_tpl_<key>_<sha256 of fingerprint>`.
    /// `fingerprint` must cover every input `build` applies (SQL text, migration checksums), so
    /// a changed input gets a new template rather than a stale copy. Concurrent test processes
    /// serialize the build on an advisory lock; it is built under a scratch name and renamed
    /// only once complete, so a copy never sees a half-built template.
    pub async fn create_from_template<F, Fut>(
        config: TestDatabaseConfig,
        template_key: &str,
        fingerprint: &[&[u8]],
        build: F,
    ) -> Result<Self>
    where
        F: FnOnce(PgPool) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let base_options = PgConnectOptions::from_str(&database_url_from_env())
            .context(config.parse_context.clone())?;
        let admin_pool = connect_admin_pool(&config, &base_options).await?;
        let template =
            ensure_template(&admin_pool, &base_options, template_key, fingerprint, build).await;
        admin_pool.close().await;
        Self::create_copy(config, Some(&template?)).await
    }

    async fn create_copy(config: TestDatabaseConfig, template: Option<&str>) -> Result<Self> {
        let base_options = PgConnectOptions::from_str(&database_url_from_env())
            .context(config.parse_context.clone())?;
        let database_name = unique_database_name(&config.name_prefix)?;
        let admin_pool = connect_admin_pool(&config, &base_options).await?;

        let template_clause = template
            .map(|template| format!(" TEMPLATE {}", quote_identifier(template)))
            .unwrap_or_default();
        sqlx::query(&format!(
            "CREATE DATABASE {}{template_clause}",
            quote_identifier(&database_name)
        ))
        .execute(&admin_pool)
        .await
        .with_context(|| format!("failed to create test database {database_name}"))?;

        let database_options = base_options.database(&database_name);
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

    /// Create and select the production phase schema for a fixture's baseline SQL.
    pub async fn create_phase_schema(&self) -> Result<()> {
        sqlx::raw_sql("CREATE SCHEMA bigname_phase")
            .execute(&self.pool)
            .await?;
        self.pool.set_connect_options(
            self.pool
                .connect_options()
                .as_ref()
                .clone()
                .options([("search_path", "bigname_phase,public")]),
        );
        // Retain each connection until every existing connection has been configured.
        let mut connections = Vec::new();
        for _ in 0..self.pool.options().get_max_connections() {
            let mut connection = self.pool.acquire().await?;
            sqlx::raw_sql("SET search_path TO bigname_phase, public")
                .execute(&mut *connection)
                .await?;
            connections.push(connection);
        }
        Ok(())
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

async fn connect_admin_pool(
    config: &TestDatabaseConfig,
    base_options: &PgConnectOptions,
) -> Result<PgPool> {
    let admin_options = match config.admin_database.as_deref() {
        Some(database) => base_options.clone().database(database),
        None => base_options.clone(),
    };
    PgPoolOptions::new()
        .max_connections(config.admin_max_connections)
        .connect_with(admin_options)
        .await
        .with_context(|| {
            format!(
                "{}. {}",
                config.admin_connect_context,
                test_database_harness_hint()
            )
        })
}

/// Build the template for `key` and `fingerprint` unless it already exists; return its name.
async fn ensure_template<F, Fut>(
    admin_pool: &PgPool,
    base_options: &PgConnectOptions,
    key: &str,
    fingerprint: &[&[u8]],
    build: F,
) -> Result<String>
where
    F: FnOnce(PgPool) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let mut connection = admin_pool.acquire().await?;
    let template = template_database_name(&mut connection, key, fingerprint).await?;
    if template_exists(&mut connection, &template).await? {
        return Ok(template);
    }
    // Session-level lock: one builder per template across every process on this server. A
    // killed process releases it when its connection closes.
    sqlx::query("SELECT pg_advisory_lock(hashtextextended($1, 0))")
        .bind(&template)
        .execute(&mut *connection)
        .await?;
    let built = build_template(&mut connection, base_options, &template, build).await;
    sqlx::query("SELECT pg_advisory_unlock(hashtextextended($1, 0))")
        .bind(&template)
        .execute(&mut *connection)
        .await?;
    built.map(|()| template)
}

async fn build_template<F, Fut>(
    connection: &mut PgConnection,
    base_options: &PgConnectOptions,
    template: &str,
    build: F,
) -> Result<()>
where
    F: FnOnce(PgPool) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    if template_exists(connection, template).await? {
        return Ok(());
    }
    let scratch = unique_database_name(TEMPLATE_SCRATCH_PREFIX)?;
    sqlx::query(&format!("CREATE DATABASE {}", quote_identifier(&scratch)))
        .execute(&mut *connection)
        .await
        .with_context(|| format!("failed to create template scratch database {scratch}"))?;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(base_options.clone().database(&scratch))
        .await
        .context("failed to connect template build pool");
    let built = match pool {
        Ok(pool) => {
            let built = build(pool.clone()).await;
            pool.close().await;
            built
        }
        Err(error) => Err(error),
    };
    if let Err(error) = built {
        let _ = sqlx::query(&format!(
            "DROP DATABASE IF EXISTS {} WITH (FORCE)",
            quote_identifier(&scratch)
        ))
        .execute(&mut *connection)
        .await;
        return Err(error.context(format!("failed to build template database {template}")));
    }
    // Flag before publishing: CREATE DATABASE ... TEMPLATE still works, a stray connection cannot.
    for statement in [
        format!(
            "ALTER DATABASE {} WITH IS_TEMPLATE true ALLOW_CONNECTIONS false",
            quote_identifier(&scratch)
        ),
        format!(
            "ALTER DATABASE {} RENAME TO {}",
            quote_identifier(&scratch),
            quote_identifier(template)
        ),
    ] {
        sqlx::query(&statement)
            .execute(&mut *connection)
            .await
            .with_context(|| format!("failed to publish template database {template}"))?;
    }
    Ok(())
}

/// `bigname_tpl_<key>_<first 32 hex digits of sha256>`, within the 63-byte identifier limit.
/// PostgreSQL computes the digest, so this crate needs no hashing dependency.
async fn template_database_name(
    connection: &mut PgConnection,
    key: &str,
    fingerprint: &[&[u8]],
) -> Result<String> {
    let mut input = Vec::new();
    for part in fingerprint {
        input.extend_from_slice(&(part.len() as u64).to_be_bytes());
        input.extend_from_slice(part);
    }
    let digest: String = sqlx::query_scalar("SELECT left(encode(sha256($1), 'hex'), 32)")
        .bind(input)
        .fetch_one(connection)
        .await?;
    let key = truncate_identifier_prefix(key, 63 - TEMPLATE_PREFIX.len() - 1 - digest.len());
    Ok(format!("{TEMPLATE_PREFIX}{key}_{digest}"))
}

async fn template_exists(connection: &mut PgConnection, template: &str) -> Result<bool> {
    Ok(
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)")
            .bind(template)
            .fetch_one(connection)
            .await?,
    )
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
