use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

static SEQ: AtomicU64 = AtomicU64::new(0);

/// Per-test database: created against the server from `BIGNAME_DATABASE_URL`
/// (see `scripts/test-db`), migrated with the shipped migrator, dropped on
/// cleanup.
pub struct HarnessDb {
    pub url: String,
    pub pool: PgPool,
    admin_url: String,
    name: String,
}

impl HarnessDb {
    pub async fn create() -> Result<Self> {
        let base_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| "postgres://bigname:bigname@127.0.0.1:5432/bigname".to_string());
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.subsec_nanos();
        let name = format!(
            "bigname_e2e_{}_{}_{}",
            std::process::id(),
            nanos,
            SEQ.fetch_add(1, Ordering::Relaxed)
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&base_url)
            .await
            .with_context(|| {
                format!("connect admin database at {base_url}; is scripts/test-db running?")
            })?;
        sqlx::query(&format!("CREATE DATABASE {name}"))
            .execute(&admin_pool)
            .await?;
        admin_pool.close().await;

        let url = replace_database(&base_url, &name)?;
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&url)
            .await?;
        bigname_storage::MIGRATOR
            .run(&pool)
            .await
            .context("run migrations")?;
        Ok(Self {
            url,
            pool,
            admin_url: base_url,
            name,
        })
    }

    pub async fn cleanup(self) -> Result<()> {
        if std::env::var_os("BIGNAME_E2E_KEEP_DB").is_some() {
            eprintln!("BIGNAME_E2E_KEEP_DB set; keeping {}", self.url);
            return Ok(());
        }
        self.pool.close().await;
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&self.admin_url)
            .await?;
        sqlx::query(&format!(
            "DROP DATABASE IF EXISTS {} WITH (FORCE)",
            self.name
        ))
        .execute(&admin_pool)
        .await?;
        admin_pool.close().await;
        Ok(())
    }
}

fn replace_database(base_url: &str, name: &str) -> Result<String> {
    let (prefix, _) = base_url
        .rsplit_once('/')
        .context("database url has no path segment")?;
    Ok(format!("{prefix}/{name}"))
}
