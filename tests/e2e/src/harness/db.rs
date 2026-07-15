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
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.subsec_nanos();
        let name = format!(
            "bigname_e2e_{}_{}_{}",
            std::process::id(),
            nanos,
            SEQ.fetch_add(1, Ordering::Relaxed)
        );
        let url = replace_database(&base_url, &name)?;

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
        // Arm cleanup as soon as CREATE DATABASE succeeds. If connection,
        // migration, setup, or a later scenario assertion fails, dropping the
        // guard removes the per-test database unless explicit keep mode is on.
        let cleanup_guard = DatabaseCleanupGuard {
            admin_url: base_url,
            name,
            database_url: url.clone(),
            armed: true,
        };
        admin_pool.close().await;
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
    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(admin_url)
        .await?;
    sqlx::query(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
        .execute(&admin_pool)
        .await?;
    admin_pool.close().await;
    Ok(())
}

fn replace_database(base_url: &str, name: &str) -> Result<String> {
    let (prefix, _) = base_url
        .rsplit_once('/')
        .context("database url has no path segment")?;
    Ok(format!("{prefix}/{name}"))
}

#[cfg(test)]
mod tests {
    use super::*;

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
            .connect(&admin_url)
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
