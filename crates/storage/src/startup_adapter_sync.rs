use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use sqlx::{Acquire, PgConnection, PgPool, Postgres, Row, migrate::Migrate, pool::PoolConnection};

use crate::RawLogStagingInputVersion;

mod lineage;

pub use lineage::{
    StartupAdapterLineageState, StartupCanonicalLineageHead, load_startup_adapter_lineage_state,
};
use lineage::{load_startup_adapter_lineage_state_from_connection, lock_canonical_lineage};

pub const STARTUP_ADAPTER_CURSOR_KIND: &str = "startup_adapter_owned_raw_log_state";
pub const STARTUP_ADAPTER_CHECKPOINT_SCOPE: &str = "startup_adapter_sync";
pub const STARTUP_DISCOVERY_ADMISSION_EPOCH_FIELD: &str = "startup_discovery_admission_epoch";
pub const STARTUP_CANONICAL_LINEAGE_HEAD_FIELD: &str = "startup_canonical_lineage_head";
pub const STARTUP_LINEAGE_MUTATION_REVISION_FIELD: &str = "startup_lineage_mutation_revision";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartupAdapterSyncKey {
    pub raw_log_input_version: RawLogStagingInputVersion,
    pub lineage_mutation_revision: i64,
    pub canonical_lineage_head: Option<StartupCanonicalLineageHead>,
    pub discovery_admission_epoch: i64,
    pub adapter_semantic_version: i64,
    pub schema_migration_count: i64,
    pub schema_migration_max_version: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StartupAdapterSyncDecision {
    ReuseCompleted,
    RunFullSync {
        started_key: Option<StartupAdapterSyncKey>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartupAdapterSyncCompletion {
    Completed,
    InputChanged,
    KeyUnknown,
}

pub async fn prepare_startup_adapter_sync(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
    adapter: &str,
    adapter_semantic_version: i64,
) -> Result<StartupAdapterSyncDecision> {
    validate_identity(deployment_profile, chain, adapter, adapter_semantic_version)?;
    let mut migration_guard = StartupMigrationLockGuard::acquire(pool).await?;
    let operation_result = async {
        let mut transaction =
            begin_startup_adapter_fence(migration_guard.connection_mut(), chain).await?;
        let key =
            load_startup_adapter_sync_key(transaction.as_mut(), chain, adapter_semantic_version)
                .await?;
        let reusable = match key.as_ref() {
            Some(key) => {
                completed_checkpoint_matches(
                    transaction.as_mut(),
                    deployment_profile,
                    chain,
                    adapter,
                    key,
                )
                .await?
            }
            None => false,
        };
        transaction
            .commit()
            .await
            .context("failed to finish startup adapter checkpoint verification")?;

        Ok(if reusable {
            StartupAdapterSyncDecision::ReuseCompleted
        } else {
            StartupAdapterSyncDecision::RunFullSync { started_key: key }
        })
    }
    .await;
    let release_result = migration_guard.release().await;
    prioritize_lock_release(operation_result, release_result)
}

pub async fn complete_startup_adapter_sync(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
    adapter: &str,
    adapter_semantic_version: i64,
    started_key: Option<StartupAdapterSyncKey>,
) -> Result<StartupAdapterSyncCompletion> {
    validate_identity(deployment_profile, chain, adapter, adapter_semantic_version)?;
    let Some(started_key) = started_key else {
        return Ok(StartupAdapterSyncCompletion::KeyUnknown);
    };

    let mut migration_guard = StartupMigrationLockGuard::acquire(pool).await?;
    let operation_result = async {
        let mut transaction =
            begin_startup_adapter_fence(migration_guard.connection_mut(), chain).await?;
        let current_key =
            load_startup_adapter_sync_key(transaction.as_mut(), chain, adapter_semantic_version)
                .await?;
        if current_key.as_ref() != Some(&started_key) {
            invalidate_startup_adapter_checkpoint(
                transaction.as_mut(),
                deployment_profile,
                chain,
                adapter,
            )
            .await?;
            let completion = if current_key.is_some() {
                StartupAdapterSyncCompletion::InputChanged
            } else {
                StartupAdapterSyncCompletion::KeyUnknown
            };
            transaction
                .commit()
                .await
                .context("failed to finish changed startup adapter input check")?;
            return Ok(completion);
        }

        publish_completed_checkpoint(
            transaction.as_mut(),
            deployment_profile,
            chain,
            adapter,
            &started_key,
        )
        .await?;
        transaction
            .commit()
            .await
            .context("failed to publish startup adapter checkpoint completion")?;
        Ok(StartupAdapterSyncCompletion::Completed)
    }
    .await;
    let release_result = migration_guard.release().await;
    prioritize_lock_release(operation_result, release_result)
}

pub async fn load_startup_adapter_schema_state(pool: &PgPool) -> Result<Option<(i64, i64)>> {
    let mut migration_guard = StartupMigrationLockGuard::acquire(pool).await?;
    let operation_result = async {
        let mut transaction = migration_guard
            .connection_mut()
            .begin()
            .await
            .context("failed to start startup adapter schema-state transaction")?;
        let state = lock_and_load_schema_state(transaction.as_mut()).await?;
        transaction
            .commit()
            .await
            .context("failed to finish startup adapter schema-state transaction")?;
        Ok(state)
    }
    .await;
    let release_result = migration_guard.release().await;
    prioritize_lock_release(operation_result, release_result)
}

async fn load_startup_adapter_sync_key(
    connection: &mut PgConnection,
    chain: &str,
    adapter_semantic_version: i64,
) -> Result<Option<StartupAdapterSyncKey>> {
    let raw_log_input = sqlx::query(
        r#"
        SELECT retention_generation, revision
        FROM raw_log_staging_input_revisions
        WHERE chain_id = $1
        "#,
    )
    .bind(chain)
    .fetch_optional(&mut *connection)
    .await
    .with_context(|| format!("failed to load strict startup raw-log input for {chain}"))?;
    let Some(raw_log_input) = raw_log_input else {
        return Ok(None);
    };
    let Some(lineage_state) =
        load_startup_adapter_lineage_state_from_connection(connection, chain).await?
    else {
        return Ok(None);
    };

    let discovery_admission_epoch = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT epoch
        FROM discovery_admission_epochs
        WHERE chain_id = $1
        FOR SHARE
        "#,
    )
    .bind(chain)
    .fetch_optional(&mut *connection)
    .await
    .with_context(|| {
        format!("failed to load fenced startup discovery-admission epoch for {chain}")
    })?;
    let Some(discovery_admission_epoch) = discovery_admission_epoch else {
        return Ok(None);
    };
    let Some((schema_migration_count, schema_migration_max_version)) =
        lock_and_load_schema_state(connection).await?
    else {
        return Ok(None);
    };

    Ok(Some(StartupAdapterSyncKey {
        raw_log_input_version: RawLogStagingInputVersion {
            retention_generation: raw_log_input.try_get("retention_generation")?,
            revision: raw_log_input.try_get("revision")?,
        },
        lineage_mutation_revision: lineage_state.mutation_revision,
        canonical_lineage_head: lineage_state.canonical_lineage_head,
        discovery_admission_epoch,
        adapter_semantic_version,
        schema_migration_count,
        schema_migration_max_version,
    }))
}

async fn lock_and_load_schema_state(connection: &mut PgConnection) -> Result<Option<(i64, i64)>> {
    let migration_table = sqlx::query_scalar::<_, Option<String>>(
        "SELECT to_regclass('public._sqlx_migrations')::TEXT",
    )
    .fetch_one(&mut *connection)
    .await
    .context("failed to inspect applied migration state for startup adapter sync")?;
    if migration_table.is_none() {
        return Ok(None);
    }
    sqlx::query("LOCK TABLE _sqlx_migrations IN SHARE MODE")
        .execute(&mut *connection)
        .await
        .context("failed to fence applied migration state for startup adapter sync")?;
    let (migration_count, max_version, all_successful) =
        sqlx::query_as::<_, (i64, Option<i64>, Option<bool>)>(
            r#"
            SELECT
                COUNT(*)::BIGINT,
                MAX(version)::BIGINT,
                BOOL_AND(success)
            FROM _sqlx_migrations
            "#,
        )
        .fetch_one(connection)
        .await
        .context("failed to load applied migration state for startup adapter sync")?;
    Ok(match (migration_count, max_version, all_successful) {
        (count, Some(max_version), Some(true)) if count > 0 && max_version > 0 => {
            Some((count, max_version))
        }
        _ => None,
    })
}

async fn completed_checkpoint_matches(
    connection: &mut PgConnection,
    deployment_profile: &str,
    chain: &str,
    adapter: &str,
    key: &StartupAdapterSyncKey,
) -> Result<bool> {
    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM normalized_replay_adapter_checkpoints
            WHERE deployment_profile = $1
              AND chain_id = $2
              AND cursor_kind = $3
              AND adapter = $4
              AND checkpoint_scope = $5
              AND status = 'completed'
              AND completed_at IS NOT NULL
              AND raw_log_retention_generation = $6
              AND raw_log_input_revision = $7
              AND adapter_semantic_version = $8
              AND schema_migration_count = $9
              AND schema_migration_max_version = $10
              AND state_payload -> $11 = to_jsonb($12::BIGINT)
              AND state_payload -> $13 = to_jsonb($14::BIGINT)
              AND state_payload -> $15 = $16::JSONB
        )
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(STARTUP_ADAPTER_CURSOR_KIND)
    .bind(adapter)
    .bind(STARTUP_ADAPTER_CHECKPOINT_SCOPE)
    .bind(key.raw_log_input_version.retention_generation)
    .bind(key.raw_log_input_version.revision)
    .bind(key.adapter_semantic_version)
    .bind(key.schema_migration_count)
    .bind(key.schema_migration_max_version)
    .bind(STARTUP_DISCOVERY_ADMISSION_EPOCH_FIELD)
    .bind(key.discovery_admission_epoch)
    .bind(STARTUP_LINEAGE_MUTATION_REVISION_FIELD)
    .bind(key.lineage_mutation_revision)
    .bind(STARTUP_CANONICAL_LINEAGE_HEAD_FIELD)
    .bind(canonical_lineage_head_payload(
        key.canonical_lineage_head.as_ref(),
    ))
    .fetch_one(connection)
    .await
    .with_context(|| {
        format!(
            "failed to verify completed startup adapter checkpoint for \
             {deployment_profile}/{chain}/{adapter}"
        )
    })
}

async fn publish_completed_checkpoint(
    connection: &mut PgConnection,
    deployment_profile: &str,
    chain: &str,
    adapter: &str,
    key: &StartupAdapterSyncKey,
) -> Result<()> {
    let state_payload = json!({
        (STARTUP_DISCOVERY_ADMISSION_EPOCH_FIELD): key.discovery_admission_epoch,
        (STARTUP_LINEAGE_MUTATION_REVISION_FIELD): key.lineage_mutation_revision,
        (STARTUP_CANONICAL_LINEAGE_HEAD_FIELD):
            canonical_lineage_head_payload(key.canonical_lineage_head.as_ref()),
    });
    sqlx::query(
        r#"
        INSERT INTO normalized_replay_adapter_checkpoints (
            deployment_profile,
            chain_id,
            cursor_kind,
            adapter,
            checkpoint_scope,
            replay_start_block_number,
            replay_target_block_number,
            status,
            state_payload,
            raw_log_retention_generation,
            raw_log_input_revision,
            adapter_semantic_version,
            schema_migration_count,
            schema_migration_max_version,
            completed_at
        )
        VALUES ($1, $2, $3, $4, $5, 0, 0, 'completed', $6, $7, $8, $9, $10, $11, now())
        ON CONFLICT (
            deployment_profile,
            chain_id,
            cursor_kind,
            adapter,
            checkpoint_scope
        )
        DO UPDATE SET
            status = 'completed',
            state_payload = (
                CASE
                    WHEN jsonb_typeof(normalized_replay_adapter_checkpoints.state_payload) = 'object'
                        THEN normalized_replay_adapter_checkpoints.state_payload
                    ELSE '{}'::JSONB
                END
            ) || EXCLUDED.state_payload,
            raw_log_retention_generation = EXCLUDED.raw_log_retention_generation,
            raw_log_input_revision = EXCLUDED.raw_log_input_revision,
            adapter_semantic_version = EXCLUDED.adapter_semantic_version,
            schema_migration_count = EXCLUDED.schema_migration_count,
            schema_migration_max_version = EXCLUDED.schema_migration_max_version,
            last_failure_reason = NULL,
            completed_at = now(),
            updated_at = now()
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(STARTUP_ADAPTER_CURSOR_KIND)
    .bind(adapter)
    .bind(STARTUP_ADAPTER_CHECKPOINT_SCOPE)
    .bind(state_payload)
    .bind(key.raw_log_input_version.retention_generation)
    .bind(key.raw_log_input_version.revision)
    .bind(key.adapter_semantic_version)
    .bind(key.schema_migration_count)
    .bind(key.schema_migration_max_version)
    .execute(connection)
    .await
    .with_context(|| {
        format!(
            "failed to publish completed startup adapter checkpoint for \
             {deployment_profile}/{chain}/{adapter}"
        )
    })?;
    Ok(())
}

async fn invalidate_startup_adapter_checkpoint(
    connection: &mut PgConnection,
    deployment_profile: &str,
    chain: &str,
    adapter: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        DELETE FROM normalized_replay_adapter_checkpoints
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
          AND adapter = $4
          AND checkpoint_scope = $5
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(STARTUP_ADAPTER_CURSOR_KIND)
    .bind(adapter)
    .bind(STARTUP_ADAPTER_CHECKPOINT_SCOPE)
    .execute(connection)
    .await
    .with_context(|| {
        format!(
            "failed to invalidate changed startup adapter checkpoint for \
             {deployment_profile}/{chain}/{adapter}"
        )
    })?;
    Ok(())
}

struct StartupMigrationLockGuard {
    connection: Option<PoolConnection<Postgres>>,
}

impl StartupMigrationLockGuard {
    async fn acquire(pool: &PgPool) -> Result<Self> {
        let mut guard = Self {
            connection: Some(
                pool.acquire()
                    .await
                    .context("failed to acquire startup migration-fence connection")?,
            ),
        };
        // SQLx takes this same session advisory lock before opening a migration
        // transaction. Startup always takes it before locking the migration
        // ledger or reading the checkpoint table. Therefore a migration can
        // never hold ACCESS EXCLUSIVE on the checkpoint table while waiting
        // behind startup's SHARE lock on `_sqlx_migrations`.
        Migrate::lock(guard.connection_mut())
            .await
            .context("failed to acquire SQLx migrator lock for startup adapter sync")?;
        Ok(guard)
    }

    fn connection_mut(&mut self) -> &mut PgConnection {
        self.connection
            .as_deref_mut()
            .expect("startup migration guard connection must be present")
    }

    async fn release(mut self) -> Result<()> {
        Migrate::unlock(self.connection_mut())
            .await
            .context("failed to release SQLx migrator lock for startup adapter sync")?;
        // Disarm Drop only after the session lock is gone. Cancellation while
        // awaiting unlock still closes the session instead of pooling it.
        drop(self.connection.take());
        Ok(())
    }
}

impl Drop for StartupMigrationLockGuard {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.as_mut() {
            connection.close_on_drop();
        }
    }
}

async fn begin_startup_adapter_fence<'a>(
    connection: &'a mut PgConnection,
    chain: &str,
) -> Result<sqlx::Transaction<'a, Postgres>> {
    let mut transaction = connection
        .begin()
        .await
        .context("failed to start startup adapter input-fence transaction")?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("raw_log_staging:{chain}"))
        .execute(transaction.as_mut())
        .await
        .with_context(|| format!("failed to fence startup raw-log mutation for {chain}"))?;
    sqlx::query("LOCK TABLE raw_logs IN ACCESS SHARE MODE")
        .execute(transaction.as_mut())
        .await
        .with_context(|| format!("failed to fence startup raw-log truncation for {chain}"))?;
    lock_canonical_lineage(transaction.as_mut(), chain).await?;
    Ok(transaction)
}

fn canonical_lineage_head_payload(head: Option<&StartupCanonicalLineageHead>) -> Value {
    json!(head)
}

fn prioritize_lock_release<T>(
    operation_result: Result<T>,
    release_result: Result<()>,
) -> Result<T> {
    match (operation_result, release_result) {
        (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        (Ok(value), Ok(())) => Ok(value),
    }
}

fn validate_identity(
    deployment_profile: &str,
    chain: &str,
    adapter: &str,
    adapter_semantic_version: i64,
) -> Result<()> {
    ensure!(
        !deployment_profile.trim().is_empty(),
        "startup adapter deployment profile must not be empty"
    );
    ensure!(
        !chain.trim().is_empty(),
        "startup adapter chain must not be empty"
    );
    ensure!(
        !adapter.trim().is_empty(),
        "startup adapter name must not be empty"
    );
    ensure!(
        adapter_semantic_version > 0,
        "startup adapter semantic version must be positive"
    );
    Ok(())
}

#[cfg(test)]
#[path = "startup_adapter_sync/tests.rs"]
mod tests;
