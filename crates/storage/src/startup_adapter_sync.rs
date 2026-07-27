use anyhow::{Context, Result, ensure};
use sqlx::{Acquire, PgConnection, PgPool, Postgres, Row, migrate::Migrate, pool::PoolConnection};

use crate::{
    RawLogStagingInputVersion,
    raw_staging_revision::raw_log_staging_block_range_changed_since_from_connection,
};

mod checkpoint;
mod lineage;

use checkpoint::{
    completed_checkpoint_matches, downgrade_completed_checkpoint_to_boundary_resume,
    invalidate_completed_startup_adapter_checkpoint, invalidate_startup_adapter_checkpoint,
    publish_completed_checkpoint,
};
use lineage::{
    CompletedLineageExtentDecision, completed_lineage_extent_decision,
    load_startup_adapter_lineage_state_from_connection, lock_canonical_lineage,
};
pub use lineage::{
    StartupAdapterLineageState, StartupCanonicalLineageHead, load_startup_adapter_lineage_state,
};

pub const STARTUP_ADAPTER_CURSOR_KIND: &str = "startup_adapter_owned_raw_log_state";
pub const STARTUP_ADAPTER_CHECKPOINT_SCOPE: &str = "startup_adapter_sync";
pub const STARTUP_DISCOVERY_ADMISSION_EPOCH_FIELD: &str = "startup_discovery_admission_epoch";
pub const STARTUP_CANONICAL_LINEAGE_HEAD_FIELD: &str = "startup_canonical_lineage_head";
pub const STARTUP_LINEAGE_MUTATION_REVISION_FIELD: &str = "startup_lineage_mutation_revision";
pub const STARTUP_LINEAGE_SCAN_EXTENT_FIELD: &str = "startup_lineage_scan_extent";

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartupAdapterLineageTailPolicy {
    ReuseCompleted,
    ResumeFromScannedExtent,
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
    ResumeFromScannedExtent,
}

pub async fn prepare_startup_adapter_sync(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
    adapter: &str,
    adapter_semantic_version: i64,
    lineage_tail_policy: StartupAdapterLineageTailPolicy,
) -> Result<StartupAdapterSyncDecision> {
    validate_identity(deployment_profile, chain, adapter, adapter_semantic_version)?;
    let mut migration_guard = StartupMigrationLockGuard::acquire(pool).await?;
    let operation_result = async {
        let mut transaction =
            begin_startup_adapter_fence(migration_guard.connection_mut(), chain).await?;
        let key =
            load_startup_adapter_sync_key(transaction.as_mut(), chain, adapter_semantic_version)
                .await?;
        let lineage_decision = match key.as_ref() {
            Some(key) => {
                completed_checkpoint_matches(
                    transaction.as_mut(),
                    deployment_profile,
                    chain,
                    adapter,
                    key,
                    lineage_tail_policy,
                )
                .await?
            }
            None => CompletedLineageExtentDecision::Reject,
        };
        let decision = match lineage_decision {
            CompletedLineageExtentDecision::ReuseCompleted => {
                StartupAdapterSyncDecision::ReuseCompleted
            }
            CompletedLineageExtentDecision::ResumeFromScannedExtent => {
                let downgraded = downgrade_completed_checkpoint_to_boundary_resume(
                    transaction.as_mut(),
                    deployment_profile,
                    chain,
                    adapter,
                    key.as_ref()
                        .expect("a reusable completed checkpoint must have a current key"),
                )
                .await?;
                if !downgraded {
                    invalidate_completed_startup_adapter_checkpoint(
                        transaction.as_mut(),
                        deployment_profile,
                        chain,
                        adapter,
                    )
                    .await?;
                }
                StartupAdapterSyncDecision::RunFullSync {
                    started_key: key.clone(),
                }
            }
            CompletedLineageExtentDecision::Reject => {
                invalidate_completed_startup_adapter_checkpoint(
                    transaction.as_mut(),
                    deployment_profile,
                    chain,
                    adapter,
                )
                .await?;
                StartupAdapterSyncDecision::RunFullSync {
                    started_key: key.clone(),
                }
            }
        };
        transaction
            .commit()
            .await
            .context("failed to finish startup adapter checkpoint verification")?;

        Ok(decision)
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
    lineage_tail_policy: StartupAdapterLineageTailPolicy,
) -> Result<StartupAdapterSyncCompletion> {
    validate_identity(deployment_profile, chain, adapter, adapter_semantic_version)?;
    let mut migration_guard = StartupMigrationLockGuard::acquire(pool).await?;
    let operation_result = async {
        let mut transaction =
            begin_startup_adapter_fence(migration_guard.connection_mut(), chain).await?;
        let Some(started_key) = started_key else {
            // A private ENSv1 checkpoint may have become `completed` after the
            // outer prepare observed an unknown key. Delete the exact startup
            // scope while every input is fenced so that row cannot become
            // trusted if the missing key component appears later.
            invalidate_startup_adapter_checkpoint(
                transaction.as_mut(),
                deployment_profile,
                chain,
                adapter,
            )
            .await?;
            transaction
                .commit()
                .await
                .context("failed to fence unknown startup adapter completion")?;
            return Ok(StartupAdapterSyncCompletion::KeyUnknown);
        };
        let current_key =
            load_startup_adapter_sync_key(transaction.as_mut(), chain, adapter_semantic_version)
                .await?;
        let Some(current_key) = current_key else {
            invalidate_startup_adapter_checkpoint(
                transaction.as_mut(),
                deployment_profile,
                chain,
                adapter,
            )
            .await?;
            transaction
                .commit()
                .await
                .context("failed to finish changed startup adapter input check")?;
            return Ok(StartupAdapterSyncCompletion::KeyUnknown);
        };
        let non_extent_key_matches = current_key.raw_log_input_version.retention_generation
            == started_key.raw_log_input_version.retention_generation
            && current_key.discovery_admission_epoch == started_key.discovery_admission_epoch
            && current_key.adapter_semantic_version == started_key.adapter_semantic_version
            && current_key.schema_migration_count == started_key.schema_migration_count
            && current_key.schema_migration_max_version == started_key.schema_migration_max_version;
        let scanned_through_block = started_key
            .canonical_lineage_head
            .as_ref()
            .map_or(0, |head| head.block_number);
        let raw_log_extent_reusable = if non_extent_key_matches {
            !raw_log_staging_block_range_changed_since_from_connection(
                transaction.as_mut(),
                chain,
                started_key.raw_log_input_version.revision,
                0,
                scanned_through_block,
            )
            .await?
        } else {
            false
        };
        let lineage_decision = if raw_log_extent_reusable {
            completed_lineage_extent_decision(
                transaction.as_mut(),
                chain,
                started_key.lineage_mutation_revision,
                started_key.canonical_lineage_head.as_ref(),
                started_key.canonical_lineage_head.as_ref(),
                &StartupAdapterLineageState {
                    mutation_revision: current_key.lineage_mutation_revision,
                    canonical_lineage_head: current_key.canonical_lineage_head.clone(),
                },
                lineage_tail_policy,
            )
            .await?
        } else {
            CompletedLineageExtentDecision::Reject
        };
        match lineage_decision {
            CompletedLineageExtentDecision::Reject => {
                invalidate_startup_adapter_checkpoint(
                    transaction.as_mut(),
                    deployment_profile,
                    chain,
                    adapter,
                )
                .await?;
                transaction
                    .commit()
                    .await
                    .context("failed to finish changed startup adapter input check")?;
                return Ok(StartupAdapterSyncCompletion::InputChanged);
            }
            CompletedLineageExtentDecision::ResumeFromScannedExtent => {
                let downgraded = downgrade_completed_checkpoint_to_boundary_resume(
                    transaction.as_mut(),
                    deployment_profile,
                    chain,
                    adapter,
                    &current_key,
                )
                .await?;
                if !downgraded {
                    invalidate_startup_adapter_checkpoint(
                        transaction.as_mut(),
                        deployment_profile,
                        chain,
                        adapter,
                    )
                    .await?;
                    transaction
                        .commit()
                        .await
                        .context("failed to reset unavailable startup boundary resume")?;
                    return Ok(StartupAdapterSyncCompletion::InputChanged);
                }
                transaction
                    .commit()
                    .await
                    .context("failed to publish startup adapter boundary resume")?;
                return Ok(StartupAdapterSyncCompletion::ResumeFromScannedExtent);
            }
            CompletedLineageExtentDecision::ReuseCompleted => {}
        }

        publish_completed_checkpoint(
            transaction.as_mut(),
            deployment_profile,
            chain,
            adapter,
            &current_key,
            started_key.canonical_lineage_head.as_ref(),
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
