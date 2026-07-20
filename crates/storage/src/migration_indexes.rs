use std::sync::LazyLock;

use anyhow::{Context, Result, ensure};
use sqlx::{
    Either, Executor, PgConnection, PgPool, Postgres,
    pool::PoolConnection,
    postgres::{PgAdvisoryLock, PgAdvisoryLockGuard, PgAdvisoryLockKey},
};

pub const RECORD_INVENTORY_REPLAY_INDEX: &str =
    "normalized_events_record_inventory_resource_replay_idx";
const ACTIVE_CONTRACT_ADDRESS_INDEX: &str = "contract_instance_addresses_active_lower_address_idx";
const HISTORICAL_CONTRACT_ADDRESS_INDEX: &str =
    "contract_instance_addresses_historical_lower_address_idx";
const NON_ORPHANED_RAW_CODE_LOWER_ADDRESS_INDEX: &str =
    "raw_code_hashes_non_orphaned_lower_address_idx";

#[derive(Clone, Copy)]
struct RequiredIndexDescriptor {
    name: &'static str,
    table: &'static str,
    create_concurrently_sql: &'static str,
}

const RECORD_INVENTORY_REPLAY_INDEX_DESCRIPTOR: RequiredIndexDescriptor = RequiredIndexDescriptor {
    name: RECORD_INVENTORY_REPLAY_INDEX,
    table: "public.normalized_events",
    create_concurrently_sql: r#"
            CREATE INDEX CONCURRENTLY normalized_events_record_inventory_resource_replay_idx
            ON public.normalized_events (
                resource_id,
                block_number,
                log_index NULLS FIRST,
                normalized_event_id
            )
            WHERE resource_id IS NOT NULL
              AND logical_name_id IS NOT NULL
              AND chain_id IS NOT NULL
              AND block_number IS NOT NULL
              AND block_hash IS NOT NULL
              AND derivation_kind IN (
                  'ens_v1_unwrapped_authority',
                  'ens_v2_registry_resource_surface',
                  'ens_v2_resolver'
              )
              AND event_kind IN (
                  'RecordChanged',
                  'RecordVersionChanged',
                  'ResolverChanged'
              )
              AND canonicality_state IN (
                  'canonical'::public.canonicality_state,
                  'safe'::public.canonicality_state,
                  'finalized'::public.canonicality_state
              )
        "#,
};

const REQUIRED_WATCH_LOOKUP_INDEXES: &[RequiredIndexDescriptor] = &[
    RequiredIndexDescriptor {
        name: ACTIVE_CONTRACT_ADDRESS_INDEX,
        table: "public.contract_instance_addresses",
        create_concurrently_sql: r#"
            CREATE INDEX CONCURRENTLY contract_instance_addresses_active_lower_address_idx
            ON public.contract_instance_addresses (chain_id, LOWER(address))
            WHERE deactivated_at IS NULL
        "#,
    },
    RequiredIndexDescriptor {
        name: HISTORICAL_CONTRACT_ADDRESS_INDEX,
        table: "public.contract_instance_addresses",
        create_concurrently_sql: r#"
            CREATE INDEX CONCURRENTLY contract_instance_addresses_historical_lower_address_idx
            ON public.contract_instance_addresses (chain_id, LOWER(address))
            WHERE deactivated_at IS NOT NULL
              AND active_to_block_number IS NOT NULL
        "#,
    },
    RequiredIndexDescriptor {
        name: NON_ORPHANED_RAW_CODE_LOWER_ADDRESS_INDEX,
        table: "public.raw_code_hashes",
        create_concurrently_sql: r#"
            CREATE INDEX CONCURRENTLY raw_code_hashes_non_orphaned_lower_address_idx
            ON public.raw_code_hashes (chain_id, LOWER(contract_address))
            WHERE canonicality_state <> 'orphaned'::public.canonicality_state
        "#,
    },
];

#[derive(Clone, Copy)]
struct NormalizedEventIndexDescriptor {
    name: &'static str,
    create_concurrently_sql: &'static str,
}

macro_rules! define_runtime_rebuilt_normalized_event_indexes {
    ($( $name:literal => $create_concurrently_sql:literal ),+ $(,)?) => {
        const RUNTIME_REBUILT_NORMALIZED_EVENT_INDEXES: &[NormalizedEventIndexDescriptor] = &[
            $(NormalizedEventIndexDescriptor {
                name: $name,
                create_concurrently_sql: $create_concurrently_sql,
            }),+
        ];

        pub const DEFERRED_NORMALIZED_EVENT_INDEXES: &[&str] = &[
            $($name),+,
            RECORD_INVENTORY_REPLAY_INDEX,
        ];
    };
}

define_runtime_rebuilt_normalized_event_indexes! {
    "normalized_events_namespace_idx" => r#"
        CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_namespace_idx
        ON normalized_events (namespace, normalized_event_id DESC)
    "#,
    "normalized_events_kind_idx" => r#"
        CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_kind_idx
        ON normalized_events (event_kind, normalized_event_id DESC)
    "#,
    "normalized_events_manifest_idx" => r#"
        CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_manifest_idx
        ON normalized_events (source_manifest_id, event_kind, normalized_event_id DESC)
        WHERE source_manifest_id IS NOT NULL
    "#,
    "normalized_events_chain_position_idx" => r#"
        CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_chain_position_idx
        ON normalized_events (chain_id, block_number DESC, normalized_event_id DESC)
        WHERE block_number IS NOT NULL
    "#,
    "normalized_events_name_projection_replay_idx" => r#"
        CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_name_projection_replay_idx
        ON normalized_events (
            logical_name_id,
            block_number DESC NULLS LAST,
            chain_id ASC NULLS LAST,
            block_hash DESC NULLS LAST,
            transaction_hash DESC NULLS LAST,
            log_index DESC NULLS LAST,
            event_identity DESC
        )
        WHERE logical_name_id IS NOT NULL
          AND canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
    "#,
    "normalized_events_resource_projection_replay_idx" => r#"
        CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_resource_projection_replay_idx
        ON normalized_events (
            resource_id,
            block_number DESC NULLS LAST,
            chain_id ASC NULLS LAST,
            block_hash DESC NULLS LAST,
            transaction_hash DESC NULLS LAST,
            log_index DESC NULLS LAST,
            event_identity DESC
        )
        WHERE resource_id IS NOT NULL
          AND canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
    "#,
    "normalized_events_name_relevant_projection_idx" => r#"
        CREATE INDEX CONCURRENTLY IF NOT EXISTS normalized_events_name_relevant_projection_idx
        ON normalized_events (
            logical_name_id,
            block_number ASC NULLS FIRST,
            log_index ASC NULLS LAST,
            event_identity ASC
        )
        WHERE logical_name_id IS NOT NULL
          AND canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
    "#,
}
pub const TEMPORARY_NORMALIZED_REPLAY_INDEXES: &[&str] = &[
    "normalized_events_replay_latest_resolver_tmp_idx",
    "normalized_events_replay_latest_record_version_tmp_idx",
    "normalized_events_replay_latest_registrar_tmp_idx",
];
const NORMALIZED_REPLAY_INDEX_DDL_LOCK_KEY: i64 = 0x4249474e414d4502_i64;
static NORMALIZED_REPLAY_INDEX_DDL_LOCK: LazyLock<PgAdvisoryLock> = LazyLock::new(|| {
    PgAdvisoryLock::with_key(PgAdvisoryLockKey::BigInt(
        NORMALIZED_REPLAY_INDEX_DDL_LOCK_KEY,
    ))
});

/// Cross-process fence for normalized-replay index deferral, restoration, and
/// post-migration repair. The guard queues an unlock before its pooled
/// connection is returned even when a caller exits through an error path.
pub struct NormalizedReplayIndexDdlGuard {
    guard: PgAdvisoryLockGuard<'static, PoolConnection<Postgres>>,
}

impl NormalizedReplayIndexDdlGuard {
    /// Reuse the fenced connection for catalog reads and DDL that must remain
    /// serialized with normalized-replay index changes.
    pub fn connection_mut(&mut self) -> &mut PgConnection {
        &mut self.guard
    }

    pub async fn count_unready_normalized_event_indexes(
        &mut self,
        indexes: &[&str],
    ) -> Result<i64> {
        count_unready_normalized_event_indexes_with(self.connection_mut(), indexes).await
    }

    pub async fn normalized_event_index_is_ready(&mut self, index: &str) -> Result<bool> {
        Ok(self
            .count_unready_normalized_event_indexes(&[index])
            .await?
            == 0)
    }

    /// Drop every normalized-event index deferred during a fresh replay.
    /// The same descriptor set owns runtime restoration, so names and DDL
    /// cannot drift between the two paths.
    pub async fn defer_deferred_normalized_event_indexes(&mut self) -> Result<()> {
        for index in RUNTIME_REBUILT_NORMALIZED_EVENT_INDEXES {
            sqlx::query(&format!("DROP INDEX IF EXISTS {}", index.name))
                .execute(self.connection_mut())
                .await
                .with_context(|| {
                    format!("failed to defer normalized-event index {}", index.name)
                })?;
        }
        self.defer_record_inventory_replay_index().await
    }

    /// Restore every deferred normalized-event index from the storage-owned
    /// runtime descriptor set, repairing invalid catalog entries first.
    pub async fn ensure_deferred_normalized_event_indexes_ready(&mut self) -> Result<()> {
        for index in RUNTIME_REBUILT_NORMALIZED_EVENT_INDEXES {
            if index_relation_exists(self.connection_mut(), index.name).await?
                && !self.normalized_event_index_is_ready(index.name).await?
            {
                sqlx::query(&format!("DROP INDEX CONCURRENTLY IF EXISTS {}", index.name))
                    .execute(self.connection_mut())
                    .await
                    .with_context(|| {
                        format!(
                            "failed to drop unready normalized-event index {}",
                            index.name
                        )
                    })?;
            }
        }

        for index in RUNTIME_REBUILT_NORMALIZED_EVENT_INDEXES {
            sqlx::query(index.create_concurrently_sql)
                .execute(self.connection_mut())
                .await
                .with_context(|| {
                    format!("failed to rebuild normalized-event index {}", index.name)
                })?;
        }
        self.ensure_record_inventory_replay_index_ready().await
    }

    pub async fn defer_record_inventory_replay_index(&mut self) -> Result<()> {
        sqlx::query(&format!(
            "DROP INDEX IF EXISTS {RECORD_INVENTORY_REPLAY_INDEX}"
        ))
        .execute(self.connection_mut())
        .await
        .context("failed to defer record-inventory replay index")?;
        Ok(())
    }

    pub async fn ensure_record_inventory_replay_index_ready(&mut self) -> Result<()> {
        ensure_record_inventory_replay_index_ready(self.connection_mut()).await
    }

    pub async fn release(self) -> Result<()> {
        self.guard
            .release_now()
            .await
            .context("failed to release normalized replay index DDL lock")?;
        Ok(())
    }
}

async fn index_relation_exists(connection: &mut PgConnection, index: &str) -> Result<bool> {
    sqlx::query_scalar::<_, bool>("SELECT to_regclass($1) IS NOT NULL")
        .bind(index)
        .fetch_one(connection)
        .await
        .with_context(|| format!("failed to inspect index relation {index}"))
}

pub async fn acquire_normalized_replay_index_ddl_guard(
    pool: &PgPool,
) -> Result<NormalizedReplayIndexDdlGuard> {
    let mut connection = pool
        .acquire()
        .await
        .context("failed to acquire normalized replay index DDL lock connection")?;
    loop {
        // A statement blocked in pg_advisory_lock can itself prevent a
        // concurrent index build from advancing. Poll without holding an
        // advisory-lock wait so CREATE INDEX CONCURRENTLY can finish.
        match NORMALIZED_REPLAY_INDEX_DDL_LOCK
            .try_acquire(connection)
            .await
            .context("failed to try normalized replay index DDL lock")?
        {
            Either::Left(guard) => return Ok(NormalizedReplayIndexDdlGuard { guard }),
            Either::Right(mut unlocked) => {
                sqlx::query("SELECT pg_sleep(0.05)")
                    .execute(&mut *unlocked)
                    .await
                    .context("failed while polling normalized replay index DDL lock")?;
                connection = unlocked;
            }
        }
    }
}

pub async fn count_unready_normalized_event_indexes(
    pool: &PgPool,
    indexes: &[&str],
) -> Result<i64> {
    count_unready_normalized_event_indexes_with(pool, indexes).await
}

fn prioritize_operation_error<T>(
    operation_result: Result<T>,
    release_result: Result<()>,
) -> Result<T> {
    match (operation_result, release_result) {
        (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        (Ok(value), Ok(())) => Ok(value),
    }
}

async fn count_unready_normalized_event_indexes_with<'e, E>(
    executor: E,
    indexes: &[&str],
) -> Result<i64>
where
    E: Executor<'e, Database = Postgres>,
{
    count_unready_indexes_with(executor, indexes, "public.normalized_events")
        .await
        .context("failed to inspect normalized-event index readiness")
}

async fn count_unready_indexes_with<'e, E>(
    executor: E,
    indexes: &[&str],
    table: &str,
) -> Result<i64>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM unnest($1::TEXT[]) AS required(index_name)
        WHERE NOT EXISTS (
            SELECT 1
            FROM pg_index AS index
            WHERE index.indexrelid = to_regclass(required.index_name)
              AND index.indrelid = to_regclass($2)
              AND index.indisvalid
              AND index.indisready
        )
        "#,
    )
    .bind(indexes)
    .bind(table)
    .fetch_one(executor)
    .await
    .with_context(|| format!("failed to inspect readiness for indexes on {table}"))
}

pub(super) async fn run_migrations_and_ensure_required_indexes_ready(
    pool: &PgPool,
    migrator: &sqlx::migrate::Migrator,
) -> Result<()> {
    let mut guard = acquire_normalized_replay_index_ddl_guard(pool).await?;

    let migration_result = async {
        migrator
            .run_direct(guard.connection_mut())
            .await
            .context("failed to apply checked-in migrations")?;
        if !record_inventory_replay_index_ready(guard.connection_mut()).await?
            && !normalized_replay_indexes_intentionally_deferred(guard.connection_mut()).await?
        {
            guard.ensure_record_inventory_replay_index_ready().await?;
        }
        ensure_required_watch_lookup_indexes_ready(guard.connection_mut()).await?;
        Ok(())
    }
    .await;
    let unlock_result = guard.release().await;

    prioritize_operation_error(migration_result, unlock_result)
}

async fn ensure_record_inventory_replay_index_ready(connection: &mut PgConnection) -> Result<()> {
    ensure_required_index_ready(connection, &RECORD_INVENTORY_REPLAY_INDEX_DESCRIPTOR).await
}

async fn normalized_replay_indexes_intentionally_deferred(
    connection: &mut PgConnection,
) -> Result<bool> {
    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM unnest($1::TEXT[]) AS temporary_index(index_name)
            WHERE to_regclass(temporary_index.index_name) IS NOT NULL
        )
        "#,
    )
    .bind(TEMPORARY_NORMALIZED_REPLAY_INDEXES)
    .fetch_one(connection)
    .await
    .context("failed to inspect intentional normalized replay index deferral markers")
}

async fn record_inventory_replay_index_ready(connection: &mut PgConnection) -> Result<bool> {
    required_index_ready(
        connection,
        RECORD_INVENTORY_REPLAY_INDEX_DESCRIPTOR.name,
        RECORD_INVENTORY_REPLAY_INDEX_DESCRIPTOR.table,
    )
    .await
}

async fn ensure_required_watch_lookup_indexes_ready(connection: &mut PgConnection) -> Result<()> {
    for index in REQUIRED_WATCH_LOOKUP_INDEXES {
        ensure_required_index_ready(connection, index).await?;
    }
    Ok(())
}

async fn ensure_required_index_ready(
    connection: &mut PgConnection,
    index: &RequiredIndexDescriptor,
) -> Result<()> {
    if required_index_ready(connection, index.name, index.table).await? {
        return Ok(());
    }

    if index_relation_exists(connection, index.name).await? {
        sqlx::query(&format!("DROP INDEX CONCURRENTLY IF EXISTS {}", index.name))
            .execute(&mut *connection)
            .await
            .with_context(|| format!("failed to drop unready index {}", index.name))?;
    }

    sqlx::query(index.create_concurrently_sql)
        .execute(&mut *connection)
        .await
        .with_context(|| format!("failed to rebuild index {}", index.name))?;

    ensure!(
        required_index_ready(connection, index.name, index.table).await?,
        "index {} remains unready after rebuild",
        index.name
    );
    Ok(())
}

async fn required_index_ready(
    connection: &mut PgConnection,
    index: &str,
    table: &str,
) -> Result<bool> {
    let indexes = [index];
    Ok(
        count_unready_indexes_with(&mut *connection, &indexes, table)
            .await
            .with_context(|| format!("failed to inspect index readiness for {index}"))?
            == 0,
    )
}

#[cfg(test)]
mod tests;
