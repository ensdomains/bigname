use anyhow::{Context, Result, ensure};
use sqlx::{PgConnection, PgPool};

const RECORD_INVENTORY_REPLAY_INDEX: &str =
    "normalized_events_record_inventory_resource_replay_idx";
const RECORD_INVENTORY_REPLAY_INDEX_REPAIR_LOCK_KEY: i64 = 0x4249474e414d4502_i64;

pub(super) async fn run_migrations_and_ensure_record_inventory_replay_index_ready(
    pool: &PgPool,
    migrator: &sqlx::migrate::Migrator,
) -> Result<()> {
    let mut connection = pool
        .acquire()
        .await
        .context("failed to acquire record-inventory replay index repair connection")?;
    loop {
        // A statement blocked in pg_advisory_lock can itself prevent a concurrent
        // index build from advancing. Poll without holding the advisory-lock wait.
        let acquired = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT CASE
                WHEN pg_try_advisory_lock($1) THEN TRUE
                ELSE (SELECT FALSE FROM pg_sleep(0.05))
            END
            "#,
        )
        .bind(RECORD_INVENTORY_REPLAY_INDEX_REPAIR_LOCK_KEY)
        .fetch_one(&mut *connection)
        .await
        .context("failed to try record-inventory replay index repair lock")?;
        if acquired {
            break;
        }
    }

    let migration_result = async {
        migrator
            .run_direct(&mut *connection)
            .await
            .context("failed to apply checked-in migrations")?;
        repair_record_inventory_replay_index(&mut connection).await
    }
    .await;
    let unlock_result = sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock($1)")
        .bind(RECORD_INVENTORY_REPLAY_INDEX_REPAIR_LOCK_KEY)
        .fetch_one(&mut *connection)
        .await
        .context("failed to release record-inventory replay index repair lock")
        .and_then(|released| {
            ensure!(
                released,
                "record-inventory replay index repair lock was not held"
            );
            Ok(())
        });

    match (migration_result, unlock_result) {
        (Err(error), _) | (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

async fn repair_record_inventory_replay_index(connection: &mut PgConnection) -> Result<()> {
    if record_inventory_replay_index_ready(connection).await? {
        return Ok(());
    }

    let index_exists = sqlx::query_scalar::<_, bool>("SELECT to_regclass($1) IS NOT NULL")
        .bind(RECORD_INVENTORY_REPLAY_INDEX)
        .fetch_one(&mut *connection)
        .await
        .context("failed to inspect record-inventory replay index relation")?;
    if index_exists {
        sqlx::query(&format!(
            "DROP INDEX CONCURRENTLY IF EXISTS {RECORD_INVENTORY_REPLAY_INDEX}"
        ))
        .execute(&mut *connection)
        .await
        .context("failed to drop unready record-inventory replay index")?;
    }

    sqlx::query(
        r#"
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
          AND event_kind IN ('RecordChanged', 'RecordVersionChanged', 'ResolverChanged')
          AND canonicality_state IN (
              'canonical'::public.canonicality_state,
              'safe'::public.canonicality_state,
              'finalized'::public.canonicality_state
          )
        "#,
    )
    .execute(&mut *connection)
    .await
    .context("failed to rebuild record-inventory replay index")?;

    ensure!(
        record_inventory_replay_index_ready(connection).await?,
        "record-inventory replay index remains unready after rebuild"
    );
    Ok(())
}

async fn record_inventory_replay_index_ready(connection: &mut PgConnection) -> Result<bool> {
    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM pg_index
            WHERE indexrelid = to_regclass($1)
              AND indrelid = 'public.normalized_events'::regclass
              AND indisvalid
              AND indisready
        )
        "#,
    )
    .bind(RECORD_INVENTORY_REPLAY_INDEX)
    .fetch_one(connection)
    .await
    .context("failed to inspect record-inventory replay index readiness")
}
