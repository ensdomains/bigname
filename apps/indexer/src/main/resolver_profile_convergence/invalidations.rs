use anyhow::{Context, Result, ensure};
use serde_json::Value;
use sqlx::{Connection, PgConnection, Postgres, QueryBuilder};
use uuid::Uuid;

const INVALIDATION_PAGE_SIZE: usize = 1_000;

/// Stream every projection key from the exact staged target set before adapter
/// publication can orphan normalized events used to derive record inventories.
pub(super) async fn stage_resolver_profile_projection_invalidations(
    pool: &sqlx::PgPool,
    run_id: Uuid,
    chain: &str,
) -> Result<()> {
    let cursor_name = format!("resolver_profile_invalidations_{}", run_id.simple());
    let declare_cursor = invalidation_cursor_sql(run_id, &cursor_name);
    let mut connection = pool
        .acquire()
        .await
        .context("failed to acquire resolver-profile invalidation capture connection")?;
    let mut transaction = connection
        .begin()
        .await
        .context("failed to begin resolver-profile invalidation capture")?;
    sqlx::query(&declare_cursor)
        .execute(transaction.as_mut())
        .await
        .with_context(|| {
            format!("failed to declare resolver-profile invalidation cursor for {chain}")
        })?;
    // WITH HOLD materializes one stable pre-adapter key stream while allowing
    // each staging mutation below to commit as its own bounded statement.
    transaction
        .commit()
        .await
        .context("failed to materialize resolver-profile invalidation cursor")?;

    let capture_result =
        stage_invalidation_cursor_pages(connection.as_mut(), &cursor_name, chain).await;
    let close_result = sqlx::query(&format!("CLOSE {cursor_name}"))
        .execute(connection.as_mut())
        .await
        .context("failed to close resolver-profile invalidation cursor");
    if let Err(error) = capture_result {
        let _ = close_result;
        return Err(error);
    }
    close_result?;
    Ok(())
}

fn invalidation_cursor_sql(run_id: Uuid, cursor_name: &str) -> String {
    format!(
        r#"
        DECLARE {cursor_name} NO SCROLL CURSOR WITH HOLD FOR
        WITH targets AS (
            SELECT run.chain_id, target.resolver_address
            FROM resolver_profile_reconciliation_targets target
            JOIN resolver_profile_reconciliation_runs run
              ON run.run_id = target.run_id
            WHERE target.run_id = '{run_id}'::UUID
        ),
        bound_names AS (
            SELECT DISTINCT event.logical_name_id
            FROM normalized_events event
            JOIN LATERAL (
                VALUES
                    (event.before_state ->> 'resolver'),
                    (event.after_state ->> 'resolver')
            ) resolver(resolver_address) ON TRUE
            JOIN targets target
              ON target.chain_id = event.chain_id
             AND target.resolver_address = lower(resolver.resolver_address)
            WHERE event.event_kind = 'ResolverChanged'
              AND event.logical_name_id IS NOT NULL
              AND event.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
        ),
        inventory_resources AS (
            SELECT DISTINCT event.resource_id
            FROM normalized_events event
            JOIN bound_names name
              ON name.logical_name_id = event.logical_name_id
            JOIN resources resource
              ON resource.resource_id = event.resource_id
            WHERE event.resource_id IS NOT NULL
              AND event.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
              AND resource.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )

            UNION

            SELECT DISTINCT binding.resource_id
            FROM surface_bindings binding
            JOIN bound_names name
              ON name.logical_name_id = binding.logical_name_id
            JOIN resources resource
              ON resource.resource_id = binding.resource_id
            WHERE binding.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
              AND resource.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
        ),
        candidate_keys AS (
            SELECT
                target.chain_id,
                'resolver_current'::TEXT AS projection,
                target.chain_id || ':' || target.resolver_address AS projection_key,
                jsonb_build_object(
                    'chain_id', target.chain_id,
                    'resolver_address', target.resolver_address
                ) AS key_payload
            FROM targets target

            UNION

            SELECT
                target_chain.chain_id,
                'record_inventory_current'::TEXT AS projection,
                resource_id::TEXT AS projection_key,
                jsonb_build_object('resource_id', resource_id::TEXT) AS key_payload
            FROM inventory_resources
            CROSS JOIN (
                SELECT DISTINCT chain_id
                FROM targets
            ) target_chain
        )
        SELECT chain_id, projection, projection_key, key_payload
        FROM candidate_keys
        ORDER BY projection, projection_key
        "#,
    )
}

async fn stage_invalidation_cursor_pages(
    connection: &mut PgConnection,
    cursor_name: &str,
    chain: &str,
) -> Result<()> {
    loop {
        let rows = sqlx::query_as::<_, (String, String, String, Value)>(&format!(
            "FETCH FORWARD {INVALIDATION_PAGE_SIZE} FROM {cursor_name}"
        ))
        .fetch_all(&mut *connection)
        .await
        .context("failed to fetch resolver-profile invalidation key page")?;
        if rows.is_empty() {
            return Ok(());
        }
        ensure!(
            rows.iter().all(|(row_chain, _, _, _)| row_chain == chain),
            "resolver-profile invalidation cursor crossed its requested chain boundary"
        );
        let mut builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO resolver_profile_reconciliation_invalidation_keys \
             (chain_id, projection, projection_key, key_payload) ",
        );
        builder.push_values(&rows, |mut row, (chain, projection, key, payload)| {
            row.push_bind(chain)
                .push_bind(projection)
                .push_bind(key)
                .push_bind(payload);
        });
        builder.push(
            " ON CONFLICT (chain_id, projection, projection_key) \
             DO UPDATE SET key_payload = EXCLUDED.key_payload",
        );
        builder
            .build()
            .execute(&mut *connection)
            .await
            .with_context(|| {
                format!("failed to stage resolver-profile projection invalidation page on {chain}")
            })?;
    }
}

/// Publish and remove each staged key page atomically only after the adapter's
/// matching chain-context reconciliation is durable.
pub(super) async fn publish_resolver_profile_projection_invalidations(
    pool: &sqlx::PgPool,
    chain: &str,
) -> Result<u64> {
    let mut published_count = 0u64;
    loop {
        let (page_count, deleted_count) = sqlx::query_as::<_, (i64, i64)>(
            r#"
            WITH candidate_page AS MATERIALIZED (
                SELECT projection, projection_key, key_payload
                FROM resolver_profile_reconciliation_invalidation_keys
                WHERE chain_id = $1
                ORDER BY projection, projection_key
                LIMIT $2
            ),
            upserted AS (
                INSERT INTO projection_invalidations (
                    projection,
                    projection_key,
                    key_payload,
                    invalidated_at,
                    last_changed_at
                )
                SELECT projection, projection_key, key_payload, now(), now()
                FROM candidate_page
                ON CONFLICT (projection, projection_key)
                DO UPDATE SET
                    key_payload = EXCLUDED.key_payload,
                    generation = projection_invalidations.generation + 1,
                    invalidated_at = EXCLUDED.invalidated_at,
                    last_changed_at = EXCLUDED.last_changed_at,
                    claim_token = NULL,
                    claimed_at = NULL,
                    last_failure_reason = NULL,
                    last_failure_at = NULL
                RETURNING 1
            ),
            deleted AS (
                DELETE FROM resolver_profile_reconciliation_invalidation_keys staged
                USING candidate_page candidate
                WHERE staged.chain_id = $1
                  AND staged.projection = candidate.projection
                  AND staged.projection_key = candidate.projection_key
                RETURNING 1
            )
            SELECT
                (SELECT COUNT(*)::BIGINT FROM upserted),
                (SELECT COUNT(*)::BIGINT FROM deleted)
            "#,
        )
        .bind(chain)
        .bind(i64::try_from(INVALIDATION_PAGE_SIZE)?)
        .fetch_one(pool)
        .await
        .context("failed to publish staged resolver-profile projection invalidation page")?;
        ensure!(
            page_count == deleted_count,
            "resolver-profile invalidation publication staged {page_count} rows but deleted {deleted_count}"
        );
        if page_count == 0 {
            return Ok(published_count);
        }
        published_count = published_count
            .checked_add(u64::try_from(page_count)?)
            .context("resolver-profile invalidation count overflowed u64")?;
    }
}
