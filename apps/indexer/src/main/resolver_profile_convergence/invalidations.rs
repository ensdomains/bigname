use anyhow::{Context, Result, ensure};
use uuid::Uuid;

const INVALIDATION_PAGE_SIZE: i64 = 1_000;

/// Persist one bounded page of projection keys before adapter publication can
/// orphan normalized events used to derive the affected record inventories.
pub(super) async fn stage_resolver_profile_projection_invalidations(
    pool: &sqlx::PgPool,
    run_id: Uuid,
    chain: &str,
    addresses: &[String],
) -> Result<()> {
    if addresses.is_empty() {
        return Ok(());
    }

    sqlx::query(
        r#"
        WITH input_addresses AS (
            SELECT DISTINCT lower(address) AS resolver_address
            FROM unnest($3::TEXT[]) AS input(address)
        ),
        targets AS (
            SELECT $2::TEXT AS chain_id, target.resolver_address AS contract_address
            FROM resolver_profile_reconciliation_targets target
            JOIN input_addresses input
              ON input.resolver_address = target.resolver_address
            WHERE target.run_id = $1
        ),
        bound_names AS (
            SELECT DISTINCT event.logical_name_id
            FROM normalized_events event
            JOIN targets target
              ON target.chain_id = event.chain_id
            CROSS JOIN LATERAL (
                VALUES
                    (event.before_state ->> 'resolver'),
                    (event.after_state ->> 'resolver')
            ) resolver(resolver_address)
            WHERE event.event_kind = 'ResolverChanged'
              AND event.logical_name_id IS NOT NULL
              AND event.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
              AND lower(resolver.resolver_address) = target.contract_address
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
                'resolver_current'::TEXT AS projection,
                target.chain_id || ':' || target.contract_address AS projection_key,
                jsonb_build_object(
                    'chain_id', target.chain_id,
                    'resolver_address', target.contract_address
                ) AS key_payload
            FROM targets target

            UNION

            SELECT
                'record_inventory_current'::TEXT AS projection,
                resource_id::TEXT AS projection_key,
                jsonb_build_object('resource_id', resource_id::TEXT) AS key_payload
            FROM inventory_resources
        )
        INSERT INTO resolver_profile_reconciliation_invalidation_keys (
            run_id,
            projection,
            projection_key,
            key_payload
        )
        SELECT $1, projection, projection_key, key_payload
        FROM candidate_keys
        ON CONFLICT (run_id, projection, projection_key) DO NOTHING
        "#,
    )
    .bind(run_id)
    .bind(chain)
    .bind(addresses)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to stage resolver-profile projection invalidations for {} targets on {chain}",
            addresses.len()
        )
    })?;
    Ok(())
}

/// Publish staged projection keys only after the adapter has durably completed
/// the matching chain-context reconciliation.
pub(super) async fn publish_resolver_profile_projection_invalidations(
    pool: &sqlx::PgPool,
    run_id: Uuid,
) -> Result<u64> {
    let mut after_projection = None::<String>;
    let mut after_projection_key = None::<String>;
    let mut published_count = 0u64;
    loop {
        let (next_projection, next_projection_key, page_count) =
            sqlx::query_as::<_, (Option<String>, Option<String>, i64)>(
                r#"
                WITH candidate_page AS MATERIALIZED (
                    SELECT projection, projection_key, key_payload
                    FROM resolver_profile_reconciliation_invalidation_keys
                    WHERE run_id = $1
                      AND (
                          $2::TEXT IS NULL
                          OR (projection, projection_key) > ($2::TEXT, $3::TEXT)
                      )
                    ORDER BY projection, projection_key
                    LIMIT $4
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
                )
                SELECT
                    (
                        SELECT projection
                        FROM candidate_page
                        ORDER BY projection DESC, projection_key DESC
                        LIMIT 1
                    ),
                    (
                        SELECT projection_key
                        FROM candidate_page
                        ORDER BY projection DESC, projection_key DESC
                        LIMIT 1
                    ),
                    (SELECT COUNT(*)::BIGINT FROM upserted)
                "#,
            )
            .bind(run_id)
            .bind(after_projection.as_deref())
            .bind(after_projection_key.as_deref())
            .bind(INVALIDATION_PAGE_SIZE)
            .fetch_one(pool)
            .await
            .context("failed to publish staged resolver-profile projection invalidation page")?;

        match (next_projection, next_projection_key) {
            (None, None) => {
                ensure!(
                    page_count == 0,
                    "empty resolver-profile invalidation page unexpectedly published rows"
                );
                return Ok(published_count);
            }
            (Some(projection), Some(projection_key)) => {
                ensure!(
                    page_count > 0,
                    "non-empty resolver-profile invalidation page published no rows"
                );
                published_count = published_count
                    .checked_add(u64::try_from(page_count)?)
                    .context("resolver-profile invalidation count overflowed u64")?;
                after_projection = Some(projection);
                after_projection_key = Some(projection_key);
            }
            _ => {
                anyhow::bail!(
                    "resolver-profile invalidation page returned an incomplete key cursor"
                );
            }
        }
    }
}
