use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use uuid::Uuid;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct ResolverProfileProjectionInvalidationPlan {
    chains: Vec<String>,
    addresses: Vec<String>,
    resource_ids: Vec<Uuid>,
}

/// Capture readable current and historical binding keys before the adapter may
/// orphan events that cease to be admitted under the new profile.
pub(super) async fn load_resolver_profile_projection_invalidation_plan(
    pool: &sqlx::PgPool,
    targets_by_chain: &BTreeMap<String, BTreeSet<String>>,
) -> Result<ResolverProfileProjectionInvalidationPlan> {
    let mut plan = ResolverProfileProjectionInvalidationPlan::default();
    for (chain, chain_addresses) in targets_by_chain {
        for address in chain_addresses {
            plan.chains.push(chain.clone());
            plan.addresses.push(address.clone());
        }
    }
    if plan.chains.is_empty() {
        return Ok(plan);
    }

    plan.resource_ids = sqlx::query_scalar::<_, Uuid>(
        r#"
        WITH targets AS (
            SELECT DISTINCT chain_id, lower(contract_address) AS contract_address
            FROM unnest($1::TEXT[], $2::TEXT[])
                AS input(chain_id, contract_address)
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
        )
        SELECT resource_id
        FROM inventory_resources
        ORDER BY resource_id
        "#,
    )
    .bind(&plan.chains)
    .bind(&plan.addresses)
    .fetch_all(pool)
    .await
    .context("failed to capture resolver-profile projection invalidation keys")?;

    Ok(plan)
}

/// Enqueue the pre-adapter key plan only after normalized-event convergence is
/// durable. Profile-only changes do not necessarily create event changes from
/// which the worker could derive these keys.
pub(super) async fn enqueue_resolver_profile_projection_invalidations(
    pool: &sqlx::PgPool,
    plan: &ResolverProfileProjectionInvalidationPlan,
) -> Result<u64> {
    if plan.chains.is_empty() {
        return Ok(0);
    }

    sqlx::query(
        r#"
        WITH targets AS (
            SELECT DISTINCT chain_id, lower(contract_address) AS contract_address
            FROM unnest($1::TEXT[], $2::TEXT[])
                AS input(chain_id, contract_address)
        ),
        inventory_resources AS (
            SELECT DISTINCT resource_id
            FROM unnest($3::UUID[]) AS input(resource_id)
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
        INSERT INTO projection_invalidations (
            projection,
            projection_key,
            key_payload,
            invalidated_at,
            last_changed_at
        )
        SELECT projection, projection_key, key_payload, now(), now()
        FROM candidate_keys
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
        "#,
    )
    .bind(&plan.chains)
    .bind(&plan.addresses)
    .bind(&plan.resource_ids)
    .execute(pool)
    .await
    .context("failed to enqueue resolver-profile projection invalidations")
    .map(|result| result.rows_affected())
}
