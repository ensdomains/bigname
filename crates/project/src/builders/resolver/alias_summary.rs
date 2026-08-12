use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

pub(super) async fn stage(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    sample_limit: i32,
) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TEMP TABLE project_resolver_alias_summary ON COMMIT DROP AS
        WITH alias_candidates AS (
            SELECT candidate.*,
                   lower(COALESCE(
                       candidate.after_state ->> 'resolver',
                       candidate.before_state ->> 'resolver',
                       candidate.raw_fact_ref ->> 'emitting_address'
                   )) AS resolver_address,
                   COALESCE(
                       candidate.logical_name_id,
                       candidate.after_state ->> 'from_logical_name_id',
                       candidate.before_state ->> 'from_logical_name_id',
                       candidate.after_state ->> 'from_namehash',
                       candidate.before_state ->> 'from_namehash',
                       candidate.after_state ->> 'from_dns_encoded_name',
                       candidate.before_state ->> 'from_dns_encoded_name',
                       candidate.after_state ->> 'from_name',
                       candidate.before_state ->> 'from_name',
                       candidate.event_identity
                   ) AS alias_identity
            FROM project_events candidate
            WHERE candidate.event_kind = 'AliasChanged'
              AND candidate.chain_id = $1
        ),
        latest_aliases AS (
            SELECT alias_candidates.*,
                   row_number() OVER (
                       PARTITION BY resolver_address, alias_identity
                       ORDER BY block_number DESC NULLS LAST,
                                transaction_index DESC NULLS LAST,
                                log_index DESC NULLS LAST,
                                normalized_event_id DESC
                   ) AS latest_rank
            FROM alias_candidates
            WHERE resolver_address IS NOT NULL
        ),
        active_aliases AS (
            SELECT latest_aliases.*,
                   jsonb_strip_nulls(jsonb_build_object(
                       'logical_name_id', logical_name_id,
                       'resource_id', resource_id,
                       'binding_kind', 'resolver_alias_path',
                       'alias_state', COALESCE(
                           after_state -> 'alias_state', '"active"'::jsonb
                       ),
                       'active', COALESCE(after_state -> 'active', 'true'::jsonb),
                       'chain_id', chain_id,
                       'resolver_address', resolver_address,
                       'from_dns_encoded_name', after_state -> 'from_dns_encoded_name',
                       'to_dns_encoded_name', after_state -> 'to_dns_encoded_name',
                       'from_name', after_state -> 'from_name',
                       'to_name', after_state -> 'to_name',
                       'to_logical_name_id', after_state -> 'to_logical_name_id',
                       'to_resource_id', after_state -> 'to_resource_id',
                       'latest_event_kind', 'AliasChanged'
                   )) AS item
            FROM latest_aliases
            WHERE latest_rank = 1
              AND COALESCE((after_state ->> 'active')::boolean, true)
        ),
        ranked AS (
            SELECT active_aliases.*,
                   row_number() OVER (
                       PARTITION BY resolver_address
                       ORDER BY logical_name_id, normalized_event_id
                   ) AS sample_rank
            FROM active_aliases
        )
        SELECT resolver_address,
               count(*)::integer AS event_count,
               COALESCE(jsonb_agg(item ORDER BY logical_name_id, normalized_event_id)
                   FILTER (WHERE sample_rank <= $2), '[]'::jsonb) AS items
        FROM ranked
        GROUP BY resolver_address
        "#,
    )
    .bind(chain_id)
    .bind(sample_limit)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to group resolver aliases", error))?;
    Ok(())
}
