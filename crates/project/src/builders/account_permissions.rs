use sqlx::{Postgres, Transaction};

use crate::{Marker, ProjectError, Result};

pub(super) async fn build(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    sqlx::query(
        r#"
        WITH decoded AS (
            SELECT event.*,
                   lower(event.after_state #>> '{scope,authority_contract}') AS authority_contract,
                   (event.after_state #>> '{scope,authority_contract_instance_id}')::uuid AS authority_contract_instance_id,
                   lower(event.after_state #>> '{scope,owner}') AS owner,
                   lower(event.after_state ->> 'subject') AS subject,
                   event.after_state ->> 'relation_kind' AS relation_kind,
                   (event.after_state ->> 'approved')::boolean AS approved
            FROM project_events event
            WHERE event.event_kind = 'AccountPermissionChanged'
              AND event.after_state #>> '{scope,authority_kind}' = 'registry'
        ), ranked AS (
            SELECT event.*,
                   row_number() OVER (
                       PARTITION BY chain_id, authority_contract, owner, subject, relation_kind
                       ORDER BY block_number DESC, transaction_index DESC, log_index DESC,
                                normalized_event_id DESC
                   ) AS latest_rank,
                   jsonb_agg(to_jsonb(normalized_event_id)) OVER evidence AS event_ids,
                   jsonb_agg(raw_fact_ref) OVER evidence AS raw_fact_refs,
                   max(manifest_version) OVER evidence AS evidence_manifest_version
            FROM decoded event
            WINDOW evidence AS (
                PARTITION BY chain_id, authority_contract, owner, subject, relation_kind
                ORDER BY normalized_event_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )
        )
        INSERT INTO project_stage_account_permission_state_current (
            chain_id, authority_kind, authority_contract,
            authority_contract_instance_id, owner, subject, relation_kind, approved,
            effective_powers, grant_source, revocation_source, inheritance_path,
            transfer_behavior, provenance, chain_positions, canonicality_summary,
            manifest_version
        )
        SELECT chain_id, 'registry', authority_contract,
               authority_contract_instance_id, owner, subject, relation_kind, approved,
               after_state -> 'effective_powers', after_state -> 'grant_source',
               NULLIF(after_state -> 'revocation_source', 'null'::jsonb),
               after_state -> 'inheritance_path', after_state -> 'transfer_behavior',
               jsonb_build_object('normalized_event_ids', event_ids,
                   'raw_fact_refs', raw_fact_refs, 'chain_id', chain_id,
                   'derivation_kind', 'account_permission_state_rebuild'),
               jsonb_build_object('block_number', block_number, 'block_hash', block_hash,
                   'transaction_index', transaction_index, 'log_index', log_index,
                   'target_block_number', $2, 'target_block_hash', $3),
               jsonb_build_object('state', canonicality_state,
                   'target_block_number', $2, 'target_block_hash', $3),
               evidence_manifest_version
        FROM ranked
        WHERE latest_rank = 1
        ORDER BY chain_id, authority_contract, owner, subject, relation_kind
        "#,
    )
    .bind(chain_id)
    .bind(target.number)
    .bind(&target.hash)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to build account permissions", error))?;
    Ok(())
}
