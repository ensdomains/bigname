use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

/// Copies every NameWrapper holder row to the operators that holder approved with
/// `setApprovalForAll`, because `canModifyName` and the ERC-1155-fuse approve and transfer
/// checks accept an operator exactly as the holder. Account state is the staged rows for changed
/// keys plus the live rows for unchanged keys, so incremental builds see approvals outside the
/// window; a full rebuild has every key staged.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L214-L222 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L37-L47 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L105-L117 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L137-L150 @ ens_v1@91c966f)
pub(super) async fn build(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    full_rebuild: bool,
) -> Result<()> {
    sqlx::query(
        r#"/* project:builders.permissions.wrapper_operators.create_wrapper_operator_rows */
        CREATE TEMP TABLE project_wrapper_operator_rows ON COMMIT DROP AS
        WITH account_state AS (
            SELECT staged.authority_contract, staged.owner, staged.subject, staged.approved,
                   staged.provenance, staged.chain_positions, staged.manifest_version
            FROM project_stage_account_permission_state_current staged
            WHERE staged.chain_id = $1 AND staged.authority_kind = 'wrapper'
            UNION ALL
            SELECT live.authority_contract, live.owner, live.subject, live.approved,
                   live.provenance, live.chain_positions, live.manifest_version
            FROM account_permission_state_current live
            WHERE NOT $2
              AND live.chain_id = $1
              AND live.authority_kind = 'wrapper'
              AND NOT EXISTS (
                  SELECT 1 FROM project_scope_account_permissions scope
                  WHERE scope.chain_id = live.chain_id
                    AND scope.authority_kind = live.authority_kind
                    AND scope.authority_contract = live.authority_contract
                    AND scope.owner = live.owner
                    AND scope.subject = live.subject
                    AND scope.relation_kind = live.relation_kind
              )
        ),
        holders AS (
            SELECT holder.*
            FROM project_stage_permissions_current holder
            WHERE holder.grant_source ->> 'authority_kind' = 'wrapper'
              AND holder.grant_source ->> 'relation_kind' = 'holder'
        )
        SELECT holder.resource_id,
               operator.subject,
               holder.scope,
               holder.scope_kind,
               holder.scope_detail,
               holder.effective_powers,
               (holder.grant_source - 'relation_kind' - 'source_event_kind') || jsonb_build_object(
                   'relation_kind', 'operator',
                   'source_event_kind', 'ApprovalForAll',
                   'owner', holder.subject
               ) AS grant_source,
               holder.inheritance_path,
               jsonb_build_object(
                   'mode', 'owner_scoped',
                   'on_holder_change', 'ceases_to_apply'
               ) AS transfer_behavior,
               holder.provenance || jsonb_build_object(
                   'derivation_kind', 'wrapper_operator_fanout',
                   'holder', holder.subject,
                   'operator_normalized_event_ids', operator.provenance -> 'normalized_event_ids',
                   'operator_raw_fact_refs', operator.provenance -> 'raw_fact_refs'
               ) AS provenance,
               CASE
                   WHEN (operator.chain_positions ->> 'block_number')::bigint >
                        COALESCE((holder.chain_positions ->> 'block_number')::bigint, -1)
                       THEN (operator.chain_positions - 'target_block_number' - 'target_block_hash')
                            || jsonb_strip_nulls(jsonb_build_object(
                                'target_block_number', holder.chain_positions -> 'target_block_number',
                                'target_block_hash', holder.chain_positions -> 'target_block_hash'))
                   ELSE holder.chain_positions
               END AS chain_positions,
               holder.canonicality_summary,
               GREATEST(holder.manifest_version, operator.manifest_version) AS manifest_version
        FROM holders holder
        JOIN account_state operator
          ON operator.approved
         AND operator.owner = holder.subject
         AND operator.authority_contract = lower(holder.grant_source ->> 'authority_contract')
        "#,
    )
    .bind(chain_id)
    .bind(full_rebuild)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to derive wrapper operator rows", error))?;

    // An operator who is also the token delegate keeps the operator set, which is a superset.
    sqlx::query(
        r#"/* project:builders.permissions.wrapper_operators.update_stage_permissions_current */
        UPDATE project_stage_permissions_current existing
        SET effective_powers = operator.effective_powers,
            grant_source = operator.grant_source,
            transfer_behavior = operator.transfer_behavior,
            provenance = operator.provenance || jsonb_build_object(
                'superseded_relation_kind', existing.grant_source ->> 'relation_kind'),
            manifest_version = GREATEST(existing.manifest_version, operator.manifest_version)
        FROM project_wrapper_operator_rows operator
        WHERE existing.resource_id = operator.resource_id
          AND existing.subject = operator.subject
          AND existing.scope = operator.scope
        "#,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to merge wrapper operator rows", error))?;

    sqlx::query(
        r#"/* project:builders.permissions.wrapper_operators.insert_stage_permissions_current */
        INSERT INTO project_stage_permissions_current (
            resource_id, subject, scope, scope_kind, scope_detail,
            effective_powers, grant_source, revocation_source,
            inheritance_path, transfer_behavior, provenance,
            chain_positions, canonicality_summary, manifest_version
        )
        SELECT operator.resource_id, operator.subject, operator.scope, operator.scope_kind,
               operator.scope_detail, operator.effective_powers, operator.grant_source, NULL,
               operator.inheritance_path, operator.transfer_behavior, operator.provenance,
               operator.chain_positions, operator.canonicality_summary, operator.manifest_version
        FROM project_wrapper_operator_rows operator
        WHERE NOT EXISTS (
            SELECT 1 FROM project_stage_permissions_current existing
            WHERE existing.resource_id = operator.resource_id
              AND existing.subject = operator.subject
              AND existing.scope = operator.scope
        )
        ORDER BY operator.resource_id, operator.subject, operator.scope
        "#,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to publish wrapper operator rows", error))?;
    Ok(())
}
