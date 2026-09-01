use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

pub(super) async fn include_changed_declaration_winners(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    // Keep this EXISTS aligned with builders/resolver/declaration_precedence.rs: a declaration
    // applies when an active `resolver` edge reaches its address and the edge's manifest has the
    // same namespace. The builder filters `active_discovery_admissions.source_family` only for
    // its separate non-declaration row, so that column must not filter this check.
    sqlx::query(
        "WITH winning_declaration AS (
             SELECT DISTINCT ON (declaration.resolver_address)
                    declaration.resolver_address,
                    declaration.namespace AS classification_admission_namespace,
                    declaration.manifest_id,
                    declaration.manifest_event_id
             FROM project_declared_resolver_addresses declaration
             WHERE EXISTS (
                 SELECT 1
                 FROM discovery_edges edge
                 JOIN contract_instance_addresses address
                   ON address.contract_instance_id = edge.to_contract_instance_id
                  AND address.chain_id = edge.chain_id
                 JOIN project_manifests origin
                   ON origin.manifest_id = edge.source_manifest_id
                 WHERE edge.chain_id = $1
                   AND edge.edge_kind = 'resolver'
                   AND lower(address.address) = declaration.resolver_address
                   AND origin.namespace = declaration.namespace
                   AND edge.canonicality_state IN ('canonical', 'safe', 'finalized')
                   AND (edge.active_from_block_number IS NULL
                        OR edge.active_from_block_number <= $2)
                   AND (edge.active_to_block_number IS NULL
                        OR edge.active_to_block_number > $2)
                   AND edge.deactivated_at IS NULL
                   AND (edge.active_from_block_hash IS NULL OR EXISTS (
                       SELECT 1 FROM chain_lineage lineage
                       WHERE lineage.chain_id = edge.chain_id
                         AND lineage.block_number = edge.active_from_block_number
                         AND lineage.block_hash = edge.active_from_block_hash
                         AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                   ))
                   AND (address.active_from_block_number IS NULL
                        OR address.active_from_block_number <= $2)
                   AND (address.active_to_block_number IS NULL
                        OR address.active_to_block_number > $2)
                   AND address.deactivated_at IS NULL
                   AND (address.active_from_block_hash IS NULL OR EXISTS (
                       SELECT 1 FROM chain_lineage lineage
                       WHERE lineage.chain_id = address.chain_id
                         AND lineage.block_number = address.active_from_block_number
                         AND lineage.block_hash = address.active_from_block_hash
                         AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                   ))
             )
             ORDER BY declaration.resolver_address,
                      declaration.source_family,
                      declaration.manifest_id
         )
         INSERT INTO project_scope_resolver_dependents
         SELECT winner.resolver_address
         FROM winning_declaration winner
         LEFT JOIN resolver_current live
           ON live.chain_id = $1
          AND lower(live.resolver_address) = winner.resolver_address
         WHERE live.resolver_address IS NULL
            OR (live.provenance ->> 'manifest_id')::bigint IS DISTINCT FROM
               winner.manifest_id
            OR live.provenance ->> 'manifest_event_id' IS DISTINCT FROM
               winner.manifest_event_id::text
            OR live.provenance ->> 'classification_admission_namespace' IS DISTINCT FROM
               winner.classification_admission_namespace
         UNION
         SELECT lower(live.resolver_address)
         FROM resolver_current live
         LEFT JOIN winning_declaration winner
           ON winner.resolver_address = lower(live.resolver_address)
         WHERE live.chain_id = $1
           AND live.provenance ->> 'classification_admission_namespace' IS NOT NULL
           AND winner.resolver_address IS NULL
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .bind(target_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database(
            "failed to scope changed resolver declaration winners",
            error,
        )
    })?;
    Ok(())
}
