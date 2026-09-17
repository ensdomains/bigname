use serde_json::Value;
use sqlx::{Postgres, Transaction};

use crate::{LookupError, Result, error::database};

use super::{HeadRow, InventoryRow, NameRow};

pub(super) async fn load_name(
    transaction: &mut Transaction<'_, Postgres>,
    logical_name_id: &str,
) -> Result<NameRow> {
    sqlx::query_as::<_, NameRow>(
        r#"
        SELECT name.logical_name_id, name.namespace, name.raw_name, name.namehash,
               surface.dns_encoded_name, resource.chain_id AS resource_chain_id,
               name.declared_summary, name.provenance, name.chain_positions,
               name.xmin::text AS row_xmin
        FROM name_current name
        JOIN name_surfaces surface
          ON surface.logical_name_id = name.logical_name_id
        JOIN resources resource
          ON resource.resource_id = COALESCE(
              name.serving_resource_id, name.resource_id
          )
        LEFT JOIN surface_bindings binding
          ON binding.surface_binding_id = name.surface_binding_id
         AND binding.logical_name_id = name.logical_name_id
         AND binding.resource_id = name.resource_id
         AND binding.binding_kind = name.binding_kind
        LEFT JOIN token_lineages token_lineage
          ON token_lineage.token_lineage_id = name.token_lineage_id
        LEFT JOIN chain_lineage token_lineage_lineage
          ON token_lineage_lineage.chain_id = token_lineage.chain_id
         AND token_lineage_lineage.block_hash = token_lineage.block_hash
        JOIN chain_lineage surface_lineage
          ON surface_lineage.chain_id = surface.chain_id
         AND surface_lineage.block_hash = surface.block_hash
         AND surface_lineage.block_number = surface.block_number
        JOIN chain_lineage resource_lineage
          ON resource_lineage.chain_id = resource.chain_id
         AND resource_lineage.block_hash = resource.block_hash
         AND resource_lineage.block_number = resource.block_number
        LEFT JOIN chain_lineage binding_lineage
          ON binding_lineage.chain_id = binding.chain_id
         AND binding_lineage.block_hash = binding.block_hash
         AND binding_lineage.block_number = binding.block_number
        WHERE name.logical_name_id = $1
          AND name.support_status = 'supported'
          AND surface.visibility_state = 'active'
          AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND resource.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND (
              (
                  name.surface_binding_id IS NULL
                  AND name.resource_id IS NULL
                  AND name.binding_kind IS NULL
                  AND name.serving_resource_id IS NOT NULL
              )
              OR (
                  binding.canonicality_state IN ('canonical', 'safe', 'finalized')
                  AND binding.active_to IS NULL
                  AND binding_lineage.canonicality_state IN (
                      'canonical', 'safe', 'finalized'
                  )
              )
          )
          AND (
              name.token_lineage_id IS NULL
              OR (
                  token_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                  AND token_lineage_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
              )
          )
          AND surface_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND resource_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        "#,
    )
    .bind(logical_name_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database("load readable name projection"))?
    .ok_or_else(|| LookupError::unsupported("verified lookup name is not readable or supported"))
}

pub(super) async fn load_inventory(
    transaction: &mut Transaction<'_, Postgres>,
    resource_id: &str,
    boundary: &Value,
) -> Result<InventoryRow> {
    sqlx::query_as::<_, InventoryRow>(
        r#"
        SELECT inventory.resource_id::text AS resource_id,
               inventory.record_version_boundary_key, inventory.entries,
               inventory.provenance,
               CASE WHEN inventory.support_status = 'supported'
                   THEN jsonb_build_object('status', 'projected', 'exhaustiveness', 'not_asserted')
                   ELSE jsonb_build_object(
                       'status', 'unsupported', 'exhaustiveness', 'not_asserted',
                       'unsupported_reason', inventory.unsupported_reason
                   )
               END AS coverage,
               inventory.chain_positions, inventory.xmin::text AS row_xmin
        FROM record_inventory_current inventory
        JOIN resources resource
          ON resource.resource_id = inventory.resource_id
        JOIN chain_lineage resource_lineage
          ON resource_lineage.chain_id = resource.chain_id
         AND resource_lineage.block_hash = resource.block_hash
        WHERE inventory.resource_id = $1::uuid
          AND inventory.record_version_boundary = $2
          -- A cleared registration's history-only row serves no records. Its boundary anchors on
          -- the clearing ResolverChanged, so no caller boundary can equal it; the guard keeps the
          -- rule uniform with the record-serving read filter rather than resting on that.
          AND inventory.provenance ->> 'record_serving' IS DISTINCT FROM 'false'
          AND resource.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND resource_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        "#,
    )
    .bind(resource_id)
    .bind(boundary)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database("load indexed record answer"))?
    .ok_or_else(|| LookupError::unsupported("verified lookup requires an indexed record boundary"))
}

pub(super) async fn load_head(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<HeadRow> {
    sqlx::query_as::<_, HeadRow>(
        r#"
        SELECT head.chain_id, head.latest_block_hash AS block_hash,
               head.latest_block_number AS block_number,
               to_char(
                   lineage.block_timestamp AT TIME ZONE 'UTC',
                   'YYYY-MM-DD"T"HH24:MI:SS"Z"'
               ) AS timestamp
        FROM chain_heads head
        JOIN chain_lineage lineage
          ON lineage.chain_id = head.chain_id
         AND lineage.block_hash = head.latest_block_hash
         AND lineage.block_number = head.latest_block_number
        WHERE head.chain_id = $1
          AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        "#,
    )
    .bind(chain_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database("load lookup chain head"))?
    .ok_or_else(|| LookupError::stale(format!("chain {chain_id} has no readable latest head")))
}
