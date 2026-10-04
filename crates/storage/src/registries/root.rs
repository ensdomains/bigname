use anyhow::Result;
use sqlx::{PgPool, types::Uuid};

use crate::identity::ens_v2_registry_root_resource_id;

/// The root resource of the ENSv2 registry at one address.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RegistryRootResource {
    pub resource_id: Uuid,
    /// The registry's contract instance was deployed by an ENSv1→ENSv2 migration of a locked
    /// name, so it is a migration `WrapperRegistry`.
    pub migration_registry: bool,
}

/// The root resource of the registry at `address` as of the publication's `block`, derived from
/// the contract instance holding the address at that block exactly as the schema-v2 adapter
/// derives it. An instance admitted after the publication, or dropped as if it never existed,
/// does not count. `None` when no instance holds the address at `block`. The id is computed, not
/// looked up: an address that is not an ENSv2 registry yields an id that no grant carries.
///
/// The migration flag reads the instance's migration registry creation record only through its
/// readable lineage block and the matching canonical registry announcement edge, as the children
/// reader does, because the record's own canonicality is copied when it is written.
pub async fn load_registry_root_resource(
    pool: &PgPool,
    chain_id: &str,
    address: &str,
    block: i64,
) -> Result<Option<RegistryRootResource>> {
    let row: Option<(Uuid, bool)> = sqlx::query_as(
        "WITH holder AS (
             SELECT address.chain_id, address.contract_instance_id, address.active_from_block_number
             FROM bigname_phase.contract_instance_addresses address
             WHERE address.chain_id = $1 AND lower(address.address) = lower($2)
               AND address.deactivated_at IS NULL
               AND (address.active_from_block_number IS NULL OR address.active_from_block_number <= $3)
               AND (address.active_to_block_number IS NULL OR address.active_to_block_number >= $3)
             UNION ALL
             SELECT address.chain_id, address.contract_instance_id, address.active_from_block_number
             FROM bigname_phase.contract_instance_addresses address
             WHERE address.chain_id = $1 AND address.active_to_block_number >= $3
               AND address.deactivated_at IS NOT NULL
               AND lower(address.address) = lower($2)
               AND (address.active_from_block_number IS NULL OR address.active_from_block_number <= $3)
         )
         SELECT holder.contract_instance_id,
                EXISTS (
                    SELECT 1 FROM bigname_phase.migration_discovery_associations association
                    WHERE association.chain_id = holder.chain_id
                      AND association.registry_contract_instance_id = holder.contract_instance_id
                      AND association.consumer_visibility = 'activated'
                      AND association.block_number <= $3
                      AND EXISTS (
                          SELECT 1 FROM bigname_phase.chain_lineage lineage
                          WHERE lineage.chain_id = association.chain_id
                            AND lineage.block_hash = association.block_hash
                            AND lineage.block_number = association.block_number
                            AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized'))
                      AND EXISTS (
                          SELECT 1 FROM bigname_phase.discovery_edges edge
                          WHERE edge.chain_id = association.chain_id
                            AND edge.edge_kind = 'registry_announcement'
                            AND edge.to_contract_instance_id = association.registry_contract_instance_id
                            AND edge.source_manifest_id = association.source_manifest_id
                            AND (edge.active_from_block_number, edge.active_from_block_hash) =
                                (association.block_number, association.block_hash)
                            AND (edge.provenance ->> 'transaction_index')::bigint =
                                association.transaction_index
                            AND (edge.provenance ->> 'log_index')::bigint = association.log_index
                            AND edge.canonicality_state IN ('canonical', 'safe', 'finalized'))
                )
         FROM holder
         ORDER BY holder.active_from_block_number DESC NULLS LAST, holder.contract_instance_id
         LIMIT 1",
    )
    .bind(chain_id)
    .bind(address)
    .bind(block)
    .fetch_optional(pool)
    .await?;
    Ok(
        row.map(|(instance, migration_registry)| RegistryRootResource {
            resource_id: ens_v2_registry_root_resource_id(chain_id, instance),
            migration_registry,
        }),
    )
}
