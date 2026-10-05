use anyhow::Result;
use sqlx::{PgConnection, types::Uuid};

use crate::identity::ens_v2_registry_root_resource_id;

/// The root resource of the ENSv2 registry at one address.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RegistryRootResource {
    pub resource_id: Uuid,
    /// A manifest declares the instance as an ENSv2 root or registry contract. A registry that
    /// discovery admitted runs code bigname does not read.
    pub manifest_declared: bool,
}

/// The root resource of the registry at `address` as of the publication's `block`, derived from
/// the contract instance holding the address at that block exactly as the schema-v2 adapter
/// derives it. An instance admitted after the publication, or dropped as if it never existed,
/// does not count. `None` when no instance holds the address at `block`. The id is computed, not
/// looked up: an address that is not an ENSv2 registry yields an id that no grant carries.
/// Callers pass the request's snapshot connection so the identity rows and the permission rows
/// are read at the same point.
pub async fn load_registry_root_resource(
    conn: &mut PgConnection,
    chain_id: &str,
    address: &str,
    block: i64,
) -> Result<Option<RegistryRootResource>> {
    let row: Option<(Uuid, bool)> = sqlx::query_as(
        "WITH holder AS (
             SELECT address.chain_id, address.contract_instance_id, address.address,
                    address.active_from_block_number
             FROM bigname_phase.contract_instance_addresses address
             WHERE address.chain_id = $1 AND lower(address.address) = lower($2)
               AND address.deactivated_at IS NULL
               AND (address.active_from_block_number IS NULL OR address.active_from_block_number <= $3)
               AND (address.active_to_block_number IS NULL OR address.active_to_block_number >= $3)
             UNION ALL
             SELECT address.chain_id, address.contract_instance_id, address.address,
                    address.active_from_block_number
             FROM bigname_phase.contract_instance_addresses address
             WHERE address.chain_id = $1 AND address.active_to_block_number >= $3
               AND address.deactivated_at IS NOT NULL
               AND lower(address.address) = lower($2)
               AND (address.active_from_block_number IS NULL OR address.active_from_block_number <= $3)
         )
         SELECT holder.contract_instance_id,
                EXISTS (
                    SELECT 1
                    FROM bigname_phase.manifest_contract_instances declaration
                    JOIN bigname_phase.manifest_versions manifest
                      ON manifest.manifest_id = declaration.manifest_id
                     AND manifest.chain_id = declaration.chain_id
                    WHERE declaration.chain_id = holder.chain_id
                      AND declaration.contract_instance_id = holder.contract_instance_id
                      AND lower(declaration.declared_address) = lower(holder.address)
                      AND declaration.role IN ('root_registry', 'registry')
                      AND manifest.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
                )
         FROM holder
         ORDER BY holder.active_from_block_number DESC NULLS LAST, holder.contract_instance_id
         LIMIT 1",
    )
    .bind(chain_id)
    .bind(address)
    .bind(block)
    .fetch_optional(conn)
    .await?;
    Ok(
        row.map(|(instance, manifest_declared)| RegistryRootResource {
            resource_id: ens_v2_registry_root_resource_id(chain_id, instance),
            manifest_declared,
        }),
    )
}
