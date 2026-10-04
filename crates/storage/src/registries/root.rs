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

/// The root resource of the registry at `address`, derived from the address's active contract
/// instance exactly as the schema-v2 adapter derives it. `None` when no active instance holds the
/// address. The id is computed, not looked up: an address that is not an ENSv2 registry yields an
/// id that no grant carries.
pub async fn load_registry_root_resource(
    pool: &PgPool,
    chain_id: &str,
    address: &str,
) -> Result<Option<RegistryRootResource>> {
    let row: Option<(Uuid, bool)> = sqlx::query_as(
        "SELECT address.contract_instance_id,
                EXISTS (
                    SELECT 1 FROM bigname_phase.migration_discovery_associations association
                    WHERE association.chain_id = address.chain_id
                      AND association.registry_contract_instance_id = address.contract_instance_id
                      AND association.consumer_visibility = 'activated'
                      AND association.canonicality_state IN ('canonical', 'safe', 'finalized')
                )
         FROM bigname_phase.contract_instance_addresses address
         WHERE address.chain_id = $1 AND lower(address.address) = lower($2)
           AND address.deactivated_at IS NULL",
    )
    .bind(chain_id)
    .bind(address)
    .fetch_optional(pool)
    .await?;
    Ok(
        row.map(|(instance, migration_registry)| RegistryRootResource {
            resource_id: ens_v2_registry_root_resource_id(chain_id, instance),
            migration_registry,
        }),
    )
}
