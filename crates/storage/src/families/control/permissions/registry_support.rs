//! Exact factory-origin and uninterrupted-code recognition at the served publication.
//! This is deliberately independent of permission grants and migration membership.
use anyhow::{Context, Result};
use sqlx::PgConnection;
use uuid::Uuid;

use crate::{families::name::FamilyPublication, identity::ens_v2_registry_root_resource_id};

// Explicit reviewed implementation identities; manifest roles alone cannot establish a model.
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/WrapperRegistryImpl.json:L2 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/UserRegistryImpl.json:L2 @ ens_v2_sepolia_20261001@07e55a05)
const WRAPPER: &str = "0xbe768b63e5fbbfbb0ae97e9064e0002df8001880";
const USER: &str = "0x9bd8a88719068d09ecee662f36c0e3856708366a";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Model {
    Declared,
    User,
    Wrapper,
}

#[derive(Clone, Debug)]
pub(super) struct SupportedRegistry {
    pub root: Uuid,
    pub namespace: String,
    pub model: Model,
}

// Declaration support is intentionally address/code specific, not every future `registry` role.
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/RootRegistry.json:L2 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/ETHRegistry.json:L2 @ ens_v2_sepolia_20261001@07e55a05)
const HOLDER: &str = "/* storage:families.control.permissions.registry_support_holder */
WITH holder AS (
    SELECT address.* FROM bigname_phase.contract_instance_addresses address
    WHERE address.chain_id = $1 AND lower(address.address) = $2 AND address.deactivated_at IS NULL
      AND (address.active_from_block_number IS NULL OR address.active_from_block_number <= $3)
      AND (address.active_to_block_number IS NULL OR address.active_to_block_number >= $3)
    UNION ALL
    SELECT address.* FROM bigname_phase.contract_instance_addresses address
    WHERE address.chain_id = $1 AND lower(address.address) = $2 AND address.deactivated_at IS NOT NULL
      AND address.active_to_block_number >= $3
      AND (address.active_from_block_number IS NULL OR address.active_from_block_number <= $3)
)
SELECT holder.contract_instance_id, (
    SELECT manifest.namespace
    FROM bigname_phase.manifest_contract_instances declaration
    JOIN bigname_phase.manifest_versions manifest
      ON manifest.manifest_id = declaration.manifest_id AND manifest.chain_id = declaration.chain_id
    WHERE declaration.chain_id = holder.chain_id
      AND declaration.contract_instance_id = holder.contract_instance_id
      AND declaration.manifest_id = COALESCE(holder.source_manifest_id,
          (holder.provenance ->> 'manifest_id')::bigint)
      AND holder.provenance ->> 'source' = 'manifest_declaration'
      AND holder.deactivated_at IS NULL
      AND lower(declaration.declared_address) = lower(holder.address)
      AND declaration.proxy_kind = 'none'
      AND declaration.start_block_number <= $3
      AND manifest.rollout_status = 'active'
      AND manifest.deployment_label = 'ens_v2_sepolia_20261001'
      AND ((manifest.source_family = 'ens_v2_root_l1' AND declaration.role = 'root_registry'
            AND lower(holder.address) = '0xb458d6a3a77919449d03e7a6903c26827c1ec43f')
        OR (manifest.source_family = 'ens_v2_registry_l1' AND declaration.role = 'registry'
            AND lower(holder.address) = '0xd4ebcbbdf463c9c45784603db0ddd499bc44a8b4'))
    LIMIT 1) AS declared_namespace
FROM holder
ORDER BY holder.active_from_block_number DESC NULLS LAST, holder.contract_instance_id
LIMIT 1";

// Read at most two physical origins before validating them: ambiguous history is not a proof.
// Initial Upgraded and RegistryCreated can precede the factory log in the same transaction.
// The announcement may also be later, when initialization was deliberately delayed.
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/build-info/solc-0_8_25-b30e6dc9a03b37f6a0b89af5d02a73d3993944f7.json:L1561 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/build-info/solc-0_8_25-b30e6dc9a03b37f6a0b89af5d02a73d3993944f7.json:L1564 @ ens_v2_sepolia_20261001@07e55a05)
const ORIGINS: &str = "/* storage:families.control.permissions.registry_support_origins */
SELECT origin.namespace, lower(origin.after_state ->> 'implementation'), origin.block_number,
    EXISTS (
        SELECT 1 FROM bigname_phase.manifest_versions manifest
        JOIN bigname_phase.manifest_contract_instances factory
          ON factory.manifest_id = manifest.manifest_id AND factory.chain_id = manifest.chain_id
        JOIN bigname_phase.manifest_contract_instances implementation
          ON implementation.manifest_id = manifest.manifest_id
         AND implementation.chain_id = manifest.chain_id
        WHERE manifest.manifest_id = origin.source_manifest_id
          AND manifest.manifest_version = origin.manifest_version
          AND manifest.chain_id = origin.chain_id AND manifest.namespace = origin.namespace
          AND manifest.source_family = origin.source_family AND manifest.rollout_status = 'active'
          AND manifest.deployment_label = 'ens_v2_sepolia_20261001'
          AND factory.role = 'verifiable_factory' AND factory.proxy_kind = 'none'
          AND factory.start_block_number <= origin.block_number
          AND lower(factory.declared_address) = lower(origin.raw_fact_ref ->> 'emitting_address')
          AND lower(factory.declared_address) = '0xda70306c98e97ece36f997a21368e53298572991'
          AND implementation.proxy_kind = 'none'
          AND implementation.start_block_number <= origin.block_number
          AND lower(implementation.declared_address) = lower(origin.after_state ->> 'implementation')
          AND ((implementation.role = 'wrapper_registry_implementation'
                AND lower(implementation.declared_address) = '0xbe768b63e5fbbfbb0ae97e9064e0002df8001880')
            OR (implementation.role = 'user_registry_implementation'
                AND lower(implementation.declared_address) = '0x9bd8a88719068d09ecee662f36c0e3856708366a'))
          AND EXISTS (
              SELECT 1 FROM bigname_phase.contract_instance_addresses address
              WHERE address.chain_id = factory.chain_id
                AND address.contract_instance_id = factory.contract_instance_id
                AND lower(address.address) = lower(factory.declared_address)
                AND address.source_manifest_id = manifest.manifest_id
                AND address.provenance ->> 'source' = 'manifest_declaration'
                AND address.active_from_block_number <= origin.block_number
                AND (address.active_to_block_number IS NULL OR address.active_to_block_number >= origin.block_number)
                AND (address.deactivated_at IS NULL OR address.active_to_block_number >= origin.block_number))
    ) AND EXISTS (
        SELECT 1 FROM bigname_phase.discovery_edges admission
        JOIN bigname_phase.chain_lineage lineage
          ON lineage.chain_id = admission.chain_id
         AND lineage.block_hash = admission.active_from_block_hash
         AND lineage.block_number = admission.active_from_block_number
        JOIN bigname_phase.manifest_versions manifest
          ON manifest.manifest_id = admission.source_manifest_id AND manifest.chain_id = admission.chain_id
        WHERE admission.chain_id = $1 AND admission.from_contract_instance_id = $4
          AND admission.to_contract_instance_id = $4
          AND admission.edge_kind = 'registry_announcement'
          AND admission.discovery_source = 'RegistryCreated'
          AND lower(admission.provenance ->> 'emitting_address') = $2
          AND admission.active_from_block_number BETWEEN origin.block_number AND $3
          AND (admission.active_to_block_number IS NULL OR admission.active_to_block_number >= $3)
          AND (admission.deactivated_at IS NULL OR admission.active_to_block_number >= $3)
          AND admission.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND manifest.source_family = 'ens_v2_registry_l1'
          AND manifest.namespace = origin.namespace AND manifest.rollout_status = 'active'
          AND manifest.deployment_label = 'ens_v2_sepolia_20261001'
    ) AS valid
FROM bigname_phase.normalized_events origin
JOIN bigname_phase.chain_lineage lineage
  ON lineage.chain_id = origin.chain_id AND lineage.block_hash = origin.block_hash
 AND lineage.block_number = origin.block_number
WHERE origin.chain_id = $1 AND lower(origin.after_state ->> 'proxy_address') = $2
  AND origin.source_family = 'ens_v2_migration_l1' AND origin.event_kind = 'ContractDiscovered'
  AND origin.consumer_visibility = 'activated'
  AND origin.after_state ->> 'source_event' = 'ContractDiscovered'
  AND origin.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND origin.block_number <= $3
ORDER BY origin.block_number, origin.transaction_index, origin.log_index, origin.event_identity COLLATE \"C\"
LIMIT 2";

// Literal predicates, matching the sparse indexes even under a generic prepared plan. No
// consumer_visibility predicate: a canonical physical departure disqualifies either way.
const DEPARTURES: &str = "/* storage:families.control.permissions.registry_support_departure */
SELECT EXISTS (
    SELECT 1 FROM bigname_phase.normalized_events event
    JOIN bigname_phase.chain_lineage lineage
      ON lineage.chain_id = event.chain_id AND lineage.block_hash = event.block_hash
     AND lineage.block_number = event.block_number
    WHERE event.chain_id = $1 AND lower(event.after_state ->> 'proxy_address') = $2
      AND event.source_family = 'ens_v2_registry_l1' AND event.event_kind = 'Upgraded'
      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND event.block_number BETWEEN $3 AND $4
      AND lower(event.after_state ->> 'implementation') IS DISTINCT FROM ";

pub(super) async fn load(
    conn: &mut PgConnection,
    publication: &FamilyPublication,
    registry: &str,
) -> Result<Option<SupportedRegistry>> {
    let holder: Option<(Uuid, Option<String>)> = sqlx::query_as(HOLDER)
        .bind(&publication.chain_id)
        .bind(registry)
        .bind(publication.block_number)
        .fetch_optional(&mut *conn)
        .await
        .context("failed to load the supported registry's address interval")?;
    let Some((instance, declared)) = holder else {
        return Ok(None);
    };
    let root = ens_v2_registry_root_resource_id(&publication.chain_id, instance);
    if let Some(namespace) = declared {
        return Ok(Some(SupportedRegistry {
            root,
            namespace,
            model: Model::Declared,
        }));
    }
    let origins: Vec<(String, Option<String>, i64, bool)> = sqlx::query_as(ORIGINS)
        .bind(&publication.chain_id)
        .bind(registry)
        .bind(publication.block_number)
        .bind(instance)
        .fetch_all(&mut *conn)
        .await
        .context("failed to read the registry's factory origin")?;
    let [(namespace, implementation, creation, true)] = origins.as_slice() else {
        return Ok(None);
    };
    let (model, implementation) = match implementation.as_deref() {
        Some(WRAPPER) => (Model::Wrapper, WRAPPER),
        Some(USER) => (Model::User, USER),
        _ => return Ok(None),
    };
    let departed: bool = sqlx::query_scalar(&format!("{DEPARTURES}'{implementation}')"))
        .bind(&publication.chain_id)
        .bind(registry)
        .bind(creation)
        .bind(publication.block_number)
        .fetch_one(conn)
        .await
        .context("failed to check the registry's implementation history")?;
    Ok((!departed).then(|| SupportedRegistry {
        root,
        namespace: namespace.clone(),
        model,
    }))
}
