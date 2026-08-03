use serde_json::{Map, Value};
use sqlx::{FromRow, PgPool, Postgres, Transaction};

use crate::{
    BASE_MAINNET_CHAIN_ID, BASENAMES_EXECUTION_SOURCE_FAMILY, BASENAMES_L1_RESOLVER_ROLE,
    BASENAMES_NAMESPACE, ENS_EXECUTION_SOURCE_FAMILY, ENS_NAMESPACE, ENS_UNIVERSAL_RESOLVER_ROLE,
    ENS_V1_REGISTRY_SOURCE_FAMILY, ETHEREUM_MAINNET_CHAIN_ID, LookupError, LookupRequest,
    RecordSelector, Result, abi::ResolutionResultAbi, call::ExecutionBlock, error::database,
};

mod indexed;
mod manifests;
mod persistence;
mod positions;
mod topology;
#[cfg(test)]
pub(crate) use indexed::answer as indexed_answer;
pub(crate) use persistence::persist_comparisons;

#[derive(Clone, Debug)]
pub(crate) struct LookupSnapshot {
    pub logical_name_id: String,
    pub name: String,
    pub dns_name: Vec<u8>,
    pub node: [u8; 32],
    pub resolver_chain_id: String,
    pub resolver_address: String,
    pub entrypoint_chain_id: String,
    pub entrypoint_address: String,
    pub execution_block: ExecutionBlock,
    pub follow_ccip: bool,
    pub result_abi: ResolutionResultAbi,
    pub observed_positions: Value,
    comparison: Option<IndexedComparison>,
}

#[derive(Clone, Debug)]
pub(super) struct IndexedComparison {
    pub resource_id: String,
    pub boundary_key: String,
    pub row_xmin: String,
    pub entries: Value,
}

#[derive(Clone, Debug)]
pub(crate) struct EnsPrimaryNameAuthority {
    pub registry_address: String,
    pub universal_resolver_address: String,
    pub block_number: i64,
    pub block_hash: String,
}

impl LookupSnapshot {
    pub fn indexed_answer(&self, selector: &RecordSelector) -> Option<Value> {
        self.comparison
            .as_ref()
            .map(|comparison| indexed::answer(&comparison.entries, selector))
    }
}

#[derive(FromRow)]
struct NameRow {
    logical_name_id: String,
    namespace: String,
    raw_name: String,
    namehash: String,
    dns_encoded_name: Vec<u8>,
    resource_chain_id: String,
    declared_summary: Value,
    chain_positions: Value,
}

#[derive(FromRow)]
struct InventoryRow {
    resource_id: String,
    record_version_boundary_key: String,
    entries: Value,
    chain_positions: Value,
    row_xmin: String,
}

#[derive(Clone, Debug, FromRow)]
struct HeadRow {
    chain_id: String,
    block_hash: String,
    block_number: i64,
}

pub(crate) async fn load_snapshot(
    pool: &PgPool,
    request: &LookupRequest,
) -> Result<LookupSnapshot> {
    let mut transaction = pool.begin().await.map_err(database("start lookup read"))?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *transaction)
        .await
        .map_err(database("set lookup read isolation"))?;
    let name = load_name(&mut transaction, &request.logical_name_id).await?;
    let topology = name
        .declared_summary
        .get("topology")
        .filter(|value| value.is_object())
        .ok_or_else(|| LookupError::unsupported("verified lookup requires projected topology"))?;
    let path_class = topology::ensure_supported_execution_path(
        &name.namespace,
        &name.logical_name_id,
        topology,
    )?;
    let (resolver_chain_id, resolver_address) = selected_resolver(topology)?;
    if resolver_chain_id != name.resource_chain_id {
        return Err(LookupError::unsupported(
            "projected resolver and indexed authority object are on different chains",
        ));
    }
    let boundary = topology
        .pointer("/version_boundaries/record_version_boundary")
        .filter(|value| value.is_object())
        .ok_or_else(|| {
            LookupError::unsupported("verified lookup requires a projected record boundary")
        })?;
    let inventory = if path_class == topology::ExecutionPathClass::EnsWildcard {
        None
    } else {
        let boundary_resource_id = boundary
            .get("resource_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                LookupError::unsupported(
                    "verified lookup record boundary has no comparison resource",
                )
            })?;
        Some(load_inventory(&mut transaction, boundary_resource_id, boundary).await?)
    };
    let resolver_head = load_head(&mut transaction, &resolver_chain_id).await?;
    let resolver_position =
        positions::position_for_chain(&name.chain_positions, &resolver_chain_id)?;
    positions::ensure_canonical(&mut transaction, &resolver_position).await?;
    positions::ensure_at_head("name_current", &resolver_position, &resolver_head)?;
    if let Some(inventory) = &inventory {
        positions::ensure_inventory_at_head(
            "record_inventory_current",
            &inventory.chain_positions,
            &resolver_head,
        )?;
    }

    let entrypoint = entrypoint_authority(&name.namespace, topology, &resolver_chain_id)?;
    let execution_position = if entrypoint.chain_id == resolver_position.chain_id {
        resolver_position.clone()
    } else {
        let position = positions::position_for_chain(&name.chain_positions, entrypoint.chain_id)?;
        positions::ensure_canonical(&mut transaction, &position).await?;
        position
    };
    let entrypoint_address = manifests::load_entrypoint(
        &mut transaction,
        manifests::EntrypointQuery {
            namespace: &name.namespace,
            source_family: entrypoint.source_family,
            chain_id: entrypoint.chain_id,
            role: entrypoint.role,
            allow_shadow: entrypoint.allow_shadow,
            execution_block_number: execution_position.block_number,
            required_manifest_version: entrypoint.required_manifest_version,
            require_resolution_capability: true,
        },
    )
    .await?;
    if let Some(expected) = entrypoint.transport_address
        && !entrypoint_address.eq_ignore_ascii_case(expected)
    {
        return Err(LookupError::unsupported(
            "projected transport address does not match the execution manifest",
        ));
    }
    transaction
        .commit()
        .await
        .map_err(database("commit lookup read"))?;

    let observed_positions =
        positions::observed_positions(&resolver_position, &execution_position)?;
    Ok(LookupSnapshot {
        logical_name_id: name.logical_name_id,
        name: name.raw_name,
        dns_name: name.dns_encoded_name,
        node: crate::abi::parse_node(&name.namehash).map_err(|error| {
            LookupError::unsupported(format!("indexed namehash is malformed: {error:#}"))
        })?,
        resolver_chain_id,
        resolver_address: resolver_address.to_ascii_lowercase(),
        entrypoint_chain_id: entrypoint.chain_id.to_owned(),
        entrypoint_address: entrypoint_address.to_ascii_lowercase(),
        execution_block: ExecutionBlock {
            chain_id: execution_position.chain_id,
            block_number: execution_position.block_number,
            block_hash: execution_position.block_hash,
        },
        follow_ccip: entrypoint.follow_ccip,
        result_abi: entrypoint.result_abi,
        observed_positions,
        comparison: inventory.map(|inventory| IndexedComparison {
            resource_id: inventory.resource_id,
            boundary_key: inventory.record_version_boundary_key,
            row_xmin: inventory.row_xmin,
            entries: inventory.entries,
        }),
    })
}

pub(crate) async fn load_ens_primary_name_authority(
    pool: &PgPool,
) -> Result<EnsPrimaryNameAuthority> {
    let mut transaction = pool
        .begin()
        .await
        .map_err(database("start primary-name authority read"))?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *transaction)
        .await
        .map_err(database("set primary-name authority read isolation"))?;
    let head = load_head(&mut transaction, ETHEREUM_MAINNET_CHAIN_ID).await?;
    let registry_address = manifests::load_entrypoint(
        &mut transaction,
        manifests::EntrypointQuery {
            namespace: ENS_NAMESPACE,
            source_family: ENS_V1_REGISTRY_SOURCE_FAMILY,
            chain_id: ETHEREUM_MAINNET_CHAIN_ID,
            role: crate::ENS_REGISTRY_ROLE,
            allow_shadow: false,
            execution_block_number: head.block_number,
            required_manifest_version: None,
            require_resolution_capability: false,
        },
    )
    .await?;
    let universal_resolver_address = manifests::load_entrypoint(
        &mut transaction,
        manifests::EntrypointQuery {
            namespace: ENS_NAMESPACE,
            source_family: ENS_EXECUTION_SOURCE_FAMILY,
            chain_id: ETHEREUM_MAINNET_CHAIN_ID,
            role: ENS_UNIVERSAL_RESOLVER_ROLE,
            allow_shadow: true,
            execution_block_number: head.block_number,
            required_manifest_version: None,
            require_resolution_capability: true,
        },
    )
    .await?;
    transaction
        .commit()
        .await
        .map_err(database("commit primary-name authority read"))?;
    Ok(EnsPrimaryNameAuthority {
        registry_address: registry_address.to_ascii_lowercase(),
        universal_resolver_address: universal_resolver_address.to_ascii_lowercase(),
        block_number: head.block_number,
        block_hash: head.block_hash,
    })
}

async fn load_name(
    transaction: &mut Transaction<'_, Postgres>,
    logical_name_id: &str,
) -> Result<NameRow> {
    sqlx::query_as::<_, NameRow>(
        r#"
        SELECT name.logical_name_id, name.namespace, name.raw_name, name.namehash,
               surface.dns_encoded_name, resource.chain_id AS resource_chain_id,
               name.declared_summary, name.chain_positions
        FROM name_current name
        JOIN name_surfaces surface
          ON surface.logical_name_id = name.logical_name_id
        JOIN resources resource ON resource.resource_id = name.resource_id
        JOIN surface_bindings binding
          ON binding.surface_binding_id = name.surface_binding_id
         AND binding.logical_name_id = name.logical_name_id
         AND binding.resource_id = name.resource_id
         AND binding.binding_kind = name.binding_kind
        LEFT JOIN token_lineages token_lineage
          ON token_lineage.token_lineage_id = name.token_lineage_id
        JOIN chain_lineage surface_lineage
          ON surface_lineage.chain_id = surface.chain_id
         AND surface_lineage.block_hash = surface.block_hash
         AND surface_lineage.block_number = surface.block_number
        JOIN chain_lineage resource_lineage
          ON resource_lineage.chain_id = resource.chain_id
         AND resource_lineage.block_hash = resource.block_hash
         AND resource_lineage.block_number = resource.block_number
        JOIN chain_lineage binding_lineage
          ON binding_lineage.chain_id = binding.chain_id
         AND binding_lineage.block_hash = binding.block_hash
         AND binding_lineage.block_number = binding.block_number
        WHERE name.logical_name_id = $1
          AND name.support_status = 'supported'
          AND surface.visibility_state = 'active'
          AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND resource.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND binding.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND binding.active_to IS NULL
          AND (
              name.token_lineage_id IS NULL
              OR token_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
          )
          AND surface_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND resource_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND binding_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        "#,
    )
    .bind(logical_name_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database("load readable name projection"))?
    .ok_or_else(|| LookupError::unsupported("verified lookup name is not readable or supported"))
}

async fn load_inventory(
    transaction: &mut Transaction<'_, Postgres>,
    resource_id: &str,
    boundary: &Value,
) -> Result<InventoryRow> {
    sqlx::query_as::<_, InventoryRow>(
        r#"
        SELECT inventory.resource_id::text AS resource_id,
               inventory.record_version_boundary_key, inventory.entries,
               inventory.chain_positions, inventory.xmin::text AS row_xmin
        FROM record_inventory_current inventory
        JOIN resources resource
          ON resource.resource_id = inventory.resource_id
        WHERE inventory.resource_id = $1::uuid
          AND inventory.record_version_boundary = $2
          AND resource.canonicality_state IN ('canonical', 'safe', 'finalized')
        "#,
    )
    .bind(resource_id)
    .bind(boundary)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database("load indexed record answer"))?
    .ok_or_else(|| LookupError::unsupported("verified lookup requires an indexed record boundary"))
}

async fn load_head(transaction: &mut Transaction<'_, Postgres>, chain_id: &str) -> Result<HeadRow> {
    sqlx::query_as::<_, HeadRow>(
        r#"
        SELECT head.chain_id, head.latest_block_hash AS block_hash,
               head.latest_block_number AS block_number
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

struct EntrypointAuthority<'a> {
    source_family: &'a str,
    chain_id: &'a str,
    role: &'a str,
    transport_address: Option<&'a str>,
    follow_ccip: bool,
    result_abi: ResolutionResultAbi,
    allow_shadow: bool,
    required_manifest_version: Option<i64>,
}

fn entrypoint_authority<'a>(
    namespace: &str,
    topology: &'a Value,
    resolver_chain_id: &str,
) -> Result<EntrypointAuthority<'a>> {
    let transport = topology.get("transport").and_then(Value::as_object);
    let transport_source = transport.and_then(|value| string(value, "source_chain_id"));
    let transport_target = transport.and_then(|value| string(value, "target_chain_id"));
    let transport_address = transport.and_then(|value| string(value, "contract_address"));
    match namespace {
        ENS_NAMESPACE
            if resolver_chain_id == ETHEREUM_MAINNET_CHAIN_ID
                && transport_source.is_none()
                && transport_target.is_none()
                && transport_address.is_none() =>
        {
            Ok(EntrypointAuthority {
                source_family: ENS_EXECUTION_SOURCE_FAMILY,
                chain_id: ETHEREUM_MAINNET_CHAIN_ID,
                role: ENS_UNIVERSAL_RESOLVER_ROLE,
                transport_address: None,
                follow_ccip: false,
                result_abi: ResolutionResultAbi::EnsUniversalResolver,
                allow_shadow: true,
                required_manifest_version: None,
            })
        }
        BASENAMES_NAMESPACE
            if resolver_chain_id == BASE_MAINNET_CHAIN_ID
                && transport_source == Some(BASE_MAINNET_CHAIN_ID)
                && transport_target == Some(ETHEREUM_MAINNET_CHAIN_ID)
                && transport_address.is_some() =>
        {
            Ok(EntrypointAuthority {
                source_family: BASENAMES_EXECUTION_SOURCE_FAMILY,
                chain_id: ETHEREUM_MAINNET_CHAIN_ID,
                role: BASENAMES_L1_RESOLVER_ROLE,
                transport_address,
                follow_ccip: true,
                result_abi: ResolutionResultAbi::BasenamesL1Resolver,
                allow_shadow: false,
                required_manifest_version: Some(2),
            })
        }
        _ => Err(LookupError::unsupported(
            "projected resolution topology is outside the supported lookup paths",
        )),
    }
}

fn selected_resolver(topology: &Value) -> Result<(String, String)> {
    let hop = topology
        .get("resolver_path")
        .and_then(Value::as_array)
        .and_then(|path| path.last())
        .ok_or_else(|| LookupError::unsupported("projected topology has no selected resolver"))?;
    let chain_id = hop.get("chain_id").and_then(Value::as_str);
    let address = hop.get("address").and_then(Value::as_str);
    match (chain_id, address) {
        (Some(chain_id), Some(address)) if !chain_id.is_empty() && !address.is_empty() => {
            Ok((chain_id.to_owned(), address.to_owned()))
        }
        _ => Err(LookupError::unsupported(
            "projected topology has no concrete resolver",
        )),
    }
}

fn string<'a>(value: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
}
